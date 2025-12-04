import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';
import { PrismaService } from '../prisma/prisma.service';
import { WalletDetectionService } from './wallet-detection.service';
import { ArbiscanApiResponse } from '../common/interfaces';
import { ConsoleLogger } from '../common/utils/console-logger';

interface SyncGap {
  start: number;
  end: number;
}

@Injectable()
export class ArbitrumSyncService {
  private readonly console = new ConsoleLogger('Arbitrum');
  private readonly baseUrl: string;
  private readonly apiKey: string;
  private readonly chainId: string;
  private readonly usdcContract: string;
  private readonly bridgeAddress: string;
  private readonly offsetSize: number;

  constructor(
    private readonly prisma: PrismaService,
    private readonly walletDetectionService: WalletDetectionService,
    private readonly configService: ConfigService,
  ) {
    this.baseUrl = this.configService.get<string>('ARBISCAN_BASE_URL', 'https://api.etherscan.io/v2/api');
    this.apiKey = this.configService.get<string>('ARBISCAN_API_KEY', '');
    this.chainId = this.configService.get<string>('ARBISCAN_CHAIN_ID', '42161');
    this.usdcContract = this.configService.get<string>('ARBISCAN_USDC_CONTRACT', '0xaf88d065e77c8cC2239327C5EDb3A432268e5831');
    this.bridgeAddress = this.configService.get<string>('ARBISCAN_BRIDGE_ADDRESS', '0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7');
    this.offsetSize = this.configService.get<number>('ARBISCAN_OFFSET_SIZE', 10000);
  }

  async sync(): Promise<{ totalTransactions: number; totalWhales: number; duration: number }> {
    const startTime = Date.now();
    let grandTotalTransactions = 0;
    let grandTotalWhales = 0;

    this.console.header('Arbitrum Bridge Sync');

    const currentChainBlock = await this.getCurrentBlockNumber();

    if (!currentChainBlock) {
      this.console.error('Failed to get current block number from Arbitrum chain');
      return { totalTransactions: 0, totalWhales: 0, duration: Date.now() - startTime };
    }

    this.console.info(`Current Arbitrum block: ${currentChainBlock.toLocaleString()}`);

    const gaps = await this.findGaps(currentChainBlock);

    if (gaps.length === 0) {
      this.console.success('All blocks are synced!');
      return { totalTransactions: 0, totalWhales: 0, duration: Date.now() - startTime };
    }

    this.console.info(`Found ${gaps.length} gap(s) to sync`);

    for (let index = 0; index < gaps.length; index++) {
      const gap = gaps[index];
      this.console.gap(index + 1, gaps.length, gap.start, gap.end);

      const result = await this.syncGap(gap.start, gap.end, currentChainBlock);
      grandTotalTransactions += result.transactions;
      grandTotalWhales += result.whales;
    }

    const duration = Date.now() - startTime;

    this.console.syncSummary({
      totalIterations: gaps.length,
      totalTransactions: grandTotalTransactions,
      totalWhales: grandTotalWhales,
      duration,
    });

    return { totalTransactions: grandTotalTransactions, totalWhales: grandTotalWhales, duration };
  }

  private async findGaps(currentChainBlock: number): Promise<SyncGap[]> {
    const completedRanges = await this.prisma.transactionSyncStatus.findMany({
      where: { status: 'completed' },
      orderBy: { start_block: 'asc' },
      select: { start_block: true, end_block: true },
    });

    const gaps: SyncGap[] = [];

    if (completedRanges.length === 0) {
      this.console.warn('No synced ranges found - entire chain needs sync');
      return [{ start: 0, end: currentChainBlock }];
    }

    // Check gap from 0 to first synced range
    const firstRange = completedRanges[0];
    const firstStart = Number(firstRange.start_block);
    if (firstStart > 0) {
      gaps.push({ start: 0, end: firstStart - 1 });
    }

    // Check gaps between consecutive ranges
    for (let i = 0; i < completedRanges.length - 1; i++) {
      const currentRange = completedRanges[i];
      const nextRange = completedRanges[i + 1];
      const currentEnd = Number(currentRange.end_block);
      const nextStart = Number(nextRange.start_block);

      if (currentEnd + 1 < nextStart) {
        gaps.push({ start: currentEnd + 1, end: nextStart - 1 });
      }
    }

    // Check gap from last synced range to current block
    const lastRange = completedRanges[completedRanges.length - 1];
    const lastEnd = Number(lastRange.end_block);
    if (lastEnd < currentChainBlock) {
      gaps.push({ start: lastEnd + 1, end: currentChainBlock });
    }

    return gaps;
  }

  private async syncGap(startBlock: number, endBlock: number, currentChainBlock: number): Promise<{ transactions: number; whales: number }> {
    let totalTransactionsProcessed = 0;
    let totalWhaleTransactions = 0;

    endBlock = Math.min(endBlock, currentChainBlock);

    this.console.subHeader(`Syncing Block ${startBlock.toLocaleString()} → ${endBlock.toLocaleString()}`);

    let currentEndBlock = endBlock;
    let iteration = 1;

    while (currentEndBlock >= startBlock) {
      const data = await this.fetchTransactions(startBlock, currentEndBlock, 1);

      if (!data || data.status !== '1') {
        if (data?.message === 'No transactions found') {
          this.console.info('No more transactions found in this gap');
          if (currentEndBlock >= startBlock) {
            await this.saveCompletedRange(startBlock, currentEndBlock);
          }
          break;
        }

        this.console.error(`Failed to fetch transactions: ${data?.message ?? 'Unknown error'}`);
        break;
      }

      const transactions = Array.isArray(data.result) ? data.result : [];
      const transactionsCount = transactions.length;

      if (transactionsCount === 0) {
        this.console.info('No transactions in this range');
        await this.saveCompletedRange(startBlock, currentEndBlock);
        break;
      }

      totalTransactionsProcessed += transactionsCount;
      let iterationWhaleCount = 0;
      let minBlock = Number.MAX_SAFE_INTEGER;
      let maxBlock = 0;

      for (const tx of transactions) {
        const currentBlock = parseInt(tx.blockNumber, 10);
        minBlock = Math.min(minBlock, currentBlock);
        maxBlock = Math.max(maxBlock, currentBlock);

        const isWhale = await this.walletDetectionService.processTransaction(tx);
        if (isWhale) {
          iterationWhaleCount++;
          totalWhaleTransactions++;
        }
      }

      // Display colored block sync info
      this.console.blockSync({
        fromBlock: minBlock,
        toBlock: maxBlock,
        totalTransactions: transactionsCount,
        whaleCount: iterationWhaleCount,
        iteration,
      });

      await this.saveCompletedRange(minBlock, currentEndBlock);

      currentEndBlock = minBlock - 1;

      if (currentEndBlock < startBlock) {
        break;
      }

      if (transactionsCount < this.offsetSize) {
        if (currentEndBlock >= startBlock) {
          await this.saveCompletedRange(startBlock, currentEndBlock);
        }
        break;
      }

      iteration++;

      // Rate limiting
      await this.sleep(200);
    }

    this.console.success(`Gap completed: ${totalTransactionsProcessed.toLocaleString()} tx, ${totalWhaleTransactions} whales`);

    return { transactions: totalTransactionsProcessed, whales: totalWhaleTransactions };
  }

  private async saveCompletedRange(startBlock: number, endBlock: number): Promise<void> {
    try {
      const startBlockInt = startBlock;
      const endBlockInt = endBlock;

      // Check if this exact range already exists
      const exists = await this.prisma.transactionSyncStatus.findFirst({
        where: {
          start_block: startBlockInt,
          end_block: endBlockInt,
        },
      });

      if (exists) {
        return;
      }

      // Check for overlapping ranges
      const overlapping = await this.prisma.transactionSyncStatus.findFirst({
        where: {
          OR: [
            {
              AND: [{ start_block: { lte: startBlockInt } }, { end_block: { gte: startBlockInt } }],
            },
            {
              AND: [{ start_block: { lte: endBlockInt } }, { end_block: { gte: endBlockInt } }],
            },
            {
              AND: [{ start_block: { gte: startBlockInt } }, { end_block: { lte: endBlockInt } }],
            },
          ],
        },
      });

      if (overlapping) {
        this.console.warn(`Overlapping range detected, skipping: ${startBlock}-${endBlock}`);
        return;
      }

      await this.prisma.transactionSyncStatus.create({
        data: {
          start_block: startBlockInt,
          end_block: endBlockInt,
          last_synced_block: startBlockInt,
          status: 'completed',
          completed_at: new Date(),
        },
      });
    } catch (error) {
      this.console.error(`Failed to save range ${startBlock}-${endBlock}: ${error.message}`);
    }
  }

  private async fetchTransactions(startBlock: number, endBlock: number, page: number): Promise<ArbiscanApiResponse | null> {
    try {
      const response = await axios.get(this.baseUrl, {
        params: {
          chainid: this.chainId,
          module: 'account',
          action: 'tokentx',
          contractaddress: this.usdcContract,
          address: this.bridgeAddress,
          startblock: startBlock,
          endblock: endBlock,
          page,
          offset: this.offsetSize,
          sort: 'desc',
          apikey: this.apiKey,
        },
        timeout: 30000,
      });

      return response.data;
    } catch (error) {
      this.console.error(`Exception fetching transactions: ${error.message}`);
      return null;
    }
  }

  private async getCurrentBlockNumber(): Promise<number | null> {
    try {
      const response = await axios.get(this.baseUrl, {
        params: {
          chainid: this.chainId,
          module: 'proxy',
          action: 'eth_blockNumber',
          apikey: this.apiKey,
        },
        timeout: 10000,
      });

      if (response.data?.result) {
        const blockNumber = parseInt(response.data.result, 16);
        return blockNumber;
      }

      this.console.error(`Failed to get current block number: ${JSON.stringify(response.data)}`);
      return null;
    } catch (error) {
      this.console.error(`Exception fetching current block number: ${error.message}`);
      return null;
    }
  }

  async getSyncStatus() {
    const [totalSynced, currentSync, lastCompleted, highestBlock] = await Promise.all([
      this.prisma.transactionSyncStatus.count({
        where: { status: 'completed' },
      }),
      this.prisma.transactionSyncStatus.findFirst({
        where: { status: 'syncing' },
      }),
      this.prisma.transactionSyncStatus.findFirst({
        where: { status: 'completed' },
        orderBy: { completed_at: 'desc' },
      }),
      this.prisma.transactionSyncStatus.aggregate({
        where: { status: 'completed' },
        _max: { end_block: true },
      }),
    ]);

    const currentBlock = await this.getCurrentBlockNumber();
    const highestSyncedBlock = highestBlock._max.end_block ? Number(highestBlock._max.end_block) : 0;

    return {
      currentArbitrumBlock: currentBlock,
      highestSyncedBlock,
      totalCompletedRanges: totalSynced,
      currentSyncingRange: currentSync
        ? {
            startBlock: Number(currentSync.start_block),
            endBlock: Number(currentSync.end_block),
            lastSyncedBlock: currentSync.last_synced_block ? Number(currentSync.last_synced_block) : null,
            createdAt: currentSync.created_at?.toISOString(),
          }
        : null,
      lastCompletedSync: lastCompleted
        ? {
            startBlock: Number(lastCompleted.start_block),
            endBlock: Number(lastCompleted.end_block),
            completedAt: lastCompleted.completed_at?.toISOString(),
          }
        : null,
      syncPercentage: currentBlock ? Math.round((highestSyncedBlock / currentBlock) * 10000) / 100 : 0,
    };
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Merge adjacent block ranges to reduce table size
   * Example: [1-100], [101-200], [201-300] -> [1-300]
   */
  async mergeAdjacentRanges(): Promise<{ mergedCount: number; deletedCount: number }> {
    this.console.info('Starting block range merge...');

    const completedRanges = await this.prisma.transactionSyncStatus.findMany({
      where: { status: 'completed' },
      orderBy: { start_block: 'asc' },
    });

    if (completedRanges.length <= 1) {
      this.console.info('No ranges to merge');
      return { mergedCount: 0, deletedCount: 0 };
    }

    const rangesToDelete: number[] = [];
    const rangesToCreate: { start: number; end: number }[] = [];

    let currentMergeStart = completedRanges[0].start_block;
    let currentMergeEnd = completedRanges[0].end_block;
    let mergeStartId = completedRanges[0].id;

    for (let i = 1; i < completedRanges.length; i++) {
      const range = completedRanges[i];

      // Check if ranges are adjacent (end_block + 1 = next start_block)
      if (currentMergeEnd + 1 >= range.start_block) {
        // Ranges are adjacent or overlapping, extend the merge
        currentMergeEnd = Math.max(currentMergeEnd, range.end_block);
        rangesToDelete.push(range.id);
      } else {
        // Gap found, save current merge if it spans multiple ranges
        if (rangesToDelete.length > 0 || currentMergeEnd !== completedRanges[i - 1].end_block) {
          rangesToCreate.push({ start: currentMergeStart, end: currentMergeEnd });
          rangesToDelete.push(mergeStartId);
        }

        // Start new merge
        currentMergeStart = range.start_block;
        currentMergeEnd = range.end_block;
        mergeStartId = range.id;
      }
    }

    // Handle the last merge group
    const lastRange = completedRanges[completedRanges.length - 1];
    if (rangesToDelete.includes(lastRange.id) || currentMergeStart !== lastRange.start_block) {
      rangesToCreate.push({ start: currentMergeStart, end: currentMergeEnd });
      if (!rangesToDelete.includes(mergeStartId)) {
        rangesToDelete.push(mergeStartId);
      }
    }

    if (rangesToDelete.length === 0) {
      this.console.info('No adjacent ranges found to merge');
      return { mergedCount: 0, deletedCount: 0 };
    }

    // Perform the merge in a transaction
    await this.prisma.$transaction(async (tx) => {
      // Delete old ranges
      await tx.transactionSyncStatus.deleteMany({
        where: { id: { in: rangesToDelete } },
      });

      // Create new merged ranges
      for (const range of rangesToCreate) {
        await tx.transactionSyncStatus.create({
          data: {
            start_block: range.start,
            end_block: range.end,
            last_synced_block: range.end,
            status: 'completed',
            completed_at: new Date(),
          },
        });
      }
    });

    this.console.success(`Merged ${rangesToCreate.length} range(s), deleted ${rangesToDelete.length} old record(s)`);

    return { mergedCount: rangesToCreate.length, deletedCount: rangesToDelete.length };
  }
}
