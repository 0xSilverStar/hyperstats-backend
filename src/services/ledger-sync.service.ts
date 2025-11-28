import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { HyperLiquidInfoService } from './hyperliquid-info.service';
import { Prisma } from '@prisma/client';

// HyperLiquid API returns max 500 elements per request
const MAX_RESULTS_PER_REQUEST = 500;
// Cache TTL in minutes (30 minutes)
const LEDGER_CACHE_TTL_MINUTES = 30;
// Delay between API calls to avoid rate limiting
const API_DELAY_MS = 100;
// Sync timeout in minutes (15 minutes) - force restart if stuck
const SYNC_TIMEOUT_MINUTES = 15;

interface LedgerDelta {
  type: string;
  usdc?: string;
  user?: string;
  destination?: string;
  sourceDex?: string;
  destinationDex?: string;
  token?: string;
  amount?: string;
  usdcValue?: string;
  fee?: string;
  nativeTokenFee?: string;
  nonce?: number | null;
  feeToken?: string;
}

interface LedgerUpdateFromApi {
  time: number;
  hash: string;
  delta: LedgerDelta;
}

@Injectable()
export class LedgerSyncService {
  private readonly logger = new Logger(LedgerSyncService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly hlService: HyperLiquidInfoService,
  ) {}

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Get the last synced time for a wallet's ledger
   */
  async getLastSyncedTime(address: string): Promise<bigint> {
    const normalizedAddress = address.toLowerCase();
    const syncStatus = await this.prisma.ledgerSyncStatus.findUnique({
      where: { wallet_address: normalizedAddress },
    });
    return syncStatus?.last_synced_time ?? BigInt(0);
  }

  /**
   * Check if ledger cache is still valid (within 30 minutes)
   */
  async isCacheValid(address: string): Promise<boolean> {
    const normalizedAddress = address.toLowerCase();
    const syncStatus = await this.prisma.ledgerSyncStatus.findUnique({
      where: { wallet_address: normalizedAddress },
    });

    if (!syncStatus || !syncStatus.last_synced_at) {
      return false;
    }

    const cacheExpiry = new Date(syncStatus.last_synced_at.getTime() + LEDGER_CACHE_TTL_MINUTES * 60 * 1000);
    return cacheExpiry > new Date();
  }

  /**
   * Check if sync is currently in progress and not timed out
   */
  async isSyncInProgress(address: string): Promise<boolean> {
    const normalizedAddress = address.toLowerCase();
    const syncStatus = await this.prisma.ledgerSyncStatus.findUnique({
      where: { wallet_address: normalizedAddress },
    });

    if (!syncStatus || !syncStatus.is_syncing) {
      return false;
    }

    // Check if sync has timed out (15 minutes)
    if (syncStatus.sync_started_at) {
      const timeoutAt = new Date(syncStatus.sync_started_at.getTime() + SYNC_TIMEOUT_MINUTES * 60 * 1000);
      if (timeoutAt < new Date()) {
        this.logger.warn(`Sync for ${normalizedAddress} timed out after ${SYNC_TIMEOUT_MINUTES} minutes, will force restart`);
        return false;
      }
    }

    return true;
  }

  /**
   * Acquire sync lock for a wallet
   * Returns true if lock acquired, false if another sync is in progress
   */
  private async acquireSyncLock(address: string): Promise<boolean> {
    const normalizedAddress = address.toLowerCase();

    // Check if sync is already in progress
    if (await this.isSyncInProgress(normalizedAddress)) {
      this.logger.log(`Sync already in progress for ${normalizedAddress}, skipping`);
      return false;
    }

    // Set sync lock
    await this.prisma.ledgerSyncStatus.upsert({
      where: { wallet_address: normalizedAddress },
      update: {
        is_syncing: true,
        sync_started_at: new Date(),
      },
      create: {
        wallet_address: normalizedAddress,
        is_syncing: true,
        sync_started_at: new Date(),
      },
    });

    return true;
  }

  /**
   * Release sync lock for a wallet
   */
  private async releaseSyncLock(address: string, success: boolean, lastTime?: bigint, totalCount?: number): Promise<void> {
    const normalizedAddress = address.toLowerCase();

    const updateData: Record<string, unknown> = {
      is_syncing: false,
      sync_started_at: null,
    };

    if (success && lastTime !== undefined && totalCount !== undefined) {
      updateData.last_synced_time = lastTime;
      updateData.total_count = totalCount;
      updateData.last_synced_at = new Date();
    }

    await this.prisma.ledgerSyncStatus.update({
      where: { wallet_address: normalizedAddress },
      data: updateData,
    });
  }

  /**
   * Sync ledger updates for a wallet with pagination
   * Uses startTime to fetch incrementally
   * Includes sync locking to prevent concurrent syncs
   */
  async syncLedgerUpdates(address: string, forceFullSync = false): Promise<{ synced: number; total: number }> {
    const normalizedAddress = address.toLowerCase();
    this.logger.log(`Starting ledger sync for ${normalizedAddress}, forceFullSync: ${forceFullSync}`);

    // Check if cache is still valid (unless forcing full sync)
    if (!forceFullSync && (await this.isCacheValid(normalizedAddress))) {
      const syncStatus = await this.prisma.ledgerSyncStatus.findUnique({
        where: { wallet_address: normalizedAddress },
      });
      this.logger.log(`Ledger cache still valid for ${normalizedAddress}, skipping sync`);
      return { synced: 0, total: syncStatus?.total_count ?? 0 };
    }

    // Try to acquire sync lock
    const lockAcquired = await this.acquireSyncLock(normalizedAddress);
    if (!lockAcquired) {
      // Another sync is in progress, return current count
      const syncStatus = await this.prisma.ledgerSyncStatus.findUnique({
        where: { wallet_address: normalizedAddress },
      });
      return { synced: 0, total: syncStatus?.total_count ?? 0 };
    }

    // Get the last synced time (for incremental sync)
    let startTime = forceFullSync ? BigInt(0) : await this.getLastSyncedTime(normalizedAddress);
    let totalSynced = 0;
    let hasMore = true;
    let lastTime = startTime;

    try {
      while (hasMore) {
        // Fetch ledger updates from API
        const updates: LedgerUpdateFromApi[] = await this.hlService.getUserLedgerUpdates(normalizedAddress, Number(startTime));

        if (!updates || updates.length === 0) {
          hasMore = false;
          break;
        }

        this.logger.debug(`Fetched ${updates.length} ledger updates for ${normalizedAddress} starting from ${startTime}`);

        // Save updates to database
        await this.saveLedgerUpdates(normalizedAddress, updates);
        totalSynced += updates.length;

        // Find the last timestamp for pagination
        const maxTime = Math.max(...updates.map((u) => u.time));
        lastTime = BigInt(maxTime);

        // If we got less than max results, we've reached the end
        if (updates.length < MAX_RESULTS_PER_REQUEST) {
          hasMore = false;
        } else {
          // Use last timestamp + 1 as next startTime to avoid duplicates
          startTime = BigInt(maxTime + 1);
          // Delay to avoid rate limiting
          await this.sleep(API_DELAY_MS);
        }
      }

      // Get total count and release lock with success
      const totalCount = await this.prisma.ledgerUpdate.count({
        where: { wallet_address: normalizedAddress },
      });

      await this.releaseSyncLock(normalizedAddress, true, lastTime, totalCount);

      this.logger.log(`Ledger sync completed for ${normalizedAddress}: synced ${totalSynced}, total ${totalCount}`);
      return { synced: totalSynced, total: totalCount };
    } catch (error) {
      // Release lock on error
      await this.releaseSyncLock(normalizedAddress, false);
      this.logger.error(`Error syncing ledger for ${normalizedAddress}`, {
        error: error.message,
        startTime: startTime.toString(),
      });
      throw error;
    }
  }

  /**
   * Save ledger updates to database
   */
  private async saveLedgerUpdates(address: string, updates: LedgerUpdateFromApi[]): Promise<void> {
    const normalizedAddress = address.toLowerCase();

    for (const update of updates) {
      try {
        await this.prisma.ledgerUpdate.upsert({
          where: {
            wallet_address_tx_hash_time: {
              wallet_address: normalizedAddress,
              tx_hash: update.hash,
              time: BigInt(update.time),
            },
          },
          update: {
            type: update.delta.type,
            usdc: update.delta.usdc ? new Prisma.Decimal(update.delta.usdc) : null,
            usdc_value: update.delta.usdcValue ? new Prisma.Decimal(update.delta.usdcValue) : null,
            token: update.delta.token ?? null,
            amount: update.delta.amount ? new Prisma.Decimal(update.delta.amount) : null,
            fee: update.delta.fee ? new Prisma.Decimal(update.delta.fee) : null,
            user: update.delta.user ?? null,
            destination: update.delta.destination ?? null,
            source_dex: update.delta.sourceDex ?? null,
            destination_dex: update.delta.destinationDex ?? null,
            nonce: update.delta.nonce != null ? BigInt(update.delta.nonce) : null,
            fee_token: update.delta.feeToken ?? null,
          },
          create: {
            wallet_address: normalizedAddress,
            tx_hash: update.hash,
            time: BigInt(update.time),
            type: update.delta.type,
            usdc: update.delta.usdc ? new Prisma.Decimal(update.delta.usdc) : null,
            usdc_value: update.delta.usdcValue ? new Prisma.Decimal(update.delta.usdcValue) : null,
            token: update.delta.token ?? null,
            amount: update.delta.amount ? new Prisma.Decimal(update.delta.amount) : null,
            fee: update.delta.fee ? new Prisma.Decimal(update.delta.fee) : null,
            user: update.delta.user ?? null,
            destination: update.delta.destination ?? null,
            source_dex: update.delta.sourceDex ?? null,
            destination_dex: update.delta.destinationDex ?? null,
            nonce: update.delta.nonce != null ? BigInt(update.delta.nonce) : null,
            fee_token: update.delta.feeToken ?? null,
          },
        });
      } catch (error) {
        // Log but don't fail on individual record errors
        this.logger.warn(`Failed to save ledger update for ${normalizedAddress}`, {
          hash: update.hash,
          time: update.time,
          error: error.message,
        });
      }
    }
  }

  /**
   * Get ledger updates from database with pagination
   */
  async getLedgerUpdates(address: string, startTime = 0, limit = 100, offset = 0) {
    const normalizedAddress = address.toLowerCase();

    // First, try to sync if needed
    await this.syncLedgerUpdates(normalizedAddress);

    // Fetch from database
    const [updates, total] = await Promise.all([
      this.prisma.ledgerUpdate.findMany({
        where: {
          wallet_address: normalizedAddress,
          time: { gte: BigInt(startTime) },
        },
        orderBy: { time: 'desc' },
        take: limit,
        skip: offset,
      }),
      this.prisma.ledgerUpdate.count({
        where: {
          wallet_address: normalizedAddress,
          time: { gte: BigInt(startTime) },
        },
      }),
    ]);

    // Transform to API format
    const ledgerUpdates = updates.map((u) => ({
      time: Number(u.time),
      hash: u.tx_hash,
      delta: {
        type: u.type,
        usdc: u.usdc?.toString(),
        usdcValue: u.usdc_value?.toString(),
        token: u.token,
        amount: u.amount?.toString(),
        fee: u.fee?.toString(),
        user: u.user,
        destination: u.destination,
        sourceDex: u.source_dex,
        destinationDex: u.destination_dex,
        nonce: u.nonce != null ? Number(u.nonce) : null,
        feeToken: u.fee_token,
      },
    }));

    return {
      address: normalizedAddress,
      ledger_updates: ledgerUpdates,
      total_count: total,
      pagination: {
        total,
        limit,
        offset,
        has_more: offset + limit < total,
      },
    };
  }

  /**
   * Get ledger statistics for a wallet (aggregated data)
   */
  async getLedgerStatistics(address: string) {
    const normalizedAddress = address.toLowerCase();

    // First, ensure data is synced
    await this.syncLedgerUpdates(normalizedAddress);

    // Get aggregated statistics
    const [totalCount, depositStats, withdrawStats, depositCount, withdrawCount, firstEntry, lastEntry] = await Promise.all([
      this.prisma.ledgerUpdate.count({
        where: { wallet_address: normalizedAddress },
      }),
      this.prisma.ledgerUpdate.aggregate({
        where: { wallet_address: normalizedAddress, type: 'deposit' },
        _sum: { usdc: true },
        _max: { usdc: true },
      }),
      this.prisma.ledgerUpdate.aggregate({
        where: { wallet_address: normalizedAddress, type: 'withdraw' },
        _sum: { usdc: true },
        _max: { usdc: true },
      }),
      this.prisma.ledgerUpdate.count({
        where: { wallet_address: normalizedAddress, type: 'deposit' },
      }),
      this.prisma.ledgerUpdate.count({
        where: { wallet_address: normalizedAddress, type: 'withdraw' },
      }),
      this.prisma.ledgerUpdate.findFirst({
        where: { wallet_address: normalizedAddress },
        orderBy: { time: 'asc' },
        select: { time: true },
      }),
      this.prisma.ledgerUpdate.findFirst({
        where: { wallet_address: normalizedAddress },
        orderBy: { time: 'desc' },
        select: { time: true },
      }),
    ]);

    const totalDeposit = depositStats._sum.usdc?.toNumber() ?? 0;
    const totalWithdraw = withdrawStats._sum.usdc?.toNumber() ?? 0;
    const largestDeposit = depositStats._max.usdc?.toNumber() ?? 0;
    const largestWithdraw = withdrawStats._max.usdc?.toNumber() ?? 0;

    const firstSeen = firstEntry ? Number(firstEntry.time) : null;
    const lastActivity = lastEntry ? Number(lastEntry.time) : null;

    const netBalance = totalDeposit - totalWithdraw;
    const transactionCount = depositCount + withdrawCount;
    const avgTransaction = transactionCount > 0 ? (totalDeposit + totalWithdraw) / transactionCount : 0;

    // Calculate activity days
    let activityDays = 0;
    if (firstSeen && lastActivity) {
      activityDays = Math.floor((lastActivity - firstSeen) / (1000 * 60 * 60 * 24));
    }

    return {
      address: normalizedAddress,
      total_count: totalCount,
      deposit_count: depositCount,
      withdraw_count: withdrawCount,
      transaction_count: transactionCount,
      total_deposit: totalDeposit,
      total_withdraw: totalWithdraw,
      net_balance: netBalance,
      largest_deposit: largestDeposit,
      largest_withdraw: largestWithdraw,
      avg_transaction: avgTransaction,
      first_seen: firstSeen,
      last_activity: lastActivity,
      activity_days: activityDays,
    };
  }

  /**
   * Force a full resync of ledger updates for a wallet
   */
  async forceResync(address: string): Promise<{ synced: number; total: number }> {
    const normalizedAddress = address.toLowerCase();

    // Delete existing ledger updates
    await this.prisma.ledgerUpdate.deleteMany({
      where: { wallet_address: normalizedAddress },
    });

    // Reset sync status
    await this.prisma.ledgerSyncStatus
      .delete({
        where: { wallet_address: normalizedAddress },
      })
      .catch(() => {
        // Ignore if doesn't exist
      });

    // Sync from scratch
    return this.syncLedgerUpdates(normalizedAddress, true);
  }
}
