import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { HyperLiquidInfoService } from './hyperliquid-info.service';
import { Prisma } from '../../generated/prisma/client';

// HyperLiquid API returns max 2000 elements per request for fills
const MAX_RESULTS_PER_REQUEST = 2000;
// Cache TTL in minutes (30 minutes)
const FILL_CACHE_TTL_MINUTES = 30;
// Delay between API calls to avoid rate limiting
const API_DELAY_MS = 100;
// Sync timeout in minutes (15 minutes) - force restart if stuck
const SYNC_TIMEOUT_MINUTES = 15;

interface FillFromApi {
  time: number;
  coin: string;
  px: string;
  sz: string;
  side: 'B' | 'A';
  startPosition?: string;
  dir?: string;
  closedPnl?: string;
  hash: string;
  oid: number;
  crossed: boolean;
  fee?: string;
  tid?: number;
  feeToken?: string;
}

// Individual fill record stored in JSON
export interface IndividualFillRecord {
  order_id: number;
  coin: string;
  side: string;
  price: string;
  size: string;
  direction: string | null;
  closed_pnl: string | null;
  fee: string | null;
  fee_token: string | null;
  start_position: string | null;
  crossed: boolean;
  tid: number | null;
  timestamp: number;
}

// Grouped fill statistics
interface GroupedFillData {
  wallet_address: string;
  tx_hash: string;
  coin: string;
  side: string;
  total_size: number;
  avg_price: number;
  total_value: number;
  total_pnl: number | null;
  total_fee: number | null;
  fill_count: number;
  records: IndividualFillRecord[];
  fill_timestamp: bigint;
}

@Injectable()
export class FillSyncService {
  private readonly logger = new Logger(FillSyncService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly hlService: HyperLiquidInfoService,
  ) {}

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Get the last synced time for a wallet's fills
   */
  async getLastSyncedTime(address: string): Promise<bigint> {
    const normalizedAddress = address.toLowerCase();
    const syncStatus = await this.prisma.fillSyncStatus.findUnique({
      where: { wallet_address: normalizedAddress },
    });
    return syncStatus?.last_synced_time ?? BigInt(0);
  }

  /**
   * Check if fill cache is still valid (within 30 minutes)
   */
  async isCacheValid(address: string): Promise<boolean> {
    const normalizedAddress = address.toLowerCase();
    const syncStatus = await this.prisma.fillSyncStatus.findUnique({
      where: { wallet_address: normalizedAddress },
    });

    if (!syncStatus || !syncStatus.last_synced_at) {
      return false;
    }

    const cacheExpiry = new Date(syncStatus.last_synced_at.getTime() + FILL_CACHE_TTL_MINUTES * 60 * 1000);
    return cacheExpiry > new Date();
  }

  /**
   * Check if sync is currently in progress and not timed out
   */
  async isSyncInProgress(address: string): Promise<boolean> {
    const normalizedAddress = address.toLowerCase();
    const syncStatus = await this.prisma.fillSyncStatus.findUnique({
      where: { wallet_address: normalizedAddress },
    });

    if (!syncStatus || !syncStatus.is_syncing) {
      return false;
    }

    // Check if sync has timed out (15 minutes)
    if (syncStatus.sync_started_at) {
      const timeoutAt = new Date(syncStatus.sync_started_at.getTime() + SYNC_TIMEOUT_MINUTES * 60 * 1000);
      if (timeoutAt < new Date()) {
        this.logger.warn(`Fill sync for ${normalizedAddress} timed out after ${SYNC_TIMEOUT_MINUTES} minutes, will force restart`);
        return false;
      }
    }

    return true;
  }

  /**
   * Acquire sync lock for a wallet
   */
  private async acquireSyncLock(address: string): Promise<boolean> {
    const normalizedAddress = address.toLowerCase();

    if (await this.isSyncInProgress(normalizedAddress)) {
      this.logger.log(`Fill sync already in progress for ${normalizedAddress}, skipping`);
      return false;
    }

    await this.prisma.fillSyncStatus.upsert({
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

    await this.prisma.fillSyncStatus.update({
      where: { wallet_address: normalizedAddress },
      data: updateData,
    });
  }

  /**
   * Sync fills for a wallet with pagination using startTime
   */
  async syncFills(address: string, forceFullSync = false): Promise<{ synced: number; total: number }> {
    const normalizedAddress = address.toLowerCase();
    this.logger.log(`Starting fill sync for ${normalizedAddress}, forceFullSync: ${forceFullSync}`);

    // Check if cache is still valid
    if (!forceFullSync && (await this.isCacheValid(normalizedAddress))) {
      const syncStatus = await this.prisma.fillSyncStatus.findUnique({
        where: { wallet_address: normalizedAddress },
      });
      this.logger.log(`Fill cache still valid for ${normalizedAddress}, skipping sync`);
      return { synced: 0, total: syncStatus?.total_count ?? 0 };
    }

    // Try to acquire sync lock
    const lockAcquired = await this.acquireSyncLock(normalizedAddress);
    if (!lockAcquired) {
      const syncStatus = await this.prisma.fillSyncStatus.findUnique({
        where: { wallet_address: normalizedAddress },
      });
      return { synced: 0, total: syncStatus?.total_count ?? 0 };
    }

    let startTime = forceFullSync ? BigInt(0) : await this.getLastSyncedTime(normalizedAddress);
    let totalSynced = 0;
    let hasMore = true;
    let lastTime = startTime;

    try {
      while (hasMore) {
        const fills: FillFromApi[] = await this.hlService.getUserFillsByTime(normalizedAddress, Number(startTime));

        if (!fills || fills.length === 0) {
          hasMore = false;
          break;
        }

        this.logger.debug(`Fetched ${fills.length} fills for ${normalizedAddress} starting from ${startTime}`);

        await this.saveFills(normalizedAddress, fills);
        totalSynced += fills.length;

        const maxTime = Math.max(...fills.map((f) => f.time));
        lastTime = BigInt(maxTime);

        if (fills.length < MAX_RESULTS_PER_REQUEST) {
          hasMore = false;
        } else {
          startTime = BigInt(maxTime + 1);
          await this.sleep(API_DELAY_MS);
        }
      }

      const totalCount = await this.prisma.fill.count({
        where: { wallet_address: normalizedAddress },
      });

      await this.releaseSyncLock(normalizedAddress, true, lastTime, totalCount);

      this.logger.log(`Fill sync completed for ${normalizedAddress}: synced ${totalSynced}, total ${totalCount}`);
      return { synced: totalSynced, total: totalCount };
    } catch (error) {
      await this.releaseSyncLock(normalizedAddress, false);
      this.logger.error(`Error syncing fills for ${normalizedAddress}`, {
        error: error.message,
        startTime: startTime.toString(),
      });
      throw error;
    }
  }

  /**
   * Group fills by tx_hash and calculate statistics
   */
  private groupFillsByTxHash(address: string, fills: FillFromApi[]): GroupedFillData[] {
    const normalizedAddress = address.toLowerCase();
    const groupedMap = new Map<string, FillFromApi[]>();

    // Group fills by tx_hash
    for (const fill of fills) {
      const existing = groupedMap.get(fill.hash) || [];
      existing.push(fill);
      groupedMap.set(fill.hash, existing);
    }

    // Calculate statistics for each group
    const groupedFills: GroupedFillData[] = [];

    for (const [txHash, txFills] of groupedMap) {
      // Calculate total size and total value for weighted average
      let totalSize = 0;
      let totalValue = 0;
      let totalPnl = 0;
      let totalFee = 0;
      let hasPnl = false;
      let hasFee = false;

      // Determine side: 'buy', 'sell', or 'mixed'
      const sides = new Set(txFills.map((f) => f.side));
      let side: string;
      if (sides.size === 1) {
        side = sides.has('B') ? 'buy' : 'sell';
      } else {
        side = 'mixed';
      }

      // Use the first fill's coin (assuming same tx_hash has same coin)
      const coin = txFills[0].coin;

      // Build individual records and calculate totals
      const records: IndividualFillRecord[] = [];
      let minTimestamp = BigInt(txFills[0].time);

      for (const fill of txFills) {
        const size = parseFloat(fill.sz);
        const price = parseFloat(fill.px);

        totalSize += size;
        totalValue += size * price;

        if (fill.closedPnl) {
          totalPnl += parseFloat(fill.closedPnl);
          hasPnl = true;
        }

        if (fill.fee) {
          totalFee += parseFloat(fill.fee);
          hasFee = true;
        }

        // Track minimum timestamp
        if (BigInt(fill.time) < minTimestamp) {
          minTimestamp = BigInt(fill.time);
        }

        // Create individual record
        records.push({
          order_id: fill.oid,
          coin: fill.coin,
          side: fill.side === 'B' ? 'buy' : 'sell',
          price: fill.px,
          size: fill.sz,
          direction: fill.dir || null,
          closed_pnl: fill.closedPnl || null,
          fee: fill.fee || null,
          fee_token: fill.feeToken || null,
          start_position: fill.startPosition || null,
          crossed: fill.crossed,
          tid: fill.tid || null,
          timestamp: fill.time,
        });
      }

      // Calculate weighted average price
      const avgPrice = totalSize > 0 ? totalValue / totalSize : 0;

      groupedFills.push({
        wallet_address: normalizedAddress,
        tx_hash: txHash,
        coin,
        side,
        total_size: totalSize,
        avg_price: avgPrice,
        total_value: totalValue,
        total_pnl: hasPnl ? totalPnl : null,
        total_fee: hasFee ? totalFee : null,
        fill_count: txFills.length,
        records,
        fill_timestamp: minTimestamp,
      });
    }

    return groupedFills;
  }

  /**
   * Save fills to database (grouped by tx_hash)
   */
  private async saveFills(address: string, fills: FillFromApi[]): Promise<void> {
    const groupedFills = this.groupFillsByTxHash(address, fills);

    for (const grouped of groupedFills) {
      try {
        await this.prisma.fill.upsert({
          where: {
            tx_hash: grouped.tx_hash,
          },
          update: {
            coin: grouped.coin,
            side: grouped.side,
            total_size: new Prisma.Decimal(grouped.total_size),
            avg_price: new Prisma.Decimal(grouped.avg_price),
            total_value: new Prisma.Decimal(grouped.total_value),
            total_pnl: grouped.total_pnl !== null ? new Prisma.Decimal(grouped.total_pnl) : null,
            total_fee: grouped.total_fee !== null ? new Prisma.Decimal(grouped.total_fee) : null,
            fill_count: grouped.fill_count,
            records: grouped.records as unknown as Prisma.InputJsonValue,
          },
          create: {
            wallet_address: grouped.wallet_address,
            tx_hash: grouped.tx_hash,
            coin: grouped.coin,
            side: grouped.side,
            total_size: new Prisma.Decimal(grouped.total_size),
            avg_price: new Prisma.Decimal(grouped.avg_price),
            total_value: new Prisma.Decimal(grouped.total_value),
            total_pnl: grouped.total_pnl !== null ? new Prisma.Decimal(grouped.total_pnl) : null,
            total_fee: grouped.total_fee !== null ? new Prisma.Decimal(grouped.total_fee) : null,
            fill_count: grouped.fill_count,
            records: grouped.records as unknown as Prisma.InputJsonValue,
            fill_timestamp: grouped.fill_timestamp,
          },
        });
      } catch (error) {
        this.logger.warn(`Failed to save grouped fill for ${grouped.wallet_address}`, {
          tx_hash: grouped.tx_hash,
          fill_count: grouped.fill_count,
          error: error.message,
        });
      }
    }
  }

  /**
   * Get fills from database with pagination
   */
  async getFills(address: string, limit = 100, offset = 0) {
    const normalizedAddress = address.toLowerCase();

    // First, try to sync if needed
    await this.syncFills(normalizedAddress);

    const [fills, total] = await Promise.all([
      this.prisma.fill.findMany({
        where: { wallet_address: normalizedAddress },
        orderBy: { fill_timestamp: 'desc' },
        take: limit,
        skip: offset,
      }),
      this.prisma.fill.count({
        where: { wallet_address: normalizedAddress },
      }),
    ]);

    // Transform to API format
    const fillsData = fills.map((f) => ({
      id: f.id,
      wallet_address: f.wallet_address,
      tx_hash: f.tx_hash,
      coin: f.coin,
      side: f.side,
      total_size: f.total_size.toString(),
      avg_price: f.avg_price.toString(),
      total_value: f.total_value.toString(),
      total_pnl: f.total_pnl?.toString() ?? null,
      total_fee: f.total_fee?.toString() ?? null,
      fill_count: f.fill_count,
      records: f.records as unknown as IndividualFillRecord[],
      fill_timestamp: f.fill_timestamp.toString(),
    }));

    return {
      address: normalizedAddress,
      fills: fillsData,
      pagination: {
        total,
        limit,
        offset,
        has_more: offset + limit < total,
      },
    };
  }

  /**
   * Get fill statistics for a wallet
   */
  async getFillStatistics(address: string) {
    const normalizedAddress = address.toLowerCase();

    // First, ensure data is synced
    await this.syncFills(normalizedAddress);

    const [txCount, buyTxCount, sellTxCount, totalFillCount, totalVolume, totalPnl, totalFees, firstFill, lastFill] = await Promise.all([
      // Count of grouped transactions (tx_hash)
      this.prisma.fill.count({
        where: { wallet_address: normalizedAddress },
      }),
      this.prisma.fill.count({
        where: { wallet_address: normalizedAddress, side: 'buy' },
      }),
      this.prisma.fill.count({
        where: { wallet_address: normalizedAddress, side: 'sell' },
      }),
      // Sum of all individual fills
      this.prisma.fill.aggregate({
        where: { wallet_address: normalizedAddress },
        _sum: { fill_count: true },
      }),
      this.prisma.fill.aggregate({
        where: { wallet_address: normalizedAddress },
        _sum: { total_value: true },
      }),
      this.prisma.fill.aggregate({
        where: { wallet_address: normalizedAddress },
        _sum: { total_pnl: true },
      }),
      this.prisma.fill.aggregate({
        where: { wallet_address: normalizedAddress },
        _sum: { total_fee: true },
      }),
      this.prisma.fill.findFirst({
        where: { wallet_address: normalizedAddress },
        orderBy: { fill_timestamp: 'asc' },
        select: { fill_timestamp: true },
      }),
      this.prisma.fill.findFirst({
        where: { wallet_address: normalizedAddress },
        orderBy: { fill_timestamp: 'desc' },
        select: { fill_timestamp: true },
      }),
    ]);

    const totalFillCountNum = totalFillCount._sum.fill_count ?? 0;
    const totalVolumeNum = totalVolume._sum.total_value?.toNumber() ?? 0;
    const totalPnlNum = totalPnl._sum.total_pnl?.toNumber() ?? 0;
    const totalFeesNum = totalFees._sum.total_fee?.toNumber() ?? 0;

    const firstTime = firstFill ? Number(firstFill.fill_timestamp) : null;
    const lastTime = lastFill ? Number(lastFill.fill_timestamp) : null;

    let activityDays = 0;
    if (firstTime && lastTime) {
      activityDays = Math.floor((lastTime - firstTime) / (1000 * 60 * 60 * 24));
    }

    return {
      address: normalizedAddress,
      tx_count: txCount, // Number of unique transactions
      total_fill_count: totalFillCountNum, // Total individual fills
      buy_tx_count: buyTxCount,
      sell_tx_count: sellTxCount,
      total_volume: totalVolumeNum,
      total_pnl: totalPnlNum,
      total_fees: totalFeesNum,
      first_fill: firstTime,
      last_fill: lastTime,
      activity_days: activityDays,
    };
  }

  /**
   * Force a full resync of fills for a wallet
   */
  async forceResync(address: string): Promise<{ synced: number; total: number }> {
    const normalizedAddress = address.toLowerCase();

    await this.prisma.fill.deleteMany({
      where: { wallet_address: normalizedAddress },
    });

    await this.prisma.fillSyncStatus
      .delete({
        where: { wallet_address: normalizedAddress },
      })
      .catch(() => {
        // Ignore if doesn't exist
      });

    return this.syncFills(normalizedAddress, true);
  }
}
