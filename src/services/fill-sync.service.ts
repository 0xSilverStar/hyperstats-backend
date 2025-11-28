import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { HyperLiquidInfoService } from './hyperliquid-info.service';
import { Prisma } from '@prisma/client';

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
   * Save fills to database
   */
  private async saveFills(address: string, fills: FillFromApi[]): Promise<void> {
    const normalizedAddress = address.toLowerCase();

    for (const fill of fills) {
      try {
        await this.prisma.fill.upsert({
          where: {
            tx_hash_order_id_fill_timestamp: {
              tx_hash: fill.hash,
              order_id: BigInt(fill.oid),
              fill_timestamp: BigInt(fill.time),
            },
          },
          update: {
            coin: fill.coin,
            side: fill.side,
            price: new Prisma.Decimal(fill.px),
            size: new Prisma.Decimal(fill.sz),
            direction: fill.dir || null,
            closed_pnl: fill.closedPnl ? new Prisma.Decimal(fill.closedPnl) : null,
            fee: fill.fee ? new Prisma.Decimal(fill.fee) : null,
            fee_token: fill.feeToken ?? null,
            start_position: fill.startPosition ? new Prisma.Decimal(fill.startPosition) : null,
            crossed: fill.crossed,
            tid: fill.tid ? BigInt(fill.tid) : null,
          },
          create: {
            wallet_address: normalizedAddress,
            tx_hash: fill.hash,
            order_id: BigInt(fill.oid),
            coin: fill.coin,
            side: fill.side,
            price: new Prisma.Decimal(fill.px),
            size: new Prisma.Decimal(fill.sz),
            direction: fill.dir || null,
            closed_pnl: fill.closedPnl ? new Prisma.Decimal(fill.closedPnl) : null,
            fee: fill.fee ? new Prisma.Decimal(fill.fee) : null,
            fee_token: fill.feeToken ?? null,
            start_position: fill.startPosition ? new Prisma.Decimal(fill.startPosition) : null,
            crossed: fill.crossed,
            fill_timestamp: BigInt(fill.time),
            tid: fill.tid ? BigInt(fill.tid) : null,
          },
        });
      } catch (error) {
        this.logger.warn(`Failed to save fill for ${normalizedAddress}`, {
          hash: fill.hash,
          time: fill.time,
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
      order_id: f.order_id?.toString() ?? null,
      coin: f.coin,
      side: f.side,
      price: f.price.toString(),
      size: f.size.toString(),
      direction: f.direction,
      closed_pnl: f.closed_pnl?.toString() ?? null,
      fee: f.fee?.toString() ?? null,
      fee_token: f.fee_token,
      start_position: f.start_position?.toString() ?? null,
      crossed: f.crossed,
      fill_timestamp: f.fill_timestamp.toString(),
      tid: f.tid?.toString() ?? null,
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

    const [totalCount, buyCount, sellCount, totalVolume, totalPnl, totalFees, firstFill, lastFill] = await Promise.all([
      this.prisma.fill.count({
        where: { wallet_address: normalizedAddress },
      }),
      this.prisma.fill.count({
        where: { wallet_address: normalizedAddress, side: 'B' },
      }),
      this.prisma.fill.count({
        where: { wallet_address: normalizedAddress, side: 'A' },
      }),
      this.prisma.$queryRaw<[{ total: Prisma.Decimal | null }]>`
        SELECT SUM(price * size) as total FROM fills WHERE wallet_address = ${normalizedAddress}
      `,
      this.prisma.fill.aggregate({
        where: { wallet_address: normalizedAddress },
        _sum: { closed_pnl: true },
      }),
      this.prisma.fill.aggregate({
        where: { wallet_address: normalizedAddress },
        _sum: { fee: true },
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

    const totalVolumeNum = totalVolume[0]?.total?.toNumber() ?? 0;
    const totalPnlNum = totalPnl._sum.closed_pnl?.toNumber() ?? 0;
    const totalFeesNum = totalFees._sum.fee?.toNumber() ?? 0;

    const firstTime = firstFill ? Number(firstFill.fill_timestamp) : null;
    const lastTime = lastFill ? Number(lastFill.fill_timestamp) : null;

    let activityDays = 0;
    if (firstTime && lastTime) {
      activityDays = Math.floor((lastTime - firstTime) / (1000 * 60 * 60 * 24));
    }

    return {
      address: normalizedAddress,
      total_count: totalCount,
      buy_count: buyCount,
      sell_count: sellCount,
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
