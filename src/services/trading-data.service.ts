import { Injectable, Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bull';
import type { Queue } from 'bull';
import { PrismaService } from '../prisma/prisma.service';
import { HyperLiquidInfoService } from './hyperliquid-info.service';
import { WalletSyncLockService } from './wallet-sync-lock.service';
import { LedgerSyncService } from './ledger-sync.service';
import { FillSyncService } from './fill-sync.service';
import { Prisma } from '../../generated/prisma/client';
import { getDailyKey, getHourlyKey, utcFromString } from '../lib/dayjs';

const CACHE_TTL_MINUTES = 30;

type CacheType = 'positions' | 'orders' | 'fills' | 'balances' | 'profile';

@Injectable()
export class TradingDataService {
  private readonly logger = new Logger(TradingDataService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly hlService: HyperLiquidInfoService,
    private readonly syncLock: WalletSyncLockService,
    private readonly ledgerSyncService: LedgerSyncService,
    private readonly fillSyncService: FillSyncService,
    @InjectQueue('trading-data') private readonly tradingDataQueue: Queue,
  ) {}

  // Cache helper methods using Cache model
  private async isCacheValid(address: string, type: CacheType): Promise<boolean> {
    const cache = await this.prisma.cache.findUnique({
      where: {
        wallet_address_cache_type: {
          wallet_address: address.toLowerCase(),
          cache_type: type,
        },
      },
    });
    if (!cache) return false;
    return cache.expires_at > new Date();
  }

  private async setCacheTimestamp(address: string, type: CacheType): Promise<void> {
    const now = new Date();
    const expires_at = new Date(Date.now() + CACHE_TTL_MINUTES * 60 * 1000);
    await this.prisma.cache.upsert({
      where: {
        wallet_address_cache_type: {
          wallet_address: address.toLowerCase(),
          cache_type: type,
        },
      },
      update: { synced_at: now, expires_at },
      create: {
        wallet_address: address.toLowerCase(),
        cache_type: type,
        synced_at: now,
        expires_at,
      },
    });
  }

  private async ensureWalletExists(address: string): Promise<void> {
    await this.prisma.wallet.upsert({
      where: { address },
      update: {},
      create: {
        address,
        total_deposit: new Prisma.Decimal(0),
        total_withdraw: new Prisma.Decimal(0),
        transaction_count: 0,
      },
    });
  }

  async getPositions(address: string) {
    const normalizedAddress = address.toLowerCase();
    await this.ensureWalletExists(normalizedAddress);

    // Check cache
    if (await this.isCacheValid(normalizedAddress, 'positions')) {
      this.logger.debug(`Cache hit for positions: ${normalizedAddress}`);
      return this.getPositionsFromDb(normalizedAddress);
    }

    // Fetch fresh data
    this.logger.debug(`Cache miss for positions: ${normalizedAddress}`);
    const freshData = await this.hlService.getUserPositions(normalizedAddress);

    // Queue background save
    await this.tradingDataQueue.add('save-positions', {
      address: normalizedAddress,
      data: freshData,
    });

    // Return transformed data immediately
    return this.transformPositionsResponse(normalizedAddress, freshData);
  }

  async getOrders(address: string) {
    const normalizedAddress = address.toLowerCase();
    await this.ensureWalletExists(normalizedAddress);

    if (await this.isCacheValid(normalizedAddress, 'orders')) {
      this.logger.debug(`Cache hit for orders: ${normalizedAddress}`);
      return this.getOrdersFromDb(normalizedAddress);
    }

    this.logger.debug(`Cache miss for orders: ${normalizedAddress}`);
    const freshData = await this.hlService.getUserOpenOrders(normalizedAddress);

    await this.tradingDataQueue.add('save-orders', {
      address: normalizedAddress,
      data: freshData,
    });

    return this.transformOrdersResponse(normalizedAddress, freshData);
  }

  async getFills(address: string, limit = 50, offset = 0) {
    const normalizedAddress = address.toLowerCase();
    await this.ensureWalletExists(normalizedAddress);

    if (await this.isCacheValid(normalizedAddress, 'fills')) {
      this.logger.debug(`Cache hit for fills: ${normalizedAddress}`);
      return this.getFillsFromDb(normalizedAddress, limit, offset);
    }

    this.logger.debug(`Cache miss for fills: ${normalizedAddress}`);
    const freshData = await this.hlService.getUserFills(normalizedAddress);

    await this.tradingDataQueue.add('save-fills', {
      address: normalizedAddress,
      data: freshData,
    });

    // Return paginated data from fresh response
    const paginatedFills = freshData.slice(offset, offset + limit);
    return {
      address: normalizedAddress,
      fills: paginatedFills,
      pagination: {
        total: freshData.length,
        limit,
        offset,
        hasMore: offset + limit < freshData.length,
      },
      fromCache: false,
    };
  }

  async getBalances(address: string) {
    const normalizedAddress = address.toLowerCase();
    await this.ensureWalletExists(normalizedAddress);

    if (await this.isCacheValid(normalizedAddress, 'balances')) {
      this.logger.debug(`Cache hit for balances: ${normalizedAddress}`);
      return this.getBalancesFromDb(normalizedAddress);
    }

    this.logger.debug(`Cache miss for balances: ${normalizedAddress}`);
    const freshData = await this.hlService.getUserSpotBalances(normalizedAddress);

    await this.tradingDataQueue.add('save-balances', {
      address: normalizedAddress,
      data: freshData,
    });

    return this.transformBalancesResponse(normalizedAddress, freshData);
  }

  async getProfile(address: string) {
    const normalizedAddress = address.toLowerCase();
    await this.ensureWalletExists(normalizedAddress);

    if (await this.isCacheValid(normalizedAddress, 'profile')) {
      this.logger.debug(`Cache hit for profile: ${normalizedAddress}`);
      return this.getProfileFromDb(normalizedAddress);
    }

    this.logger.debug(`Cache miss for profile: ${normalizedAddress}`);
    const profile = await this.hlService.getCompleteProfile(normalizedAddress);

    await this.tradingDataQueue.add('save-profile', {
      address: normalizedAddress,
      data: profile,
    });

    return this.transformProfileResponse(normalizedAddress, profile);
  }

  /**
   * Get wallet data with synchronous sync if cache is invalid.
   * Uses lock to prevent duplicate syncs for the same wallet.
   * Returns positions, orders, and balances together.
   * Also starts background sync for trades and ledger.
   */
  async getWalletDataSync(address: string) {
    const normalizedAddress = address.toLowerCase();

    const data = await this.syncLock.acquireLock(normalizedAddress, async () => {
      await this.ensureWalletExists(normalizedAddress);

      // Check if cache is valid
      if (await this.isCacheValid(normalizedAddress, 'profile')) {
        this.logger.debug(`Cache hit for wallet data: ${normalizedAddress}`);
        return this.getWalletDataFromDb(normalizedAddress);
      }

      // Sync first, then return
      this.logger.debug(`Cache miss, syncing wallet: ${normalizedAddress}`);
      await this.fullSync(normalizedAddress);

      return this.getWalletDataFromDb(normalizedAddress);
    });

    // Start background sync for trades and ledger (non-blocking)
    this.startBackgroundSync(normalizedAddress);

    return data;
  }

  /**
   * Start background sync for trades and ledger
   */
  private startBackgroundSync(address: string) {
    // Non-blocking background sync
    Promise.all([
      this.fillSyncService.syncFills(address).catch((e) => this.logger.error(`Fill sync error: ${e.message}`)),
      this.ledgerSyncService.syncLedgerUpdates(address).catch((e) => this.logger.error(`Ledger sync error: ${e.message}`)),
    ]);
  }

  /**
   * Get sync status for a wallet (lightweight endpoint for polling)
   */
  async getSyncStatus(address: string) {
    const normalizedAddress = address.toLowerCase();

    const [tradesCount, ledgerCount, fillSyncStatus, ledgerSyncStatus] = await Promise.all([
      this.prisma.fill.count({ where: { wallet_address: normalizedAddress } }),
      this.prisma.ledgerUpdate.count({ where: { wallet_address: normalizedAddress } }),
      this.prisma.fillSyncStatus.findUnique({ where: { wallet_address: normalizedAddress } }),
      this.prisma.ledgerSyncStatus.findUnique({ where: { wallet_address: normalizedAddress } }),
    ]);

    // Determine if sync is needed/in progress
    const tradesSyncing = fillSyncStatus?.is_syncing || (!fillSyncStatus?.last_synced_at && tradesCount === 0);
    const ledgerSyncing = ledgerSyncStatus?.is_syncing || (!ledgerSyncStatus?.last_synced_at && ledgerCount === 0);

    return {
      address: normalizedAddress,
      trades: {
        count: tradesCount,
        syncing: tradesSyncing,
        syncedAt: fillSyncStatus?.last_synced_at ?? null,
      },
      ledger: {
        count: ledgerCount,
        syncing: ledgerSyncing,
        syncedAt: ledgerSyncStatus?.last_synced_at ?? null,
      },
    };
  }

  private async getWalletDataFromDb(address: string) {
    const [wallet, positions, orders, balances, cache, tradesCount, ledgerCount, fillSyncStatus, ledgerSyncStatus] = await Promise.all([
      this.prisma.wallet.findUnique({ where: { address } }),
      this.prisma.position.findMany({ where: { wallet_address: address } }),
      this.prisma.order.findMany({ where: { wallet_address: address, status: 'open' } }),
      this.prisma.balance.findMany({ where: { wallet_address: address } }),
      this.prisma.cache.findUnique({
        where: { wallet_address_cache_type: { wallet_address: address, cache_type: 'profile' } },
      }),
      this.prisma.fill.count({ where: { wallet_address: address } }),
      this.prisma.ledgerUpdate.count({ where: { wallet_address: address } }),
      this.prisma.fillSyncStatus.findUnique({ where: { wallet_address: address } }),
      this.prisma.ledgerSyncStatus.findUnique({ where: { wallet_address: address } }),
    ]);

    const totalUnrealizedPnl = positions.reduce((sum, p) => sum + parseFloat(p.unrealized_pnl.toString()), 0);
    const totalPositionValue = positions.reduce((sum, p) => sum + parseFloat(p.position_value.toString()), 0);
    const totalMarginUsed = positions.reduce((sum, p) => sum + parseFloat(p.margin_used.toString()), 0);

    return {
      address,
      wallet: wallet
        ? {
            totalDeposit: wallet.total_deposit,
            totalWithdraw: wallet.total_withdraw,
            netDeposit: parseFloat(wallet.total_deposit.toString()) - parseFloat(wallet.total_withdraw.toString()),
            transactionCount: wallet.transaction_count,
            firstSeenAt: wallet.first_seen_at,
            lastActivityAt: wallet.last_activity_at,
          }
        : null,
      positions: positions.map((p) => ({
        coin: p.coin,
        size: p.position_size.toString(),
        side: p.side,
        leverage: p.leverage,
        entryPrice: p.entry_price.toString(),
        markPrice: p.mark_price.toString(),
        positionValue: p.position_value.toString(),
        unrealizedPnl: p.unrealized_pnl.toString(),
        returnOnEquity: p.return_on_equity?.toString() ?? null,
        liquidationPrice: p.liquidation_price?.toString() ?? null,
        marginUsed: p.margin_used.toString(),
        cumFunding:
          p.cum_funding_all_time || p.cum_funding_since_open || p.cum_funding_since_change
            ? {
                allTime: p.cum_funding_all_time?.toString() ?? '0',
                sinceOpen: p.cum_funding_since_open?.toString() ?? '0',
                sinceChange: p.cum_funding_since_change?.toString() ?? '0',
              }
            : null,
      })),
      orders: orders.map((o) => ({
        orderId: o.order_id.toString(),
        coin: o.coin,
        side: o.side,
        orderType: o.order_type,
        limitPrice: o.limit_price?.toString() ?? null,
        size: o.size.toString(),
        reduceOnly: o.reduce_only,
        timestamp: o.order_timestamp.toString(),
      })),
      balances: balances.map((b) => ({
        coin: b.coin,
        total: b.total_balance.toString(),
        hold: b.hold_balance.toString(),
        available: b.available_balance.toString(),
        entryValue: b.entry_value?.toString() ?? null,
      })),
      summary: {
        positionsCount: positions.length,
        longCount: positions.filter((p) => p.side === 'long').length,
        shortCount: positions.filter((p) => p.side === 'short').length,
        totalUnrealizedPnl,
        totalPositionValue,
        totalMarginUsed,
        openOrdersCount: orders.length,
        balancesCount: balances.length,
      },
      trades: {
        count: tradesCount,
        syncing: fillSyncStatus?.is_syncing || (!fillSyncStatus?.last_synced_at && tradesCount === 0),
      },
      ledger: {
        count: ledgerCount,
        syncing: ledgerSyncStatus?.is_syncing || (!ledgerSyncStatus?.last_synced_at && ledgerCount === 0),
      },
      syncedAt: cache?.synced_at ?? null,
    };
  }

  // Database read methods
  private async getPositionsFromDb(address: string) {
    const wallet = await this.prisma.wallet.findUnique({
      where: { address },
    });

    const positions = await this.prisma.position.findMany({
      where: { wallet_address: address },
    });

    const longCount = positions.filter((p) => p.side === 'long').length;
    const shortCount = positions.filter((p) => p.side === 'short').length;
    const totalUnrealizedPnl = positions.reduce((sum, p) => sum + parseFloat(p.unrealized_pnl.toString()), 0);
    const totalPositionValue = positions.reduce((sum, p) => sum + parseFloat(p.position_value.toString()), 0);
    const totalMarginUsed = positions.reduce((sum, p) => sum + parseFloat(p.margin_used.toString()), 0);

    return {
      address,
      walletInfo: {
        totalDeposit: wallet?.total_deposit ?? 0,
        totalWithdraw: wallet?.total_withdraw ?? 0,
        transactionCount: wallet?.transaction_count ?? 0,
      },
      positions: positions.map((p) => ({
        coin: p.coin,
        szi: p.position_size.toString(),
        side: p.side,
        leverage: p.leverage,
        entryPx: p.entry_price.toString(),
        positionValue: p.position_value.toString(),
        unrealizedPnl: p.unrealized_pnl.toString(),
        returnOnEquity: p.return_on_equity?.toString() ?? '0',
        liquidationPx: p.liquidation_price?.toString() ?? null,
        marginUsed: p.margin_used.toString(),
        cumFunding:
          p.cum_funding_all_time || p.cum_funding_since_open || p.cum_funding_since_change
            ? {
                allTime: p.cum_funding_all_time?.toString() ?? '0',
                sinceOpen: p.cum_funding_since_open?.toString() ?? '0',
                sinceChange: p.cum_funding_since_change?.toString() ?? '0',
              }
            : null,
      })),
      positionsCount: positions.length,
      longCount,
      shortCount,
      totalUnrealizedPnl,
      totalPositionValue,
      totalMarginUsed,
      fromCache: true,
    };
  }

  private async getOrdersFromDb(address: string) {
    const orders = await this.prisma.order.findMany({
      where: { wallet_address: address, status: 'open' },
      orderBy: { order_timestamp: 'desc' },
    });

    const buyOrders = orders.filter((o) => o.side === 'buy').length;
    const sellOrders = orders.filter((o) => o.side === 'sell').length;
    const totalSize = orders.reduce((sum, o) => sum + parseFloat(o.size.toString()), 0);

    return {
      address,
      orders,
      ordersCount: orders.length,
      buyOrders,
      sellOrders,
      totalSize,
      fromCache: true,
    };
  }

  private async getFillsFromDb(address: string, limit: number, offset: number) {
    const [fills, total] = await Promise.all([
      this.prisma.fill.findMany({
        where: { wallet_address: address },
        orderBy: { fill_timestamp: 'desc' },
        skip: offset,
        take: limit,
      }),
      this.prisma.fill.count({ where: { wallet_address: address } }),
    ]);

    return {
      address,
      fills,
      pagination: {
        total,
        limit,
        offset,
        hasMore: offset + limit < total,
      },
      fromCache: true,
    };
  }

  private async getBalancesFromDb(address: string) {
    const balances = await this.prisma.balance.findMany({
      where: { wallet_address: address },
    });

    const totalValue = balances.reduce((sum, b) => sum + parseFloat(b.entry_value?.toString() ?? '0'), 0);

    return {
      address,
      balances: balances.map((b) => ({
        coin: b.coin,
        token: b.token_id,
        total: b.total_balance.toString(),
        hold: b.hold_balance.toString(),
        entryNtl: b.entry_value?.toString() ?? '0',
      })),
      balancesCount: balances.length,
      totalValue,
      fromCache: true,
    };
  }

  private async getProfileFromDb(address: string) {
    const wallet = await this.prisma.wallet.findUnique({
      where: { address },
    });

    const [positions, orders, fills, balances, totalFillsCount] = await Promise.all([
      this.prisma.position.findMany({ where: { wallet_address: address } }),
      this.prisma.order.findMany({ where: { wallet_address: address, status: 'open' } }),
      this.prisma.fill.findMany({
        where: { wallet_address: address },
        orderBy: { fill_timestamp: 'desc' },
        take: 50,
      }),
      this.prisma.balance.findMany({ where: { wallet_address: address } }),
      this.prisma.fill.count({ where: { wallet_address: address } }),
    ]);

    const totalUnrealizedPnl = positions.reduce((sum, p) => sum + parseFloat(p.unrealized_pnl.toString()), 0);
    const totalPositionValue = positions.reduce((sum, p) => sum + parseFloat(p.position_value.toString()), 0);
    const totalMarginUsed = positions.reduce((sum, p) => sum + parseFloat(p.margin_used.toString()), 0);

    return {
      address,
      walletInfo: {
        totalDeposit: wallet?.total_deposit,
        totalWithdraw: wallet?.total_withdraw,
        netDeposit: parseFloat(wallet?.total_deposit.toString() ?? '0') - parseFloat(wallet?.total_withdraw.toString() ?? '0'),
        transactionCount: wallet?.transaction_count ?? 0,
        firstSeenAt: wallet?.first_seen_at,
        lastActivityAt: wallet?.last_activity_at,
      },
      tradingSummary: {
        positionsCount: positions.length,
        totalUnrealizedPnl,
        totalPositionValue,
        totalMarginUsed,
        openOrdersCount: orders.length,
        totalFillsCount,
      },
      positions,
      openOrders: orders,
      recentFills: fills,
      spotBalances: balances,
      fromCache: true,
    };
  }

  // Transform methods for fresh API data
  private transformPositionsResponse(address: string, data: any) {
    const assetPositions = data.assetPositions ?? [];
    const marginSummary: any = data.marginSummary ?? data.crossMarginSummary ?? {};

    const positions: any[] = [];
    let longCount = 0;
    let shortCount = 0;
    let totalUnrealizedPnl = 0;
    let totalPositionValue = 0;
    let totalMarginUsed = 0;

    for (const asset of assetPositions) {
      if (asset.position) {
        const pos = asset.position;
        const szi = parseFloat(pos.szi ?? '0');

        if (szi !== 0) {
          const side = szi > 0 ? 'long' : 'short';
          if (side === 'long') longCount++;
          else shortCount++;

          const pnl = parseFloat(pos.unrealizedPnl ?? '0');
          const value = parseFloat(pos.positionValue ?? '0');
          const margin = parseFloat(pos.marginUsed ?? '0');

          totalUnrealizedPnl += pnl;
          totalPositionValue += value;
          totalMarginUsed += margin;

          positions.push({
            coin: pos.coin ?? '',
            szi: pos.szi ?? '0',
            side,
            leverage: pos.leverage ?? null,
            entryPx: pos.entryPx ?? '0',
            positionValue: pos.positionValue ?? '0',
            unrealizedPnl: pos.unrealizedPnl ?? '0',
            returnOnEquity: pos.returnOnEquity ?? '0',
            liquidationPx: pos.liquidationPx ?? null,
            marginUsed: pos.marginUsed ?? '0',
          });
        }
      }
    }

    return {
      address,
      marginSummary,
      positions,
      positionsCount: positions.length,
      longCount,
      shortCount,
      totalUnrealizedPnl,
      totalPositionValue,
      totalMarginUsed,
      accountValue: marginSummary.accountValue ?? '0',
      fromCache: false,
    };
  }

  private transformOrdersResponse(address: string, data: any[]) {
    const buyOrders = data.filter((o) => o.side === 'B').length;
    const sellOrders = data.filter((o) => o.side === 'A').length;
    const totalSize = data.reduce((sum, o) => sum + parseFloat(o.sz ?? '0'), 0);

    return {
      address,
      orders: data.map((o) => ({
        ...o,
        side: o.side === 'B' ? 'buy' : 'sell',
      })),
      ordersCount: data.length,
      buyOrders,
      sellOrders,
      totalSize,
      fromCache: false,
    };
  }

  private transformBalancesResponse(address: string, data: any) {
    const balances = data.balances ?? [];
    const totalValue = balances.reduce((sum: number, b: any) => sum + parseFloat(b.entryNtl ?? '0'), 0);

    return {
      address,
      balances,
      balancesCount: balances.length,
      totalValue,
      fromCache: false,
    };
  }

  private transformProfileResponse(address: string, profile: any) {
    return {
      ...profile,
      fromCache: false,
    };
  }

  // Retry helper with exponential backoff for transaction failures
  private async withRetry<T>(operation: () => Promise<T>, maxRetries = 3, baseDelayMs = 500): Promise<T> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        const isRetryable =
          error.message?.includes('Unable to start a transaction') ||
          error.message?.includes('Connection pool') ||
          error.message?.includes('timeout') ||
          error.code === 'P2024'; // Prisma connection pool timeout

        if (!isRetryable || attempt === maxRetries) {
          throw error;
        }

        const delay = baseDelayMs * Math.pow(2, attempt - 1) + Math.random() * 100;
        this.logger.warn(`Retry ${attempt}/${maxRetries} after ${Math.round(delay)}ms: ${error.message}`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
    throw new Error('Max retries exceeded');
  }

  // Method for full sync (used by cron job)
  async fullSync(address: string): Promise<void> {
    const normalizedAddress = address.toLowerCase();
    const startTime = Date.now();

    try {
      await this.ensureWalletExists(normalizedAddress);

      // Fetch all data in parallel (API calls don't use DB connections)
      const [positions, orders, fills, balances] = await Promise.all([
        this.hlService.getUserPositions(normalizedAddress),
        this.hlService.getUserOpenOrders(normalizedAddress),
        this.hlService.getUserFills(normalizedAddress),
        this.hlService.getUserSpotBalances(normalizedAddress),
      ]);

      // Save data SEQUENTIALLY to avoid connection pool exhaustion
      // Each save operation uses a transaction, running them in parallel causes pool exhaustion
      await this.withRetry(() => this.savePositions(normalizedAddress, positions));
      await this.withRetry(() => this.saveOrders(normalizedAddress, orders));
      await this.withRetry(() => this.saveFills(normalizedAddress, fills));
      await this.withRetry(() => this.saveBalances(normalizedAddress, balances));

      // Update cache timestamps (simple upserts, no transactions needed)
      await this.setCacheTimestamp(normalizedAddress, 'profile');

      const duration = Date.now() - startTime;
      const posCount = positions.assetPositions?.filter((p: any) => Math.abs(parseFloat(p.position?.szi ?? '0')) > 0.00001).length ?? 0;
      this.logger.log(
        `${normalizedAddress.slice(0, 10)}... | Position:${posCount} Orders:${orders.length} Balances:${balances.balances?.length ?? 0} | ${duration}ms`,
      );
    } catch (error) {
      this.logger.error(`${normalizedAddress.slice(0, 10)}... failed: ${error.message}`);
      throw error;
    }
  }

  // Save methods (used by queue processor)
  async savePositions(address: string, data: any): Promise<void> {
    const assetPositions = data.assetPositions ?? [];

    await this.prisma.$transaction(async (tx) => {
      // Delete existing positions for this wallet
      await tx.position.deleteMany({ where: { wallet_address: address } });

      // Insert new positions
      for (const assetPosition of assetPositions) {
        const position = assetPosition.position;
        if (!position) continue;

        const szi = parseFloat(position.szi ?? '0');
        if (Math.abs(szi) < 0.00001) continue;

        const leverage = position.leverage;
        const leverageValue = Math.round(typeof leverage === 'object' ? parseFloat(leverage?.value ?? '1') : parseFloat(leverage ?? '1'));

        await tx.position.create({
          data: {
            wallet_address: address,
            coin: position.coin ?? 'UNKNOWN',
            position_size: new Prisma.Decimal(szi),
            entry_price: new Prisma.Decimal(parseFloat(position.entryPx ?? '0')),
            mark_price: new Prisma.Decimal(parseFloat(position.markPx ?? position.entryPx ?? '0')),
            unrealized_pnl: new Prisma.Decimal(parseFloat(position.unrealizedPnl ?? '0')),
            liquidation_price: position.liquidationPx ? new Prisma.Decimal(parseFloat(position.liquidationPx)) : null,
            leverage: leverageValue,
            margin_used: new Prisma.Decimal(parseFloat(position.marginUsed ?? '0')),
            side: szi > 0 ? 'long' : 'short',
            position_value: new Prisma.Decimal(parseFloat(position.positionValue ?? '0')),
            return_on_equity: position.returnOnEquity ? new Prisma.Decimal(parseFloat(position.returnOnEquity)) : null,
            leverage_type: typeof leverage === 'object' ? (leverage?.type ?? null) : null,
            cum_funding_all_time: position.cumFunding?.allTime ? new Prisma.Decimal(parseFloat(position.cumFunding.allTime)) : null,
            cum_funding_since_open: position.cumFunding?.sinceOpen ? new Prisma.Decimal(parseFloat(position.cumFunding.sinceOpen)) : null,
            cum_funding_since_change: position.cumFunding?.sinceChange ? new Prisma.Decimal(parseFloat(position.cumFunding.sinceChange)) : null,
            last_updated_at: new Date(),
          },
        });
      }
    }, { timeout: 30000 });

    // Update cache timestamp
    await this.setCacheTimestamp(address, 'positions');
  }

  async saveOrders(address: string, orders: any[]): Promise<void> {
    // Get current order IDs from the new data
    const newOrderIds = orders.map((o) => BigInt(o.oid ?? 0));

    await this.prisma.$transaction(async (tx) => {
      // Delete orders for this wallet that are no longer in the new data
      await tx.order.deleteMany({
        where: {
          wallet_address: address,
          order_id: { notIn: newOrderIds },
        },
      });

      // Upsert each order
      for (const order of orders) {
        const side = (order.side ?? 'B') === 'B' ? 'buy' : 'sell';
        const orderId = BigInt(order.oid ?? 0);

        await tx.order.upsert({
          where: { order_id: orderId },
          update: {
            wallet_address: address,
            coin: order.coin ?? 'UNKNOWN',
            side,
            order_type: order.orderType ?? 'Limit',
            limit_price: order.limitPx ? new Prisma.Decimal(parseFloat(order.limitPx)) : null,
            size: new Prisma.Decimal(parseFloat(order.sz ?? '0')),
            status: 'open',
            reduce_only: order.reduceOnly ?? false,
            trigger_condition: order.triggerCondition ? JSON.stringify(order.triggerCondition) : null,
            is_position_tpsl: order.isPositionTpsl ?? false,
            cloid: order.cloid ?? null,
            order_timestamp: BigInt(order.timestamp ?? Date.now()),
          },
          create: {
            wallet_address: address,
            order_id: orderId,
            coin: order.coin ?? 'UNKNOWN',
            side,
            order_type: order.orderType ?? 'Limit',
            limit_price: order.limitPx ? new Prisma.Decimal(parseFloat(order.limitPx)) : null,
            size: new Prisma.Decimal(parseFloat(order.sz ?? '0')),
            filled_size: new Prisma.Decimal(0),
            status: 'open',
            reduce_only: order.reduceOnly ?? false,
            trigger_condition: order.triggerCondition ? JSON.stringify(order.triggerCondition) : null,
            is_position_tpsl: order.isPositionTpsl ?? false,
            cloid: order.cloid ?? null,
            order_timestamp: BigInt(order.timestamp ?? Date.now()),
          },
        });
      }
    }, { timeout: 30000 });

    await this.setCacheTimestamp(address, 'orders');
  }

  async saveFills(address: string, fills: any[]): Promise<void> {
    // Group fills by tx_hash
    const groupedMap = new Map<string, any[]>();
    for (const fill of fills.slice(0, 500)) {
      const hash = fill.hash ?? '';
      const existing = groupedMap.get(hash) || [];
      existing.push(fill);
      groupedMap.set(hash, existing);
    }

    await this.prisma.$transaction(async (tx) => {
      for (const [txHash, txFills] of groupedMap) {
        // Calculate statistics
        let totalSize = 0;
        let totalValue = 0;
        let totalPnl = 0;
        let totalFee = 0;
        let hasPnl = false;
        let hasFee = false;

        // Determine side
        const sides = new Set(txFills.map((f: any) => f.side));
        let side: string;
        if (sides.size === 1) {
          side = sides.has('B') ? 'buy' : 'sell';
        } else {
          side = 'mixed';
        }

        const coin = txFills[0].coin ?? 'UNKNOWN';
        const records: any[] = [];
        let minTimestamp = BigInt(txFills[0].time ?? Date.now());

        for (const fill of txFills) {
          const size = parseFloat(fill.sz ?? '0');
          const price = parseFloat(fill.px ?? '0');
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

          const fillTime = BigInt(fill.time ?? Date.now());
          if (fillTime < minTimestamp) {
            minTimestamp = fillTime;
          }

          records.push({
            order_id: fill.oid ?? null,
            coin: fill.coin ?? 'UNKNOWN',
            side: fill.side === 'B' ? 'buy' : 'sell',
            price: fill.px ?? '0',
            size: fill.sz ?? '0',
            direction: fill.dir ?? null,
            closed_pnl: fill.closedPnl ?? null,
            fee: fill.fee ?? null,
            fee_token: fill.feeToken ?? null,
            start_position: fill.startPosition ?? null,
            crossed: fill.crossed ?? false,
            tid: fill.tid ?? null,
            timestamp: fill.time ?? Date.now(),
          });
        }

        const avgPrice = totalSize > 0 ? totalValue / totalSize : 0;

        await tx.fill.upsert({
          where: { tx_hash: txHash },
          update: {
            coin,
            side,
            total_size: new Prisma.Decimal(totalSize),
            avg_price: new Prisma.Decimal(avgPrice),
            total_value: new Prisma.Decimal(totalValue),
            total_pnl: hasPnl ? new Prisma.Decimal(totalPnl) : null,
            total_fee: hasFee ? new Prisma.Decimal(totalFee) : null,
            fill_count: txFills.length,
            records: records as any,
          },
          create: {
            wallet_address: address,
            tx_hash: txHash,
            coin,
            side,
            total_size: new Prisma.Decimal(totalSize),
            avg_price: new Prisma.Decimal(avgPrice),
            total_value: new Prisma.Decimal(totalValue),
            total_pnl: hasPnl ? new Prisma.Decimal(totalPnl) : null,
            total_fee: hasFee ? new Prisma.Decimal(totalFee) : null,
            fill_count: txFills.length,
            records: records as any,
            fill_timestamp: minTimestamp,
          },
        });
      }
    }, { timeout: 30000 });

    await this.setCacheTimestamp(address, 'fills');
  }

  async saveBalances(address: string, data: any): Promise<void> {
    const balances = data.balances ?? [];

    await this.prisma.$transaction(async (tx) => {
      // Delete existing balances for this wallet
      await tx.balance.deleteMany({ where: { wallet_address: address } });

      // Insert new balances
      for (const balance of balances) {
        const total = parseFloat(balance.total ?? '0');
        if (total < 0.00001) continue;

        const hold = parseFloat(balance.hold ?? '0');
        const available = total - hold;

        await tx.balance.create({
          data: {
            wallet_address: address,
            coin: balance.coin ?? 'UNKNOWN',
            token_id: balance.token ?? null,
            total_balance: new Prisma.Decimal(total),
            hold_balance: new Prisma.Decimal(hold),
            available_balance: new Prisma.Decimal(available),
            entry_value: balance.entryNtl ? new Prisma.Decimal(parseFloat(balance.entryNtl)) : null,
            last_updated_at: new Date(),
          },
        });
      }
    }, { timeout: 30000 });

    await this.setCacheTimestamp(address, 'balances');
  }

  async saveProfile(address: string, profile: any): Promise<void> {
    // Profile consists of positions, orders, fills, and balances
    // We save them individually
    await Promise.all([
      this.savePositions(address, { assetPositions: profile.positions }),
      this.saveOrders(address, profile.openOrders ?? []),
      this.saveFills(address, profile.recentFills ?? []),
      this.saveBalances(address, { balances: profile.spotBalances ?? [] }),
    ]);

    await this.setCacheTimestamp(address, 'profile');
  }

  /**
   * Get trading analytics for a wallet
   * Calculates performance metrics from fills, positions, and ledger data
   * @param address - wallet address
   * @param period - time period: '1h', '24h', '7d', '30d', '3m', '1y', 'all'
   */
  async getAnalytics(address: string, period: string = 'all') {
    const normalizedAddress = address.toLowerCase();

    // Calculate time filter based on period
    const now = Date.now();
    const periodMs: Record<string, number> = {
      '1h': 60 * 60 * 1000,
      '24h': 24 * 60 * 60 * 1000,
      '7d': 7 * 24 * 60 * 60 * 1000,
      '30d': 30 * 24 * 60 * 60 * 1000,
      '3m': 90 * 24 * 60 * 60 * 1000,
      '1y': 365 * 24 * 60 * 60 * 1000,
    };

    const sinceTimestamp = period === 'all' ? 0 : now - (periodMs[period] || periodMs['30d']);

    // Get all fills for analysis
    const allFills = await this.prisma.fill.findMany({
      where: { wallet_address: normalizedAddress },
      orderBy: { fill_timestamp: 'asc' },
    });

    // Filter fills by period
    const fills = period === 'all' ? allFills : allFills.filter((f) => Number(f.fill_timestamp) >= sinceTimestamp);

    // Get current positions for position distribution
    const positions = await this.prisma.position.findMany({
      where: { wallet_address: normalizedAddress },
    });

    // Get wallet info
    const wallet = await this.prisma.wallet.findUnique({
      where: { address: normalizedAddress },
    });

    // Calculate basic metrics for the period (using grouped fill statistics)
    const totalTrades = fills.reduce((sum, f) => sum + f.fill_count, 0);
    const tradesWithPnl = fills.filter((f) => f.total_pnl !== null);
    const winningTrades = tradesWithPnl.filter((f) => parseFloat(f.total_pnl?.toString() ?? '0') > 0);
    const losingTrades = tradesWithPnl.filter((f) => parseFloat(f.total_pnl?.toString() ?? '0') < 0);

    // Win rate = wins / (wins + losses), excluding breakeven trades
    const totalWinLossTrades = winningTrades.length + losingTrades.length;
    const winRate = totalWinLossTrades > 0 ? (winningTrades.length / totalWinLossTrades) * 100 : 0;

    // Calculate total volume for the period (using pre-calculated total_value)
    const totalVolume = fills.reduce((sum, f) => {
      return sum + parseFloat(f.total_value?.toString() ?? '0');
    }, 0);

    // Calculate total realized P&L for the period
    const totalRealizedPnl = tradesWithPnl.reduce((sum, f) => sum + parseFloat(f.total_pnl?.toString() ?? '0'), 0);

    // Calculate total fees for the period
    const totalFees = fills.reduce((sum, f) => sum + parseFloat(f.total_fee?.toString() ?? '0'), 0);

    // Time-based P&L calculations (always calculate these for reference)
    const oneHourAgo = now - 60 * 60 * 1000;
    const oneDayAgo = now - 24 * 60 * 60 * 1000;
    const sevenDaysAgo = now - 7 * 24 * 60 * 60 * 1000;
    const thirtyDaysAgo = now - 30 * 24 * 60 * 60 * 1000;

    const allTradesWithPnl = allFills.filter((f) => f.total_pnl !== null);
    const pnl1h = this.calculatePeriodMetrics(allTradesWithPnl, allFills, oneHourAgo);
    const pnl24h = this.calculatePeriodMetrics(allTradesWithPnl, allFills, oneDayAgo);
    const pnl7d = this.calculatePeriodMetrics(allTradesWithPnl, allFills, sevenDaysAgo);
    const pnl30d = this.calculatePeriodMetrics(allTradesWithPnl, allFills, thirtyDaysAgo);

    // P&L aggregation for charts (hourly for 1h/24h, daily for 7d/30d)
    const aggregatedPnl = this.calculateAggregatedPnl(tradesWithPnl, fills, period);

    // Trading statistics for the period
    const tradingStats = this.calculateTradingStats(tradesWithPnl, aggregatedPnl);

    // Position distribution (current, not affected by period)
    const longPositions = positions.filter((p) => p.side === 'long');
    const shortPositions = positions.filter((p) => p.side === 'short');
    const totalPositionValue = positions.reduce((sum, p) => sum + parseFloat(p.position_value?.toString() ?? '0'), 0);
    const longValue = longPositions.reduce((sum, p) => sum + parseFloat(p.position_value?.toString() ?? '0'), 0);
    const shortValue = shortPositions.reduce((sum, p) => sum + parseFloat(p.position_value?.toString() ?? '0'), 0);

    // Calculate performance metrics for the period
    const performanceMetrics = this.calculatePerformanceMetrics(aggregatedPnl, totalRealizedPnl, winningTrades, losingTrades);

    // Average position size for the period
    const avgPositionSize = totalTrades > 0 ? totalVolume / totalTrades : 0;

    // Current unrealized P&L (current, not affected by period)
    const totalUnrealizedPnl = positions.reduce((sum, p) => sum + parseFloat(p.unrealized_pnl?.toString() ?? '0'), 0);

    // Chart data for the selected period (include unrealized PnL from current positions)
    const chartData = this.generateChartDataForPeriod(aggregatedPnl, period, totalUnrealizedPnl);

    return {
      address: normalizedAddress,
      period,
      summary: {
        totalTrades,
        totalVolume,
        totalRealizedPnl,
        totalUnrealizedPnl,
        totalFees,
        winRate,
        avgPositionSize,
      },
      pnl: {
        pnl1h: pnl1h,
        pnl24h: pnl24h,
        pnl7d: pnl7d,
        pnl30d: pnl30d,
      },
      positionDistribution: {
        longCount: longPositions.length,
        shortCount: shortPositions.length,
        longValue,
        shortValue,
        longPercent: totalPositionValue > 0 ? (longValue / totalPositionValue) * 100 : 50,
        shortPercent: totalPositionValue > 0 ? (shortValue / totalPositionValue) * 100 : 50,
      },
      tradingStats,
      performanceMetrics,
      chartData,
      wallet: wallet
        ? {
            totalDeposit: parseFloat(wallet.total_deposit?.toString() ?? '0'),
            totalWithdraw: parseFloat(wallet.total_withdraw?.toString() ?? '0'),
            netDeposit: parseFloat(wallet.total_deposit?.toString() ?? '0') - parseFloat(wallet.total_withdraw?.toString() ?? '0'),
          }
        : null,
    };
  }

  private calculatePeriodMetrics(tradesWithPnl: any[], allFills: any[], sinceTimestamp: number) {
    const periodTrades = tradesWithPnl.filter((t) => Number(t.fill_timestamp) >= sinceTimestamp);
    const periodFills = allFills.filter((f) => Number(f.fill_timestamp) >= sinceTimestamp);

    const pnl = periodTrades.reduce((sum, t) => sum + parseFloat(t.total_pnl?.toString() ?? '0'), 0);
    const volume = periodFills.reduce((sum, f) => {
      return sum + parseFloat(f.total_value?.toString() ?? '0');
    }, 0);
    const tradesCount = periodFills.reduce((sum, f) => sum + (f.fill_count ?? 1), 0);

    return {
      amount: pnl,
      volume: volume,
      trades: tradesCount,
      percentage: volume > 0 ? (pnl / volume) * 100 : 0,
    };
  }

  /**
   * Get aggregation key based on period using dayjs wrapper (UTC consistently)
   * - 1h, 24h: hourly aggregation
   * - 7d, 30d, 3m, 1y, all: daily aggregation
   */
  private getAggregationKey(timestamp: number, period: string): string {
    if (period === '1h' || period === '24h') {
      return getHourlyKey(timestamp);
    } else {
      // Daily aggregation for all other periods (7d, 30d, 3m, 1y, all)
      return getDailyKey(timestamp);
    }
  }

  private calculateAggregatedPnl(trades: any[], fills: any[], period: string): Map<string, { pnl: number; volume: number; trades: number }> {
    const aggregatedPnl = new Map<string, { pnl: number; volume: number; trades: number }>();

    for (const trade of trades) {
      const timestamp = Number(trade.fill_timestamp);
      const key = this.getAggregationKey(timestamp, period);

      const current = aggregatedPnl.get(key) || { pnl: 0, volume: 0, trades: 0 };
      const pnl = parseFloat(trade.total_pnl?.toString() ?? '0');
      const volume = parseFloat(trade.total_value?.toString() ?? '0');

      aggregatedPnl.set(key, {
        pnl: current.pnl + pnl,
        volume: current.volume + volume,
        trades: current.trades + (trade.fill_count ?? 1),
      });
    }

    // Add volume from fills without P&L
    for (const fill of fills) {
      if (fill.total_pnl !== null) continue; // Already counted

      const timestamp = Number(fill.fill_timestamp);
      const key = this.getAggregationKey(timestamp, period);

      const current = aggregatedPnl.get(key) || { pnl: 0, volume: 0, trades: 0 };
      const volume = parseFloat(fill.total_value?.toString() ?? '0');

      aggregatedPnl.set(key, {
        pnl: current.pnl,
        volume: current.volume + volume,
        trades: current.trades + (fill.fill_count ?? 1),
      });
    }

    return aggregatedPnl;
  }

  private generateChartDataForPeriod(
    aggregatedPnl: Map<string, { pnl: number; volume: number; trades: number }>,
    period: string,
    unrealizedPnl: number = 0,
  ) {
    const chartData: Array<{
      date: string;
      time: string;
      pnl: number;
      cumulativePnl: number;
      totalPnl: number; // Cumulative realized + unrealized (only meaningful for last point)
      volume: number;
      trades: number;
    }> = [];

    // Sort keys chronologically
    const sortedKeys = Array.from(aggregatedPnl.keys()).sort();

    // Calculate cumulative P&L
    let cumulativePnl = 0;

    for (let i = 0; i < sortedKeys.length; i++) {
      const key = sortedKeys[i];
      const data = aggregatedPnl.get(key)!;
      cumulativePnl += data.pnl;
      const isLastPoint = i === sortedKeys.length - 1;

      // Format time label based on period using dayjs
      let timeLabel: string;
      if (period === '1h' || period === '24h') {
        // For hourly data (YYYY-MM-DD HH:00), show hour
        const parts = key.split(' ');
        timeLabel = parts[1] || key;
      } else {
        // For daily data (YYYY-MM-DD), parse and format using dayjs UTC
        const d = utcFromString(key);
        timeLabel = d.format('MMM D');
      }

      // Total P&L = cumulative realized + unrealized (only add unrealized to last point)
      const totalPnl = isLastPoint ? cumulativePnl + unrealizedPnl : cumulativePnl;

      chartData.push({
        date: key,
        time: timeLabel,
        pnl: data.pnl,
        cumulativePnl,
        totalPnl,
        volume: data.volume,
        trades: data.trades,
      });
    }

    // If we have unrealized PnL but no chart data, add a "Now" point
    if (chartData.length === 0 && unrealizedPnl !== 0) {
      const now = new Date();
      const timeLabel = period === '1h' || period === '24h' ? now.toISOString().slice(11, 16) : now.toISOString().slice(5, 10).replace('-', '/');
      chartData.push({
        date: now.toISOString().split('T')[0],
        time: 'Now',
        pnl: 0,
        cumulativePnl: 0,
        totalPnl: unrealizedPnl,
        volume: 0,
        trades: 0,
      });
    }

    return chartData;
  }

  private calculatePnlForPeriod(trades: any[], sinceTimestamp: number): number {
    return trades.filter((t) => Number(t.fill_timestamp) >= sinceTimestamp).reduce((sum, t) => sum + parseFloat(t.total_pnl?.toString() ?? '0'), 0);
  }

  private calculatePnlPercentage(pnl: number, volume: number): number {
    if (volume === 0) return 0;
    return (pnl / volume) * 100;
  }

  private calculateDailyPnl(trades: any[]): Map<string, { pnl: number; volume: number; trades: number }> {
    const dailyPnl = new Map<string, { pnl: number; volume: number; trades: number }>();

    for (const trade of trades) {
      const date = new Date(Number(trade.fill_timestamp)).toISOString().split('T')[0];
      const current = dailyPnl.get(date) || { pnl: 0, volume: 0, trades: 0 };
      const pnl = parseFloat(trade.total_pnl?.toString() ?? '0');
      const volume = parseFloat(trade.total_value?.toString() ?? '0');

      dailyPnl.set(date, {
        pnl: current.pnl + pnl,
        volume: current.volume + volume,
        trades: current.trades + (trade.fill_count ?? 1),
      });
    }

    return dailyPnl;
  }

  private calculateTradingStats(trades: any[], dailyPnl: Map<string, { pnl: number; volume: number; trades: number }>) {
    // Find best and worst days
    let bestDay = { date: '', amount: 0 };
    let worstDay = { date: '', amount: 0 };
    let profitableDays = 0;
    let totalDays = 0;

    for (const [date, data] of dailyPnl) {
      totalDays++;
      if (data.pnl > 0) profitableDays++;
      if (data.pnl > bestDay.amount) {
        bestDay = { date, amount: data.pnl };
      }
      if (data.pnl < worstDay.amount) {
        worstDay = { date, amount: data.pnl };
      }
    }

    // Calculate win/loss streaks (using grouped fill total_pnl)
    let currentWinStreak = 0;
    let currentLossStreak = 0;
    let maxWinStreak = 0;
    let maxLossStreak = 0;

    for (const trade of trades) {
      const pnl = parseFloat(trade.total_pnl?.toString() ?? '0');
      if (pnl > 0) {
        currentWinStreak++;
        currentLossStreak = 0;
        maxWinStreak = Math.max(maxWinStreak, currentWinStreak);
      } else if (pnl < 0) {
        currentLossStreak++;
        currentWinStreak = 0;
        maxLossStreak = Math.max(maxLossStreak, currentLossStreak);
      }
    }

    // Find largest win and loss
    let largestWin = { amount: 0, coin: '' };
    let largestLoss = { amount: 0, coin: '' };

    for (const trade of trades) {
      const pnl = parseFloat(trade.total_pnl?.toString() ?? '0');
      if (pnl > largestWin.amount) {
        largestWin = { amount: pnl, coin: trade.coin };
      }
      if (pnl < largestLoss.amount) {
        largestLoss = { amount: pnl, coin: trade.coin };
      }
    }

    // Average daily P&L
    const avgDailyPnl = totalDays > 0 ? Array.from(dailyPnl.values()).reduce((sum, d) => sum + d.pnl, 0) / totalDays : 0;

    return {
      bestDay: { date: bestDay.date, amount: bestDay.amount },
      worstDay: { date: worstDay.date, amount: worstDay.amount },
      winStreak: maxWinStreak,
      lossStreak: maxLossStreak,
      profitableDays,
      totalDays,
      avgDailyPnl,
      largestWin,
      largestLoss,
    };
  }

  private calculatePerformanceMetrics(
    dailyPnl: Map<string, { pnl: number; volume: number; trades: number }>,
    totalRealizedPnl: number,
    winningTrades: any[],
    losingTrades: any[],
  ) {
    const dailyReturns = Array.from(dailyPnl.values()).map((d) => d.pnl);

    // Calculate Sharpe Ratio (simplified - assumes risk-free rate of 0)
    const avgReturn = dailyReturns.length > 0 ? dailyReturns.reduce((a, b) => a + b, 0) / dailyReturns.length : 0;
    const variance = dailyReturns.length > 1 ? dailyReturns.reduce((sum, r) => sum + Math.pow(r - avgReturn, 2), 0) / (dailyReturns.length - 1) : 0;
    const stdDev = Math.sqrt(variance);
    const sharpeRatio = stdDev > 0 ? (avgReturn / stdDev) * Math.sqrt(252) : 0; // Annualized

    // Calculate Sortino Ratio (uses only negative returns for downside deviation)
    const negativeReturns = dailyReturns.filter((r) => r < 0);
    const downsideVariance = negativeReturns.length > 1 ? negativeReturns.reduce((sum, r) => sum + Math.pow(r, 2), 0) / negativeReturns.length : 0;
    const downsideStdDev = Math.sqrt(downsideVariance);
    const sortinoRatio = downsideStdDev > 0 ? (avgReturn / downsideStdDev) * Math.sqrt(252) : 0;

    // Calculate Profit Factor (gross profit / gross loss)
    const grossProfit = winningTrades.reduce((sum, t) => sum + parseFloat(t.total_pnl?.toString() ?? '0'), 0);
    const grossLoss = Math.abs(losingTrades.reduce((sum, t) => sum + parseFloat(t.total_pnl?.toString() ?? '0'), 0));
    const profitFactor = grossLoss > 0 ? grossProfit / grossLoss : grossProfit > 0 ? Infinity : 0;

    // Calculate Max Drawdown
    let maxDrawdown = 0;
    let peak = 0;
    let cumPnl = 0;

    for (const dailyReturn of dailyReturns) {
      cumPnl += dailyReturn;
      if (cumPnl > peak) peak = cumPnl;
      const drawdown = peak - cumPnl;
      if (drawdown > maxDrawdown) maxDrawdown = drawdown;
    }

    const maxDrawdownPercent = peak > 0 ? (maxDrawdown / peak) * 100 : 0;

    return {
      sharpeRatio: isFinite(sharpeRatio) ? sharpeRatio : 0,
      sortinoRatio: isFinite(sortinoRatio) ? sortinoRatio : 0,
      profitFactor: isFinite(profitFactor) ? profitFactor : 0,
      maxDrawdown,
      maxDrawdownPercent,
      grossProfit,
      grossLoss,
    };
  }

  private generateChartData(dailyPnl: Map<string, { pnl: number; volume: number; trades: number }>, fills: any[]) {
    // Generate last 30 days of data
    const chartData: Array<{
      date: string;
      time: string;
      pnl: number;
      cumulativePnl: number;
      volume: number;
      trades: number;
    }> = [];

    // Sort dates
    const sortedDates = Array.from(dailyPnl.keys()).sort();

    // Calculate cumulative P&L
    let cumulativePnl = 0;

    for (const date of sortedDates) {
      const data = dailyPnl.get(date)!;
      cumulativePnl += data.pnl;

      chartData.push({
        date,
        time: new Date(date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
        pnl: data.pnl,
        cumulativePnl,
        volume: data.volume,
        trades: data.trades,
      });
    }

    // Return last 30 days
    return chartData.slice(-30);
  }
}
