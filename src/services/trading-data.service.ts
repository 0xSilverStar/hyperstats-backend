import { Injectable, Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bull';
import type { Queue } from 'bull';
import { PrismaService } from '../prisma/prisma.service';
import { HyperLiquidInfoService } from './hyperliquid-info.service';
import { WalletSyncLockService } from './wallet-sync-lock.service';
import { LedgerSyncService } from './ledger-sync.service';
import { FillSyncService } from './fill-sync.service';
import { Prisma } from '../../generated/prisma/client';

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

  // Method for full sync (used by cron job)
  async fullSync(address: string): Promise<void> {
    const normalizedAddress = address.toLowerCase();
    this.logger.debug(`Full sync for: ${normalizedAddress}`);

    try {
      await this.ensureWalletExists(normalizedAddress);

      const [positions, orders, fills, balances] = await Promise.all([
        this.hlService.getUserPositions(normalizedAddress),
        this.hlService.getUserOpenOrders(normalizedAddress),
        this.hlService.getUserFills(normalizedAddress),
        this.hlService.getUserSpotBalances(normalizedAddress),
      ]);

      // Save all data synchronously for cron job
      await Promise.all([
        this.savePositions(normalizedAddress, positions),
        this.saveOrders(normalizedAddress, orders),
        this.saveFills(normalizedAddress, fills),
        this.saveBalances(normalizedAddress, balances),
      ]);

      // Update all cache timestamps
      await Promise.all([
        this.setCacheTimestamp(normalizedAddress, 'positions'),
        this.setCacheTimestamp(normalizedAddress, 'orders'),
        this.setCacheTimestamp(normalizedAddress, 'fills'),
        this.setCacheTimestamp(normalizedAddress, 'balances'),
        this.setCacheTimestamp(normalizedAddress, 'profile'),
      ]);

      this.logger.debug(`Full sync completed for: ${normalizedAddress}`);
    } catch (error) {
      this.logger.error(`Full sync failed for ${normalizedAddress}: ${error.message}`);
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
            last_updated_at: new Date(),
          },
        });
      }
    });

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
    });

    await this.setCacheTimestamp(address, 'orders');
  }

  async saveFills(address: string, fills: any[]): Promise<void> {
    await this.prisma.$transaction(async (tx) => {
      for (const fill of fills.slice(0, 500)) {
        const side = (fill.side ?? 'B') === 'B' ? 'buy' : 'sell';

        await tx.fill.upsert({
          where: {
            tx_hash_order_id_fill_timestamp: {
              tx_hash: fill.hash ?? '',
              order_id: BigInt(fill.oid ?? 0),
              fill_timestamp: BigInt(fill.time ?? Date.now()),
            },
          },
          update: {
            wallet_address: address,
            coin: fill.coin ?? 'UNKNOWN',
            side,
            price: new Prisma.Decimal(parseFloat(fill.px ?? '0')),
            size: new Prisma.Decimal(parseFloat(fill.sz ?? '0')),
            direction: fill.dir ?? null,
            closed_pnl: fill.closedPnl ? new Prisma.Decimal(parseFloat(fill.closedPnl)) : null,
            fee: fill.fee ? new Prisma.Decimal(parseFloat(fill.fee)) : null,
            fee_token: fill.feeToken ?? 'USDC',
            start_position: fill.startPosition ? new Prisma.Decimal(parseFloat(fill.startPosition)) : null,
            crossed: fill.crossed ?? false,
            tid: fill.tid ? BigInt(fill.tid) : null,
          },
          create: {
            wallet_address: address,
            tx_hash: fill.hash ?? '',
            order_id: fill.oid ? BigInt(fill.oid) : null,
            coin: fill.coin ?? 'UNKNOWN',
            side,
            price: new Prisma.Decimal(parseFloat(fill.px ?? '0')),
            size: new Prisma.Decimal(parseFloat(fill.sz ?? '0')),
            direction: fill.dir ?? null,
            closed_pnl: fill.closedPnl ? new Prisma.Decimal(parseFloat(fill.closedPnl)) : null,
            fee: fill.fee ? new Prisma.Decimal(parseFloat(fill.fee)) : null,
            fee_token: fill.feeToken ?? 'USDC',
            start_position: fill.startPosition ? new Prisma.Decimal(parseFloat(fill.startPosition)) : null,
            crossed: fill.crossed ?? false,
            fill_timestamp: BigInt(fill.time ?? Date.now()),
            tid: fill.tid ? BigInt(fill.tid) : null,
          },
        });
      }
    });

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
    });

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
}
