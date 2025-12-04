import { Command, CommandRunner, Option } from 'nest-commander';
import { PrismaService } from '../prisma/prisma.service';
import { HyperLiquidInfoService } from '../services/hyperliquid-info.service';
import { SyncLockService } from '../services/sync-lock.service';
import { Prisma } from '../../generated/prisma/client';
import { ConsoleLogger } from '../common/utils/console-logger';

interface TradingSyncOptions {
  address?: string;
  limit?: number;
  force?: boolean;
}

const SYNC_INTERVAL_SECONDS = 60; // 1 minute between full cycles

const LOCK_NAME = 'trading:sync';
const LOCK_TIMEOUT_MINUTES = 120; // 2 hours

@Command({
  name: 'trading:sync',
  description: 'Sync trading data (positions, orders, fills, balances) from HyperLiquid (runs continuously every 60 seconds)',
})
export class TradingSyncCommand extends CommandRunner {
  private readonly console = new ConsoleLogger('Trading');

  constructor(
    private readonly prisma: PrismaService,
    private readonly hlService: HyperLiquidInfoService,
    private readonly lockService: SyncLockService,
  ) {
    super();
  }

  async run(_passedParams: string[], options?: TradingSyncOptions): Promise<void> {
    this.console.header('Trading Data Sync');

    // Check and display lock status
    const lockInfo = await this.lockService.getLockInfo(LOCK_NAME);
    if (lockInfo?.isLocked) {
      this.console.warn(`Sync is currently running by: ${lockInfo.lockedBy}`);
      this.console.warn(`Started at: ${lockInfo.lockedAt?.toISOString()}`);
      this.console.warn(`Expires at: ${lockInfo.expiresAt?.toISOString()}`);

      if (!options?.force) {
        this.console.error('Another sync process is running. Use --force to override (not recommended).');
        return;
      }

      this.console.warn('Force mode enabled - releasing existing lock...');
      await this.lockService.forceReleaseLock(LOCK_NAME);
    }

    // Try to acquire lock
    const lockAcquired = await this.lockService.acquireLock(LOCK_NAME, LOCK_TIMEOUT_MINUTES);
    if (!lockAcquired) {
      this.console.error('Failed to acquire sync lock. Another process may have started.');
      return;
    }

    this.console.info('Lock acquired successfully');
    this.console.info(`Running continuously (interval: ${SYNC_INTERVAL_SECONDS}s)`);
    this.console.warn('Press Ctrl+C to stop');

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
      this.console.warn('Received SIGINT, releasing lock...');
      await this.lockService.releaseLock(LOCK_NAME);
      process.exit(0);
    });

    process.on('SIGTERM', async () => {
      this.console.warn('Received SIGTERM, releasing lock...');
      await this.lockService.releaseLock(LOCK_NAME);
      process.exit(0);
    });

    // Run continuously
    while (true) {
      await this.syncWallets(options);

      // Extend lock before sleeping
      await this.lockService.extendLock(LOCK_NAME, LOCK_TIMEOUT_MINUTES);
      this.console.info(`Waiting ${SYNC_INTERVAL_SECONDS} seconds until next sync...`);
      await this.sleep(SYNC_INTERVAL_SECONDS * 1000);
    }
  }

  private async syncWallets(options?: TradingSyncOptions): Promise<void> {
    const startTime = Date.now();
    this.console.subHeader('Syncing from HyperLiquid API');

    try {
      const addresses = await this.getWalletAddresses(options);

      if (addresses.length === 0) {
        this.console.warn('No wallet addresses found to sync');
        return;
      }

      this.console.info(`Found ${addresses.length} wallet(s) to sync`);

      let successCount = 0;
      let failCount = 0;
      let totalPositions = 0;
      let totalOrders = 0;
      let totalFills = 0;

      for (let i = 0; i < addresses.length; i++) {
        const address = addresses[i];
        try {
          const result = await this.syncWalletData(address);
          successCount++;
          totalPositions += result.positions;
          totalOrders += result.orders;
          totalFills += result.fills;

          this.console.success(
            `[${i + 1}/${addresses.length}] ${address}... | ` + `Position: ${result.positions} | Orders: ${result.orders} | Fills: ${result.fills}`,
          );
        } catch (error) {
          this.console.error(`[${i + 1}/${addresses.length}] ${address}... failed: ${error.message}`);
          failCount++;
        }

        // 2 second delay (proxy rotation handles rate limiting)
        await this.sleep(2000);
      }

      const duration = Date.now() - startTime;

      this.console.syncSummary({
        totalIterations: addresses.length,
        totalTransactions: totalPositions + totalOrders + totalFills,
        totalWhales: successCount,
        duration,
      });

      await this.displaySummary();
    } catch (error) {
      this.console.error(`Sync failed: ${error.message}`);
    }
  }

  private async getWalletAddresses(options?: TradingSyncOptions): Promise<string[]> {
    if (options?.address) {
      return [options.address];
    }

    const wallets = await this.prisma.wallet.findMany({
      orderBy: { total_deposit: 'desc' },
      ...(options?.limit && { take: options.limit }),
      select: { address: true },
    });

    return wallets.map((w) => w.address);
  }

  private async syncWalletData(address: string): Promise<{ positions: number; orders: number; fills: number }> {
    const [positions, orders, fills, balances] = await Promise.all([
      this.hlService.getUserPositions(address),
      this.hlService.getUserOpenOrders(address),
      this.hlService.getUserFills(address),
      this.hlService.getUserSpotBalances(address),
    ]);

    const [posCount, orderCount, fillCount] = await Promise.all([
      this.syncPositions(address, positions),
      this.syncOrders(address, orders),
      this.syncFills(address, fills.slice(0, 100)),
      this.syncBalances(address, balances),
    ]);

    return { positions: posCount, orders: orderCount, fills: fillCount };
  }

  private async syncPositions(address: string, data: any): Promise<number> {
    const assetPositions = data.assetPositions ?? [];

    // Delete all existing positions for this wallet
    await this.prisma.position.deleteMany({
      where: { wallet_address: address },
    });

    let count = 0;

    // Insert new positions
    for (const assetPosition of assetPositions) {
      const position = assetPosition.position;

      if (!position) continue;

      const szi = parseFloat(position.szi ?? '0');

      if (Math.abs(szi) < 0.00001) continue;

      const leverage = position.leverage;
      const leverageValue = Math.round(typeof leverage === 'object' ? parseFloat(leverage?.value ?? '1') : parseFloat(leverage ?? '1'));

      await this.prisma.position.create({
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
      count++;
    }

    return count;
  }

  private async syncOrders(address: string, orders: any[]): Promise<number> {
    // Get current order IDs from the new data
    const newOrderIds = orders.map((o) => BigInt(o.oid ?? 0));

    // Delete orders for this wallet that are no longer in the new data
    await this.prisma.order.deleteMany({
      where: {
        wallet_address: address,
        order_id: { notIn: newOrderIds },
      },
    });

    // Upsert each order
    for (const order of orders) {
      const side = (order.side ?? 'B') === 'B' ? 'buy' : 'sell';
      const orderId = BigInt(order.oid ?? 0);

      await this.prisma.order.upsert({
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

    return orders.length;
  }

  private async syncFills(address: string, fills: any[]): Promise<number> {
    // Group fills by tx_hash
    const groupedMap = new Map<string, any[]>();
    for (const fill of fills) {
      const hash = fill.hash ?? '';
      const existing = groupedMap.get(hash) || [];
      existing.push(fill);
      groupedMap.set(hash, existing);
    }

    let count = 0;

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

      await this.prisma.fill.upsert({
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
      count++;
    }

    return count;
  }

  private async syncBalances(address: string, data: any): Promise<number> {
    const balances = data.balances ?? [];

    // Delete all existing balances for this wallet
    await this.prisma.balance.deleteMany({
      where: { wallet_address: address },
    });

    let count = 0;

    // Insert new balances
    for (const balance of balances) {
      const total = parseFloat(balance.total ?? '0');

      if (total < 0.00001) continue;

      const hold = parseFloat(balance.hold ?? '0');
      const available = total - hold;

      await this.prisma.balance.create({
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
      count++;
    }

    return count;
  }

  private async displaySummary(): Promise<void> {
    const [totalPositions, totalOrders, totalFills, totalBalances, longPositions, shortPositions] = await Promise.all([
      this.prisma.position.count(),
      this.prisma.order.count(),
      this.prisma.fill.count(),
      this.prisma.balance.count(),
      this.prisma.position.count({ where: { side: 'long' } }),
      this.prisma.position.count({ where: { side: 'short' } }),
    ]);

    // Use colored table format
    const colors = {
      cyan: '\x1b[36m',
      green: '\x1b[32m',
      yellow: '\x1b[33m',
      blue: '\x1b[34m',
      reset: '\x1b[0m',
      bright: '\x1b[1m',
    };

    console.log(`\n${colors.cyan}┌─────────────────────────────────────────┐${colors.reset}`);
    console.log(`${colors.cyan}│${colors.reset} ${colors.bright}Trading Data Summary${colors.reset}                    ${colors.cyan}│${colors.reset}`);
    console.log(`${colors.cyan}├─────────────────────────────────────────┤${colors.reset}`);
    console.log(
      `${colors.cyan}│${colors.reset}  Positions:    ${colors.green}${String(totalPositions).padStart(22)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(
      `${colors.cyan}│${colors.reset}    └ Long:     ${colors.green}${String(longPositions).padStart(22)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(
      `${colors.cyan}│${colors.reset}    └ Short:    ${colors.yellow}${String(shortPositions).padStart(22)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(
      `${colors.cyan}│${colors.reset}  Open Orders:  ${colors.blue}${String(totalOrders).padStart(22)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(`${colors.cyan}│${colors.reset}  Fills:        ${colors.blue}${String(totalFills).padStart(22)}${colors.reset} ${colors.cyan}│${colors.reset}`);
    console.log(
      `${colors.cyan}│${colors.reset}  Balances:     ${colors.blue}${String(totalBalances).padStart(22)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(`${colors.cyan}└─────────────────────────────────────────┘${colors.reset}\n`);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  @Option({
    flags: '-a, --address <address>',
    description: 'Specific wallet address to sync',
  })
  parseAddress(val: string): string {
    return val;
  }

  @Option({
    flags: '-l, --limit <limit>',
    description: 'Number of wallets to sync (default: all)',
  })
  parseLimit(val: string): number {
    return parseInt(val, 10);
  }

  @Option({
    flags: '-f, --force',
    description: 'Force start even if another sync is running (releases existing lock)',
  })
  parseForce(): boolean {
    return true;
  }
}
