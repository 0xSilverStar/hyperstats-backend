import { Command, CommandRunner, Option } from 'nest-commander';
import { PrismaService } from '../prisma/prisma.service';
import { HyperLiquidInfoService } from '../services/hyperliquid-info.service';
import { Prisma } from '../../generated/prisma/client';
import { ConsoleLogger } from '../common/utils/console-logger';

interface TradingSyncOptions {
  address?: string;
  limit?: number;
  continuous?: boolean;
  interval?: number;
}

@Command({
  name: 'trading:sync',
  description: 'Sync trading data (positions, orders, fills, balances) from HyperLiquid',
})
export class TradingSyncCommand extends CommandRunner {
  private readonly console = new ConsoleLogger('Trading');

  constructor(
    private readonly prisma: PrismaService,
    private readonly hlService: HyperLiquidInfoService,
  ) {
    super();
  }

  async run(_passedParams: string[], options?: TradingSyncOptions): Promise<void> {
    this.console.header('Trading Data Sync');

    if (options?.continuous) {
      const interval = options.interval ?? 60;
      this.console.info(`Continuous mode enabled (interval: ${interval}s)`);
      this.console.warn('Press Ctrl+C to stop');

      while (true) {
        await this.syncWallets(options);
        this.console.info(`Waiting ${interval} seconds until next sync...`);
        await this.sleep(interval * 1000);
      }
    } else {
      await this.syncWallets(options);
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
            `[${i + 1}/${addresses.length}] ${address.slice(0, 10)}... | ` + `Pos: ${result.positions} | Orders: ${result.orders} | Fills: ${result.fills}`,
          );
        } catch (error) {
          this.console.error(`[${i + 1}/${addresses.length}] ${address.slice(0, 10)}... failed: ${error.message}`);
          failCount++;
        }

        // Small delay to avoid rate limiting
        await this.sleep(100);
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

    const limit = options?.limit ?? 10;

    const wallets = await this.prisma.wallet.findMany({
      orderBy: { total_deposit: 'desc' },
      take: limit,
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
    let count = 0;

    for (const fill of fills) {
      const side = (fill.side ?? 'B') === 'B' ? 'buy' : 'sell';

      await this.prisma.fill.upsert({
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
    description: 'Number of wallets to sync (default: 10)',
  })
  parseLimit(val: string): number {
    return parseInt(val, 10);
  }

  @Option({
    flags: '-c, --continuous',
    description: 'Run continuously',
  })
  parseContinuous(): boolean {
    return true;
  }

  @Option({
    flags: '-i, --interval <interval>',
    description: 'Sync interval in seconds for continuous mode',
  })
  parseInterval(val: string): number {
    return parseInt(val, 10);
  }
}
