import { Command, CommandRunner, Option } from 'nest-commander';
import { PrismaService } from '../prisma/prisma.service';
import { HyperLiquidInfoService } from '../services/hyperliquid-info.service';
import { ConsoleLogger } from '../common/utils/console-logger';

interface PairsSyncOptions {
  force?: boolean;
}

@Command({
  name: 'pairs:sync',
  description: 'Sync trading pairs metadata from HyperLiquid',
})
export class PairsSyncCommand extends CommandRunner {
  private readonly console = new ConsoleLogger('Pairs');

  constructor(
    private readonly prisma: PrismaService,
    private readonly hlService: HyperLiquidInfoService,
  ) {
    super();
  }

  async run(_passedParams: string[], _options?: PairsSyncOptions): Promise<void> {
    this.console.header('Trading Pairs Sync');

    try {
      const perpResult = await this.syncPerpetualPairs();
      const spotResult = await this.syncSpotPairs();

      this.console.syncSummary({
        totalIterations: 2,
        totalTransactions: perpResult.total + spotResult.total,
        totalWhales: perpResult.new + spotResult.new,
        duration: 0,
      });

      await this.displaySummary();
    } catch (error) {
      this.console.error(`Failed to sync trading pairs: ${error.message}`);
    }
  }

  private async syncPerpetualPairs(): Promise<{ new: number; updated: number; total: number }> {
    this.console.subHeader('Perpetual Pairs');

    const meta = await this.hlService.getMeta();

    if (!meta.universe || !Array.isArray(meta.universe)) {
      throw new Error('Invalid meta response: universe not found');
    }

    const universe = meta.universe;
    let syncedCount = 0;
    let updatedCount = 0;

    for (const asset of universe) {
      const data = {
        symbol: asset.name,
        sz_decimals: asset.szDecimals,
        max_leverage: asset.maxLeverage,
        margin_table_id: asset.marginTableId ?? 0,
        only_isolated: asset.onlyIsolated ?? false,
        is_delisted: asset.isDelisted ?? false,
        margin_mode: asset.marginMode ?? null,
        pair_type: 'perp',
      };

      const existing = await this.prisma.tradingPair.findUnique({
        where: { symbol: data.symbol },
      });

      if (existing) {
        await this.prisma.tradingPair.update({
          where: { symbol: data.symbol },
          data,
        });
        updatedCount++;
      } else {
        await this.prisma.tradingPair.create({ data });
        syncedCount++;
      }
    }

    this.console.success(`Perpetual: ${syncedCount} new, ${updatedCount} updated (${universe.length} total)`);

    return { new: syncedCount, updated: updatedCount, total: universe.length };
  }

  private async syncSpotPairs(): Promise<{ new: number; updated: number; total: number }> {
    this.console.subHeader('Spot Pairs');

    try {
      const spotMeta = await this.hlService.getSpotMeta();

      if (!spotMeta.tokens || !Array.isArray(spotMeta.tokens)) {
        this.console.warn('No spot tokens found, skipping spot sync');
        return { new: 0, updated: 0, total: 0 };
      }

      const tokens = spotMeta.tokens;
      let syncedCount = 0;
      let updatedCount = 0;

      for (const token of tokens) {
        const data = {
          symbol: token.name,
          sz_decimals: token.szDecimals ?? 8,
          max_leverage: 1,
          margin_table_id: 0,
          only_isolated: false,
          is_delisted: false,
          margin_mode: null,
          pair_type: 'spot',
        };

        const existing = await this.prisma.tradingPair.findUnique({
          where: { symbol: data.symbol },
        });

        if (existing) {
          await this.prisma.tradingPair.update({
            where: { symbol: data.symbol },
            data,
          });
          updatedCount++;
        } else {
          await this.prisma.tradingPair.create({ data });
          syncedCount++;
        }
      }

      this.console.success(`Spot: ${syncedCount} new, ${updatedCount} updated (${tokens.length} total)`);

      return { new: syncedCount, updated: updatedCount, total: tokens.length };
    } catch (error) {
      this.console.warn(`Failed to sync spot pairs: ${error.message}`);
      return { new: 0, updated: 0, total: 0 };
    }
  }

  private async displaySummary(): Promise<void> {
    const [totalPairs, perpPairs, spotPairs, activePairs, delistedPairs] = await Promise.all([
      this.prisma.tradingPair.count(),
      this.prisma.tradingPair.count({ where: { pair_type: 'perp' } }),
      this.prisma.tradingPair.count({ where: { pair_type: 'spot' } }),
      this.prisma.tradingPair.count({ where: { is_delisted: false } }),
      this.prisma.tradingPair.count({ where: { is_delisted: true } }),
    ]);

    const colors = {
      cyan: '\x1b[36m',
      green: '\x1b[32m',
      yellow: '\x1b[33m',
      blue: '\x1b[34m',
      magenta: '\x1b[35m',
      reset: '\x1b[0m',
      bright: '\x1b[1m',
      dim: '\x1b[2m',
    };

    console.log(`\n${colors.cyan}┌─────────────────────────────────────────┐${colors.reset}`);
    console.log(`${colors.cyan}│${colors.reset} ${colors.bright}Trading Pairs Summary${colors.reset}                   ${colors.cyan}│${colors.reset}`);
    console.log(`${colors.cyan}├─────────────────────────────────────────┤${colors.reset}`);
    console.log(
      `${colors.cyan}│${colors.reset}  Total Pairs:   ${colors.bright}${String(totalPairs).padStart(21)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(`${colors.cyan}│${colors.reset}  Perpetual:     ${colors.blue}${String(perpPairs).padStart(21)}${colors.reset} ${colors.cyan}│${colors.reset}`);
    console.log(
      `${colors.cyan}│${colors.reset}  Spot:          ${colors.magenta}${String(spotPairs).padStart(21)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(
      `${colors.cyan}│${colors.reset}  Active:        ${colors.green}${String(activePairs).padStart(21)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(
      `${colors.cyan}│${colors.reset}  Delisted:      ${colors.dim}${String(delistedPairs).padStart(21)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(`${colors.cyan}└─────────────────────────────────────────┘${colors.reset}`);

    const topPairs = await this.prisma.tradingPair.findMany({
      where: { is_delisted: false, pair_type: 'perp' },
      orderBy: { max_leverage: 'desc' },
      take: 10,
      select: { symbol: true, max_leverage: true },
    });

    console.log(`\n${colors.cyan}Top 10 Perpetual Pairs by Leverage:${colors.reset}`);
    console.log(`${colors.dim}${'─'.repeat(35)}${colors.reset}`);

    for (const pair of topPairs) {
      const leverageBar = '█'.repeat(Math.min(pair.max_leverage / 10, 20));
      console.log(
        `  ${colors.yellow}${pair.symbol.padEnd(12)}${colors.reset} ` +
          `${colors.green}${leverageBar}${colors.reset} ` +
          `${colors.bright}${pair.max_leverage}x${colors.reset}`,
      );
    }
    console.log('');
  }

  @Option({
    flags: '-f, --force',
    description: 'Force re-sync all pairs',
  })
  parseForce(): boolean {
    return true;
  }
}
