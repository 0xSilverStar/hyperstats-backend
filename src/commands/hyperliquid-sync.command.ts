import { Command, CommandRunner, Option } from 'nest-commander';
import { ArbitrumSyncService } from '../services/arbitrum-sync.service';
import { ConsoleLogger } from '../common/utils/console-logger';

interface HyperliquidSyncOptions {
  continuous?: boolean;
  interval?: number;
}

@Command({
  name: 'hyperliquid:sync',
  description: 'Sync HyperLiquid Bridge transactions from Arbitrum Network',
})
export class HyperliquidSyncCommand extends CommandRunner {
  private readonly console = new ConsoleLogger('HyperLiquid');

  constructor(private readonly syncService: ArbitrumSyncService) {
    super();
  }

  async run(_passedParams: string[], options?: HyperliquidSyncOptions): Promise<void> {
    this.console.header('HyperLiquid Bridge Sync');
    this.console.info('Fetching transactions from Arbitrum Network');

    if (options?.continuous) {
      const interval = options.interval ?? 300;
      this.console.info(`Continuous mode enabled (interval: ${interval}s)`);
      this.console.warn('Press Ctrl+C to stop');

      while (true) {
        await this.runSync();
        this.console.info(`Waiting ${interval} seconds until next sync...`);
        await this.sleep(interval * 1000);
      }
    } else {
      await this.runSync();
    }
  }

  private async runSync(): Promise<void> {
    try {
      await this.syncService.sync();
      this.console.success('Sync completed successfully!');

      const status = await this.syncService.getSyncStatus();
      this.displayStatus(status);
    } catch (error) {
      this.console.error(`Sync failed: ${error.message}`);
    }
  }

  private displayStatus(status: any): void {
    this.console.status({
      currentBlock: status.currentArbitrumBlock ?? 0,
      highestSynced: status.highestSyncedBlock ?? 0,
      completedRanges: status.totalCompletedRanges,
      percentage: status.syncPercentage,
    });
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
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
