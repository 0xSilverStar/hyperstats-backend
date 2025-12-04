import { Command, CommandRunner, Option } from 'nest-commander';
import { ArbitrumSyncService } from '../services/arbitrum-sync.service';
import { SyncLockService } from '../services/sync-lock.service';
import { ConsoleLogger } from '../common/utils/console-logger';

interface HyperliquidSyncOptions {
  force?: boolean;
}

const LOCK_NAME = 'hyperliquid:sync';
const LOCK_TIMEOUT_MINUTES = 120; // 2 hours
const SYNC_INTERVAL_SECONDS = 300; // 5 minutes

@Command({
  name: 'hyperliquid:sync',
  description: 'Sync HyperLiquid Bridge transactions from Arbitrum Network (runs continuously every 5 minutes)',
})
export class HyperliquidSyncCommand extends CommandRunner {
  private readonly console = new ConsoleLogger('HyperLiquid');
  private lastMergeDate: string | null = null;

  constructor(
    private readonly syncService: ArbitrumSyncService,
    private readonly lockService: SyncLockService,
  ) {
    super();
  }

  async run(_passedParams: string[], options?: HyperliquidSyncOptions): Promise<void> {
    this.console.header('HyperLiquid Bridge Sync');
    this.console.info(`Running continuously (interval: ${SYNC_INTERVAL_SECONDS}s)`);

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
      await this.runSync();

      // Run daily cleanup (merge adjacent ranges)
      await this.runDailyCleanup();

      // Extend lock before sleeping
      await this.lockService.extendLock(LOCK_NAME, LOCK_TIMEOUT_MINUTES);
      this.console.info(`Waiting ${SYNC_INTERVAL_SECONDS} seconds until next sync...`);
      await this.sleep(SYNC_INTERVAL_SECONDS * 1000);
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

  /**
   * Run daily cleanup to merge adjacent block ranges
   * Only runs once per day
   */
  private async runDailyCleanup(): Promise<void> {
    const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD

    if (this.lastMergeDate === today) {
      return; // Already ran today
    }

    this.console.info('Running daily cleanup (merging adjacent ranges)...');

    try {
      const result = await this.syncService.mergeAdjacentRanges();
      if (result.deletedCount > 0) {
        this.console.success(`Daily cleanup: merged ${result.mergedCount} ranges, removed ${result.deletedCount} records`);
      }
      this.lastMergeDate = today;
    } catch (error) {
      this.console.error(`Daily cleanup failed: ${error.message}`);
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
    flags: '-f, --force',
    description: 'Force start even if another sync is running (releases existing lock)',
  })
  parseForce(): boolean {
    return true;
  }
}
