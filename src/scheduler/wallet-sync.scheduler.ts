import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { InjectQueue } from '@nestjs/bull';
import type { Queue } from 'bull';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class WalletSyncScheduler implements OnModuleInit {
  private readonly logger = new Logger(WalletSyncScheduler.name);
  private isSyncing = false;
  private syncAborted = false;

  constructor(
    private readonly prisma: PrismaService,
    @InjectQueue('trading-data') private readonly tradingDataQueue: Queue,
  ) {}

  async onModuleInit() {
    this.logger.log('WalletSyncScheduler initialized');
  }

  /**
   * Daily cron job that syncs all wallets
   * Runs at midnight (00:00) every day
   * Iterates through all wallets with 1-second interval between each
   */
  @Cron(CronExpression.EVERY_DAY_AT_MIDNIGHT)
  async handleDailySync() {
    if (this.isSyncing) {
      this.logger.warn('Daily sync already in progress, skipping...');
      return;
    }

    this.isSyncing = true;
    this.syncAborted = false;
    this.logger.log('Starting daily wallet sync...');

    try {
      const startTime = Date.now();
      const wallets = await this.prisma.wallet.findMany({
        select: { address: true },
        orderBy: { last_activity_at: 'asc' }, // Sync oldest first
      });

      const totalWallets = wallets.length;
      this.logger.log(`Found ${totalWallets} wallets to sync`);

      let successCount = 0;
      let errorCount = 0;

      for (let i = 0; i < wallets.length; i++) {
        if (this.syncAborted) {
          this.logger.warn('Daily sync aborted');
          break;
        }

        const wallet = wallets[i];

        try {
          // Add to queue for processing
          await this.tradingDataQueue.add(
            'full-sync',
            { address: wallet.address },
            {
              attempts: 3,
              backoff: {
                type: 'exponential',
                delay: 2000,
              },
              removeOnComplete: true,
              removeOnFail: false,
            },
          );

          successCount++;

          // Log progress every 100 wallets
          if ((i + 1) % 100 === 0) {
            const progress = (((i + 1) / totalWallets) * 100).toFixed(2);
            const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
            this.logger.log(`Progress: ${i + 1}/${totalWallets} (${progress}%) - Elapsed: ${elapsed} min`);
          }

          // Wait 5 seconds before next wallet to avoid rate limiting
          await this.sleep(5000);
        } catch (error) {
          errorCount++;
          this.logger.error(`Failed to queue sync for wallet ${wallet.address}: ${error.message}`);
        }
      }

      const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
      this.logger.log(`Daily sync completed: ${successCount} queued, ${errorCount} errors, ${totalTime} minutes`);
    } catch (error) {
      this.logger.error(`Daily sync failed: ${error.message}`);
    } finally {
      this.isSyncing = false;
    }
  }

  /**
   * Manual trigger for wallet sync
   * Can be called via API or CLI command
   */
  async triggerManualSync(limit?: number): Promise<{ queued: number; total: number }> {
    if (this.isSyncing) {
      throw new Error('Sync already in progress');
    }

    this.isSyncing = true;
    this.syncAborted = false;

    try {
      const wallets = await this.prisma.wallet.findMany({
        select: { address: true },
        orderBy: { last_activity_at: 'asc' },
        take: limit,
      });

      let queued = 0;
      for (const wallet of wallets) {
        if (this.syncAborted) break;

        await this.tradingDataQueue.add(
          'full-sync',
          { address: wallet.address },
          {
            attempts: 3,
            backoff: { type: 'exponential', delay: 2000 },
            removeOnComplete: true,
          },
        );
        queued++;

        // 5 second interval to avoid rate limiting
        await this.sleep(5000);
      }

      return { queued, total: wallets.length };
    } finally {
      this.isSyncing = false;
    }
  }

  /**
   * Abort the current sync operation
   */
  abortSync(): void {
    if (this.isSyncing) {
      this.syncAborted = true;
      this.logger.warn('Sync abort requested');
    }
  }

  /**
   * Get current sync status
   */
  getSyncStatus(): { isSyncing: boolean; syncAborted: boolean } {
    return {
      isSyncing: this.isSyncing,
      syncAborted: this.syncAborted,
    };
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
