import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { TradingDataService } from '../services/trading-data.service';
import { ArbitrumSyncService } from '../services/arbitrum-sync.service';
import { PositionSnapshotService } from '../services/position-snapshot.service';

@Injectable()
export class WalletSyncScheduler implements OnModuleInit {
  private readonly logger = new Logger(WalletSyncScheduler.name);
  private isWalletSyncing = false;
  private isBlockchainSyncing = false;
  private isSnapshotting = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly tradingDataService: TradingDataService,
    private readonly arbitrumSyncService: ArbitrumSyncService,
    private readonly positionSnapshotService: PositionSnapshotService,
  ) {}

  async onModuleInit() {
    this.logger.log('WalletSyncScheduler initialized');

    // Start background sync tasks after a short delay
    setTimeout(() => {
      void this.startContinuousWalletSync();
      void this.startContinuousBlockchainSync();
      void this.startContinuousPositionSnapshots();
    }, 5000);
  }

  /**
   * Continuous wallet sync - 15 seconds interval between each wallet
   */
  private async startContinuousWalletSync() {
    this.logger.log('Starting continuous wallet sync...');

    while (true) {
      if (this.isWalletSyncing) {
        await this.sleep(5000);
        continue;
      }

      this.isWalletSyncing = true;

      try {
        const wallets = await this.prisma.wallet.findMany({
          select: { address: true },
          orderBy: { last_activity_at: 'asc' },
        });

        this.logger.log(`Wallet sync cycle starting: ${wallets.length} wallets`);

        for (const wallet of wallets) {
          try {
            await this.tradingDataService.fullSync(wallet.address);
            this.logger.debug(`Synced wallet: ${wallet.address.slice(0, 10)}...`);
          } catch (error) {
            this.logger.error(`Failed to sync ${wallet.address}: ${error.message}`);
          }

          // 2 second interval between wallets (proxy rotation handles rate limiting)
          await this.sleep(2000);
        }

        this.logger.log('Wallet sync cycle completed');
      } catch (error) {
        this.logger.error(`Wallet sync error: ${error.message}`);
      } finally {
        this.isWalletSyncing = false;
      }

      // Short pause before starting next cycle
      await this.sleep(5000);
    }
  }

  /**
   * Continuous blockchain sync - 5 minute interval
   */
  private async startContinuousBlockchainSync() {
    this.logger.log('Starting continuous blockchain sync...');

    while (true) {
      if (this.isBlockchainSyncing) {
        await this.sleep(5000);
        continue;
      }

      this.isBlockchainSyncing = true;

      try {
        await this.arbitrumSyncService.sync();
        this.logger.debug('Blockchain sync completed');
      } catch (error) {
        this.logger.error(`Blockchain sync error: ${error.message}`);
      } finally {
        this.isBlockchainSyncing = false;
      }

      // 5 minute interval
      await this.sleep(300000);
    }
  }

  /**
   * Continuous position snapshots - 2 minute interval for faster detection
   */
  private async startContinuousPositionSnapshots() {
    this.logger.log('Starting continuous position snapshots (2 minute interval)...');

    while (true) {
      if (this.isSnapshotting) {
        await this.sleep(5000);
        continue;
      }

      this.isSnapshotting = true;

      try {
        const result = await this.positionSnapshotService.takeSnapshot();
        this.logger.log(`Position snapshot: ${result.snapshotsCreated} positions, ${result.changesDetected} changes`);

        // Cleanup old snapshots daily (check every snapshot cycle)
        const now = new Date();
        if (now.getHours() === 0 && now.getMinutes() < 5) {
          await this.positionSnapshotService.cleanupOldSnapshots(7);
          await this.positionSnapshotService.cleanupOldChanges(30);
        }
      } catch (error) {
        this.logger.error(`Position snapshot error: ${error.message}`);
      } finally {
        this.isSnapshotting = false;
      }

      // 2 minute interval for faster position change detection
      await this.sleep(120000);
    }
  }

  getSyncStatus() {
    return {
      isWalletSyncing: this.isWalletSyncing,
      isBlockchainSyncing: this.isBlockchainSyncing,
      isSnapshotting: this.isSnapshotting,
    };
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
