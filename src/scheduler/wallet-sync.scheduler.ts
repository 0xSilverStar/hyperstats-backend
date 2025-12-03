import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { TradingDataService } from '../services/trading-data.service';
import { ArbitrumSyncService } from '../services/arbitrum-sync.service';

@Injectable()
export class WalletSyncScheduler implements OnModuleInit {
  private readonly logger = new Logger(WalletSyncScheduler.name);
  private isWalletSyncing = false;
  private isBlockchainSyncing = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly tradingDataService: TradingDataService,
    private readonly arbitrumSyncService: ArbitrumSyncService,
  ) {}

  async onModuleInit() {
    this.logger.log('WalletSyncScheduler initialized');

    // Start background sync tasks after a short delay
    setTimeout(() => {
      void this.startContinuousWalletSync();
      void this.startContinuousBlockchainSync();
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

          // 15 second interval between wallets
          await this.sleep(15000);
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

  getSyncStatus() {
    return {
      isWalletSyncing: this.isWalletSyncing,
      isBlockchainSyncing: this.isBlockchainSyncing,
    };
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
