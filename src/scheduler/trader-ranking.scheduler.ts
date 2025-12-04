import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { TraderRankingService } from '../services/trader-ranking.service';

@Injectable()
export class TraderRankingScheduler implements OnModuleInit {
  private readonly logger = new Logger(TraderRankingScheduler.name);
  private isRankingCalculating = false;

  constructor(private readonly traderRankingService: TraderRankingService) {}

  async onModuleInit() {
    this.logger.log('TraderRankingScheduler initialized');

    // Run initial ranking calculation after startup (30 seconds delay)
    setTimeout(() => {
      void this.calculateRankings();
    }, 30000);
  }

  /**
   * Calculate trader rankings every 30 minutes
   * Runs at 0 and 30 minutes of every hour
   */
  @Cron('0 0,30 * * * *')
  async handleRankingCron() {
    this.logger.log('Scheduled ranking calculation triggered');
    await this.calculateRankings();
  }

  /**
   * Calculate rankings with concurrency protection
   */
  private async calculateRankings() {
    if (this.isRankingCalculating) {
      this.logger.warn('Ranking calculation already in progress, skipping');
      return;
    }

    this.isRankingCalculating = true;
    const startTime = Date.now();

    try {
      this.logger.log('Starting trader ranking calculation...');
      const result = await this.traderRankingService.calculateRankings();
      const duration = ((Date.now() - startTime) / 1000).toFixed(2);

      this.logger.log(`Ranking calculation completed in ${duration}s: ${result.updated} traders ranked`);
    } catch (error) {
      this.logger.error(`Ranking calculation failed: ${error.message}`);
    } finally {
      this.isRankingCalculating = false;
    }
  }

  /**
   * Get scheduler status
   */
  getStatus() {
    return {
      isRankingCalculating: this.isRankingCalculating,
    };
  }

  /**
   * Manually trigger ranking calculation
   */
  async triggerCalculation(): Promise<{ total: number; updated: number }> {
    if (this.isRankingCalculating) {
      throw new Error('Ranking calculation already in progress');
    }

    this.isRankingCalculating = true;

    try {
      return await this.traderRankingService.calculateRankings();
    } finally {
      this.isRankingCalculating = false;
    }
  }
}
