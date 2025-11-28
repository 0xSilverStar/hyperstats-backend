import { Processor, Process } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import type { Job } from 'bull';
import { TradingDataService } from '../services/trading-data.service';

@Processor('trading-data')
export class TradingDataProcessor {
  private readonly logger = new Logger(TradingDataProcessor.name);

  constructor(private readonly tradingDataService: TradingDataService) {}

  @Process('save-positions')
  async handleSavePositions(job: Job<{ address: string; data: any }>) {
    this.logger.debug(`Processing save-positions job for ${job.data.address}`);
    try {
      await this.tradingDataService.savePositions(job.data.address, job.data.data);
      this.logger.debug(`Saved positions for ${job.data.address}`);
    } catch (error) {
      this.logger.error(`Failed to save positions for ${job.data.address}: ${error.message}`);
      throw error;
    }
  }

  @Process('save-orders')
  async handleSaveOrders(job: Job<{ address: string; data: any }>) {
    this.logger.debug(`Processing save-orders job for ${job.data.address}`);
    try {
      await this.tradingDataService.saveOrders(job.data.address, job.data.data);
      this.logger.debug(`Saved orders for ${job.data.address}`);
    } catch (error) {
      this.logger.error(`Failed to save orders for ${job.data.address}: ${error.message}`);
      throw error;
    }
  }

  @Process('save-fills')
  async handleSaveFills(job: Job<{ address: string; data: any }>) {
    this.logger.debug(`Processing save-fills job for ${job.data.address}`);
    try {
      await this.tradingDataService.saveFills(job.data.address, job.data.data);
      this.logger.debug(`Saved fills for ${job.data.address}`);
    } catch (error) {
      this.logger.error(`Failed to save fills for ${job.data.address}: ${error.message}`);
      throw error;
    }
  }

  @Process('save-balances')
  async handleSaveBalances(job: Job<{ address: string; data: any }>) {
    this.logger.debug(`Processing save-balances job for ${job.data.address}`);
    try {
      await this.tradingDataService.saveBalances(job.data.address, job.data.data);
      this.logger.debug(`Saved balances for ${job.data.address}`);
    } catch (error) {
      this.logger.error(`Failed to save balances for ${job.data.address}: ${error.message}`);
      throw error;
    }
  }

  @Process('save-profile')
  async handleSaveProfile(job: Job<{ address: string; data: any }>) {
    this.logger.debug(`Processing save-profile job for ${job.data.address}`);
    try {
      await this.tradingDataService.saveProfile(job.data.address, job.data.data);
      this.logger.debug(`Saved profile for ${job.data.address}`);
    } catch (error) {
      this.logger.error(`Failed to save profile for ${job.data.address}: ${error.message}`);
      throw error;
    }
  }

  @Process('full-sync')
  async handleFullSync(job: Job<{ address: string }>) {
    this.logger.debug(`Processing full-sync job for ${job.data.address}`);
    try {
      await this.tradingDataService.fullSync(job.data.address);
      this.logger.debug(`Full sync completed for ${job.data.address}`);
    } catch (error) {
      this.logger.error(`Full sync failed for ${job.data.address}: ${error.message}`);
      throw error;
    }
  }
}
