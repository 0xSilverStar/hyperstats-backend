import { Controller, Get } from '@nestjs/common';
import { ArbitrumSyncService } from '../services/arbitrum-sync.service';

@Controller('v1/sync-status')
export class SyncStatusController {
  constructor(private readonly syncService: ArbitrumSyncService) {}

  @Get()
  async index() {
    const status = await this.syncService.getSyncStatus();

    return {
      success: true,
      data: status,
    };
  }
}
