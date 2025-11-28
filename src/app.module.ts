import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { BullModule } from '@nestjs/bull';
import { PrismaModule } from './prisma/prisma.module';

// Controllers
import { WalletController, TransactionController, SyncStatusController, TradingController } from './controllers';

// Services
import { HyperLiquidInfoService, ArbitrumSyncService, WalletDetectionService, TradingDataService, LedgerSyncService, FillSyncService } from './services';

// Queue
import { TradingDataProcessor } from './queue';

// Scheduler
import { WalletSyncScheduler } from './scheduler';

// Commands
import { HyperliquidSyncCommand, TradingSyncCommand, PairsSyncCommand } from './commands';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: '.env',
    }),
    ScheduleModule.forRoot(),
    BullModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: (configService: ConfigService) => ({
        redis: {
          host: configService.get('REDIS_HOST', 'localhost'),
          port: configService.get('REDIS_PORT', 6379),
        },
      }),
      inject: [ConfigService],
    }),
    BullModule.registerQueue({
      name: 'trading-data',
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 2000,
        },
        removeOnComplete: 100,
        removeOnFail: 50,
      },
    }),
    PrismaModule,
  ],
  controllers: [WalletController, TransactionController, SyncStatusController, TradingController],
  providers: [
    // Services
    HyperLiquidInfoService,
    ArbitrumSyncService,
    WalletDetectionService,
    TradingDataService,
    LedgerSyncService,
    FillSyncService,
    // Queue Processor
    TradingDataProcessor,
    // Scheduler
    WalletSyncScheduler,
    // Commands
    HyperliquidSyncCommand,
    TradingSyncCommand,
    PairsSyncCommand,
  ],
})
export class AppModule {}
