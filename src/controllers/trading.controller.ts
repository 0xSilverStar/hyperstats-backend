import { Controller, Get, Post, Param, Query, HttpException, HttpStatus } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { HyperLiquidInfoService } from '../services/hyperliquid-info.service';
import { TradingDataService } from '../services/trading-data.service';
import { LedgerSyncService } from '../services/ledger-sync.service';
import { FillSyncService } from '../services/fill-sync.service';
import { GetFillsDto, GetPositionsSummaryDto, GetActivityByCoinDto, GetLedgerUpdatesDto } from '../common/dto';

@Controller('v1/wallets')
export class TradingController {
  constructor(
    private readonly prisma: PrismaService,
    private readonly hlService: HyperLiquidInfoService,
    private readonly tradingDataService: TradingDataService,
    private readonly ledgerSyncService: LedgerSyncService,
    private readonly fillSyncService: FillSyncService,
  ) {}

  @Get('positions/summary')
  async getPositionsSummary(@Query() query: GetPositionsSummaryDto) {
    const { limit = 20 } = query;
    const effectiveLimit = Math.min(limit, 100);

    const [topPositions, totalPositions, totalLongs, totalShorts, totalPnl, walletsWithPositions] = await Promise.all([
      this.prisma.position.findMany({
        where: {
          OR: [{ position_size: { gt: 0.00001 } }, { position_size: { lt: -0.00001 } }],
        },
        orderBy: { position_value: 'desc' },
        take: effectiveLimit,
      }),
      this.prisma.position.count({
        where: {
          OR: [{ position_size: { gt: 0.00001 } }, { position_size: { lt: -0.00001 } }],
        },
      }),
      this.prisma.position.count({
        where: { side: 'long' },
      }),
      this.prisma.position.count({
        where: { side: 'short' },
      }),
      this.prisma.position.aggregate({
        where: {
          OR: [{ position_size: { gt: 0.00001 } }, { position_size: { lt: -0.00001 } }],
        },
        _sum: { unrealized_pnl: true },
      }),
      this.prisma.position.groupBy({
        by: ['wallet_address'],
        where: {
          OR: [{ position_size: { gt: 0.00001 } }, { position_size: { lt: -0.00001 } }],
        },
      }),
    ]);

    return {
      summary: {
        totalPositions,
        longPositions: totalLongs,
        shortPositions: totalShorts,
        totalUnrealizedPnl: totalPnl._sum.unrealized_pnl ?? 0,
        walletsWithPositions: walletsWithPositions.length,
      },
      topPositions,
    };
  }

  @Get('activity/coin/:coin')
  async getActivityByCoin(@Param('coin') coin: string, @Query() query: GetActivityByCoinDto) {
    const { limit = 20 } = query;
    const effectiveLimit = Math.min(limit, 100);

    const [positions, fills, stats] = await Promise.all([
      this.prisma.position.findMany({
        where: {
          coin,
          OR: [{ position_size: { gt: 0.00001 } }, { position_size: { lt: -0.00001 } }],
        },
        orderBy: { position_value: 'desc' },
        take: effectiveLimit,
      }),
      this.prisma.fill.findMany({
        where: { coin },
        orderBy: { fill_timestamp: 'desc' },
        take: effectiveLimit,
      }),
      Promise.all([
        this.prisma.position.count({
          where: {
            coin,
            OR: [{ position_size: { gt: 0.00001 } }, { position_size: { lt: -0.00001 } }],
          },
        }),
        this.prisma.position.aggregate({
          where: {
            coin,
            OR: [{ position_size: { gt: 0.00001 } }, { position_size: { lt: -0.00001 } }],
          },
          _sum: { position_value: true },
        }),
        this.prisma.position.count({
          where: { coin, side: 'long' },
        }),
        this.prisma.position.count({
          where: { coin, side: 'short' },
        }),
      ]),
    ]);

    return {
      coin,
      positions,
      recentFills: fills,
      stats: {
        totalPositions: stats[0],
        totalPositionValue: stats[1]._sum.position_value ?? 0,
        longCount: stats[2],
        shortCount: stats[3],
      },
    };
  }

  /**
   * Get user positions with 30-minute cache
   * - If cache is valid (< 30 min), returns from database
   * - If cache miss, fetches from HyperLiquid API, queues background save, returns immediately
   */
  @Get(':address/positions')
  async getPositions(@Param('address') address: string) {
    try {
      return await this.tradingDataService.getPositions(address);
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch positions',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get user orders with 30-minute cache
   */
  @Get(':address/orders')
  async getOrders(@Param('address') address: string) {
    try {
      return await this.tradingDataService.getOrders(address);
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch orders',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get fill statistics (aggregated data for statistics cards)
   */
  @Get(':address/fills/statistics')
  async getFillStatistics(@Param('address') address: string) {
    const normalizedAddress = address.toLowerCase();

    try {
      return await this.fillSyncService.getFillStatistics(normalizedAddress);
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch fill statistics',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get user fills with incremental sync and pagination
   */
  @Get(':address/fills')
  async getFills(@Param('address') address: string, @Query() query: GetFillsDto) {
    const { limit = 50, offset = 0 } = query;
    const effectiveLimit = Math.min(limit, 500);
    const normalizedAddress = address.toLowerCase();

    try {
      // Use FillSyncService which handles caching and incremental sync
      return await this.fillSyncService.getFills(normalizedAddress, effectiveLimit, offset);
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch fills',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get user spot balances with 30-minute cache
   */
  @Get(':address/balances')
  async getBalances(@Param('address') address: string) {
    try {
      return await this.tradingDataService.getBalances(address);
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch balances',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get complete user profile with 30-minute cache
   */
  @Get(':address/profile')
  async getProfile(@Param('address') address: string) {
    try {
      return await this.tradingDataService.getProfile(address);
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch profile',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get ledger statistics (aggregated data for statistics cards)
   */
  @Get(':address/ledger/statistics')
  async getLedgerStatistics(@Param('address') address: string) {
    const normalizedAddress = address.toLowerCase();

    try {
      return await this.ledgerSyncService.getLedgerStatistics(normalizedAddress);
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch ledger statistics',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  @Get(':address/ledger')
  async getLedgerUpdates(@Param('address') address: string, @Query() query: GetLedgerUpdatesDto) {
    const normalizedAddress = address.toLowerCase();
    const { startTime = 0, limit = 100, offset = 0 } = query;

    try {
      // Use LedgerSyncService which handles caching and incremental sync
      return await this.ledgerSyncService.getLedgerUpdates(normalizedAddress, startTime, limit, offset);
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch ledger updates',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Force sync wallet data (bypasses cache)
   */
  @Post(':address/sync')
  async sync(@Param('address') address: string) {
    const normalizedAddress = address.toLowerCase();

    try {
      await this.tradingDataService.fullSync(normalizedAddress);

      return {
        message: 'Wallet data synced successfully',
        address: normalizedAddress,
        syncedAt: new Date().toISOString(),
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Sync failed',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
