import { Controller, Get, Param, Query, HttpException, HttpStatus } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { PositionSnapshotService } from '../services/position-snapshot.service';
import { Prisma } from '../../generated/prisma/client';

interface GetLiveActivityQuery {
  limit?: number;
  offset?: number;
  type?: string;
  asset?: string;
  side?: string;
  minValue?: number;
  grade?: string;
}

interface GetPositionChangesQuery {
  limit?: number;
  offset?: number;
  walletAddress?: string;
  coin?: string;
  changeType?: string;
  since?: string;
}

@Controller('v1/activity')
export class ActivityController {
  constructor(
    private readonly prisma: PrismaService,
    private readonly positionSnapshotService: PositionSnapshotService,
  ) {}

  /**
   * Get live activity feed from top traders
   * GET /v1/activity/live
   */
  @Get('live')
  async getLiveActivity(@Query() query: GetLiveActivityQuery) {
    const {
      limit = 50,
      offset = 0,
      type,
      asset,
      side,
      minValue,
      grade,
    } = query;

    const effectiveLimit = Math.min(Number(limit), 200);

    try {
      // Build where clause for fills
      const fillsWhere: Prisma.FillWhereInput = {};

      if (asset) {
        fillsWhere.coin = asset;
      }
      if (side) {
        fillsWhere.side = side;
      }
      if (minValue) {
        fillsWhere.total_value = { gte: new Prisma.Decimal(Number(minValue)) };
      }

      // If grade filter is set, get addresses from rankings first
      let addressFilter: string[] | undefined;
      if (grade) {
        const rankings = await this.prisma.traderRanking.findMany({
          where: { grade },
          select: { wallet_address: true },
        });
        addressFilter = rankings.map((r) => r.wallet_address);
        if (addressFilter.length === 0) {
          return {
            activities: [],
            pagination: {
              total: 0,
              limit: effectiveLimit,
              offset: Number(offset),
              hasMore: false,
            },
          };
        }
        fillsWhere.wallet_address = { in: addressFilter };
      }

      // Get fills with trader ranking info
      const [fills, total] = await Promise.all([
        this.prisma.fill.findMany({
          where: fillsWhere,
          orderBy: { fill_timestamp: 'desc' },
          take: effectiveLimit,
          skip: Number(offset),
        }),
        this.prisma.fill.count({ where: fillsWhere }),
      ]);

      // Get rankings for these traders
      const addresses = [...new Set(fills.map((f) => f.wallet_address))];
      const rankings = await this.prisma.traderRanking.findMany({
        where: { wallet_address: { in: addresses } },
        select: {
          wallet_address: true,
          rank: true,
          grade: true,
        },
      });
      const rankingMap = new Map(rankings.map((r) => [r.wallet_address, r]));

      // Get positions to retrieve leverage for each wallet+coin
      const walletCoinPairs = fills.map((f) => ({
        wallet_address: f.wallet_address,
        coin: f.coin,
      }));
      const uniquePairs = [...new Map(walletCoinPairs.map((p) => [`${p.wallet_address}-${p.coin}`, p])).values()];

      const positions = await this.prisma.position.findMany({
        where: {
          OR: uniquePairs.map((p) => ({
            wallet_address: p.wallet_address,
            coin: p.coin,
          })),
        },
        select: {
          wallet_address: true,
          coin: true,
          leverage: true,
        },
      });
      const positionMap = new Map(positions.map((p) => [`${p.wallet_address}-${p.coin}`, p]));

      // Transform to activity format
      const activities = fills.map((fill) => {
        const ranking = rankingMap.get(fill.wallet_address);
        const position = positionMap.get(`${fill.wallet_address}-${fill.coin}`);

        // Determine activity type
        let activityType = 'trade';
        const value = fill.total_value.toNumber();
        if (value > 100000) {
          activityType = 'large_order';
        }

        return {
          id: fill.tx_hash,
          type: activityType,
          trader: {
            address: fill.wallet_address,
            rank: ranking?.rank ?? null,
            grade: ranking?.grade ?? null,
          },
          asset: fill.coin,
          side: fill.side.toUpperCase(),
          size: fill.total_size.toString(),
          price: fill.avg_price.toString(),
          value: fill.total_value.toString(),
          pnl: fill.total_pnl ? {
            amount: fill.total_pnl.toString(),
            percentage: null, // Would need position data to calculate
          } : null,
          leverage: position?.leverage ?? null,
          fillCount: fill.fill_count,
          timestamp: new Date(Number(fill.fill_timestamp)).toISOString(),
        };
      });

      return {
        activities,
        pagination: {
          total,
          limit: effectiveLimit,
          offset: Number(offset),
          hasMore: Number(offset) + effectiveLimit < total,
        },
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch live activity',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get activity for specific trader
   * GET /v1/activity/trader/:address
   */
  @Get('trader/:address')
  async getTraderActivity(@Param('address') address: string, @Query('limit') limit = 20) {
    const normalizedAddress = address.toLowerCase();
    const effectiveLimit = Math.min(Number(limit), 100);

    try {
      // Get trader ranking
      const ranking = await this.prisma.traderRanking.findUnique({
        where: { wallet_address: normalizedAddress },
        select: { rank: true, grade: true },
      });

      // Get recent fills
      const fills = await this.prisma.fill.findMany({
        where: { wallet_address: normalizedAddress },
        orderBy: { fill_timestamp: 'desc' },
        take: effectiveLimit,
      });

      const activities = fills.map((fill) => ({
        id: fill.tx_hash,
        type: fill.total_value.toNumber() > 100000 ? 'large_order' : 'trade',
        asset: fill.coin,
        side: fill.side.toUpperCase(),
        size: fill.total_size.toString(),
        price: fill.avg_price.toString(),
        value: fill.total_value.toString(),
        pnl: fill.total_pnl ? {
          amount: fill.total_pnl.toString(),
          percentage: null,
        } : null,
        fillCount: fill.fill_count,
        timestamp: new Date(Number(fill.fill_timestamp)).toISOString(),
      }));

      return {
        trader: {
          address: normalizedAddress,
          rank: ranking?.rank ?? null,
          grade: ranking?.grade ?? null,
        },
        activities,
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch trader activity',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get activity for specific asset
   * GET /v1/activity/asset/:asset
   */
  @Get('asset/:asset')
  async getAssetActivity(@Param('asset') asset: string, @Query('limit') limit = 50) {
    const effectiveLimit = Math.min(Number(limit), 200);

    try {
      // Get recent fills for this asset
      const [fills, total] = await Promise.all([
        this.prisma.fill.findMany({
          where: { coin: asset },
          orderBy: { fill_timestamp: 'desc' },
          take: effectiveLimit,
        }),
        this.prisma.fill.count({ where: { coin: asset } }),
      ]);

      // Get rankings for traders
      const addresses = [...new Set(fills.map((f) => f.wallet_address))];
      const rankings = await this.prisma.traderRanking.findMany({
        where: { wallet_address: { in: addresses } },
        select: { wallet_address: true, rank: true, grade: true },
      });
      const rankingMap = new Map(rankings.map((r) => [r.wallet_address, r]));

      const activities = fills.map((fill) => {
        const ranking = rankingMap.get(fill.wallet_address);
        return {
          id: fill.tx_hash,
          type: fill.total_value.toNumber() > 100000 ? 'large_order' : 'trade',
          trader: {
            address: fill.wallet_address,
            rank: ranking?.rank ?? null,
            grade: ranking?.grade ?? null,
          },
          side: fill.side.toUpperCase(),
          size: fill.total_size.toString(),
          price: fill.avg_price.toString(),
          value: fill.total_value.toString(),
          pnl: fill.total_pnl ? {
            amount: fill.total_pnl.toString(),
            percentage: null,
          } : null,
          fillCount: fill.fill_count,
          timestamp: new Date(Number(fill.fill_timestamp)).toISOString(),
        };
      });

      return {
        asset,
        activities,
        total,
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch asset activity',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get activity statistics
   * GET /v1/activity/stats
   */
  @Get('stats')
  async getActivityStats() {
    try {
      const now = Date.now();
      const oneDayAgo = now - 24 * 60 * 60 * 1000;
      const oneHourAgo = now - 60 * 60 * 1000;

      const [
        totalTrades24h,
        totalVolume24h,
        tradesLastHour,
        topAssets,
        topTraders,
      ] = await Promise.all([
        this.prisma.fill.count({
          where: { fill_timestamp: { gte: BigInt(oneDayAgo) } },
        }),
        this.prisma.fill.aggregate({
          where: { fill_timestamp: { gte: BigInt(oneDayAgo) } },
          _sum: { total_value: true },
        }),
        this.prisma.fill.count({
          where: { fill_timestamp: { gte: BigInt(oneHourAgo) } },
        }),
        this.prisma.fill.groupBy({
          by: ['coin'],
          where: { fill_timestamp: { gte: BigInt(oneDayAgo) } },
          _sum: { total_value: true },
          _count: { coin: true },
          orderBy: { _sum: { total_value: 'desc' } },
          take: 10,
        }),
        this.prisma.fill.groupBy({
          by: ['wallet_address'],
          where: { fill_timestamp: { gte: BigInt(oneDayAgo) } },
          _sum: { total_value: true },
          _count: { wallet_address: true },
          orderBy: { _sum: { total_value: 'desc' } },
          take: 10,
        }),
      ]);

      // Get rankings for top traders
      const topTraderAddresses = topTraders.map((t) => t.wallet_address);
      const rankings = await this.prisma.traderRanking.findMany({
        where: { wallet_address: { in: topTraderAddresses } },
        select: { wallet_address: true, rank: true, grade: true },
      });
      const rankingMap = new Map(rankings.map((r) => [r.wallet_address, r]));

      return {
        period: '24h',
        totalTrades: totalTrades24h,
        totalVolume: totalVolume24h._sum.total_value?.toString() ?? '0',
        tradesLastHour,
        topAssets: topAssets.map((a) => ({
          asset: a.coin,
          volume: a._sum.total_value?.toString() ?? '0',
          trades: a._count.coin,
        })),
        topTraders: topTraders.map((t) => {
          const ranking = rankingMap.get(t.wallet_address);
          return {
            address: t.wallet_address,
            rank: ranking?.rank ?? null,
            grade: ranking?.grade ?? null,
            volume: t._sum.total_value?.toString() ?? '0',
            trades: t._count.wallet_address,
          };
        }),
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch activity stats',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get position changes (opens/closes detected from snapshots)
   * GET /v1/activity/position-changes
   */
  @Get('position-changes')
  async getPositionChanges(@Query() query: GetPositionChangesQuery) {
    const {
      limit = 50,
      offset = 0,
      walletAddress,
      coin,
      changeType,
      since,
    } = query;

    try {
      const result = await this.positionSnapshotService.getPositionChanges({
        limit: Math.min(Number(limit), 1000),
        offset: Number(offset),
        walletAddress,
        coin,
        changeType,
        since: since ? new Date(since) : undefined,
      });

      // Enrich with trader ranking info
      const addresses = [...new Set(result.changes.map((c) => c.walletAddress))];
      const rankings = await this.prisma.traderRanking.findMany({
        where: { wallet_address: { in: addresses } },
        select: { wallet_address: true, rank: true, grade: true },
      });
      const rankingMap = new Map(rankings.map((r) => [r.wallet_address, r]));

      const enrichedChanges = result.changes.map((change) => {
        const ranking = rankingMap.get(change.walletAddress);
        return {
          ...change,
          trader: {
            address: change.walletAddress,
            rank: ranking?.rank ?? null,
            grade: ranking?.grade ?? null,
          },
        };
      });

      return {
        changes: enrichedChanges,
        pagination: result.pagination,
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch position changes',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get position history for a wallet
   * GET /v1/activity/position-history/:address
   */
  @Get('position-history/:address')
  async getPositionHistory(
    @Param('address') address: string,
    @Query('coin') coin?: string,
    @Query('limit') limit = 100,
  ) {
    try {
      const history = await this.positionSnapshotService.getPositionHistory(
        address,
        coin,
        Math.min(Number(limit), 500),
      );

      return {
        address: address.toLowerCase(),
        coin: coin || 'all',
        history,
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch position history',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get PnL history from snapshots
   * GET /v1/activity/pnl-history/:address
   */
  @Get('pnl-history/:address')
  async getPnlHistory(
    @Param('address') address: string,
    @Query('period') period: '1h' | '24h' | '7d' | '30d' = '24h',
  ) {
    try {
      const history = await this.positionSnapshotService.getPnlHistory(address, period);

      return {
        address: address.toLowerCase(),
        period,
        history,
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch PnL history',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
