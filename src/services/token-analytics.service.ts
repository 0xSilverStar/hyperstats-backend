import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

export interface TokenPositionStats {
  coin: string;
  totalNotional: number;
  longNotional: number;
  shortNotional: number;
  longCount: number;
  shortCount: number;
  totalTraderCount: number;
  avgLeverage: number;
  totalUnrealizedPnl: number;
  longUnrealizedPnl: number;
  shortUnrealizedPnl: number;
  lsRatio: number;
  majoritySide: 'LONG' | 'SHORT';
  majoritySidePercentage: number;
}

export interface TokenAnalyticsSummary {
  totalNotional: number;
  totalLongNotional: number;
  totalShortNotional: number;
  totalPositions: number;
  uniqueTraders: number;
  globalBias: 'LONG' | 'SHORT';
}

export interface TokenAnalyticsResponse {
  tokens: TokenPositionStats[];
  summary: TokenAnalyticsSummary;
  lastUpdated: string;
}

@Injectable()
export class TokenAnalyticsService {
  private readonly logger = new Logger(TokenAnalyticsService.name);

  constructor(private readonly prisma: PrismaService) {}

  /**
   * Get aggregated position analytics for all coins across all tracked wallets
   */
  async getTokenAnalytics(sortBy: string = 'totalNotional', order: 'asc' | 'desc' = 'desc'): Promise<TokenAnalyticsResponse> {
    try {
      // Get all positions grouped by coin with aggregations
      const positionsByCoin = await this.prisma.position.groupBy({
        by: ['coin'],
        _sum: {
          position_value: true,
          unrealized_pnl: true,
        },
        _avg: {
          leverage: true,
        },
        _count: {
          wallet_address: true,
        },
      });

      // Get long/short specific aggregations
      const longPositions = await this.prisma.position.groupBy({
        by: ['coin'],
        where: { side: 'long' },
        _sum: {
          position_value: true,
          unrealized_pnl: true,
        },
        _count: {
          wallet_address: true,
        },
      });

      const shortPositions = await this.prisma.position.groupBy({
        by: ['coin'],
        where: { side: 'short' },
        _sum: {
          position_value: true,
          unrealized_pnl: true,
        },
        _count: {
          wallet_address: true,
        },
      });

      // Get unique trader count per coin
      const traderCounts = await this.prisma.position.groupBy({
        by: ['coin'],
        _count: {
          _all: true,
        },
      });

      // Build lookup maps
      const longMap = new Map(longPositions.map((p) => [p.coin, p]));
      const shortMap = new Map(shortPositions.map((p) => [p.coin, p]));
      const traderCountMap = new Map(traderCounts.map((p) => [p.coin, p._count._all]));

      // Calculate token stats
      const tokens: TokenPositionStats[] = positionsByCoin.map((pos) => {
        const longData = longMap.get(pos.coin);
        const shortData = shortMap.get(pos.coin);

        const totalNotional = Math.abs(pos._sum.position_value?.toNumber() ?? 0);
        const longNotional = Math.abs(longData?._sum.position_value?.toNumber() ?? 0);
        const shortNotional = Math.abs(shortData?._sum.position_value?.toNumber() ?? 0);
        const longCount = longData?._count.wallet_address ?? 0;
        const shortCount = shortData?._count.wallet_address ?? 0;
        const totalTraderCount = traderCountMap.get(pos.coin) ?? 0;
        const avgLeverage = pos._avg.leverage ?? 1;
        const totalUnrealizedPnl = pos._sum.unrealized_pnl?.toNumber() ?? 0;
        const longUnrealizedPnl = longData?._sum.unrealized_pnl?.toNumber() ?? 0;
        const shortUnrealizedPnl = shortData?._sum.unrealized_pnl?.toNumber() ?? 0;

        // Calculate L/S ratio (long count / short count)
        const lsRatio = shortCount > 0 ? longCount / shortCount : longCount > 0 ? 999 : 0;

        // Determine majority side
        const majoritySide: 'LONG' | 'SHORT' = longNotional >= shortNotional ? 'LONG' : 'SHORT';
        const totalValue = longNotional + shortNotional;
        const majoritySidePercentage = totalValue > 0 ? (Math.max(longNotional, shortNotional) / totalValue) * 100 : 50;

        return {
          coin: pos.coin,
          totalNotional,
          longNotional,
          shortNotional,
          longCount,
          shortCount,
          totalTraderCount,
          avgLeverage,
          totalUnrealizedPnl,
          longUnrealizedPnl,
          shortUnrealizedPnl,
          lsRatio: Math.round(lsRatio * 100) / 100,
          majoritySide,
          majoritySidePercentage: Math.round(majoritySidePercentage * 10) / 10,
        };
      });

      // Sort tokens
      tokens.sort((a, b) => {
        const aVal = a[sortBy as keyof TokenPositionStats] ?? 0;
        const bVal = b[sortBy as keyof TokenPositionStats] ?? 0;
        const aNum = typeof aVal === 'number' ? aVal : 0;
        const bNum = typeof bVal === 'number' ? bVal : 0;
        return order === 'desc' ? bNum - aNum : aNum - bNum;
      });

      // Calculate summary
      const summary: TokenAnalyticsSummary = {
        totalNotional: tokens.reduce((sum, t) => sum + t.totalNotional, 0),
        totalLongNotional: tokens.reduce((sum, t) => sum + t.longNotional, 0),
        totalShortNotional: tokens.reduce((sum, t) => sum + t.shortNotional, 0),
        totalPositions: tokens.reduce((sum, t) => sum + t.longCount + t.shortCount, 0),
        uniqueTraders: await this.prisma.position.groupBy({ by: ['wallet_address'] }).then((r) => r.length),
        globalBias: tokens.reduce((sum, t) => sum + t.longNotional, 0) >= tokens.reduce((sum, t) => sum + t.shortNotional, 0) ? 'LONG' : 'SHORT',
      };

      return {
        tokens,
        summary,
        lastUpdated: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`Failed to get token analytics: ${error.message}`);
      throw error;
    }
  }

  /**
   * Get price history for a specific coin from position snapshots
   */
  async getPriceHistory(
    coin: string,
    timeframe: '1h' | '4h' | '24h' | '7d' = '24h'
  ): Promise<{
    coin: string;
    prices: Array<{
      timestamp: string;
      price: number;
      volume: number;
    }>;
    currentPrice: number | null;
  }> {
    try {
      const periodMs: Record<string, number> = {
        '1h': 60 * 60 * 1000,
        '4h': 4 * 60 * 60 * 1000,
        '24h': 24 * 60 * 60 * 1000,
        '7d': 7 * 24 * 60 * 60 * 1000,
      };

      const since = new Date(Date.now() - periodMs[timeframe]);

      // Get position snapshots for this coin
      const snapshots = await this.prisma.positionSnapshot.findMany({
        where: {
          coin,
          snapshot_at: { gte: since },
        },
        select: {
          mark_price: true,
          position_value: true,
          snapshot_at: true,
        },
        orderBy: { snapshot_at: 'asc' },
      });

      // Get current price from positions
      const currentPosition = await this.prisma.position.findFirst({
        where: { coin },
        select: { mark_price: true },
        orderBy: { last_updated_at: 'desc' },
      });

      // Group by snapshot time and calculate average price
      const priceByTime = new Map<string, { prices: number[]; volumes: number[] }>();
      for (const snap of snapshots) {
        const timeKey = snap.snapshot_at.toISOString();
        const existing = priceByTime.get(timeKey) || { prices: [], volumes: [] };
        existing.prices.push(snap.mark_price.toNumber());
        existing.volumes.push(Math.abs(snap.position_value.toNumber()));
        priceByTime.set(timeKey, existing);
      }

      const prices = Array.from(priceByTime.entries()).map(([timestamp, data]) => ({
        timestamp,
        price: data.prices.reduce((a, b) => a + b, 0) / data.prices.length,
        volume: data.volumes.reduce((a, b) => a + b, 0),
      }));

      return {
        coin,
        prices,
        currentPrice: currentPosition?.mark_price.toNumber() ?? null,
      };
    } catch (error) {
      this.logger.error(`Failed to get price history for ${coin}: ${error.message}`);
      throw error;
    }
  }

  /**
   * Get detailed analytics for a specific coin
   */
  async getTokenDetails(coin: string): Promise<{
    coin: string;
    stats: TokenPositionStats;
    topTraders: Array<{
      walletAddress: string;
      rank: number | null;
      grade: string | null;
      side: string;
      positionValue: number;
      unrealizedPnl: number;
      leverage: number;
      entryPrice: number;
    }>;
  }> {
    try {
      // Get all positions for this coin
      const positions = await this.prisma.position.findMany({
        where: { coin },
        orderBy: { position_value: 'desc' },
        take: 100,
      });

      if (positions.length === 0) {
        throw new Error(`No positions found for coin: ${coin}`);
      }

      // Get trader rankings for enrichment
      const addresses = positions.map((p) => p.wallet_address);
      const rankings = await this.prisma.traderRanking.findMany({
        where: { wallet_address: { in: addresses } },
        select: { wallet_address: true, rank: true, grade: true },
      });
      const rankingMap = new Map(rankings.map((r) => [r.wallet_address, r]));

      // Calculate stats
      const longPositions = positions.filter((p) => p.side === 'long');
      const shortPositions = positions.filter((p) => p.side === 'short');

      const longNotional = longPositions.reduce((sum, p) => sum + Math.abs(p.position_value?.toNumber() ?? 0), 0);
      const shortNotional = shortPositions.reduce((sum, p) => sum + Math.abs(p.position_value?.toNumber() ?? 0), 0);
      const totalNotional = longNotional + shortNotional;

      const stats: TokenPositionStats = {
        coin,
        totalNotional,
        longNotional,
        shortNotional,
        longCount: longPositions.length,
        shortCount: shortPositions.length,
        totalTraderCount: positions.length,
        avgLeverage: positions.reduce((sum, p) => sum + (p.leverage ?? 1), 0) / positions.length,
        totalUnrealizedPnl: positions.reduce((sum, p) => sum + (p.unrealized_pnl?.toNumber() ?? 0), 0),
        longUnrealizedPnl: longPositions.reduce((sum, p) => sum + (p.unrealized_pnl?.toNumber() ?? 0), 0),
        shortUnrealizedPnl: shortPositions.reduce((sum, p) => sum + (p.unrealized_pnl?.toNumber() ?? 0), 0),
        lsRatio: shortPositions.length > 0 ? Math.round((longPositions.length / shortPositions.length) * 100) / 100 : longPositions.length,
        majoritySide: longNotional >= shortNotional ? 'LONG' : 'SHORT',
        majoritySidePercentage: totalNotional > 0 ? Math.round((Math.max(longNotional, shortNotional) / totalNotional) * 1000) / 10 : 50,
      };

      // Build top traders list
      const topTraders = positions.map((p) => {
        const ranking = rankingMap.get(p.wallet_address);
        return {
          walletAddress: p.wallet_address,
          rank: ranking?.rank ?? null,
          grade: ranking?.grade ?? null,
          side: p.side,
          positionValue: Math.abs(p.position_value?.toNumber() ?? 0),
          unrealizedPnl: p.unrealized_pnl?.toNumber() ?? 0,
          leverage: p.leverage ?? 1,
          entryPrice: p.entry_price?.toNumber() ?? 0,
        };
      });

      return {
        coin,
        stats,
        topTraders,
      };
    } catch (error) {
      this.logger.error(`Failed to get token details for ${coin}: ${error.message}`);
      throw error;
    }
  }
}
