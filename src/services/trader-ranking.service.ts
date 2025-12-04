import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { Prisma } from '../../generated/prisma/client';

// Grade thresholds based on rank percentile
const GRADE_THRESHOLDS = {
  'S+': 1, // Top 1%
  S: 5, // Top 5%
  A: 15, // Top 15%
  B: 35, // Top 35%
  C: 60, // Top 60%
  D: 85, // Top 85%
  F: 100, // Bottom 15%
};

// Score weights for ranking calculation
const SCORE_WEIGHTS = {
  totalPnl: 0.3, // 30%
  winRate: 0.2, // 20%
  totalVolume: 0.2, // 20%
  recentPnl: 0.15, // 15% (7d PnL)
  portfolioValue: 0.15, // 15%
};

// Normalization ranges
const NORMALIZATION = {
  totalPnl: { min: -1000000, max: 10000000 },
  totalVolume: { min: 0, max: 100000000 },
  recentPnl: { min: -100000, max: 1000000 },
  portfolioValue: { min: 0, max: 50000000 },
};

interface TraderMetrics {
  walletAddress: string;
  totalPnl: number;
  pnl24h: number;
  pnl7d: number;
  pnl30d: number;
  winRate: number;
  totalTrades: number;
  totalVolume: number;
  avgTradeSize: number;
  portfolioValue: number;
  activePositions: number;
  activeOrders: number;
  mainToken: string | null;
  avgPositionValue: number;
  longPositions: number;
  shortPositions: number;
  lastTradeAt: Date | null;
  lastActivityAt: Date | null;
}

export interface TopTraderFilters {
  grade?: string;
  minVolume?: number;
  minWinRate?: number;
  sortBy?: 'rank' | 'score' | 'total_pnl' | 'win_rate' | 'total_volume' | 'portfolio_value';
  order?: 'asc' | 'desc';
}

@Injectable()
export class TraderRankingService {
  private readonly logger = new Logger(TraderRankingService.name);

  constructor(private readonly prisma: PrismaService) {}

  /**
   * Normalize a value to 0-100 scale
   */
  private normalizeValue(value: number, min: number, max: number): number {
    if (value <= min) return 0;
    if (value >= max) return 100;
    return ((value - min) / (max - min)) * 100;
  }

  /**
   * Calculate trader score from metrics
   */
  private calculateScore(metrics: TraderMetrics): number {
    const pnlScore = this.normalizeValue(metrics.totalPnl, NORMALIZATION.totalPnl.min, NORMALIZATION.totalPnl.max);
    const winRateScore = metrics.winRate * 100;
    const volumeScore = this.normalizeValue(metrics.totalVolume, NORMALIZATION.totalVolume.min, NORMALIZATION.totalVolume.max);
    const recentPnlScore = this.normalizeValue(metrics.pnl7d, NORMALIZATION.recentPnl.min, NORMALIZATION.recentPnl.max);
    const portfolioScore = this.normalizeValue(metrics.portfolioValue, NORMALIZATION.portfolioValue.min, NORMALIZATION.portfolioValue.max);

    const score =
      pnlScore * SCORE_WEIGHTS.totalPnl +
      winRateScore * SCORE_WEIGHTS.winRate +
      volumeScore * SCORE_WEIGHTS.totalVolume +
      recentPnlScore * SCORE_WEIGHTS.recentPnl +
      portfolioScore * SCORE_WEIGHTS.portfolioValue;

    return Math.max(0, Math.min(100, score));
  }

  /**
   * Calculate grade from rank and total traders
   */
  private calculateGrade(rank: number, totalTraders: number): string {
    const percentile = (rank / totalTraders) * 100;

    if (percentile <= GRADE_THRESHOLDS['S+']) return 'S+';
    if (percentile <= GRADE_THRESHOLDS['S']) return 'S';
    if (percentile <= GRADE_THRESHOLDS['A']) return 'A';
    if (percentile <= GRADE_THRESHOLDS['B']) return 'B';
    if (percentile <= GRADE_THRESHOLDS['C']) return 'C';
    if (percentile <= GRADE_THRESHOLDS['D']) return 'D';
    return 'F';
  }

  /**
   * Calculate metrics for a single trader
   */
  private async calculateTraderMetrics(walletAddress: string): Promise<TraderMetrics | null> {
    const normalizedAddress = walletAddress.toLowerCase();

    // Get time boundaries
    const now = Date.now();
    const oneDayAgo = now - 24 * 60 * 60 * 1000;
    const sevenDaysAgo = now - 7 * 24 * 60 * 60 * 1000;
    const thirtyDaysAgo = now - 30 * 24 * 60 * 60 * 1000;

    try {
      // Get fill statistics
      const [allFills, fills24h, fills7d, fills30d, positions, orders] = await Promise.all([
        // All fills for total PnL, volume, win rate, and main token
        this.prisma.fill.findMany({
          where: { wallet_address: normalizedAddress },
          select: {
            coin: true,
            total_pnl: true,
            total_value: true,
            fill_timestamp: true,
          },
        }),
        // 24h fills
        this.prisma.fill.findMany({
          where: {
            wallet_address: normalizedAddress,
            fill_timestamp: { gte: BigInt(oneDayAgo) },
          },
          select: { total_pnl: true },
        }),
        // 7d fills
        this.prisma.fill.findMany({
          where: {
            wallet_address: normalizedAddress,
            fill_timestamp: { gte: BigInt(sevenDaysAgo) },
          },
          select: { total_pnl: true },
        }),
        // 30d fills
        this.prisma.fill.findMany({
          where: {
            wallet_address: normalizedAddress,
            fill_timestamp: { gte: BigInt(thirtyDaysAgo) },
          },
          select: { total_pnl: true },
        }),
        // Active positions with side info
        this.prisma.position.findMany({
          where: { wallet_address: normalizedAddress },
          select: {
            coin: true,
            side: true,
            position_value: true,
            unrealized_pnl: true,
            margin_used: true,
            last_updated_at: true,
          },
        }),
        // Active orders
        this.prisma.order.count({
          where: { wallet_address: normalizedAddress, status: 'open' },
        }),
      ]);

      if (allFills.length === 0) {
        return null; // No trading history
      }

      // Calculate total PnL
      const totalPnl = allFills.reduce((sum, f) => sum + (f.total_pnl?.toNumber() ?? 0), 0);

      // Calculate period PnL
      const pnl24h = fills24h.reduce((sum, f) => sum + (f.total_pnl?.toNumber() ?? 0), 0);
      const pnl7d = fills7d.reduce((sum, f) => sum + (f.total_pnl?.toNumber() ?? 0), 0);
      const pnl30d = fills30d.reduce((sum, f) => sum + (f.total_pnl?.toNumber() ?? 0), 0);

      // Calculate win rate
      const tradesWithPnl = allFills.filter((f) => f.total_pnl !== null);
      const winningTrades = tradesWithPnl.filter((f) => (f.total_pnl?.toNumber() ?? 0) > 0);
      const winRate = tradesWithPnl.length > 0 ? winningTrades.length / tradesWithPnl.length : 0;

      // Calculate volume
      const totalVolume = allFills.reduce((sum, f) => sum + (f.total_value?.toNumber() ?? 0), 0);
      const avgTradeSize = allFills.length > 0 ? totalVolume / allFills.length : 0;

      // Calculate main token (most frequently traded)
      const tokenCounts: Record<string, number> = {};
      for (const fill of allFills) {
        tokenCounts[fill.coin] = (tokenCounts[fill.coin] || 0) + 1;
      }
      let mainToken: string | null = null;
      let maxCount = 0;
      for (const [token, count] of Object.entries(tokenCounts)) {
        if (count > maxCount) {
          maxCount = count;
          mainToken = token;
        }
      }

      // Calculate portfolio value from positions
      const portfolioValue = positions.reduce((sum, p) => {
        return sum + (p.position_value?.toNumber() ?? 0) + (p.unrealized_pnl?.toNumber() ?? 0);
      }, 0);

      // Calculate average position value
      const totalPositionValue = positions.reduce((sum, p) => sum + Math.abs(p.position_value?.toNumber() ?? 0), 0);
      const avgPositionValue = positions.length > 0 ? totalPositionValue / positions.length : 0;

      // Count long and short positions
      const longPositions = positions.filter((p) => p.side?.toLowerCase() === 'long').length;
      const shortPositions = positions.filter((p) => p.side?.toLowerCase() === 'short').length;

      // Find last trade timestamp
      const lastFillTimestamp = allFills.reduce((max, f) => (f.fill_timestamp > max ? f.fill_timestamp : max), BigInt(0));
      const lastTradeAt = lastFillTimestamp > 0 ? new Date(Number(lastFillTimestamp)) : null;

      // Find last activity (most recent of trade or position update)
      const lastPositionUpdate = positions.reduce((max, p) => (p.last_updated_at > max ? p.last_updated_at : max), new Date(0));
      const lastActivityAt = lastTradeAt && lastTradeAt > lastPositionUpdate ? lastTradeAt : lastPositionUpdate;

      return {
        walletAddress: normalizedAddress,
        totalPnl,
        pnl24h,
        pnl7d,
        pnl30d,
        winRate,
        totalTrades: allFills.length,
        totalVolume,
        avgTradeSize,
        portfolioValue: Math.abs(portfolioValue),
        activePositions: positions.length,
        activeOrders: orders,
        mainToken,
        avgPositionValue,
        longPositions,
        shortPositions,
        lastTradeAt,
        lastActivityAt: lastActivityAt > new Date(0) ? lastActivityAt : null,
      };
    } catch (error) {
      this.logger.error(`Error calculating metrics for ${normalizedAddress}`, error);
      return null;
    }
  }

  /**
   * Calculate and update rankings for all traders
   */
  async calculateRankings(): Promise<{ total: number; updated: number }> {
    this.logger.log('Starting ranking calculation...');

    // Get all unique wallet addresses with fills
    const walletsWithFills = await this.prisma.fill.findMany({
      select: { wallet_address: true },
      distinct: ['wallet_address'],
    });

    const walletAddresses = walletsWithFills.map((w) => w.wallet_address);
    this.logger.log(`Found ${walletAddresses.length} wallets with trading activity`);

    // Calculate metrics for all traders
    const metricsPromises = walletAddresses.map((addr) => this.calculateTraderMetrics(addr));
    const allMetrics = await Promise.all(metricsPromises);

    // Filter out null metrics and calculate scores
    const tradersWithScores = allMetrics
      .filter((m): m is TraderMetrics => m !== null)
      .map((metrics) => ({
        ...metrics,
        score: this.calculateScore(metrics),
      }))
      .sort((a, b) => b.score - a.score); // Sort by score descending

    this.logger.log(`Calculated scores for ${tradersWithScores.length} traders`);

    // Assign ranks and grades
    const totalTraders = tradersWithScores.length;
    const rankedTraders = tradersWithScores.map((trader, index) => ({
      ...trader,
      rank: index + 1,
      grade: this.calculateGrade(index + 1, totalTraders),
    }));

    // Update database in batches
    const BATCH_SIZE = 100;
    let updated = 0;

    for (let i = 0; i < rankedTraders.length; i += BATCH_SIZE) {
      const batch = rankedTraders.slice(i, i + BATCH_SIZE);

      await Promise.all(
        batch.map((trader) =>
          this.prisma.traderRanking.upsert({
            where: { wallet_address: trader.walletAddress },
            update: {
              rank: trader.rank,
              grade: trader.grade,
              score: new Prisma.Decimal(trader.score),
              total_pnl: new Prisma.Decimal(trader.totalPnl),
              pnl_24h: new Prisma.Decimal(trader.pnl24h),
              pnl_7d: new Prisma.Decimal(trader.pnl7d),
              pnl_30d: new Prisma.Decimal(trader.pnl30d),
              win_rate: new Prisma.Decimal(trader.winRate),
              total_trades: trader.totalTrades,
              total_volume: new Prisma.Decimal(trader.totalVolume),
              avg_trade_size: new Prisma.Decimal(trader.avgTradeSize),
              portfolio_value: new Prisma.Decimal(trader.portfolioValue),
              active_positions: trader.activePositions,
              active_orders: trader.activeOrders,
              main_token: trader.mainToken,
              avg_position_value: trader.avgPositionValue ? new Prisma.Decimal(trader.avgPositionValue) : null,
              long_positions: trader.longPositions,
              short_positions: trader.shortPositions,
              last_trade_at: trader.lastTradeAt,
              last_activity_at: trader.lastActivityAt,
            },
            create: {
              wallet_address: trader.walletAddress,
              rank: trader.rank,
              grade: trader.grade,
              score: new Prisma.Decimal(trader.score),
              total_pnl: new Prisma.Decimal(trader.totalPnl),
              pnl_24h: new Prisma.Decimal(trader.pnl24h),
              pnl_7d: new Prisma.Decimal(trader.pnl7d),
              pnl_30d: new Prisma.Decimal(trader.pnl30d),
              win_rate: new Prisma.Decimal(trader.winRate),
              total_trades: trader.totalTrades,
              total_volume: new Prisma.Decimal(trader.totalVolume),
              avg_trade_size: new Prisma.Decimal(trader.avgTradeSize),
              portfolio_value: new Prisma.Decimal(trader.portfolioValue),
              active_positions: trader.activePositions,
              active_orders: trader.activeOrders,
              main_token: trader.mainToken,
              avg_position_value: trader.avgPositionValue ? new Prisma.Decimal(trader.avgPositionValue) : null,
              long_positions: trader.longPositions,
              short_positions: trader.shortPositions,
              last_trade_at: trader.lastTradeAt,
              last_activity_at: trader.lastActivityAt,
            },
          }),
        ),
      );

      updated += batch.length;
      this.logger.debug(`Updated ${updated}/${totalTraders} rankings`);
    }

    // Clean up rankings for wallets that no longer have fills
    const existingRankings = await this.prisma.traderRanking.findMany({
      select: { wallet_address: true },
    });
    const currentWallets = new Set(tradersWithScores.map((t) => t.walletAddress));
    const staleRankings = existingRankings.filter((r) => !currentWallets.has(r.wallet_address));

    if (staleRankings.length > 0) {
      await this.prisma.traderRanking.deleteMany({
        where: {
          wallet_address: {
            in: staleRankings.map((r) => r.wallet_address),
          },
        },
      });
      this.logger.log(`Removed ${staleRankings.length} stale rankings`);
    }

    this.logger.log(`Ranking calculation completed: ${updated} traders ranked`);
    return { total: totalTraders, updated };
  }

  /**
   * Get top traders with pagination and filters
   */
  async getTopTraders(
    limit = 50,
    offset = 0,
    filters?: TopTraderFilters,
  ): Promise<{
    traders: any[];
    pagination: { total: number; limit: number; offset: number; hasMore: boolean };
  }> {
    const where: Prisma.TraderRankingWhereInput = {};

    if (filters?.grade) {
      where.grade = filters.grade;
    }
    if (filters?.minVolume) {
      where.total_volume = { gte: new Prisma.Decimal(filters.minVolume) };
    }
    if (filters?.minWinRate) {
      where.win_rate = { gte: new Prisma.Decimal(filters.minWinRate) };
    }

    // Default sort by rank ascending
    const orderBy: Prisma.TraderRankingOrderByWithRelationInput = {};
    const sortBy = filters?.sortBy || 'rank';
    const sortOrder = filters?.order || (sortBy === 'rank' ? 'asc' : 'desc');
    orderBy[sortBy] = sortOrder;

    const [traders, total] = await Promise.all([
      this.prisma.traderRanking.findMany({
        where,
        orderBy,
        take: limit,
        skip: offset,
      }),
      this.prisma.traderRanking.count({ where }),
    ]);

    const tradersData = traders.map((t) => ({
      rank: t.rank,
      address: t.wallet_address,
      grade: t.grade,
      score: t.score.toNumber(),
      portfolioValue: t.portfolio_value.toString(),
      totalPnl: t.total_pnl.toString(),
      pnl24h: {
        amount: t.pnl_24h.toString(),
        percentage: t.portfolio_value.toNumber() > 0 ? (t.pnl_24h.toNumber() / t.portfolio_value.toNumber()) * 100 : 0,
      },
      pnl7d: {
        amount: t.pnl_7d.toString(),
        percentage: t.portfolio_value.toNumber() > 0 ? (t.pnl_7d.toNumber() / t.portfolio_value.toNumber()) * 100 : 0,
      },
      pnl30d: {
        amount: t.pnl_30d.toString(),
        percentage: t.portfolio_value.toNumber() > 0 ? (t.pnl_30d.toNumber() / t.portfolio_value.toNumber()) * 100 : 0,
      },
      winRate: t.win_rate.toNumber(),
      totalTrades: t.total_trades,
      totalVolume: t.total_volume.toString(),
      activePositions: t.active_positions,
      activeOrders: t.active_orders,
      mainToken: t.main_token,
      avgPositionValue: t.avg_position_value?.toString() ?? '0',
      longPositions: t.long_positions,
      shortPositions: t.short_positions,
      lastActivityAt: t.last_activity_at?.toISOString() ?? null,
    }));

    return {
      traders: tradersData,
      pagination: {
        total,
        limit,
        offset,
        hasMore: offset + limit < total,
      },
    };
  }

  /**
   * Get ranking for a single trader
   */
  async getTraderRank(address: string): Promise<any | null> {
    const normalizedAddress = address.toLowerCase();

    const ranking = await this.prisma.traderRanking.findUnique({
      where: { wallet_address: normalizedAddress },
    });

    if (!ranking) {
      return null;
    }

    return {
      rank: ranking.rank,
      address: ranking.wallet_address,
      grade: ranking.grade,
      score: ranking.score.toNumber(),
      portfolioValue: ranking.portfolio_value.toString(),
      totalPnl: ranking.total_pnl.toString(),
      pnl24h: {
        amount: ranking.pnl_24h.toString(),
        percentage: ranking.portfolio_value.toNumber() > 0 ? (ranking.pnl_24h.toNumber() / ranking.portfolio_value.toNumber()) * 100 : 0,
      },
      pnl7d: {
        amount: ranking.pnl_7d.toString(),
        percentage: ranking.portfolio_value.toNumber() > 0 ? (ranking.pnl_7d.toNumber() / ranking.portfolio_value.toNumber()) * 100 : 0,
      },
      pnl30d: {
        amount: ranking.pnl_30d.toString(),
        percentage: ranking.portfolio_value.toNumber() > 0 ? (ranking.pnl_30d.toNumber() / ranking.portfolio_value.toNumber()) * 100 : 0,
      },
      winRate: ranking.win_rate.toNumber(),
      totalTrades: ranking.total_trades,
      totalVolume: ranking.total_volume.toString(),
      avgTradeSize: ranking.avg_trade_size.toString(),
      activePositions: ranking.active_positions,
      activeOrders: ranking.active_orders,
      mainToken: ranking.main_token,
      avgPositionValue: ranking.avg_position_value?.toString() ?? '0',
      longPositions: ranking.long_positions,
      shortPositions: ranking.short_positions,
      lastTradeAt: ranking.last_trade_at?.toISOString() ?? null,
      lastActivityAt: ranking.last_activity_at?.toISOString() ?? null,
      updatedAt: ranking.updated_at.toISOString(),
    };
  }

  /**
   * Get ranking statistics summary
   */
  async getRankingStats(): Promise<{
    totalTraders: number;
    gradeDistribution: Record<string, number>;
    lastUpdated: Date | null;
  }> {
    const [total, grades, lastUpdate] = await Promise.all([
      this.prisma.traderRanking.count(),
      this.prisma.traderRanking.groupBy({
        by: ['grade'],
        _count: { grade: true },
      }),
      this.prisma.traderRanking.findFirst({
        orderBy: { updated_at: 'desc' },
        select: { updated_at: true },
      }),
    ]);

    const gradeDistribution: Record<string, number> = {
      'S+': 0,
      S: 0,
      A: 0,
      B: 0,
      C: 0,
      D: 0,
      F: 0,
    };

    for (const grade of grades) {
      gradeDistribution[grade.grade] = grade._count.grade;
    }

    return {
      totalTraders: total,
      gradeDistribution,
      lastUpdated: lastUpdate?.updated_at ?? null,
    };
  }
}
