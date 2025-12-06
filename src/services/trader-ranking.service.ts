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
  totalPnl: 0.4, // 40% - Most important: actual profit
  winRate: 0.35, // 35% - Consistency indicator
  recentPnl: 0.25, // 25% - Recent performance (7d PnL)
};

// Normalization ranges
const NORMALIZATION = {
  totalPnl: { min: -1000000, max: 10000000 },
  recentPnl: { min: -100000, max: 1000000 },
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
  sortBy?: 'rank' | 'score' | 'total_pnl' | 'win_rate' | 'total_volume' | 'portfolio_value' | 'pnl_30d';
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
    const recentPnlScore = this.normalizeValue(metrics.pnl7d, NORMALIZATION.recentPnl.min, NORMALIZATION.recentPnl.max);

    const score =
      pnlScore * SCORE_WEIGHTS.totalPnl +
      winRateScore * SCORE_WEIGHTS.winRate +
      recentPnlScore * SCORE_WEIGHTS.recentPnl;

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
      const [allFills, fills24h, fills7d, fills30d, positions, orders, balances] = await Promise.all([
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
            leverage: true,
            last_updated_at: true,
          },
        }),
        // Active orders
        this.prisma.order.count({
          where: { wallet_address: normalizedAddress, status: 'open' },
        }),
        // Spot balances for portfolio calculation
        this.prisma.balance.findMany({
          where: { wallet_address: normalizedAddress },
          select: {
            coin: true,
            entry_value: true,
          },
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

      // Calculate win rate (wins vs losses, excluding breakeven trades)
      // Note: Each fill record represents a tx_hash grouped trade, so this is trade-level accuracy
      const tradesWithPnl = allFills.filter((f) => f.total_pnl !== null);
      const winningTrades = tradesWithPnl.filter((f) => (f.total_pnl?.toNumber() ?? 0) > 0);
      const losingTrades = tradesWithPnl.filter((f) => (f.total_pnl?.toNumber() ?? 0) < 0);
      const totalWinLossTrades = winningTrades.length + losingTrades.length;
      const winRate = totalWinLossTrades > 0 ? winningTrades.length / totalWinLossTrades : 0;

      // Calculate volume
      const totalVolume = allFills.reduce((sum, f) => sum + (f.total_value?.toNumber() ?? 0), 0);
      const avgTradeSize = allFills.length > 0 ? totalVolume / allFills.length : 0;

      // Calculate main token from CURRENT POSITIONS (largest position by value)
      // This fixes the bug where mainToken was showing most traded historically instead of current position
      let mainToken: string | null = null;
      if (positions.length > 0) {
        let maxPositionValue = 0;
        for (const pos of positions) {
          const posValue = Math.abs(pos.position_value?.toNumber() ?? 0);
          if (posValue > maxPositionValue) {
            maxPositionValue = posValue;
            mainToken = pos.coin;
          }
        }
      }

      // Calculate FUTURES value (margin = position_value / leverage)
      const futuresMargin = positions.reduce((sum, p) => {
        const posValue = Math.abs(p.position_value?.toNumber() ?? 0);
        const leverage = p.leverage || 1;
        return sum + (posValue / leverage);
      }, 0);

      // Calculate SPOT value from balances
      const spotValue = balances.reduce((sum, b) => {
        return sum + (b.entry_value?.toNumber() ?? 0);
      }, 0);

      // Portfolio = FUTURES margin + SPOT value (this is what the user actually has)
      const portfolioValue = futuresMargin + spotValue;

      // Calculate average position value (margin-based, not notional)
      const avgPositionValue = positions.length > 0 ? futuresMargin / positions.length : 0;

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
   * Process items in batches with limited concurrency
   */
  private async processBatchedWithConcurrency<T, R>(
    items: T[],
    processor: (item: T) => Promise<R>,
    batchSize = 10,
  ): Promise<R[]> {
    const results: R[] = [];

    for (let i = 0; i < items.length; i += batchSize) {
      const batch = items.slice(i, i + batchSize);
      const batchResults = await Promise.all(batch.map(processor));
      results.push(...batchResults);

      // Small delay between batches to prevent connection pool exhaustion
      if (i + batchSize < items.length) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }

    return results;
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

    // Calculate metrics in batches to avoid connection pool exhaustion
    // Process 10 wallets at a time instead of all 2500+ in parallel
    const allMetrics = await this.processBatchedWithConcurrency(
      walletAddresses,
      (addr) => this.calculateTraderMetrics(addr),
      10, // Process 10 wallets concurrently
    );

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

    // Update database in batches (reduced batch size to prevent connection pool exhaustion)
    const BATCH_SIZE = 20;
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

      // Small delay between batches to prevent connection pool exhaustion
      if (i + BATCH_SIZE < rankedTraders.length) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
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

    const tradersData = traders.map((t) => {
      const totalVolume = t.total_volume.toNumber();
      // Calculate percentage based on total volume for more stable/meaningful percentages
      // This represents "return on trading volume" rather than "return on portfolio"
      const calcPercentage = (pnl: number) => (totalVolume > 0 ? (pnl / totalVolume) * 100 : 0);

      return {
        rank: t.rank,
        address: t.wallet_address,
        grade: t.grade,
        score: t.score.toNumber(),
        portfolioValue: t.portfolio_value.toString(),
        totalPnl: t.total_pnl.toString(),
        pnl24h: {
          amount: t.pnl_24h.toString(),
          percentage: calcPercentage(t.pnl_24h.toNumber()),
        },
        pnl7d: {
          amount: t.pnl_7d.toString(),
          percentage: calcPercentage(t.pnl_7d.toNumber()),
        },
        pnl30d: {
          amount: t.pnl_30d.toString(),
          percentage: calcPercentage(t.pnl_30d.toNumber()),
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
      };
    });

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

    const totalVolume = ranking.total_volume.toNumber();
    // Calculate percentage based on total volume for more stable/meaningful percentages
    const calcPercentage = (pnl: number) => (totalVolume > 0 ? (pnl / totalVolume) * 100 : 0);

    return {
      rank: ranking.rank,
      address: ranking.wallet_address,
      grade: ranking.grade,
      score: ranking.score.toNumber(),
      portfolioValue: ranking.portfolio_value.toString(),
      totalPnl: ranking.total_pnl.toString(),
      pnl24h: {
        amount: ranking.pnl_24h.toString(),
        percentage: calcPercentage(ranking.pnl_24h.toNumber()),
      },
      pnl7d: {
        amount: ranking.pnl_7d.toString(),
        percentage: calcPercentage(ranking.pnl_7d.toNumber()),
      },
      pnl30d: {
        amount: ranking.pnl_30d.toString(),
        percentage: calcPercentage(ranking.pnl_30d.toNumber()),
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
   * Update position-related fields for a trader (called when positions sync)
   * This keeps mainToken and position counts up-to-date in real-time
   */
  async updatePositionFields(walletAddress: string): Promise<void> {
    const normalizedAddress = walletAddress.toLowerCase();

    try {
      // Get current positions
      const positions = await this.prisma.position.findMany({
        where: { wallet_address: normalizedAddress },
        select: {
          coin: true,
          side: true,
          position_value: true,
          leverage: true,
          last_updated_at: true,
        },
      });

      // Calculate main token from current largest position
      let mainToken: string | null = null;
      let maxPositionValue = 0;
      for (const pos of positions) {
        const posValue = Math.abs(pos.position_value?.toNumber() ?? 0);
        if (posValue > maxPositionValue) {
          maxPositionValue = posValue;
          mainToken = pos.coin;
        }
      }

      // Count positions by side
      const longPositions = positions.filter((p) => p.side?.toLowerCase() === 'long').length;
      const shortPositions = positions.filter((p) => p.side?.toLowerCase() === 'short').length;
      const activePositions = positions.length;

      // Calculate average position value (margin-based)
      const futuresMargin = positions.reduce((sum, p) => {
        const posValue = Math.abs(p.position_value?.toNumber() ?? 0);
        const leverage = p.leverage || 1;
        return sum + (posValue / leverage);
      }, 0);
      const avgPositionValue = activePositions > 0 ? futuresMargin / activePositions : 0;

      // Find last activity
      const lastPositionUpdate = positions.reduce(
        (max, p) => (p.last_updated_at > max ? p.last_updated_at : max),
        new Date(0),
      );

      // Update only position-related fields (preserving rank, score, grade, etc.)
      await this.prisma.traderRanking.updateMany({
        where: { wallet_address: normalizedAddress },
        data: {
          main_token: mainToken,
          active_positions: activePositions,
          long_positions: longPositions,
          short_positions: shortPositions,
          avg_position_value: new Prisma.Decimal(avgPositionValue),
          last_activity_at: lastPositionUpdate > new Date(0) ? lastPositionUpdate : undefined,
        },
      });

      this.logger.debug(`Updated position fields for ${normalizedAddress}: mainToken=${mainToken}, positions=${activePositions}`);
    } catch (error) {
      // Don't fail if ranking doesn't exist yet
      this.logger.debug(`Could not update position fields for ${normalizedAddress}: ${error.message}`);
    }
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
