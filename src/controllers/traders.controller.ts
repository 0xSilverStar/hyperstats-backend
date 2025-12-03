import { Controller, Get, Post, Param, Query, HttpException, HttpStatus } from '@nestjs/common';
import { TraderRankingService, TopTraderFilters } from '../services/trader-ranking.service';
import { TraderRankingScheduler } from '../scheduler/trader-ranking.scheduler';
import { PrismaService } from '../prisma/prisma.service';

interface GetTopTradersQuery {
  limit?: number;
  offset?: number;
  grade?: string;
  minVolume?: number;
  minWinRate?: number;
  sortBy?: 'rank' | 'score' | 'total_pnl' | 'win_rate' | 'total_volume' | 'portfolio_value';
  order?: 'asc' | 'desc';
}

interface GetActivityQuery {
  limit?: number;
  offset?: number;
  type?: string;
  asset?: string;
  side?: string;
  minValue?: number;
  grade?: string;
}

@Controller('v1/traders')
export class TradersController {
  constructor(
    private readonly traderRankingService: TraderRankingService,
    private readonly traderRankingScheduler: TraderRankingScheduler,
    private readonly prisma: PrismaService,
  ) {}

  /**
   * Get top traders with pagination and filters
   * GET /v1/traders/top
   */
  @Get('top')
  async getTopTraders(@Query() query: GetTopTradersQuery) {
    const {
      limit = 50,
      offset = 0,
      grade,
      minVolume,
      minWinRate,
      sortBy,
      order,
    } = query;

    const effectiveLimit = Math.min(Number(limit), 200);

    try {
      const filters: TopTraderFilters = {};
      if (grade) filters.grade = grade;
      if (minVolume) filters.minVolume = Number(minVolume);
      if (minWinRate) filters.minWinRate = Number(minWinRate);
      if (sortBy) filters.sortBy = sortBy;
      if (order) filters.order = order;

      return await this.traderRankingService.getTopTraders(effectiveLimit, Number(offset), filters);
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch top traders',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get single trader ranking
   * GET /v1/traders/:address/rank
   */
  @Get(':address/rank')
  async getTraderRank(@Param('address') address: string) {
    try {
      const ranking = await this.traderRankingService.getTraderRank(address);

      if (!ranking) {
        throw new HttpException(
          {
            error: 'Trader not found',
            message: `No ranking data found for address ${address}`,
          },
          HttpStatus.NOT_FOUND,
        );
      }

      return ranking;
    } catch (error) {
      if (error instanceof HttpException) throw error;

      throw new HttpException(
        {
          error: 'Failed to fetch trader rank',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get ranking statistics
   * GET /v1/traders/rankings/stats
   */
  @Get('rankings/stats')
  async getRankingStats() {
    try {
      return await this.traderRankingService.getRankingStats();
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch ranking stats',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Manually trigger ranking recalculation (admin)
   * POST /v1/traders/rankings/refresh
   */
  @Post('rankings/refresh')
  async refreshRankings() {
    try {
      const result = await this.traderRankingScheduler.triggerCalculation();
      return {
        message: 'Ranking calculation completed',
        ...result,
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to refresh rankings',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
