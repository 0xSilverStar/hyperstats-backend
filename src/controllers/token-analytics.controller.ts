import { Controller, Get, Param, Query, HttpException, HttpStatus } from '@nestjs/common';
import { TokenAnalyticsService } from '../services/token-analytics.service';

class GetTokenAnalyticsQuery {
  sortBy?: string;
  order?: 'asc' | 'desc';
}

@Controller('v1/token-analytics')
export class TokenAnalyticsController {
  constructor(private readonly tokenAnalyticsService: TokenAnalyticsService) {}

  /**
   * Get aggregated token analytics for all coins
   * GET /v1/token-analytics
   */
  @Get()
  async getTokenAnalytics(@Query() query: GetTokenAnalyticsQuery) {
    const { sortBy = 'totalNotional', order = 'desc' } = query;

    try {
      const result = await this.tokenAnalyticsService.getTokenAnalytics(sortBy, order);
      return result;
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch token analytics',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get price history for a specific coin
   * GET /v1/token-analytics/:coin/price-history
   */
  @Get(':coin/price-history')
  async getPriceHistory(@Param('coin') coin: string, @Query('timeframe') timeframe?: '1h' | '4h' | '24h' | '7d') {
    try {
      const result = await this.tokenAnalyticsService.getPriceHistory(coin.toUpperCase(), timeframe || '24h');
      return result;
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch price history',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get detailed analytics for a specific coin
   * GET /v1/token-analytics/:coin
   */
  @Get(':coin')
  async getTokenDetails(@Param('coin') coin: string) {
    try {
      const result = await this.tokenAnalyticsService.getTokenDetails(coin.toUpperCase());
      return result;
    } catch (error) {
      if (error.message.includes('No positions found')) {
        throw new HttpException(
          {
            error: 'Token not found',
            message: `No positions found for coin: ${coin}`,
          },
          HttpStatus.NOT_FOUND,
        );
      }

      throw new HttpException(
        {
          error: 'Failed to fetch token details',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
