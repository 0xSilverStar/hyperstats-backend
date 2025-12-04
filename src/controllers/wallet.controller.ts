import { Controller, Get, Param, Query, Put, Body, HttpException, HttpStatus } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { WalletDetectionService } from '../services/wallet-detection.service';
import { TradingDataService } from '../services/trading-data.service';
import { GetWalletsDto } from '../common/dto';

@Controller('v1/wallets')
export class WalletController {
  constructor(
    private readonly prisma: PrismaService,
    private readonly walletService: WalletDetectionService,
    private readonly tradingDataService: TradingDataService,
  ) {}

  @Get('stats')
  async stats() {
    const stats = await this.walletService.getWalletStats();

    return {
      success: true,
      data: stats,
    };
  }

  @Get()
  async index(@Query() query: GetWalletsDto) {
    const { search, sort = 'total_deposit', order = 'desc', limit = 50, offset = 0 } = query;

    const sortMapping: Record<string, string> = {
      deposit: 'total_deposit',
      withdraw: 'total_withdraw',
      total_deposit: 'total_deposit',
      total_withdraw: 'total_withdraw',
      transaction_count: 'transaction_count',
      last_activity_at: 'last_activity_at',
      address: 'address',
    };

    const sortField = sortMapping[sort] ?? 'total_deposit';
    const effectiveLimit = Math.min(limit, 100);

    const whereClause: any = {};
    if (search) {
      whereClause.address = {
        contains: search.toLowerCase(),
      };
    }

    const [wallets, total] = await Promise.all([
      this.prisma.wallet.findMany({
        where: whereClause,
        orderBy: { [sortField]: order },
        skip: offset,
        take: effectiveLimit,
      }),
      this.prisma.wallet.count({ where: whereClause }),
    ]);

    return {
      success: true,
      data: {
        wallets,
        total,
        limit: effectiveLimit,
        offset,
      },
    };
  }

  /**
   * Get all favorite wallets with trader ranking data
   * GET /v1/wallets/favorites
   */
  @Get('favorites')
  async favorites(@Query('limit') limit = 100, @Query('offset') offset = 0) {
    const effectiveLimit = Math.min(Number(limit), 200);

    const [wallets, total] = await Promise.all([
      this.prisma.wallet.findMany({
        where: { is_favorite: true },
        orderBy: { last_activity_at: 'desc' },
        skip: Number(offset),
        take: effectiveLimit,
      }),
      this.prisma.wallet.count({ where: { is_favorite: true } }),
    ]);

    // Get trader rankings for favorite wallets
    const addresses = wallets.map((w) => w.address.toLowerCase());
    const rankings = await this.prisma.traderRanking.findMany({
      where: { wallet_address: { in: addresses } },
    });

    const rankingMap = new Map(rankings.map((r) => [r.wallet_address, r]));

    return {
      success: true,
      data: {
        wallets: wallets.map((w) => {
          const ranking = rankingMap.get(w.address.toLowerCase());
          return {
            address: w.address,
            totalDeposit: w.total_deposit.toString(),
            totalWithdraw: w.total_withdraw.toString(),
            transactionCount: w.transaction_count,
            isFavorite: w.is_favorite,
            note: w.note,
            lastActivityAt: w.last_activity_at?.toISOString() ?? null,
            ranking: ranking
              ? {
                  rank: ranking.rank,
                  grade: ranking.grade,
                  score: ranking.score.toNumber(),
                  portfolioValue: ranking.portfolio_value.toString(),
                  totalPnl: ranking.total_pnl.toString(),
                  pnl24h: {
                    amount: ranking.pnl_24h.toString(),
                    percentage:
                      ranking.portfolio_value.toNumber() > 0
                        ? (ranking.pnl_24h.toNumber() / ranking.portfolio_value.toNumber()) * 100
                        : 0,
                  },
                  pnl7d: {
                    amount: ranking.pnl_7d.toString(),
                    percentage:
                      ranking.portfolio_value.toNumber() > 0
                        ? (ranking.pnl_7d.toNumber() / ranking.portfolio_value.toNumber()) * 100
                        : 0,
                  },
                  winRate: ranking.win_rate.toNumber(),
                  totalTrades: ranking.total_trades,
                  totalVolume: ranking.total_volume.toString(),
                  activePositions: ranking.active_positions,
                  activeOrders: ranking.active_orders,
                  lastActivityAt: ranking.last_activity_at?.toISOString() ?? null,
                }
              : null,
          };
        }),
        total,
        limit: effectiveLimit,
        offset: Number(offset),
      },
    };
  }

  /**
   * Get activity for all favorite wallets
   * GET /v1/wallets/favorites-activity
   */
  @Get('favorites-activity')
  async favoritesActivity(@Query('limit') limit = 100) {
    const effectiveLimit = Math.min(Number(limit), 200);

    try {
      // Get all favorite wallet addresses
      const favoriteWallets = await this.prisma.wallet.findMany({
        where: { is_favorite: true },
        select: { address: true },
      });

      if (favoriteWallets.length === 0) {
        return {
          activities: [],
          favouriteCount: 0,
        };
      }

      const addresses = favoriteWallets.map((w) => w.address.toLowerCase());

      // Get recent fills from favorite wallets
      const fills = await this.prisma.fill.findMany({
        where: { wallet_address: { in: addresses } },
        orderBy: { fill_timestamp: 'desc' },
        take: effectiveLimit,
      });

      // Get rankings for these traders
      const rankings = await this.prisma.traderRanking.findMany({
        where: { wallet_address: { in: addresses } },
        select: {
          wallet_address: true,
          rank: true,
          grade: true,
        },
      });
      const rankingMap = new Map(rankings.map((r) => [r.wallet_address, r]));

      // Get positions for leverage info
      const walletCoinPairs = fills.map((f) => ({
        wallet_address: f.wallet_address,
        coin: f.coin,
      }));
      const uniquePairs = [...new Map(walletCoinPairs.map((p) => [`${p.wallet_address}-${p.coin}`, p])).values()];

      const positions = uniquePairs.length > 0 ? await this.prisma.position.findMany({
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
      }) : [];
      const positionMap = new Map(positions.map((p) => [`${p.wallet_address}-${p.coin}`, p]));

      // Transform to activity format
      const activities = fills.map((fill) => {
        const ranking = rankingMap.get(fill.wallet_address);
        const position = positionMap.get(`${fill.wallet_address}-${fill.coin}`);

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
          pnl: fill.total_pnl
            ? {
                amount: fill.total_pnl.toString(),
                percentage: null,
              }
            : null,
          leverage: position?.leverage ?? null,
          fillCount: fill.fill_count,
          timestamp: new Date(Number(fill.fill_timestamp)).toISOString(),
        };
      });

      return {
        activities,
        favouriteCount: favoriteWallets.length,
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch favorites activity',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get positions for all favorite wallets
   * GET /v1/wallets/favorites-positions
   */
  @Get('favorites-positions')
  async favoritesPositions() {
    try {
      // Get all favorite wallet addresses
      const favoriteWallets = await this.prisma.wallet.findMany({
        where: { is_favorite: true },
        select: { address: true },
      });

      if (favoriteWallets.length === 0) {
        return {
          positions: [],
          favouriteCount: 0,
        };
      }

      const addresses = favoriteWallets.map((w) => w.address.toLowerCase());

      // Get positions from favorite wallets
      const positions = await this.prisma.position.findMany({
        where: { wallet_address: { in: addresses } },
        orderBy: { position_value: 'desc' },
      });

      // Get rankings for these traders
      const rankings = await this.prisma.traderRanking.findMany({
        where: { wallet_address: { in: addresses } },
        select: {
          wallet_address: true,
          rank: true,
          grade: true,
        },
      });
      const rankingMap = new Map(rankings.map((r) => [r.wallet_address, r]));

      // Transform positions
      const formattedPositions = positions.map((pos) => {
        const ranking = rankingMap.get(pos.wallet_address);
        // Calculate ROI percentage from unrealized PnL and position value
        const pnlAmount = pos.unrealized_pnl?.toNumber() || 0;
        const posValue = pos.position_value?.toNumber() || 1;
        const roiPercentage = posValue > 0 ? (pnlAmount / posValue) * 100 : 0;

        return {
          id: `${pos.wallet_address}-${pos.coin}`,
          traderAddress: pos.wallet_address,
          traderRank: ranking?.rank ?? null,
          traderGrade: ranking?.grade ?? null,
          asset: pos.coin,
          side: pos.side?.toUpperCase() || 'LONG',
          size: pos.position_size?.toString() || '0',
          value: pos.position_value?.toString() || '0',
          pnl: {
            amount: pos.unrealized_pnl?.toString() || '0',
            percentage: roiPercentage.toFixed(2),
          },
          entryPrice: pos.entry_price?.toString() || '0',
          markPrice: pos.mark_price?.toString() || '0',
          leverage: pos.leverage?.toString() || '1',
          liquidationPrice: pos.liquidation_price?.toString() || null,
        };
      });

      return {
        positions: formattedPositions,
        favouriteCount: favoriteWallets.length,
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch favorites positions',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  @Get(':address')
  async show(@Param('address') address: string) {
    const data = await this.tradingDataService.getWalletDataSync(address);

    return {
      success: true,
      data,
    };
  }

  @Get(':address/sync-status')
  async syncStatus(@Param('address') address: string) {
    const status = await this.tradingDataService.getSyncStatus(address);

    return {
      success: true,
      data: status,
    };
  }

  @Get(':address/analytics')
  async analytics(@Param('address') address: string, @Query('period') period?: string) {
    const validPeriods = ['1h', '24h', '7d', '30d', '3m', '1y', 'all'];
    const selectedPeriod = validPeriods.includes(period || '') ? period : '30d';
    const analytics = await this.tradingDataService.getAnalytics(address, selectedPeriod);

    return {
      success: true,
      data: analytics,
    };
  }

  /**
   * Toggle favorite status for a wallet
   * PUT /v1/wallets/:address/favorite
   */
  @Put(':address/favorite')
  async toggleFavorite(
    @Param('address') address: string,
    @Body() body: { isFavorite?: boolean; note?: string },
  ) {
    const normalizedAddress = address.toLowerCase();

    try {
      // Find or create wallet
      let wallet = await this.prisma.wallet.findUnique({
        where: { address: normalizedAddress },
      });

      if (!wallet) {
        // Create wallet if it doesn't exist
        wallet = await this.prisma.wallet.create({
          data: {
            address: normalizedAddress,
            is_favorite: body.isFavorite ?? true,
            note: body.note ?? null,
          },
        });
      } else {
        // Toggle or set favorite status
        const newFavoriteStatus = body.isFavorite ?? !wallet.is_favorite;

        wallet = await this.prisma.wallet.update({
          where: { address: normalizedAddress },
          data: {
            is_favorite: newFavoriteStatus,
            note: body.note !== undefined ? body.note : wallet.note,
          },
        });
      }

      return {
        success: true,
        data: {
          address: wallet.address,
          isFavorite: wallet.is_favorite,
          note: wallet.note,
        },
        message: wallet.is_favorite ? 'Added to favorites' : 'Removed from favorites',
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to toggle favorite',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get favorite status for a wallet
   * GET /v1/wallets/:address/favorite
   */
  @Get(':address/favorite')
  async getFavorite(@Param('address') address: string) {
    const normalizedAddress = address.toLowerCase();

    const wallet = await this.prisma.wallet.findUnique({
      where: { address: normalizedAddress },
      select: { is_favorite: true, note: true },
    });

    return {
      success: true,
      data: {
        address: normalizedAddress,
        isFavorite: wallet?.is_favorite ?? false,
        note: wallet?.note ?? null,
      },
    };
  }
}
