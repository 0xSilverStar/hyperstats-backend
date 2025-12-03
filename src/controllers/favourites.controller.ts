import {
  Controller,
  Get,
  Post,
  Delete,
  Param,
  Query,
  Body,
  Headers,
  HttpException,
  HttpStatus,
} from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { Prisma } from '../../generated/prisma/client';

interface AddFavouriteBody {
  notes?: string;
}

interface GetFavouritesQuery {
  limit?: number;
  offset?: number;
}

@Controller('v1/favourites')
export class FavouritesController {
  constructor(private readonly prisma: PrismaService) {}

  /**
   * Validate user ID header
   */
  private validateUserId(userId: string | undefined): string {
    if (!userId || userId.trim() === '') {
      throw new HttpException(
        {
          error: 'Missing user identification',
          message: 'X-User-ID header is required',
        },
        HttpStatus.BAD_REQUEST,
      );
    }
    return userId.trim();
  }

  /**
   * Get user's favourited traders with ranking data
   * GET /v1/favourites
   */
  @Get()
  async getFavourites(
    @Headers('x-user-id') userId: string,
    @Query() query: GetFavouritesQuery,
  ) {
    const validUserId = this.validateUserId(userId);
    const { limit = 100, offset = 0 } = query;
    const effectiveLimit = Math.min(Number(limit), 200);

    try {
      // Get user's favourites
      const [favourites, total] = await Promise.all([
        this.prisma.favourite.findMany({
          where: { user_id: validUserId },
          orderBy: { added_at: 'desc' },
          take: effectiveLimit,
          skip: Number(offset),
        }),
        this.prisma.favourite.count({
          where: { user_id: validUserId },
        }),
      ]);

      if (favourites.length === 0) {
        return {
          favourites: [],
          pagination: {
            total: 0,
            limit: effectiveLimit,
            offset: Number(offset),
            hasMore: false,
          },
        };
      }

      // Get rankings for favourited traders
      const addresses = favourites.map((f) => f.wallet_address);
      const rankings = await this.prisma.traderRanking.findMany({
        where: { wallet_address: { in: addresses } },
      });
      const rankingMap = new Map(rankings.map((r) => [r.wallet_address, r]));

      // Combine favourite data with rankings
      const favouritesWithData = favourites.map((fav) => {
        const ranking = rankingMap.get(fav.wallet_address);
        return {
          id: fav.id,
          address: fav.wallet_address,
          notes: fav.notes,
          addedAt: fav.added_at.toISOString(),
          ranking: ranking ? {
            rank: ranking.rank,
            grade: ranking.grade,
            score: ranking.score.toNumber(),
            portfolioValue: ranking.portfolio_value.toString(),
            totalPnl: ranking.total_pnl.toString(),
            pnl24h: {
              amount: ranking.pnl_24h.toString(),
              percentage: ranking.portfolio_value.toNumber() > 0
                ? (ranking.pnl_24h.toNumber() / ranking.portfolio_value.toNumber()) * 100
                : 0,
            },
            pnl7d: {
              amount: ranking.pnl_7d.toString(),
              percentage: ranking.portfolio_value.toNumber() > 0
                ? (ranking.pnl_7d.toNumber() / ranking.portfolio_value.toNumber()) * 100
                : 0,
            },
            winRate: ranking.win_rate.toNumber(),
            totalTrades: ranking.total_trades,
            totalVolume: ranking.total_volume.toString(),
            activePositions: ranking.active_positions,
            lastActivityAt: ranking.last_activity_at?.toISOString() ?? null,
          } : null,
        };
      });

      return {
        favourites: favouritesWithData,
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
          error: 'Failed to fetch favourites',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Add trader to favourites
   * POST /v1/favourites/:address
   */
  @Post(':address')
  async addFavourite(
    @Headers('x-user-id') userId: string,
    @Param('address') address: string,
    @Body() body: AddFavouriteBody,
  ) {
    const validUserId = this.validateUserId(userId);
    const normalizedAddress = address.toLowerCase();

    try {
      // Check if already favourited
      const existing = await this.prisma.favourite.findUnique({
        where: {
          user_id_wallet_address: {
            user_id: validUserId,
            wallet_address: normalizedAddress,
          },
        },
      });

      if (existing) {
        // Update notes if provided
        if (body.notes !== undefined) {
          const updated = await this.prisma.favourite.update({
            where: { id: existing.id },
            data: { notes: body.notes },
          });
          return {
            message: 'Favourite updated',
            favourite: {
              id: updated.id,
              address: updated.wallet_address,
              notes: updated.notes,
              addedAt: updated.added_at.toISOString(),
            },
          };
        }

        return {
          message: 'Already in favourites',
          favourite: {
            id: existing.id,
            address: existing.wallet_address,
            notes: existing.notes,
            addedAt: existing.added_at.toISOString(),
          },
        };
      }

      // Create new favourite
      const favourite = await this.prisma.favourite.create({
        data: {
          user_id: validUserId,
          wallet_address: normalizedAddress,
          notes: body.notes ?? null,
        },
      });

      return {
        message: 'Added to favourites',
        favourite: {
          id: favourite.id,
          address: favourite.wallet_address,
          notes: favourite.notes,
          addedAt: favourite.added_at.toISOString(),
        },
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to add favourite',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Remove trader from favourites
   * DELETE /v1/favourites/:address
   */
  @Delete(':address')
  async removeFavourite(
    @Headers('x-user-id') userId: string,
    @Param('address') address: string,
  ) {
    const validUserId = this.validateUserId(userId);
    const normalizedAddress = address.toLowerCase();

    try {
      const existing = await this.prisma.favourite.findUnique({
        where: {
          user_id_wallet_address: {
            user_id: validUserId,
            wallet_address: normalizedAddress,
          },
        },
      });

      if (!existing) {
        throw new HttpException(
          {
            error: 'Favourite not found',
            message: `Address ${normalizedAddress} is not in your favourites`,
          },
          HttpStatus.NOT_FOUND,
        );
      }

      await this.prisma.favourite.delete({
        where: { id: existing.id },
      });

      return {
        message: 'Removed from favourites',
        address: normalizedAddress,
      };
    } catch (error) {
      if (error instanceof HttpException) throw error;

      throw new HttpException(
        {
          error: 'Failed to remove favourite',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Check if address is favourited
   * GET /v1/favourites/:address/check
   */
  @Get(':address/check')
  async checkFavourite(
    @Headers('x-user-id') userId: string,
    @Param('address') address: string,
  ) {
    const validUserId = this.validateUserId(userId);
    const normalizedAddress = address.toLowerCase();

    try {
      const existing = await this.prisma.favourite.findUnique({
        where: {
          user_id_wallet_address: {
            user_id: validUserId,
            wallet_address: normalizedAddress,
          },
        },
      });

      return {
        address: normalizedAddress,
        isFavourite: !!existing,
        notes: existing?.notes ?? null,
        addedAt: existing?.added_at?.toISOString() ?? null,
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to check favourite',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get activity feed for favourited traders
   * GET /v1/favourites/activity
   */
  @Get('activity')
  async getFavouritesActivity(
    @Headers('x-user-id') userId: string,
    @Query('limit') limit = 50,
  ) {
    const validUserId = this.validateUserId(userId);
    const effectiveLimit = Math.min(Number(limit), 200);

    try {
      // Get user's favourited addresses
      const favourites = await this.prisma.favourite.findMany({
        where: { user_id: validUserId },
        select: { wallet_address: true },
      });

      if (favourites.length === 0) {
        return {
          activities: [],
          message: 'No favourites yet',
        };
      }

      const addresses = favourites.map((f) => f.wallet_address);

      // Get recent fills from favourited traders
      const fills = await this.prisma.fill.findMany({
        where: { wallet_address: { in: addresses } },
        orderBy: { fill_timestamp: 'desc' },
        take: effectiveLimit,
      });

      // Get rankings for these traders
      const rankings = await this.prisma.traderRanking.findMany({
        where: { wallet_address: { in: addresses } },
        select: { wallet_address: true, rank: true, grade: true },
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

      const activities = fills.map((fill) => {
        const ranking = rankingMap.get(fill.wallet_address);
        const position = positionMap.get(`${fill.wallet_address}-${fill.coin}`);
        return {
          id: fill.tx_hash,
          type: fill.total_value.toNumber() > 100000 ? 'large_order' : 'trade',
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
            percentage: null,
          } : null,
          leverage: position?.leverage ?? null,
          fillCount: fill.fill_count,
          timestamp: new Date(Number(fill.fill_timestamp)).toISOString(),
        };
      });

      return {
        activities,
        favouriteCount: favourites.length,
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to fetch favourites activity',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Batch add/sync favourites (for syncing localStorage)
   * POST /v1/favourites/batch
   */
  @Post('batch')
  async batchAddFavourites(
    @Headers('x-user-id') userId: string,
    @Body() body: { addresses: string[] },
  ) {
    const validUserId = this.validateUserId(userId);

    if (!body.addresses || !Array.isArray(body.addresses)) {
      throw new HttpException(
        {
          error: 'Invalid request',
          message: 'addresses array is required',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    const addresses = body.addresses.map((a) => a.toLowerCase()).slice(0, 100); // Max 100 at once

    try {
      // Get existing favourites
      const existing = await this.prisma.favourite.findMany({
        where: {
          user_id: validUserId,
          wallet_address: { in: addresses },
        },
        select: { wallet_address: true },
      });
      const existingSet = new Set(existing.map((e) => e.wallet_address));

      // Add new ones
      const newAddresses = addresses.filter((a) => !existingSet.has(a));

      if (newAddresses.length > 0) {
        await this.prisma.favourite.createMany({
          data: newAddresses.map((address) => ({
            user_id: validUserId,
            wallet_address: address,
          })),
          skipDuplicates: true,
        });
      }

      return {
        message: 'Batch sync completed',
        total: addresses.length,
        added: newAddresses.length,
        existing: existing.length,
      };
    } catch (error) {
      throw new HttpException(
        {
          error: 'Failed to batch add favourites',
          message: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
