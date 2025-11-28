import { Controller, Get, Param, Query, HttpException, HttpStatus } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { WalletDetectionService } from '../services/wallet-detection.service';
import { GetWalletsDto } from '../common/dto';

@Controller('v1/wallets')
export class WalletController {
  constructor(
    private readonly prisma: PrismaService,
    private readonly walletService: WalletDetectionService,
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

  @Get(':address')
  async show(@Param('address') address: string) {
    const details = await this.walletService.getWalletDetails(address);

    if (!details) {
      throw new HttpException(
        {
          success: false,
          message: 'Wallet not found',
        },
        HttpStatus.NOT_FOUND,
      );
    }

    return {
      success: true,
      data: details,
    };
  }
}
