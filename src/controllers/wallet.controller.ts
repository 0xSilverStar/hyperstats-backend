import { Controller, Get, Param, Query, HttpException, HttpStatus } from '@nestjs/common';
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
}
