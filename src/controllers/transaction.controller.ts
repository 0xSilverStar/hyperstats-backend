import { Controller, Get, Param, Query, HttpException, HttpStatus } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { GetTransactionsDto } from '../common/dto';

@Controller('v1/transactions')
export class TransactionController {
  constructor(private readonly prisma: PrismaService) {}

  @Get()
  async index(@Query() query: GetTransactionsDto) {
    const { wallet, type, min_amount, sort = 'timestamp', order = 'desc', limit = 50, offset = 0 } = query;

    const sortMapping: Record<string, string> = {
      timestamp: 'timestamp',
      amount: 'amount',
      block_number: 'block_number',
      wallet_address: 'wallet_address',
    };

    const sortField = sortMapping[sort] ?? 'timestamp';
    const effectiveLimit = Math.min(limit, 100);

    const whereClause: any = {};

    if (wallet) {
      whereClause.wallet_address = wallet.toLowerCase();
    }

    if (type && ['deposit', 'withdraw'].includes(type)) {
      whereClause.type = type;
    }

    if (min_amount !== undefined) {
      whereClause.amount = {
        gte: min_amount,
      };
    }

    const [transactions, total] = await Promise.all([
      this.prisma.transaction.findMany({
        where: whereClause,
        orderBy: { [sortField]: order },
        skip: offset,
        take: effectiveLimit,
      }),
      this.prisma.transaction.count({ where: whereClause }),
    ]);

    return {
      success: true,
      data: {
        transactions,
        total,
        limit: effectiveLimit,
        offset,
      },
    };
  }

  @Get(':hash')
  async show(@Param('hash') hash: string) {
    const transaction = await this.prisma.transaction.findFirst({
      where: { tx_hash: hash },
    });

    if (!transaction) {
      throw new HttpException(
        {
          success: false,
          message: 'Transaction not found',
        },
        HttpStatus.NOT_FOUND,
      );
    }

    return {
      success: true,
      data: transaction,
    };
  }
}
