import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../prisma/prisma.service';
import { ArbiscanTransaction } from '../common/interfaces';
import { Prisma } from '@prisma/client';

@Injectable()
export class WalletDetectionService {
  private readonly logger = new Logger(WalletDetectionService.name);
  private readonly minWhaleAmount: number;
  private readonly bridgeAddress: string;

  constructor(
    private readonly prisma: PrismaService,
    private readonly configService: ConfigService,
  ) {
    this.minWhaleAmount = this.configService.get<number>('MIN_WHALE_AMOUNT', 10000);
    this.bridgeAddress = this.configService.get<string>('ARBISCAN_BRIDGE_ADDRESS', '0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7').toLowerCase();
  }

  async processTransaction(txData: ArbiscanTransaction): Promise<boolean> {
    const txHash = txData.hash;
    const from = txData.from.toLowerCase();
    const to = txData.to.toLowerCase();
    const value = txData.value;
    const decimals = parseInt(txData.tokenDecimal, 10);
    const blockNumber = parseInt(txData.blockNumber, 10);
    const timestamp = parseInt(txData.timeStamp, 10);

    // Calculate amount in USDC
    const amount = parseFloat(value) / Math.pow(10, decimals);

    // Only process transactions >= minWhaleAmount
    if (amount < this.minWhaleAmount) {
      return false;
    }

    // Determine transaction type and wallet address
    let type: 'deposit' | 'withdraw' | null = null;
    let walletAddress: string | null = null;

    if (to === this.bridgeAddress) {
      type = 'deposit';
      walletAddress = from;
    } else if (from === this.bridgeAddress) {
      type = 'withdraw';
      walletAddress = to;
    } else {
      return false;
    }

    // Check if this specific transfer already exists
    const existingTx = await this.prisma.transaction.findFirst({
      where: {
        tx_hash: txHash,
        from_address: from,
        to_address: to,
      },
    });

    if (existingTx) {
      return false;
    }

    try {
      await this.prisma.$transaction(async (tx) => {
        // Create transaction record
        await tx.transaction.create({
          data: {
            tx_hash: txHash,
            from_address: from,
            to_address: to,
            wallet_address: walletAddress,
            amount: new Prisma.Decimal(amount),
            type,
            block_number: blockNumber,
            timestamp: new Date(timestamp * 1000),
          },
        });

        // Update or create wallet
        const wallet = await tx.wallet.findUnique({
          where: { address: walletAddress },
        });

        if (wallet) {
          const updateData: any = {
            transaction_count: wallet.transaction_count + 1,
            last_activity_at: new Date(timestamp * 1000),
          };

          if (type === 'deposit') {
            updateData.total_deposit = new Prisma.Decimal(wallet.total_deposit.toString()).plus(amount);
          } else {
            updateData.total_withdraw = new Prisma.Decimal(wallet.total_withdraw.toString()).plus(amount);
          }

          await tx.wallet.update({
            where: { address: walletAddress },
            data: updateData,
          });
        } else {
          await tx.wallet.create({
            data: {
              address: walletAddress,
              total_deposit: type === 'deposit' ? new Prisma.Decimal(amount) : new Prisma.Decimal(0),
              total_withdraw: type === 'withdraw' ? new Prisma.Decimal(amount) : new Prisma.Decimal(0),
              transaction_count: 1,
              first_seen_at: new Date(timestamp * 1000),
              last_activity_at: new Date(timestamp * 1000),
            },
          });
        }
      });

      return true;
    } catch (error) {
      this.logger.error(`Error processing transaction ${txHash}: ${error.message}`);
      return false;
    }
  }

  async getWalletStats() {
    const [totalWallets, totalDeposits, totalWithdraws, totalTransactions, topDepositors, topWithdrawers, mostActive] = await Promise.all([
      this.prisma.wallet.count(),
      this.prisma.wallet.aggregate({
        _sum: { total_deposit: true },
      }),
      this.prisma.wallet.aggregate({
        _sum: { total_withdraw: true },
      }),
      this.prisma.transaction.count(),
      this.prisma.wallet.findMany({
        orderBy: { total_deposit: 'desc' },
        take: 10,
      }),
      this.prisma.wallet.findMany({
        orderBy: { total_withdraw: 'desc' },
        take: 10,
      }),
      this.prisma.wallet.findMany({
        orderBy: { transaction_count: 'desc' },
        take: 10,
      }),
    ]);

    return {
      totalWallets,
      totalDeposits: totalDeposits._sum.total_deposit ?? 0,
      totalWithdraws: totalWithdraws._sum.total_withdraw ?? 0,
      totalTransactions,
      topDepositors,
      topWithdrawers,
      mostActive,
    };
  }

  async getWalletDetails(address: string) {
    const normalizedAddress = address.toLowerCase();

    const wallet = await this.prisma.wallet.findUnique({
      where: { address: normalizedAddress },
    });

    if (!wallet) {
      return null;
    }

    const netBalance = parseFloat(wallet.total_deposit.toString()) - parseFloat(wallet.total_withdraw.toString());

    return {
      wallet,
      netBalance,
    };
  }
}
