import { IsOptional, IsString, IsNumber, Min, Max, IsIn } from 'class-validator';
import { Transform, Type } from 'class-transformer';

export class GetWalletsDto {
  @IsOptional()
  @IsString()
  search?: string;

  @IsOptional()
  @IsString()
  @IsIn(['deposit', 'withdraw', 'total_deposit', 'total_withdraw', 'transaction_count', 'last_activity_at', 'address'])
  sort?: string = 'total_deposit';

  @IsOptional()
  @IsString()
  @IsIn(['asc', 'desc'])
  order?: string = 'desc';

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(1)
  @Max(100)
  limit?: number = 50;

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(0)
  offset?: number = 0;
}

export class GetWalletDetailsDto {
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(1)
  @Max(100)
  limit?: number = 20;

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(0)
  offset?: number = 0;
}

export class WalletStatsResponse {
  totalWallets: number;
  totalDeposits: number;
  totalWithdraws: number;
  totalTransactions: number;
  topDepositors: any[];
  topWithdrawers: any[];
  mostActive: any[];
}
