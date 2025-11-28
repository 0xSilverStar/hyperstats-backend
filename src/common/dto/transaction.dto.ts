import { IsOptional, IsString, IsNumber, Min, Max, IsIn } from 'class-validator';
import { Type } from 'class-transformer';

export class GetTransactionsDto {
  @IsOptional()
  @IsString()
  wallet?: string;

  @IsOptional()
  @IsString()
  @IsIn(['deposit', 'withdraw'])
  type?: string;

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  min_amount?: number;

  @IsOptional()
  @IsString()
  @IsIn(['timestamp', 'amount', 'block_number', 'wallet_address'])
  sort?: string = 'timestamp';

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
