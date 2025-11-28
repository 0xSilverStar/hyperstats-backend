import { Injectable, NestInterceptor, ExecutionContext, CallHandler } from '@nestjs/common';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

/**
 * Converts BigInt values to strings in API responses
 * This is necessary because JSON.stringify cannot serialize BigInt
 */
@Injectable()
export class BigIntSerializerInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    return next.handle().pipe(map((data) => this.serializeBigInt(data)));
  }

  private serializeBigInt(data: any): any {
    if (data === null || data === undefined) {
      return data;
    }

    if (typeof data === 'bigint') {
      return data.toString();
    }

    if (Array.isArray(data)) {
      return data.map((item) => this.serializeBigInt(item));
    }

    if (typeof data === 'object') {
      // Handle Decimal from Prisma - check by constructor name
      if (data.constructor?.name === 'Decimal') {
        return data.toString();
      }

      // Handle Decimal objects by checking for toString method and characteristic properties
      // Decimal.js objects have s (sign), e (exponent), d (digits array) properties
      if (typeof data.toString === 'function' && typeof data.s === 'number' && typeof data.e === 'number' && Array.isArray(data.d)) {
        return data.toString();
      }

      // Handle Date
      if (data instanceof Date) {
        return data.toISOString();
      }

      const result: any = {};
      for (const key of Object.keys(data)) {
        result[key] = this.serializeBigInt(data[key]);
      }
      return result;
    }

    return data;
  }
}
