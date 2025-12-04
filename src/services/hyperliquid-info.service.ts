import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  HyperLiquidClearinghouseState,
  HyperLiquidOrder,
  HyperLiquidFill,
  HyperLiquidSpotState,
  HyperLiquidMeta,
  HyperLiquidSpotMeta,
} from '../common/interfaces';
import { ProxyRotationService } from './proxy-rotation.service';

@Injectable()
export class HyperLiquidInfoService {
  private readonly logger = new Logger(HyperLiquidInfoService.name);
  private readonly nodeUrl: string;
  private readonly timeout: number;
  private readonly cacheTtl: number;
  private readonly cache: Map<string, { data: any; expiry: number }> = new Map();

  constructor(
    private readonly configService: ConfigService,
    private readonly proxyService: ProxyRotationService,
  ) {
    this.nodeUrl = this.configService.get<string>('HYPERLIQUID_NODE_URL', 'https://api.hyperliquid.xyz');
    this.timeout = this.configService.get<number>('HYPERLIQUID_TIMEOUT', 10000);
    this.cacheTtl = this.configService.get<number>('HYPERLIQUID_CACHE_TTL', 30);

    if (this.proxyService.hasProxies) {
      this.logger.log(`HyperLiquid API using ${this.proxyService.proxyCount} proxies for rotation`);
    }
  }

  private getCacheKey(body: any): string {
    return `hl_info_${JSON.stringify(body)}`;
  }

  private getFromCache(key: string): any | null {
    const cached = this.cache.get(key);
    if (cached && cached.expiry > Date.now()) {
      this.logger.debug(`Cache hit for key: ${key}`);
      return cached.data;
    }
    if (cached) {
      this.cache.delete(key);
    }
    return null;
  }

  private setCache(key: string, data: any, ttl: number): void {
    this.cache.set(key, {
      data,
      expiry: Date.now() + ttl * 1000,
    });
  }

  private async postInfo(body: any, useCache = true, cacheTtl?: number): Promise<any> {
    const cacheKey = this.getCacheKey(body);
    const ttl = cacheTtl ?? this.cacheTtl;

    if (useCache) {
      const cached = this.getFromCache(cacheKey);
      if (cached !== null) {
        return cached;
      }
    }

    try {
      // Use proxy rotation service for API calls (handles 429 errors and rotation automatically)
      const data = await this.proxyService.postWithRotation<any>(`${this.nodeUrl}/info`, body, this.timeout);

      if (data && useCache) {
        this.setCache(cacheKey, data, ttl);
      }

      return data;
    } catch (error) {
      this.logger.error('HyperLiquid API failed', {
        body,
        error: error.message,
      });
      throw error;
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async getMeta(): Promise<HyperLiquidMeta> {
    return this.postInfo({ type: 'meta' }, true, 3600);
  }

  async getMetaAndAssetCtxs(): Promise<any> {
    return this.postInfo({ type: 'metaAndAssetCtxs' }, true, 60);
  }

  async getSpotMeta(): Promise<HyperLiquidSpotMeta> {
    return this.postInfo({ type: 'spotMeta' }, true, 3600);
  }

  async getUserPositions(address: string): Promise<HyperLiquidClearinghouseState> {
    return this.postInfo(
      {
        type: 'clearinghouseState',
        user: address,
      },
      true,
      30,
    );
  }

  async getUserOpenOrders(address: string): Promise<HyperLiquidOrder[]> {
    return this.postInfo(
      {
        type: 'frontendOpenOrders',
        user: address,
      },
      true,
      15,
    );
  }

  async getUserFills(address: string, aggregateByTime = false): Promise<HyperLiquidFill[]> {
    return this.postInfo(
      {
        type: 'userFills',
        user: address,
        aggregateByTime,
      },
      true,
      30,
    );
  }

  async getUserFillsByTime(address: string, startTime: number, endTime?: number, aggregateByTime = false): Promise<HyperLiquidFill[]> {
    const body: any = {
      type: 'userFillsByTime',
      user: address,
      startTime,
      aggregateByTime,
    };

    if (endTime !== undefined) {
      body.endTime = endTime;
    }

    return this.postInfo(body, false);
  }

  async getUserSpotBalances(address: string): Promise<HyperLiquidSpotState> {
    return this.postInfo(
      {
        type: 'spotClearinghouseState',
        user: address,
      },
      true,
      30,
    );
  }

  async getUserLedgerUpdates(address: string, startTime: number, endTime?: number): Promise<any[]> {
    const body: any = {
      type: 'userNonFundingLedgerUpdates',
      user: address,
      startTime,
    };

    if (endTime !== undefined) {
      body.endTime = endTime;
    }

    return this.postInfo(body, false);
  }

  async getUserHistoricalOrders(address: string): Promise<any[]> {
    return this.postInfo(
      {
        type: 'userHistoricalOrders',
        user: address,
      },
      true,
      60,
    );
  }

  async getUserRateLimits(address: string): Promise<any> {
    return this.postInfo(
      {
        type: 'userRateLimit',
        user: address,
      },
      true,
      30,
    );
  }

  async getCompleteProfile(address: string): Promise<any> {
    try {
      const [positions, orders, fills, spotBalances] = await Promise.all([
        this.getUserPositions(address),
        this.getUserOpenOrders(address),
        this.getUserFills(address),
        this.getUserSpotBalances(address),
      ]);

      const accountValue = positions.crossMarginSummary?.accountValue ?? '0';
      const withdrawable = positions.withdrawable ?? '0';
      const totalMarginUsed = positions.crossMarginSummary?.totalMarginUsed ?? '0';

      const activePositions = (positions.assetPositions ?? []).filter((p) => {
        const szi = Math.abs(parseFloat(p.position?.szi ?? '0'));
        return szi > 0.00001;
      });

      return {
        address,
        accountValue,
        withdrawable,
        totalMarginUsed,
        positions: activePositions,
        openOrders: orders,
        recentFills: fills.slice(0, 50),
        spotBalances: spotBalances.balances ?? [],
        positionsCount: activePositions.length,
        ordersCount: orders.length,
        lastUpdated: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error('Failed to get wallet profile', {
        address,
        error: error.message,
      });
      throw error;
    }
  }

  async getBulkWalletData(addresses: string[], delayMs = 100): Promise<Record<string, any>> {
    const results: Record<string, any> = {};

    for (const address of addresses) {
      try {
        results[address] = await this.getCompleteProfile(address);

        if (delayMs > 0 && Object.keys(results).length < addresses.length) {
          await this.sleep(delayMs);
        }
      } catch (error) {
        this.logger.error('Failed to get bulk wallet data', {
          address,
          error: error.message,
        });

        results[address] = {
          error: error.message,
          address,
        };
      }
    }

    return results;
  }

  clearCacheForAddress(address: string): void {
    const types = ['clearinghouseState', 'frontendOpenOrders', 'userFills', 'spotClearinghouseState', 'userHistoricalOrders'];

    for (const type of types) {
      const cacheKey = this.getCacheKey({ type, user: address });
      this.cache.delete(cacheKey);
    }

    this.logger.log('Cleared HyperLiquid cache for address', { address });
  }

  clearAllCache(): void {
    this.cache.clear();
    this.logger.log('Cleared all HyperLiquid cache');
  }
}
