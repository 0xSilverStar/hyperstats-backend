import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios, { AxiosInstance, AxiosRequestConfig } from 'axios';
import { HttpProxyAgent } from 'http-proxy-agent';
import { HttpsProxyAgent } from 'https-proxy-agent';

@Injectable()
export class ProxyRotationService {
  private readonly logger = new Logger(ProxyRotationService.name);
  private readonly proxyHosts: string[] = [];
  private readonly proxyPort: number;
  private readonly proxyUser: string;
  private readonly proxyPassword: string;
  private currentProxyIndex = 0;
  private readonly allProxiesFailedWaitMs = 10000; // 10 seconds

  constructor(private readonly configService: ConfigService) {
    // Load shared proxy settings
    this.proxyPort = this.configService.get<number>('PROXY_PORT', 6010);
    this.proxyUser = this.configService.get<string>('PROXY_USER', '');
    this.proxyPassword = this.configService.get<string>('PROXY_PASSWORD', '');

    this.loadProxyHosts();
  }

  private loadProxyHosts(): void {
    // Load proxy hosts from environment variables
    // Format: PROXY_HOST_1, PROXY_HOST_2, ... (up to 20)
    for (let i = 1; i <= 20; i++) {
      const host = this.configService.get<string>(`PROXY_HOST_${i}`);
      if (host) {
        this.proxyHosts.push(host);
        this.logger.log(`Loaded proxy ${i}: ${host}:${this.proxyPort}`);
      }
    }

    if (this.proxyHosts.length === 0) {
      this.logger.warn('No proxies configured. API calls will be made without proxy.');
    } else {
      this.logger.log(`Loaded ${this.proxyHosts.length} proxies for rotation`);
    }
  }

  get proxyCount(): number {
    return this.proxyHosts.length;
  }

  get hasProxies(): boolean {
    return this.proxyHosts.length > 0;
  }

  private getProxyUrl(host: string): string {
    if (this.proxyUser && this.proxyPassword) {
      return `http://${this.proxyUser}:${this.proxyPassword}@${host}:${this.proxyPort}`;
    }
    return `http://${host}:${this.proxyPort}`;
  }

  private createAxiosClient(proxyHost?: string, timeout = 10000): AxiosInstance {
    const config: AxiosRequestConfig = {
      timeout,
      headers: {
        'Content-Type': 'application/json',
      },
    };

    if (proxyHost) {
      const proxyUrl = this.getProxyUrl(proxyHost);
      // Use both http and https agents to handle both types of requests
      config.httpAgent = new HttpProxyAgent(proxyUrl);
      config.httpsAgent = new HttpsProxyAgent(proxyUrl);
    }

    return axios.create(config);
  }

  private rotateProxy(): void {
    if (this.proxyHosts.length === 0) return;
    this.currentProxyIndex = (this.currentProxyIndex + 1) % this.proxyHosts.length;
    // this.logger.debug(`Rotated to proxy ${this.currentProxyIndex + 1}`);
  }

  private getCurrentProxyHost(): string | undefined {
    if (this.proxyHosts.length === 0) return undefined;
    return this.proxyHosts[this.currentProxyIndex];
  }

  /**
   * Make an API request with proxy rotation
   * - Rotates to next proxy after each successful request
   * - On 429 error, immediately switches to next proxy
   * - If all proxies fail, waits 10 seconds and retries
   */
  async postWithRotation<T>(url: string, body: any, timeout = 10000): Promise<T> {
    const startingProxyIndex = this.currentProxyIndex;
    let attemptCount = 0;
    let lastError: Error | null = null;

    // If no proxies, make direct request
    if (!this.hasProxies) {
      const client = this.createAxiosClient(undefined, timeout);
      const response = await client.post(url, body);
      return response.data;
    }

    while (true) {
      const currentHost = this.getCurrentProxyHost();
      attemptCount++;

      try {
        const client = this.createAxiosClient(currentHost, timeout);
        const response = await client.post(url, body);

        // Rotate to next proxy for next request (load balancing)
        this.rotateProxy();

        return response.data;
      } catch (error) {
        lastError = error;
        const is429 = error.response?.status === 429;
        const isTimeout = error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT';
        const isProxyError = error.code === 'ECONNREFUSED' || error.code === 'ENOTFOUND' || error.code === 'ECONNRESET';

        if (is429 || isTimeout || isProxyError) {
          const errorType = is429 ? '429 Rate Limited' : isTimeout ? 'Timeout' : 'Proxy Error';
          this.logger.warn(`${errorType} on proxy ${this.currentProxyIndex + 1} (${currentHost}), rotating...`);

          this.rotateProxy();

          // Check if we've tried all proxies
          if (this.currentProxyIndex === startingProxyIndex && attemptCount > 1) {
            this.logger.warn(`All ${this.proxyHosts.length} proxies failed. Waiting ${this.allProxiesFailedWaitMs / 1000}s before retry...`);
            await this.sleep(this.allProxiesFailedWaitMs);
            // Reset attempt count to try all proxies again
            attemptCount = 0;
          }
        } else {
          // Non-recoverable error, throw immediately
          throw error;
        }
      }
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
