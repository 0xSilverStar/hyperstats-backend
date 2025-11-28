import { Injectable } from '@nestjs/common';

@Injectable()
export class WalletSyncLockService {
  private locks = new Map<string, Promise<void>>();

  async acquireLock<T>(address: string, fn: () => Promise<T>): Promise<T> {
    const key = address.toLowerCase();

    // If there's an existing lock, wait for it
    const existingLock = this.locks.get(key);
    if (existingLock) {
      await existingLock;
      // After waiting, the data should be fresh, so just return without re-running
      return fn();
    }

    // Create new lock
    let resolve: () => void;
    const lockPromise = new Promise<void>((r) => {
      resolve = r;
    });
    this.locks.set(key, lockPromise);

    try {
      return await fn();
    } finally {
      this.locks.delete(key);
      resolve!();
    }
  }

  isLocked(address: string): boolean {
    return this.locks.has(address.toLowerCase());
  }
}
