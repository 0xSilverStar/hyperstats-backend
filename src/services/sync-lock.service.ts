import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import * as os from 'os';

// Default lock timeout in minutes
const DEFAULT_LOCK_TIMEOUT_MINUTES = 60;

@Injectable()
export class SyncLockService {
  private readonly logger = new Logger(SyncLockService.name);
  private readonly hostname: string;

  constructor(private readonly prisma: PrismaService) {
    this.hostname = `${os.hostname()}-${process.pid}`;
  }

  /**
   * Try to acquire a lock for a sync operation
   * Returns true if lock acquired, false if already locked by another process
   */
  async acquireLock(lockName: string, timeoutMinutes: number = DEFAULT_LOCK_TIMEOUT_MINUTES): Promise<boolean> {
    const now = new Date();
    const expiresAt = new Date(now.getTime() + timeoutMinutes * 60 * 1000);

    try {
      // First, clean up any expired locks
      await this.cleanupExpiredLocks();

      // Check if lock exists and is still valid
      const existingLock = await this.prisma.syncLock.findUnique({
        where: { lock_name: lockName },
      });

      if (existingLock) {
        // Check if lock is held by another process and not expired
        if (existingLock.locked_at && existingLock.expires_at) {
          if (existingLock.expires_at > now && existingLock.locked_by !== this.hostname) {
            this.logger.warn(
              `Lock "${lockName}" is held by ${existingLock.locked_by} until ${existingLock.expires_at.toISOString()}`,
            );
            return false;
          }
        }

        // Lock exists but expired or held by us, update it
        await this.prisma.syncLock.update({
          where: { lock_name: lockName },
          data: {
            locked_by: this.hostname,
            locked_at: now,
            expires_at: expiresAt,
          },
        });

        this.logger.log(`Lock "${lockName}" acquired (updated existing) by ${this.hostname}`);
        return true;
      }

      // Lock doesn't exist, create it
      await this.prisma.syncLock.create({
        data: {
          lock_name: lockName,
          locked_by: this.hostname,
          locked_at: now,
          expires_at: expiresAt,
        },
      });

      this.logger.log(`Lock "${lockName}" acquired (new) by ${this.hostname}`);
      return true;
    } catch (error) {
      // Handle race condition - another process might have created the lock
      if (error.code === 'P2002') {
        // Unique constraint violation
        this.logger.warn(`Lock "${lockName}" acquisition failed due to race condition`);
        return false;
      }
      this.logger.error(`Error acquiring lock "${lockName}": ${error.message}`);
      return false;
    }
  }

  /**
   * Release a lock
   */
  async releaseLock(lockName: string): Promise<boolean> {
    try {
      const lock = await this.prisma.syncLock.findUnique({
        where: { lock_name: lockName },
      });

      if (!lock) {
        this.logger.warn(`Lock "${lockName}" not found for release`);
        return false;
      }

      // Only release if we own the lock
      if (lock.locked_by !== this.hostname) {
        this.logger.warn(`Cannot release lock "${lockName}" - owned by ${lock.locked_by}, not ${this.hostname}`);
        return false;
      }

      await this.prisma.syncLock.update({
        where: { lock_name: lockName },
        data: {
          locked_by: null,
          locked_at: null,
          expires_at: null,
        },
      });

      this.logger.log(`Lock "${lockName}" released by ${this.hostname}`);
      return true;
    } catch (error) {
      this.logger.error(`Error releasing lock "${lockName}": ${error.message}`);
      return false;
    }
  }

  /**
   * Check if a lock is currently held
   */
  async isLocked(lockName: string): Promise<boolean> {
    const now = new Date();
    const lock = await this.prisma.syncLock.findUnique({
      where: { lock_name: lockName },
    });

    if (!lock || !lock.locked_at || !lock.expires_at) {
      return false;
    }

    return lock.expires_at > now;
  }

  /**
   * Get lock info
   */
  async getLockInfo(lockName: string): Promise<{
    isLocked: boolean;
    lockedBy: string | null;
    lockedAt: Date | null;
    expiresAt: Date | null;
  } | null> {
    const lock = await this.prisma.syncLock.findUnique({
      where: { lock_name: lockName },
    });

    if (!lock) {
      return null;
    }

    const now = new Date();
    const isLocked = lock.locked_at !== null && lock.expires_at !== null && lock.expires_at > now;

    return {
      isLocked,
      lockedBy: lock.locked_by,
      lockedAt: lock.locked_at,
      expiresAt: lock.expires_at,
    };
  }

  /**
   * Force release a lock (admin function)
   */
  async forceReleaseLock(lockName: string): Promise<boolean> {
    try {
      await this.prisma.syncLock.update({
        where: { lock_name: lockName },
        data: {
          locked_by: null,
          locked_at: null,
          expires_at: null,
        },
      });

      this.logger.log(`Lock "${lockName}" force released`);
      return true;
    } catch (error) {
      this.logger.error(`Error force releasing lock "${lockName}": ${error.message}`);
      return false;
    }
  }

  /**
   * Cleanup expired locks
   */
  private async cleanupExpiredLocks(): Promise<void> {
    const now = new Date();
    await this.prisma.syncLock.updateMany({
      where: {
        expires_at: { lt: now },
        locked_at: { not: null },
      },
      data: {
        locked_by: null,
        locked_at: null,
        expires_at: null,
      },
    });
  }

  /**
   * Extend lock timeout
   */
  async extendLock(lockName: string, additionalMinutes: number = DEFAULT_LOCK_TIMEOUT_MINUTES): Promise<boolean> {
    try {
      const lock = await this.prisma.syncLock.findUnique({
        where: { lock_name: lockName },
      });

      if (!lock || lock.locked_by !== this.hostname) {
        this.logger.warn(`Cannot extend lock "${lockName}" - not owned by this process`);
        return false;
      }

      const newExpiresAt = new Date(Date.now() + additionalMinutes * 60 * 1000);

      await this.prisma.syncLock.update({
        where: { lock_name: lockName },
        data: { expires_at: newExpiresAt },
      });

      this.logger.log(`Lock "${lockName}" extended until ${newExpiresAt.toISOString()}`);
      return true;
    } catch (error) {
      this.logger.error(`Error extending lock "${lockName}": ${error.message}`);
      return false;
    }
  }
}
