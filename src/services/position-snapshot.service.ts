import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { Prisma } from '../../generated/prisma/client';

interface PositionData {
  wallet_address: string;
  coin: string;
  position_size: number;
  entry_price: number;
  mark_price: number;
  unrealized_pnl: number;
  leverage: number;
  margin_used: number;
  side: string;
  position_value: number;
  liquidation_price: number | null;
}

interface PositionChangeData {
  wallet_address: string;
  coin: string;
  change_type: 'open_long' | 'close_long' | 'open_short' | 'close_short' | 'increase' | 'decrease';
  side: string;
  size_before: number | null;
  size_after: number | null;
  size_change: number;
  value_change: number;
  entry_price: number | null;
  mark_price: number | null;
  realized_pnl: number | null;
  leverage: number | null;
}

@Injectable()
export class PositionSnapshotService {
  private readonly logger = new Logger(PositionSnapshotService.name);

  constructor(private readonly prisma: PrismaService) {}

  /**
   * Take a snapshot of all current positions
   */
  async takeSnapshot(): Promise<{ snapshotsCreated: number; changesDetected: number }> {
    const startTime = Date.now();
    this.logger.log('Starting position snapshot...');

    // Get all current positions
    const positions = await this.prisma.position.findMany();

    if (positions.length === 0) {
      this.logger.log('No positions to snapshot');
      return { snapshotsCreated: 0, changesDetected: 0 };
    }

    // Get the latest snapshot timestamp for comparison
    const latestSnapshot = await this.prisma.positionSnapshot.findFirst({
      orderBy: { snapshot_at: 'desc' },
      select: { snapshot_at: true },
    });

    // Get previous snapshots for comparison (if exists)
    let previousSnapshots: Map<string, PositionData> = new Map();
    if (latestSnapshot) {
      const prevSnaps = await this.prisma.positionSnapshot.findMany({
        where: { snapshot_at: latestSnapshot.snapshot_at },
      });
      for (const snap of prevSnaps) {
        const key = `${snap.wallet_address}:${snap.coin}`;
        previousSnapshots.set(key, {
          wallet_address: snap.wallet_address,
          coin: snap.coin,
          position_size: parseFloat(snap.position_size.toString()),
          entry_price: parseFloat(snap.entry_price.toString()),
          mark_price: parseFloat(snap.mark_price.toString()),
          unrealized_pnl: parseFloat(snap.unrealized_pnl.toString()),
          leverage: snap.leverage,
          margin_used: parseFloat(snap.margin_used.toString()),
          side: snap.side,
          position_value: parseFloat(snap.position_value.toString()),
          liquidation_price: snap.liquidation_price ? parseFloat(snap.liquidation_price.toString()) : null,
        });
      }
    }

    const snapshotTime = new Date();
    const changes: PositionChangeData[] = [];

    // Create snapshots and detect changes
    const snapshots: Prisma.PositionSnapshotCreateManyInput[] = [];

    for (const position of positions) {
      const positionSize = parseFloat(position.position_size.toString());
      if (Math.abs(positionSize) < 0.00001) continue;

      const key = `${position.wallet_address}:${position.coin}`;
      const prevPosition = previousSnapshots.get(key);

      // Create snapshot
      snapshots.push({
        wallet_address: position.wallet_address,
        coin: position.coin,
        position_size: position.position_size,
        entry_price: position.entry_price,
        mark_price: position.mark_price,
        unrealized_pnl: position.unrealized_pnl,
        leverage: position.leverage,
        margin_used: position.margin_used,
        side: position.side,
        position_value: position.position_value,
        liquidation_price: position.liquidation_price,
        snapshot_at: snapshotTime,
      });

      // Detect changes
      if (!prevPosition) {
        // New position opened
        const changeType = position.side === 'long' ? 'open_long' : 'open_short';
        changes.push({
          wallet_address: position.wallet_address,
          coin: position.coin,
          change_type: changeType,
          side: position.side,
          size_before: null,
          size_after: positionSize,
          size_change: positionSize,
          value_change: parseFloat(position.position_value.toString()),
          entry_price: parseFloat(position.entry_price.toString()),
          mark_price: parseFloat(position.mark_price.toString()),
          realized_pnl: null,
          leverage: position.leverage,
        });
      } else {
        // Position size changed
        const sizeDiff = positionSize - prevPosition.position_size;
        if (Math.abs(sizeDiff) > 0.00001) {
          const isIncrease = Math.abs(positionSize) > Math.abs(prevPosition.position_size);
          changes.push({
            wallet_address: position.wallet_address,
            coin: position.coin,
            change_type: isIncrease ? 'increase' : 'decrease',
            side: position.side,
            size_before: prevPosition.position_size,
            size_after: positionSize,
            size_change: sizeDiff,
            value_change: parseFloat(position.position_value.toString()) - prevPosition.position_value,
            entry_price: parseFloat(position.entry_price.toString()),
            mark_price: parseFloat(position.mark_price.toString()),
            realized_pnl: null,
            leverage: position.leverage,
          });
        }
      }

      // Remove from previous to track closed positions
      previousSnapshots.delete(key);
    }

    // Detect closed positions (remaining in previousSnapshots)
    for (const [key, prevPosition] of previousSnapshots) {
      const changeType = prevPosition.side === 'long' ? 'close_long' : 'close_short';
      changes.push({
        wallet_address: prevPosition.wallet_address,
        coin: prevPosition.coin,
        change_type: changeType,
        side: prevPosition.side,
        size_before: prevPosition.position_size,
        size_after: 0,
        size_change: -prevPosition.position_size,
        value_change: -prevPosition.position_value,
        entry_price: prevPosition.entry_price,
        mark_price: prevPosition.mark_price,
        realized_pnl: prevPosition.unrealized_pnl, // Use last unrealized as approximate realized
        leverage: prevPosition.leverage,
      });
    }

    // Save snapshots in batch
    if (snapshots.length > 0) {
      await this.prisma.positionSnapshot.createMany({
        data: snapshots,
      });
    }

    // Save changes
    if (changes.length > 0) {
      await this.prisma.positionChange.createMany({
        data: changes.map((c) => ({
          wallet_address: c.wallet_address,
          coin: c.coin,
          change_type: c.change_type,
          side: c.side,
          size_before: c.size_before !== null ? new Prisma.Decimal(c.size_before) : null,
          size_after: c.size_after !== null ? new Prisma.Decimal(c.size_after) : null,
          size_change: new Prisma.Decimal(c.size_change),
          value_change: new Prisma.Decimal(c.value_change),
          entry_price: c.entry_price !== null ? new Prisma.Decimal(c.entry_price) : null,
          mark_price: c.mark_price !== null ? new Prisma.Decimal(c.mark_price) : null,
          realized_pnl: c.realized_pnl !== null ? new Prisma.Decimal(c.realized_pnl) : null,
          leverage: c.leverage,
          detected_at: snapshotTime,
        })),
      });
    }

    const duration = Date.now() - startTime;
    this.logger.log(`Snapshot completed: ${snapshots.length} positions, ${changes.length} changes detected (${duration}ms)`);

    return { snapshotsCreated: snapshots.length, changesDetected: changes.length };
  }

  /**
   * Get recent position changes with pagination
   */
  async getPositionChanges(options: {
    limit?: number;
    offset?: number;
    walletAddress?: string;
    coin?: string;
    changeType?: string;
    since?: Date;
  }) {
    const { limit = 50, offset = 0, walletAddress, coin, changeType, since } = options;

    const where: Prisma.PositionChangeWhereInput = {};
    if (walletAddress) where.wallet_address = walletAddress.toLowerCase();
    if (coin) where.coin = coin;
    if (changeType) where.change_type = changeType;
    if (since) where.detected_at = { gte: since };

    const [changes, total] = await Promise.all([
      this.prisma.positionChange.findMany({
        where,
        orderBy: { detected_at: 'desc' },
        take: limit,
        skip: offset,
      }),
      this.prisma.positionChange.count({ where }),
    ]);

    return {
      changes: changes.map((c) => ({
        id: c.id,
        walletAddress: c.wallet_address,
        coin: c.coin,
        changeType: c.change_type,
        side: c.side,
        sizeBefore: c.size_before?.toString() ?? null,
        sizeAfter: c.size_after?.toString() ?? null,
        sizeChange: c.size_change.toString(),
        valueChange: c.value_change.toString(),
        entryPrice: c.entry_price?.toString() ?? null,
        markPrice: c.mark_price?.toString() ?? null,
        realizedPnl: c.realized_pnl?.toString() ?? null,
        leverage: c.leverage,
        detectedAt: c.detected_at.toISOString(),
      })),
      pagination: {
        total,
        limit,
        offset,
        hasMore: offset + limit < total,
      },
    };
  }

  /**
   * Get position history for a wallet
   */
  async getPositionHistory(walletAddress: string, coin?: string, limit = 100) {
    const where: Prisma.PositionSnapshotWhereInput = {
      wallet_address: walletAddress.toLowerCase(),
    };
    if (coin) where.coin = coin;

    const snapshots = await this.prisma.positionSnapshot.findMany({
      where,
      orderBy: { snapshot_at: 'desc' },
      take: limit,
    });

    return snapshots.map((s) => ({
      coin: s.coin,
      positionSize: s.position_size.toString(),
      entryPrice: s.entry_price.toString(),
      markPrice: s.mark_price.toString(),
      unrealizedPnl: s.unrealized_pnl.toString(),
      leverage: s.leverage,
      marginUsed: s.margin_used.toString(),
      side: s.side,
      positionValue: s.position_value.toString(),
      liquidationPrice: s.liquidation_price?.toString() ?? null,
      snapshotAt: s.snapshot_at.toISOString(),
    }));
  }

  /**
   * Get PnL history from snapshots for chart data
   */
  async getPnlHistory(walletAddress: string, period: '1h' | '24h' | '7d' | '30d' = '24h') {
    const periodMs: Record<string, number> = {
      '1h': 60 * 60 * 1000,
      '24h': 24 * 60 * 60 * 1000,
      '7d': 7 * 24 * 60 * 60 * 1000,
      '30d': 30 * 24 * 60 * 60 * 1000,
    };

    const since = new Date(Date.now() - periodMs[period]);

    const snapshots = await this.prisma.positionSnapshot.findMany({
      where: {
        wallet_address: walletAddress.toLowerCase(),
        snapshot_at: { gte: since },
      },
      orderBy: { snapshot_at: 'asc' },
    });

    // Group by snapshot time and sum unrealized PnL
    const pnlByTime = new Map<string, number>();
    for (const snap of snapshots) {
      const timeKey = snap.snapshot_at.toISOString();
      const current = pnlByTime.get(timeKey) || 0;
      pnlByTime.set(timeKey, current + parseFloat(snap.unrealized_pnl.toString()));
    }

    return Array.from(pnlByTime.entries()).map(([time, pnl]) => ({
      time,
      unrealizedPnl: pnl,
    }));
  }

  /**
   * Clean up old snapshots (keep last 7 days)
   */
  async cleanupOldSnapshots(daysToKeep = 7): Promise<number> {
    const cutoffDate = new Date(Date.now() - daysToKeep * 24 * 60 * 60 * 1000);

    const result = await this.prisma.positionSnapshot.deleteMany({
      where: { snapshot_at: { lt: cutoffDate } },
    });

    if (result.count > 0) {
      this.logger.log(`Cleaned up ${result.count} old snapshots`);
    }

    return result.count;
  }

  /**
   * Clean up old position changes (keep last 30 days)
   */
  async cleanupOldChanges(daysToKeep = 30): Promise<number> {
    const cutoffDate = new Date(Date.now() - daysToKeep * 24 * 60 * 60 * 1000);

    const result = await this.prisma.positionChange.deleteMany({
      where: { detected_at: { lt: cutoffDate } },
    });

    if (result.count > 0) {
      this.logger.log(`Cleaned up ${result.count} old position changes`);
    }

    return result.count;
  }
}
