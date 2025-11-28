# HyperStats Backend

NestJS API server for HyperStats - a trading analytics platform for Hyperliquid. This backend tracks whale transactions on the HyperLiquid Bridge and provides trading data APIs.

## Features

- **Whale Transaction Tracking**: Monitor USDC deposits/withdrawals on HyperLiquid Bridge (≥10,000 USDC)
- **Trading Data APIs**: Positions, orders, fills, balances for any wallet address
- **30-Minute Caching**: Database-level caching to reduce API calls
- **Background Job Processing**: Bull Queue for async database operations
- **Daily Wallet Sync**: Cron job to sync all wallets with 1-second intervals
- **CLI Commands**: Artisan-like commands for blockchain sync and data management

## Tech Stack

- **Framework**: NestJS 11
- **Database**: SQLite with Prisma ORM
- **Queue**: Bull (Redis-backed)
- **Scheduler**: @nestjs/schedule
- **HTTP Client**: Axios

## Project Structure

```
src/
├── commands/           # CLI commands (nest-commander)
│   ├── hyperliquid-sync.command.ts
│   ├── trading-sync.command.ts
│   └── pairs-sync.command.ts
├── controllers/        # API endpoints
│   ├── wallet.controller.ts
│   ├── transaction.controller.ts
│   ├── sync-status.controller.ts
│   └── trading.controller.ts
├── services/           # Business logic
│   ├── hyperliquid-info.service.ts
│   ├── arbitrum-sync.service.ts
│   ├── wallet-detection.service.ts
│   └── trading-data.service.ts
├── queue/              # Background job processing
│   ├── queue.module.ts
│   └── trading-data.processor.ts
├── scheduler/          # Cron jobs
│   └── wallet-sync.scheduler.ts
├── prisma/             # Database
│   └── prisma.service.ts
└── common/             # Shared DTOs, interfaces
    ├── dto/
    └── interfaces/
```

## Installation

```bash
# Install dependencies
npm install

# Generate Prisma client
npx prisma generate

# Create database (SQLite)
npx prisma db push
```

## Configuration

Create a `.env` file based on `.env.example`:

```env
# Database
DATABASE_URL="file:./dev.db"

# Server
PORT=8001

# HyperLiquid API
HYPERLIQUID_NODE_URL=https://api.hyperliquid.xyz
HYPERLIQUID_MAINNET_URL=https://api.hyperliquid.xyz
HYPERLIQUID_TIMEOUT=10000
HYPERLIQUID_MAX_RETRIES=3
HYPERLIQUID_CACHE_TTL=30

# Arbiscan API
ARBISCAN_BASE_URL=https://api.etherscan.io/v2/api
ARBISCAN_API_KEY=your_api_key_here
ARBISCAN_CHAIN_ID=42161
ARBISCAN_USDC_CONTRACT=0xaf88d065e77c8cC2239327C5EDb3A432268e5831
ARBISCAN_BRIDGE_ADDRESS=0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7

# Whale Detection
MIN_WHALE_AMOUNT=10000

# Redis (for Bull Queue)
REDIS_HOST=localhost
REDIS_PORT=6379
```

## Running the Application

```bash
# Development
npm run start:dev

# Production
npm run build
npm run start:prod
```

## CLI Commands

```bash
# Sync blockchain transactions (one-time)
npm run hyperliquid:sync

# Sync blockchain transactions (continuous monitoring)
npm run hyperliquid:sync-continuous

# Sync trading data for all wallets
npm run trading:sync

# Sync trading pairs metadata
npm run pairs:sync
```

## API Endpoints

### Wallets

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/wallets` | List whale wallets |
| GET | `/v1/wallets/stats` | Wallet statistics |
| GET | `/v1/wallets/:address` | Get wallet details |

### Transactions

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/transactions` | List transactions |
| GET | `/v1/transactions/:hash` | Get transaction by hash |

### Sync Status

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/sync-status` | Get blockchain sync status |

### Trading Data

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/wallets/positions/summary` | Positions summary across all wallets |
| GET | `/v1/wallets/activity/coin/:coin` | Activity for specific coin |
| GET | `/v1/wallets/:address/positions` | Get wallet positions |
| GET | `/v1/wallets/:address/orders` | Get wallet open orders |
| GET | `/v1/wallets/:address/fills` | Get wallet trade fills |
| GET | `/v1/wallets/:address/balances` | Get wallet spot balances |
| GET | `/v1/wallets/:address/profile` | Get complete wallet profile |
| GET | `/v1/wallets/:address/ledger` | Get ledger updates |
| POST | `/v1/wallets/:address/sync` | Force sync wallet data |

## Database Schema

### Core Tables

- **wallets** - Whale wallet information
- **transactions** - Bridge transactions (≥10,000 USDC)
- **sync_status** - Blockchain sync progress

### Trading Tables

- **positions** - Trader positions
- **orders** - Open orders
- **fills** - Trade fills
- **balances** - Spot balances
- **trading_pairs** - Trading pair metadata

### Cache Table

- **cache** - Database-level caching for trading data (30-minute TTL)

## Caching Strategy

1. **API Request** → Check cache table for valid entry (< 30 minutes)
2. **Cache Hit** → Return data from database
3. **Cache Miss** → Fetch from HyperLiquid API, queue background save, return immediately
4. **Background Job** → Save data to database, update cache timestamp

## Daily Cron Job

The `WalletSyncScheduler` runs at midnight daily:
- Iterates through all wallets
- Queues each wallet for full sync
- 1-second interval between wallets
- Supports manual trigger and abort

## Development

```bash
# Run tests
npm run test

# Run tests with coverage
npm run test:cov

# Lint code
npm run lint

# Format code
npm run format

# Prisma Studio (database GUI)
npm run prisma:studio
```

## Migration from Laravel

This NestJS backend is a migration from the original Laravel backend. The database schema is fully compatible - you can use the same SQLite database from Laravel.

Key differences:
- Laravel Artisan commands → NestJS CLI commands (nest-commander)
- Laravel Jobs → Bull Queue
- Laravel Cache → Database Cache model
- Laravel Scheduler → @nestjs/schedule

## License

UNLICENSED
