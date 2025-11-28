# ============================================
# HyperStats Backend Makefile
# ============================================
# Usage:
#   make help                     - Show this help
#   make up                       - Start all services
#   make down                     - Stop all services
#   make sync-wallet ADDRESS=0x.. - Sync all data for a wallet
#   make sync-all                 - Sync all wallets (trading data)
#   make sync-blockchain          - Sync blockchain (whale detection)
# ============================================

.PHONY: help up down restart logs shell build rebuild migrate \
        sync-wallet sync-all sync-blockchain sync-pairs \
        ps clean prune

# Default target
.DEFAULT_GOAL := help

# Container names
APP_CONTAINER := hyperstats
COMPOSE := docker-compose

# ============================================
# Help
# ============================================
help:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║           HyperStats Backend - Makefile Commands             ║"
	@echo "╠══════════════════════════════════════════════════════════════╣"
	@echo "║ Service Commands:                                            ║"
	@echo "║   make up              - Start all services                  ║"
	@echo "║   make down            - Stop all services                   ║"
	@echo "║   make restart         - Restart all services                ║"
	@echo "║   make logs            - View logs (follow mode)             ║"
	@echo "║   make ps              - Show running containers             ║"
	@echo "║   make shell           - Open shell in app container         ║"
	@echo "╠══════════════════════════════════════════════════════════════╣"
	@echo "║ Build Commands:                                              ║"
	@echo "║   make build           - Build Docker images                 ║"
	@echo "║   make rebuild         - Rebuild without cache               ║"
	@echo "║   make migrate         - Run database migrations             ║"
	@echo "╠══════════════════════════════════════════════════════════════╣"
	@echo "║ Sync Commands:                                               ║"
	@echo "║   make sync-wallet ADDRESS=0x...                             ║"
	@echo "║                        - Sync all data for specific wallet   ║"
	@echo "║   make sync-all        - Sync all wallets (daily job)        ║"
	@echo "║   make sync-blockchain - Sync blockchain for whale detection ║"
	@echo "║   make sync-blockchain-continuous                            ║"
	@echo "║                        - Continuous blockchain monitoring    ║"
	@echo "║   make sync-pairs      - Sync trading pairs metadata         ║"
	@echo "╠══════════════════════════════════════════════════════════════╣"
	@echo "║ Cleanup Commands:                                            ║"
	@echo "║   make clean           - Stop and remove containers          ║"
	@echo "║   make prune           - Remove unused Docker resources      ║"
	@echo "╚══════════════════════════════════════════════════════════════╝"

# ============================================
# Service Commands
# ============================================
up:
	@echo "Starting services..."
	$(COMPOSE) up -d
	@echo "Services started. Run 'make logs' to view logs."

down:
	@echo "Stopping services..."
	$(COMPOSE) down

restart: down up

logs:
	$(COMPOSE) logs -f

logs-app:
	$(COMPOSE) logs -f app

ps:
	$(COMPOSE) ps

shell:
	docker exec -it $(APP_CONTAINER) /bin/sh

# ============================================
# Build Commands
# ============================================
build:
	$(COMPOSE) build

rebuild:
	$(COMPOSE) build --no-cache

migrate:
	@echo "Running database migrations..."
	$(COMPOSE) --profile migrate run --rm migrate
	@echo "Migrations completed."

# ============================================
# Sync Commands
# ============================================

# Sync all data for a specific wallet address
# Usage: make sync-wallet ADDRESS=0x1234...
sync-wallet:
ifndef ADDRESS
	@echo "Error: ADDRESS is required"
	@echo "Usage: make sync-wallet ADDRESS=0x1234..."
	@exit 1
endif
	@echo "Syncing all data for wallet: $(ADDRESS)"
	@echo "This will sync: positions, orders, fills, balances, ledger"
	docker exec $(APP_CONTAINER) node -e "\
		const { NestFactory } = require('@nestjs/core'); \
		const { AppModule } = require('./dist/app.module'); \
		async function run() { \
			const app = await NestFactory.createApplicationContext(AppModule); \
			const tradingService = app.get('TradingDataService'); \
			const ledgerService = app.get('LedgerSyncService'); \
			const fillService = app.get('FillSyncService'); \
			const addr = '$(ADDRESS)'.toLowerCase(); \
			console.log('Starting sync for:', addr); \
			await Promise.all([ \
				tradingService.fullSync(addr), \
				ledgerService.syncLedgerUpdates(addr, true), \
				fillService.syncFills(addr, true) \
			]); \
			console.log('Sync completed for:', addr); \
			await app.close(); \
		} \
		run().catch(console.error); \
	"

# Alternative simpler approach using API endpoint
sync-wallet-api:
ifndef ADDRESS
	@echo "Error: ADDRESS is required"
	@echo "Usage: make sync-wallet-api ADDRESS=0x1234..."
	@exit 1
endif
	@echo "Triggering sync via API for wallet: $(ADDRESS)"
	curl -X POST http://localhost:8001/v1/wallets/$(ADDRESS)/sync
	@echo ""

# Sync all wallets (trading data) - runs the trading:sync CLI command
sync-all:
	@echo "Syncing all wallets trading data..."
	docker exec $(APP_CONTAINER) node dist/main.js trading:sync

# Sync blockchain for whale detection
sync-blockchain:
	@echo "Running blockchain sync (one-time)..."
	docker exec $(APP_CONTAINER) node dist/main.js hyperliquid:sync

# Continuous blockchain monitoring
sync-blockchain-continuous:
	@echo "Starting continuous blockchain monitoring..."
	@echo "Press Ctrl+C to stop"
	docker exec -it $(APP_CONTAINER) node dist/main.js hyperliquid:sync --continuous

# Sync trading pairs metadata
sync-pairs:
	@echo "Syncing trading pairs..."
	docker exec $(APP_CONTAINER) node dist/main.js pairs:sync

# ============================================
# Database Commands
# ============================================
db-studio:
	@echo "Opening Prisma Studio..."
	@echo "Note: This requires the app to be running with exposed port"
	docker exec -it $(APP_CONTAINER) npx prisma studio

db-shell:
	docker exec -it hyperstats-postgres psql -U hyperstats -d hyperstats

# ============================================
# Cleanup Commands
# ============================================
clean:
	@echo "Stopping and removing containers..."
	$(COMPOSE) down -v --remove-orphans

prune:
	@echo "Removing unused Docker resources..."
	docker system prune -f
	docker volume prune -f

# ============================================
# Development Commands (local only)
# ============================================
dev-up:
	$(COMPOSE) up -d postgres redis
	@echo "Database and Redis started. Run 'npm run start:dev' locally."

dev-down:
	$(COMPOSE) stop postgres redis

# ============================================
# Status and Health Check
# ============================================
status:
	@echo "=== Container Status ==="
	$(COMPOSE) ps
	@echo ""
	@echo "=== Health Check ==="
	@curl -s http://localhost:8001/v1/sync-status | head -c 500 || echo "API not responding"
	@echo ""

health:
	@curl -s http://localhost:8001/v1/sync-status && echo "API is healthy" || echo "API is not responding"
