.PHONY: up down restart logs shell build rebuild migrate sync-wallet sync-all sync-blockchain sync-pairs db-studio db-shell clean prune

APP := hyperstats
DC := docker-compose

# Services
up:
	$(DC) up -d

down:
	$(DC) down

restart: down up

logs:
	$(DC) logs -f

logs-app:
	$(DC) logs -f app

ps:
	$(DC) ps

shell:
	docker exec -it hyperstats /bin/sh

# Build
build:
	$(DC) build

rebuild:
	$(DC) build --no-cache

migrate:
	docker exec hyperstats npx prisma db push

migrate-force:
	docker exec hyperstats npx prisma db push --force-reset

# Sync
sync-wallet:
	@test -n "$(ADDRESS)" || (echo "Usage: make sync-wallet ADDRESS=0x..." && exit 1)
	curl -X POST http://localhost:9000/v1/wallets/$(ADDRESS)/sync

sync-all:
	docker exec hyperstats node dist/src/main.js trading:sync

sync-blockchain:
	docker exec hyperstats node dist/src/main.js hyperliquid:sync

sync-blockchain-continuous:
	docker exec -it hyperstats node dist/src/main.js hyperliquid:sync --continuous

sync-pairs:
	docker exec hyperstats node dist/src/main.js pairs:sync

# Database
db-studio:
	docker exec -it hyperstats npx prisma studio

db-shell:
	docker exec -it hyperstats-postgres psql -U hyperstats -d hyperstats

# Cleanup
clean:
	$(DC) down -v --remove-orphans

prune:
	docker system prune -f && docker volume prune -f
