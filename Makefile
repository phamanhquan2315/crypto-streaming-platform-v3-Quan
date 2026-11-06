# ============================================================================
#  Crypto Streaming Platform - orchestration
#
#  Typical first run: make up
#  Stop everything: make down
#  Full reset: make clean (drops volumes - wipes all data)
#
#  NOTE: tests are separate from running the platform -> `make test`.
# ============================================================================

.PHONY: help up infra topics producers jobs down clean logs ps test

# compose detection (supports both `docker compose` and `docker-compose`)
COMPOSE := docker compose

help:  ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: infra topics jobs  ## full bring-up: infra -> wait healthy -> topics -> flink jobs
	@echo ""
	@echo "Platform is up. Start all four exchange producers in a separate terminal:"
	@echo "    make producers"
	@echo ""
	@echo "  Flink UI:    http://localhost:8080"
	@echo "  Grafana:     http://localhost:3000"
	@echo "  Prometheus:  http://localhost:9090"
	@echo "  Kafka UI:    http://localhost:8090"

infra: ## start docker services and wait until healthy
	$(COMPOSE) up -d
	@echo "Waiting for core services to become healthy..."
	@$(MAKE) --no-print-directory _wait SVC=kafka-1
	@$(MAKE) --no-print-directory _wait SVC=kafka-2
	@$(MAKE) --no-print-directory _wait SVC=kafka-3
	@$(MAKE) --no-print-directory _wait SVC=clickhouse
	@$(MAKE) --no-print-directory _wait SVC=minio
	@echo "Core services healthy."

# internal: block until a container reports healthy (max ~90s)
_wait:
	@printf "  waiting for %s " "$(SVC)"; \
	for i in $$(seq 1 45); do \
		status=$$(docker inspect -f '{{.State.Health.Status}}' $(SVC) 2>/dev/null || echo "missing"); \
		if [ "$$status" = "healthy" ]; then echo " OK"; exit 0; fi; \
		printf "."; sleep 2; \
	done; \
	echo " TIMEOUT (status=$$status)"; exit 1

topics:  ## create Kafka topics (auto-create is disabled by design)
	@echo "Creating Kafka topics..."
	python scripts/create_topics.py

jobs:  ## submit the 5 Flink jobs (copies shared modules and initializes Iceberg)
	@echo "Submitting Flink jobs..."
	cd flink_jobs && python main.py

# producers:  ## run Binance + Coinbase + OKX + Bitget producers (foreground; Ctrl-C to stop)
# 	cd producers && python main.py
.PHONY: producers producers-logs producers-stop

# Build and run all producers inside Docker
producers:
	docker compose up -d --build producers

# View producer logs
producers-logs:
	docker compose logs -f producers

# Stop producer container gracefully
producers-stop:
	docker compose stop producers

down:  ## stop all services (keeps data volumes)
	$(COMPOSE) down

clean:  ## stop services AND drop volumes (wipes all data)
	$(COMPOSE) down -v

logs:  ## tail logs of all services
	$(COMPOSE) logs -f

ps:  ## show running containers + restart policy
	@docker inspect -f '{{.Name}} -> {{.State.Status}} (restart={{.HostConfig.RestartPolicy.Name}})' $$($(COMPOSE) ps -q) 2>/dev/null

test:  ## run the unit test suite (separate from running the platform)
	pytest
