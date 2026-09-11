.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help up down logs ps test lint fmt demo restart-workers kill-workers psql

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## Start everything (database, API, 2 workers, recipient stub)
	$(COMPOSE) up -d --build --wait
	@echo
	@echo "API   http://localhost:$${API_PORT:-8000}   (docs at /docs)"
	@echo "Stub  http://localhost:$${STUB_PORT:-9000}"

down: ## Stop everything and delete the database volume
	$(COMPOSE) down -v

logs: ## Follow logs from every service
	$(COMPOSE) logs -f

ps: ## Show service status
	$(COMPOSE) ps

test: ## Run the whole test suite (unit + integration + the 10 scenarios) in Docker
	$(COMPOSE) up -d --wait db
	$(COMPOSE) run --rm --build tests pytest -v

lint: ## Check formatting and lint rules
	ruff check .
	ruff format --check .

fmt: ## Reformat
	ruff format .
	ruff check --fix .

demo: ## Walk through the scenarios from section 4 against a running stack
	./scripts/demo.sh

kill-workers: ## Hard-kill the workers (scenario 7)
	$(COMPOSE) kill -s SIGKILL worker

restart-workers: ## Bring the workers back after kill-workers
	$(COMPOSE) up -d --no-deps worker

psql: ## Open a psql shell on the service database
	$(COMPOSE) exec db psql -U intake -d intake
