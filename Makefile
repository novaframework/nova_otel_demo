DOCKER_DIR := docker
COMPOSE := docker compose -f $(DOCKER_DIR)/docker-compose.yml
BASE_URL ?= http://localhost:8080
DURATION ?= 120
MIN_DELAY ?= 0.1
MAX_DELAY ?= 1.0

.PHONY: up down shell simulate metrics help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

up: ## Start observability stack (Grafana, Collector, Tempo, Mimir)
	$(COMPOSE) up -d
	@echo ""
	@echo "Services:"
	@echo "  Grafana:         http://localhost:3000"
	@echo "  OTel Collector:  http://localhost:4318 (OTLP HTTP)"
	@echo ""
	@echo "Run 'make shell' to start the Nova app"

down: ## Stop observability stack and remove volumes
	$(COMPOSE) down -v

shell: ## Start Nova app in rebar3 shell
	rebar3 shell

simulate: ## Generate traffic (BASE_URL, DURATION, MIN_DELAY, MAX_DELAY)
	@echo "Simulating traffic against $(BASE_URL) for $(DURATION)s"
	@echo "Request interval: $(MIN_DELAY)s - $(MAX_DELAY)s"
	@echo "Press Ctrl+C to stop"
	@echo ""
	@HELLO_OK=0; HELLO_ERR=0; SLOW_OK=0; SLOW_ERR=0; ECHO_OK=0; ECHO_ERR=0; \
	STARTED=$$(date +%s); \
	while true; do \
		ELAPSED=$$(( $$(date +%s) - $$STARTED )); \
		if [ "$$ELAPSED" -ge "$(DURATION)" ]; then break; fi; \
		PICK=$$((RANDOM % 10)); \
		case $$PICK in \
			0|1|2|3) METHOD=GET;  PATH_=/hello;; \
			4|5|6)   METHOD=GET;  PATH_=/slow;; \
			7|8|9)   METHOD=POST; PATH_=/echo;; \
		esac; \
		if [ "$$METHOD" = "POST" ]; then \
			CODE=$$(curl -s -o /dev/null -w "%{http_code}" \
				-X POST -H "Content-Type: application/json" \
				-d '{"message":"hello from simulator"}' \
				"$(BASE_URL)$$PATH_" 2>/dev/null) || CODE="ERR"; \
		else \
			CODE=$$(curl -s -o /dev/null -w "%{http_code}" "$(BASE_URL)$$PATH_" 2>/dev/null) || CODE="ERR"; \
		fi; \
		IS_ERR=0; \
		if [ "$$CODE" = "ERR" ]; then IS_ERR=1; \
		elif [ "$$CODE" -ge 400 ] 2>/dev/null; then IS_ERR=1; fi; \
		case $$PATH_ in \
			/hello) if [ $$IS_ERR -eq 1 ]; then HELLO_ERR=$$((HELLO_ERR+1)); else HELLO_OK=$$((HELLO_OK+1)); fi;; \
			/slow)  if [ $$IS_ERR -eq 1 ]; then SLOW_ERR=$$((SLOW_ERR+1));  else SLOW_OK=$$((SLOW_OK+1));  fi;; \
			/echo)  if [ $$IS_ERR -eq 1 ]; then ECHO_ERR=$$((ECHO_ERR+1));  else ECHO_OK=$$((ECHO_OK+1));  fi;; \
		esac; \
		if [ $$IS_ERR -eq 1 ]; then \
			echo "[$$ELAPSED""s] $$METHOD $$PATH_ -> $$CODE (ERROR)"; \
		else \
			echo "[$$ELAPSED""s] $$METHOD $$PATH_ -> $$CODE"; \
		fi; \
		sleep $$(awk "BEGIN{srand(); printf \"%.2f\", $(MIN_DELAY) + rand() * ($(MAX_DELAY) - $(MIN_DELAY))}"); \
	done; \
	TOTAL=$$((HELLO_OK+HELLO_ERR+SLOW_OK+SLOW_ERR+ECHO_OK+ECHO_ERR)); \
	TOTAL_ERR=$$((HELLO_ERR+SLOW_ERR+ECHO_ERR)); \
	echo ""; \
	echo "=== Traffic Summary ==="; \
	echo "  GET  /hello  $(DURATION)s  ok=$$HELLO_OK err=$$HELLO_ERR"; \
	echo "  GET  /slow   $(DURATION)s  ok=$$SLOW_OK err=$$SLOW_ERR"; \
	echo "  POST /echo   $(DURATION)s  ok=$$ECHO_OK err=$$ECHO_ERR"; \
	echo "  ─────────────────────────────────"; \
	echo "  Total: $$TOTAL  Errors: $$TOTAL_ERR"; \
	echo ""; \
	echo "=== Prometheus Metrics ==="; \
	curl -s localhost:9464/metrics || echo "(metrics endpoint not available)"

metrics: ## Curl the Prometheus metrics endpoint
	@curl -s localhost:9464/metrics
