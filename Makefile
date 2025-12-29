.PHONY: all start-clickhouse wait-ingestion run-ingestion run-dbt clean

all: start-clickhouse wait-ingestion run-ingestion run-dbt

start-clickhouse:
	docker compose up -d clickhouse

wait-ingestion:
	@echo "Waiting for ClickHouse to initialize..."
	@sleep 10  # Adjust the sleep time as necessary

run-ingestion:
	docker compose run --rm ingestion

run-dbt:
	docker compose run --rm dbt run

clean:
	docker compose down -v --remove-orphans