FROM python:3.11-slim

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-core dbt-clickhouse

WORKDIR /app

ENTRYPOINT [ "dbt" ]