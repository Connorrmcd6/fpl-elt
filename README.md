# FPL-ETL: Fantasy Premier League Data Pipeline

This project showcases a modern end to end ETL pipeline built on data from the official Fantasy Premier League API. It uses a custom Python client to extract raw FPL data, loads it into a ClickHouse data warehouse via clickhouse-connect, and applies dbt transformations to produce analytics ready datasets.

The focus of the project is on clean architecture, reproducibility, and realistic data engineering patterns.

## Tech Stack

- **Data Ingestion**: Python
- **Data Warehouse**: ClickHouse
- **Data Transformation**: dbt (Data Build Tool)
- **Orchestration & Environment**: Docker & Docker Compose (Airflow to come)

## Project Structure

The project is organized into three main directories:

```
.
├── clickhouse/      # Contains ClickHouse database initialization scripts.
├── dbt/             # Contains all dbt models, tests, and configuration.
└── ingestion/       # Contains the Python script for data extraction and loading.
```

## Data Warehouse Architecture

The dbt models are structured into a layered data architecture, promoting modularity and reusability.

- **`raw`**: Stores the raw, immutable JSON data as strings, exactly as it was ingested from the API.
- **`staging`**: Cleans and prepares the data. This layer maps JSON fields to typed columns, performs basic data cleaning, and ensures data quality.
- **`intermediate`**: Contains more complex transformations, such as joining different staging models to create enriched datasets.
- **`marts`**: The final layer, providing aggregated, denormalized tables ready for business intelligence, analytics, or reporting.

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Running the Pipeline

Follow these steps to run the full ETL pipeline from your terminal.

1.  **Build the Docker Images**
    This command builds the images for the ingestion and dbt services defined in your `docker-compose.yml`.

    ```bash
    docker compose build
    ```

2.  **Start the ClickHouse Service**
    This starts the ClickHouse database container in the background.

    ```bash
    docker compose up -d clickhouse
    ```

3.  **Run the Ingestion Service**
    This runs the Python script to fetch data from the FPL API and load it into the `raw` schema in ClickHouse.

    ```bash
    docker compose run --rm ingestion
    ```

4.  **Run the dbt Transformations**
    This executes all dbt models, transforming the raw data and building the staging, intermediate, and marts layers.

    ```bash
    docker compose run --rm dbt run
    ```

### Cleaning Up

To stop and remove all containers, networks, and volumes created by the project, run:

```bash
docker compose down -v --remove-orphans
```
