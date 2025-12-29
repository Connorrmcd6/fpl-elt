# FPL-ELT: A Proof of Concept ELT Pipeline

This project serves as a Proof of Concept (PoC) demonstrating a modern, end-to-end ELT pipeline built on data from the official Fantasy Premier League (FPL) API. It showcases how to integrate Python for data ingestion, ClickHouse as a columnar data warehouse, and dbt for data transformation, all containerized with Docker and Docker Compose.

The focus of the project is on clean architecture, reproducibility, and demonstrating core data engineering patterns in a containerized environment.

## Tech Stack

- **Data Ingestion**: Python
- **Data Warehouse**: ClickHouse
- **Data Transformation**: dbt (Data Build Tool)
- **Orchestration & Environment**: Docker, Docker Compose, and Make

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
- [Make](https://www.gnu.org/software/make/)

### Running the Pipeline with Make

A `Makefile` is provided to simplify running the pipeline.

1.  **Run the Full ELT Pipeline**
    This single command builds the Docker images (if they don't exist), starts the ClickHouse service, waits for it to be healthy, runs the ingestion script, and finally executes the dbt transformations.

    ```bash
    make all
    ```

2.  **Cleaning Up**
    To stop and remove all containers, networks, and volumes created by the project, run:

    ```bash
    make clean
    ```

## Future Improvements

This PoC provides a solid foundation. Future enhancements could include:

- **Incremental Processing**: Implement incremental models in dbt and update the ingestion script to only process new or changed data. This would significantly reduce run times and computational cost on subsequent runs.
- **Dedicated Orchestration**: Integrate a workflow orchestrator like **Apache Airflow** or **Dagster** to manage scheduling, dependencies, retries, and monitoring for a more robust, production-grade pipeline.
- **Data Quality Monitoring**: Expand on the existing dbt tests with more comprehensive data quality checks and implement a tool like Great Expectations for automated data validation and alerting.
- **Enhanced Error Handling**: Improve the Python ingestion client with more robust error handling, such as exponential backoff and retry mechanisms for API requests.
- **Data Visualization**: Integrate an open-source BI tool like **Metabase** or **Apache Superset** to connect to ClickHouse and build interactive dashboards on top of the final data marts.
