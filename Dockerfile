FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV DBT_PROFILES_DIR=/app/dbt_project
ENV DUCKDB_PATH=data/warehouse/nyc_taxi_dlt.duckdb
ENV DAGSTER_HOME=/app/data/dagster_home

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY orchestration/ ./orchestration/
COPY sql/ ./sql/
COPY contracts/ ./contracts/
COPY dbt_project/ ./dbt_project/
COPY tests/ ./tests/

RUN mkdir -p data/tmp/duckdb data/dagster_home reports

ENTRYPOINT ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "orchestration.definitions"]
