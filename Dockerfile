FROM python:3.12-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Evitar archivos .pyc y mostrar logs inmediatamente
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV DBT_PROFILES_DIR=/app/dbt_project
ENV DUCKDB_PATH=data/warehouse/nyc_taxi_dlt.duckdb
ENV DAGSTER_HOME=/app/data/dagster_home

# git requerido por dbt debug
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias (incluye dlt, duckdb, dbt y dagster)
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copiar el código, orquestación, dbt y recursos
COPY src/ ./src/
COPY orchestration/ ./orchestration/
COPY sql/ ./sql/
COPY contracts/ ./contracts/
COPY dbt_project/ ./dbt_project/
COPY docs/ ./docs/
COPY tests/ ./tests/

# Crear directorios de trabajo
RUN mkdir -p data/tmp/duckdb data/dagster_home reports

# Comando de entrada por defecto (Fase 1 compatible; Fase 2/3 usan compose services)
ENTRYPOINT ["python", "-m", "src.pipeline"]

CMD ["--mes", "2025-03"]
