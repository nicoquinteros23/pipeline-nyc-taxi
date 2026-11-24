"""Punto de entrada único de definiciones Dagster para NYC Taxi."""

from dagster import Definitions

from orchestration.assets import ALL_ASSETS
from orchestration.jobs import nyc_taxi_pipeline

defs = Definitions(
    assets=ALL_ASSETS,
    jobs=[nyc_taxi_pipeline],
)
