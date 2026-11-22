"""Punto de entrada de definiciones Dagster para NYC Taxi."""

from dagster import Definitions

from orchestration.assets import ALL_ASSETS
from orchestration.jobs import nyc_taxi_pipeline
from orchestration.schedules import nyc_taxi_monthly_schedule

defs = Definitions(
    assets=ALL_ASSETS,
    jobs=[nyc_taxi_pipeline],
    schedules=[nyc_taxi_monthly_schedule],
)
