"""Schedule mensual demostrativo (deshabilitado por defecto)."""

from dagster import DefaultScheduleStatus, ScheduleDefinition

from orchestration.jobs import nyc_taxi_pipeline

nyc_taxi_monthly_schedule = ScheduleDefinition(
    name="nyc_taxi_monthly_schedule",
    job=nyc_taxi_pipeline,
    cron_schedule="0 6 5 * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Schedule mensual demostrativo (día 5 a las 06:00, deshabilitado por defecto). "
        "Requiere dagster-daemon activo para ejecutarse automáticamente."
    ),
)
