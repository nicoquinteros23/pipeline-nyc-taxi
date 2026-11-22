"""Definición del job principal de Dagster con ejecución serial y hooks de métricas."""

import time
from pathlib import Path

from dagster import (
    AssetSelection,
    HookContext,
    define_asset_job,
    failure_hook,
    in_process_executor,
    success_hook,
)

from orchestration.resources import (
    TaxiPipelineConfig,
    adquirir_lock_pipeline,
    resolver_meses,
    ruta_warehouse,
)
from src.metrics_exporter import registrar_ejecucion_metricas
from src.reconciliacion import reconciliar_fase2


def _extraer_config(context: HookContext) -> TaxiPipelineConfig:
    if isinstance(context.op_config, dict):
        return TaxiPipelineConfig(**context.op_config)
    return TaxiPipelineConfig()


def _duracion_run_segundos(context: HookContext) -> float:
    try:
        stats = context.instance.get_run_stats(context.run_id)
        if stats.start_time:
            fin = float(stats.end_time) if stats.end_time else time.time()
            return max(0.0, fin - float(stats.start_time))
    except Exception:
        pass
    return 0.0


def _duraciones_etapas(context: HookContext) -> dict[str, float]:
    """Extrae la duración en segundos de cada paso del run actual en Dagster."""
    try:
        step_stats = context.instance.get_run_step_stats(context.run_id)
        etapas: dict[str, float] = {}
        ahora = time.time()
        for st_step in step_stats:
            if st_step.start_time:
                fin = float(st_step.end_time) if st_step.end_time else ahora
                etapas[str(st_step.step_key)] = round(
                    max(0.0, fin - float(st_step.start_time)), 2
                )
        return etapas
    except Exception:
        return {}


@success_hook
def registrar_exito_pipeline(context: HookContext) -> None:
    if context.op.name != "verificacion_final":
        return
    cfg = _extraer_config(context)
    mes_cmp, mes = resolver_meses(cfg)
    base_dir = Path(cfg.base_dir)
    wh = ruta_warehouse(base_dir)
    rec = None
    try:
        with adquirir_lock_pipeline(base_dir):
            rec = reconciliar_fase2(meses=(mes_cmp, mes), warehouse_path=wh, verificar_marts=True)
    except Exception:
        pass
    registrar_ejecucion_metricas(
        exito=True,
        duracion_s=_duracion_run_segundos(context),
        mes=mes,
        mes_cmp=mes_cmp,
        tiempos_etapas=_duraciones_etapas(context),
        reconciliacion=rec,
        metrics_path=Path(cfg.reports_dir) / "pipeline_metrics.json",
        run_id=context.run_id,
    )


@failure_hook
def registrar_fallo_pipeline(context: HookContext) -> None:
    cfg = _extraer_config(context)
    mes_cmp, mes = resolver_meses(cfg)
    registrar_ejecucion_metricas(
        exito=False,
        duracion_s=_duracion_run_segundos(context),
        mes=mes,
        mes_cmp=mes_cmp,
        tiempos_etapas=_duraciones_etapas(context),
        error=f"Fallo en asset '{context.op.name}' (run_id={context.run_id})",
        metrics_path=Path(cfg.reports_dir) / "pipeline_metrics.json",
        run_id=context.run_id,
    )


nyc_taxi_pipeline = define_asset_job(
    name="nyc_taxi_pipeline",
    description=(
        "Ejecuta en serie las 6 etapas oficiales (ingesta_raw -> carga_dlt_staging -> "
        "calidad_cuarentena -> reconciliacion_staging -> dbt_marts -> verificacion_final) "
        "protegiendo DuckDB y el límite de 8 GB de RAM."
    ),
    selection=AssetSelection.all(),
    executor_def=in_process_executor,
    hooks={registrar_exito_pipeline, registrar_fallo_pipeline},
)
