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

from orchestration.resources import TaxiPipelineConfig, resolver_meses, ruta_warehouse
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


@success_hook
def registrar_exito_pipeline(context: HookContext) -> None:
    if context.op.name != "verificacion_final":
        return
    cfg = _extraer_config(context)
    mes_cmp, mes = resolver_meses(cfg)
    wh = ruta_warehouse(Path(cfg.base_dir))
    rec = None
    try:
        rec = reconciliar_fase2(meses=(mes_cmp, mes), warehouse_path=wh, verificar_marts=True)
    except Exception:
        pass
    registrar_ejecucion_metricas(
        exito=True,
        duracion_s=_duracion_run_segundos(context),
        mes=mes,
        mes_cmp=mes_cmp,
        reconciliacion=rec,
        metrics_path=Path(cfg.reports_dir) / "pipeline_metrics.json",
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
        error=f"Fallo en asset '{context.op.name}' (run_id={context.run_id})",
        metrics_path=Path(cfg.reports_dir) / "pipeline_metrics.json",
    )


nyc_taxi_pipeline = define_asset_job(
    name="nyc_taxi_pipeline",
    description=(
        "Ejecuta en serie las 9 etapas de Fase 2 (ingesta -> esquema -> dlt -> calidad -> "
        "intermediate -> reconciliación -> transformación -> dbt build -> verificación final) "
        "protegiendo DuckDB y el límite de 8 GB de RAM."
    ),
    selection=AssetSelection.all(),
    executor_def=in_process_executor,
    hooks={registrar_exito_pipeline, registrar_fallo_pipeline},
)
