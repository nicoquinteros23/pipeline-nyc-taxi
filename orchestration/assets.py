"""Assets de Dagster del flujo único oficial NYC Taxi (6 etapas)."""

import time
from pathlib import Path

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from orchestration.resources import (
    TaxiPipelineConfig,
    adquirir_lock_pipeline,
    ejecutar_dbt,
    resolver_meses,
    ruta_warehouse,
    verificar_sin_otra_ejecucion_activa,
)
from src.calidad import procesar_mes
from src.carga_dlt import cargar_mes
from src.esquema import validar_mes
from src.ingesta import ingestar, ingestar_zonas
from src.intermediate import crear_intermediate
from src.reconciliacion import imprimir_resumen, reconciliar_fase2


@asset(
    group_name="ingesta",
    description="Ingesta idempotente (o verificación local) y validación de esquema contra contrato YAML.",
)
def ingesta_raw(
    context: AssetExecutionContext, config: TaxiPipelineConfig
) -> Output[dict]:
    verificar_sin_otra_ejecucion_activa(context)
    if config.simular_fallo:
        raise RuntimeError(
            "Fallo controlado de prueba en Dagster (simular_fallo=True); no se alteraron datos reales."
        )

    t0 = time.perf_counter()
    base_dir = Path(config.base_dir)
    reports_dir = Path(config.reports_dir)
    mes_cmp, mes = resolver_meses(config)
    meses = (mes_cmp, mes)

    if not config.sin_ingesta:
        ingestar_zonas(base_dir)
        for m in meses:
            ingestar(m, base_dir)
    else:
        zonas = base_dir / "raw" / "zones" / "taxi_zone_lookup.csv"
        if not zonas.is_file():
            raise FileNotFoundError(f"Falta tabla de zonas: {zonas}")
        for m in meses:
            anio, mm = m.split("-")
            raw = base_dir / "raw" / "yellow" / f"year={anio}" / f"month={mm}" / f"yellow_tripdata_{m}.parquet"
            if not raw.is_file():
                raise FileNotFoundError(f"Falta archivo raw: {raw}")

    estados_esquema = {m: validar_mes(m, base_dir=base_dir, reports_dir=reports_dir)["estado"] for m in meses}
    dur = round(time.perf_counter() - t0, 2)
    context.log.info(f"ingesta_raw OK para {meses} (esquema={estados_esquema}) en {dur}s")
    return Output(
        {"meses": list(meses), "esquema": estados_esquema, "duracion_s": dur},
        metadata={"meses": str(meses), "esquema": MetadataValue.json(estados_esquema), "duracion_s": dur},
    )


@asset(
    deps=[ingesta_raw],
    group_name="staging",
    description="Carga idempotente por lotes Arrow hacia DuckDB (esquema staging) con dlt.",
)
def carga_dlt_staging(
    context: AssetExecutionContext, config: TaxiPipelineConfig
) -> Output[dict]:
    t0 = time.perf_counter()
    base_dir = Path(config.base_dir)
    wh = ruta_warehouse(base_dir)
    meses = resolver_meses(config)

    with adquirir_lock_pipeline(base_dir):
        for m in meses:
            cargar_mes(m, base_dir=base_dir, warehouse_path=wh, forzar=config.forzar_dlt)

    dur = round(time.perf_counter() - t0, 2)
    context.log.info(f"carga_dlt_staging OK en {wh} en {dur}s")
    return Output(
        {"warehouse": wh.as_posix(), "meses": list(meses), "duracion_s": dur},
        metadata={"warehouse": wh.as_posix(), "duracion_s": dur},
    )


@asset(
    deps=[carga_dlt_staging],
    group_name="calidad",
    description="Aplica las 11 reglas de calidad, separa aceptadas/cuarentena y actualiza vistas intermediate.",
)
def calidad_cuarentena(
    context: AssetExecutionContext, config: TaxiPipelineConfig
) -> Output[dict]:
    t0 = time.perf_counter()
    base_dir = Path(config.base_dir)
    reports_dir = Path(config.reports_dir)
    wh = ruta_warehouse(base_dir)
    meses = resolver_meses(config)

    resumen = {}
    with adquirir_lock_pipeline(base_dir):
        for m in meses:
            rep = procesar_mes(m, base_dir=base_dir, reports_dir=reports_dir)
            resumen[m] = {
                "leidas": rep["filas_leidas"],
                "aceptadas": rep["aceptadas"],
                "cuarentena": rep["cuarentena"],
                "reconciliacion": rep["reconciliacion"],
            }
        inter = crear_intermediate(meses=meses, base_dir=base_dir, warehouse_path=wh)

    dur = round(time.perf_counter() - t0, 2)
    context.log.info(f"calidad_cuarentena OK en {dur}s: {resumen}")
    return Output(
        {"resumen": resumen, "intermediate": inter, "duracion_s": dur},
        metadata={
            "resumen": MetadataValue.json(resumen),
            "total_aceptadas": inter["total_aceptadas"],
            "total_cuarentena": inter["total_cuarentena"],
            "duracion_s": dur,
        },
    )


@asset(
    deps=[calidad_cuarentena],
    group_name="calidad",
    description="Reconcilia staging (dlt) == aceptadas + cuarentena (intermediate) por mes y total.",
)
def reconciliacion_staging(
    context: AssetExecutionContext, config: TaxiPipelineConfig
) -> Output[dict]:
    t0 = time.perf_counter()
    base_dir = Path(config.base_dir)
    wh = ruta_warehouse(base_dir)
    meses = resolver_meses(config)

    with adquirir_lock_pipeline(base_dir):
        res = reconciliar_fase2(meses=meses, warehouse_path=wh, verificar_marts=False)
    imprimir_resumen(res)

    dur = round(time.perf_counter() - t0, 2)
    return Output(
        {**res, "duracion_s": dur},
        metadata={
            "staging_total": res["totales"]["staging"],
            "aceptadas_total": res["totales"]["aceptadas"],
            "cuarentena_total": res["totales"]["cuarentena"],
            "duracion_s": dur,
        },
    )


@asset(
    deps=[reconciliacion_staging],
    group_name="dbt",
    description="Ejecuta dbt build (staging, intermediate, marts y 50 tests) sobre nyc_taxi_dlt.duckdb.",
)
def dbt_marts(
    context: AssetExecutionContext, config: TaxiPipelineConfig
) -> Output[dict]:
    t0 = time.perf_counter()
    base_dir = Path(config.base_dir)
    wh = ruta_warehouse(base_dir)
    zonas_csv = base_dir / "raw" / "zones" / "taxi_zone_lookup.csv"

    with adquirir_lock_pipeline(base_dir):
        ejecutar_dbt(
            "build",
            dbt_dir=Path(config.dbt_dir),
            warehouse_path=wh,
            zones_csv_path=zonas_csv,
        )

    dur = round(time.perf_counter() - t0, 2)
    context.log.info(f"dbt_marts OK en {dur}s")
    return Output(
        {"warehouse": wh.as_posix(), "duracion_s": dur},
        metadata={"warehouse": wh.as_posix(), "duracion_s": dur},
    )


@asset(
    deps=[dbt_marts],
    group_name="verificacion",
    description="Reconciliación final integral incluyendo marts.fact_trips y marts.agg_zone_hour.",
)
def verificacion_final(
    context: AssetExecutionContext, config: TaxiPipelineConfig
) -> Output[dict]:
    t0 = time.perf_counter()
    base_dir = Path(config.base_dir)
    wh = ruta_warehouse(base_dir)
    meses = resolver_meses(config)

    with adquirir_lock_pipeline(base_dir):
        res = reconciliar_fase2(meses=meses, warehouse_path=wh, verificar_marts=True)
    imprimir_resumen(res)

    dur = round(time.perf_counter() - t0, 2)
    marts = res["marts"] or {}
    return Output(
        {**res, "duracion_s": dur},
        metadata={
            "staging_total": res["totales"]["staging"],
            "aceptadas_total": res["totales"]["aceptadas"],
            "cuarentena_total": res["totales"]["cuarentena"],
            "marts_fact_trips": marts.get("fact_trips", 0),
            "marts_agg_trips": marts.get("agg_zone_hour_trips", 0),
            "marts_revenue_usd": marts.get("fact_total_revenue", 0.0),
            "duracion_s": dur,
        },
    )


ALL_ASSETS = [
    ingesta_raw,
    carga_dlt_staging,
    calidad_cuarentena,
    reconciliacion_staging,
    dbt_marts,
    verificacion_final,
]
