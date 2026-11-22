"""Orquestador reproducible e idempotente de la Fase 2 (dlt + calidad + intermediate + dbt).

Mantiene intacto ``src/pipeline.py`` y ``data/warehouse/taxi.duckdb`` (Fase 1) y
organiza la ejecución en pasos desacoplados listos para envolver en Dagster.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from src.calidad import procesar_mes
from src.carga_dlt import cargar_mes
from src.esquema import validar_mes
from src.ingesta import ingestar, ingestar_zonas
from src.intermediate import crear_intermediate
from src.metrics_exporter import registrar_ejecucion_metricas
from src.pipeline import ErrorPipeline, _MonitorRam, mes_comparacion
from src.reconciliacion import imprimir_resumen, reconciliar_fase2
from src.transformacion import transformar_mes


def _resolver_dbt() -> str:
    """Encuentra el binario ``dbt`` junto al Python activo o en PATH."""
    scripts_dir = Path(sys.executable).parent
    for nombre in ("dbt.exe", "dbt"):
        candidato = scripts_dir / nombre
        if candidato.is_file():
            return str(candidato)
    encontrado = shutil.which("dbt")
    if encontrado:
        return encontrado
    raise FileNotFoundError("No se encontró el ejecutable 'dbt' en el entorno.")


def ejecutar_dbt(
    subcomando: str = "build",
    dbt_dir: Path = Path("dbt_project"),
    warehouse_path: Path | None = None,
    zones_csv_path: Path | None = None,
) -> None:
    dbt_bin = _resolver_dbt()
    dbt_dir = Path(dbt_dir)
    cmd = [
        dbt_bin,
        subcomando,
        "--project-dir",
        str(dbt_dir),
        "--profiles-dir",
        str(dbt_dir),
    ]
    if zones_csv_path is not None:
        vars_json = json.dumps({"zones_csv_path": Path(zones_csv_path).as_posix()})
        cmd.extend(["--vars", vars_json])

    env = os.environ.copy()
    if warehouse_path is not None:
        env["DUCKDB_PATH"] = Path(warehouse_path).as_posix()

    proc = subprocess.run(cmd, env=env, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"dbt {subcomando} devolvió código {proc.returncode}")


def correr_fase2(
    mes: str = "2025-03",
    mes_cmp: str | None = None,
    base_dir: Path = Path("data"),
    reports_dir: Path = Path("reports"),
    dbt_dir: Path = Path("dbt_project"),
    warehouse_path: Path | None = None,
    ingesta: bool = True,
    forzar_dlt: bool = False,
) -> dict:
    """Ejecuta el flujo completo de Fase 2 para ``mes`` y ``mes_cmp``."""
    base_dir = Path(base_dir)
    reports_dir = Path(reports_dir)
    mes_cmp = mes_cmp or mes_comparacion(mes)
    meses = (mes_cmp, mes)
    warehouse = Path(warehouse_path) if warehouse_path is not None else base_dir / "warehouse" / "nyc_taxi_dlt.duckdb"
    zonas_csv = base_dir / "raw" / "zones" / "taxi_zone_lookup.csv"
    tiempos: dict[str, float] = {}
    reconciliacion_final: dict = {}

    def etapa(nombre: str, funcion) -> None:
        t0 = time.perf_counter()
        try:
            funcion()
        except Exception as exc:
            raise ErrorPipeline(f"etapa '{nombre}' falló: {exc}") from exc
        tiempos[nombre] = round(time.perf_counter() - t0, 1)
        print(f"[fase2:{nombre}] ok en {tiempos[nombre]} s")

    def _ingesta() -> None:
        ingestar_zonas(base_dir)
        for m in meses:
            ingestar(m, base_dir)

    with _MonitorRam() as ram:
        inicio = time.perf_counter()
        try:
            if ingesta:
                etapa("ingesta", _ingesta)
            etapa("esquema", lambda: [validar_mes(m, base_dir, reports_dir) for m in meses])
            etapa(
                "dlt_staging",
                lambda: [
                    cargar_mes(m, base_dir=base_dir, warehouse_path=warehouse, forzar=forzar_dlt)
                    for m in meses
                ],
            )
            etapa("calidad", lambda: [procesar_mes(m, base_dir, reports_dir) for m in meses])
            etapa(
                "intermediate",
                lambda: crear_intermediate(meses=meses, base_dir=base_dir, warehouse_path=warehouse),
            )
            etapa(
                "reconciliacion",
                lambda: reconciliar_fase2(meses=meses, warehouse_path=warehouse, verificar_marts=False),
            )
            etapa("transformacion", lambda: [transformar_mes(m, base_dir) for m in meses])
            etapa(
                "dbt_build",
                lambda: ejecutar_dbt(
                    "build",
                    dbt_dir=dbt_dir,
                    warehouse_path=warehouse,
                    zones_csv_path=zonas_csv,
                ),
            )

            def _verificar_final() -> None:
                nonlocal reconciliacion_final
                reconciliacion_final = reconciliar_fase2(
                    meses=meses, warehouse_path=warehouse, verificar_marts=True
                )
                imprimir_resumen(reconciliacion_final)

            etapa("verificacion_final", _verificar_final)
        except Exception as exc:
            total_err = round(time.perf_counter() - inicio, 1)
            registrar_ejecucion_metricas(
                exito=False,
                duracion_s=total_err,
                mes=mes,
                mes_cmp=mes_cmp,
                pico_ram_gb=round(ram.pico / 1e9, 2),
                tiempos_etapas=tiempos,
                error=str(exc),
                metrics_path=reports_dir / "pipeline_metrics.json",
            )
            raise
        total = round(time.perf_counter() - inicio, 1)

    resumen = {
        "mes": mes,
        "mes_comparacion": mes_cmp,
        "fecha_utc": datetime.now(timezone.utc).isoformat(),
        "warehouse": warehouse.as_posix(),
        "tiempo_total_s": total,
        "tiempos_por_etapa_s": tiempos,
        "pico_ram_gb": round(ram.pico / 1e9, 2),
        "reconciliacion": reconciliacion_final,
    }
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / f"ejecucion_fase2_{mes}.json").write_text(
        json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    registrar_ejecucion_metricas(
        exito=True,
        duracion_s=total,
        mes=mes,
        mes_cmp=mes_cmp,
        pico_ram_gb=resumen["pico_ram_gb"],
        tiempos_etapas=tiempos,
        reconciliacion=reconciliacion_final,
        metrics_path=reports_dir / "pipeline_metrics.json",
    )
    return resumen


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Pipeline completo Fase 2 (dlt + DuckDB + dbt)")
    ap.add_argument("--mes", default="2025-03", help="Mes asignado, formato AAAA-MM")
    ap.add_argument("--mes-comparacion", default=None, help="Por defecto, mismo mes del año anterior")
    ap.add_argument("--forzar-dlt", action="store_true", help="Fuerza recarga de tablas staging en dlt")
    ap.add_argument("--sin-ingesta", action="store_true", help="Omite descarga HTTP (usa archivos raw locales)")
    args = ap.parse_args(argv)
    try:
        r = correr_fase2(
            mes=args.mes,
            mes_cmp=args.mes_comparacion,
            ingesta=not args.sin_ingesta,
            forzar_dlt=args.forzar_dlt,
        )
    except Exception as exc:
        print(f"ERROR Fase 2: {exc}", file=sys.stderr)
        return 1
    print(f"Pipeline Fase 2 completo en {r['tiempo_total_s']} s | pico RAM: {r['pico_ram_gb']} GB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
