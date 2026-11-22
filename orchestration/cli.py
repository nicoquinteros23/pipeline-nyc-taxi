"""Adaptador CLI delgado que dispara el job oficial ``nyc_taxi_pipeline`` en Dagster."""

import argparse
from pathlib import Path
import sys

from dagster import DagsterInstance
from dagster._core.instance import InstanceRef

from orchestration import defs
from orchestration.resources import TaxiPipelineConfig, liberar_lock_ejecucion, resolver_meses


def _obtener_dagster_instance(base_dir: str) -> DagsterInstance:
    """Devuelve la instancia Dagster persistente asociada a ``<base_dir>/dagster_home``."""
    dagster_home = (Path(base_dir) / "dagster_home").resolve()
    dagster_home.mkdir(parents=True, exist_ok=True)
    cfg_dst = dagster_home / "dagster.yaml"
    cfg_src = Path(__file__).resolve().parent / "dagster.yaml"
    if cfg_src.is_file() and (not cfg_dst.is_file() or cfg_dst.stat().st_size == 0):
        cfg_dst.write_text(cfg_src.read_text(encoding="utf-8"), encoding="utf-8")
    return DagsterInstance.from_ref(InstanceRef.from_dir(str(dagster_home)))


def construir_run_config(cfg: TaxiPipelineConfig) -> dict:
    """Genera el ``run_config`` uniforme para todos los assets del job."""
    cfg_dict = cfg.model_dump() if hasattr(cfg, "model_dump") else cfg.dict()
    return {
        "ops": {
            a.key.to_user_string(): {"config": dict(cfg_dict)}
            for a in defs.assets
        }
    }


def _mensaje_error(err) -> str:
    """Extrae la cadena de mensajes causales de ``SerializableErrorInfo``."""
    partes: list[str] = []
    actual = err
    while actual is not None:
        msg = (getattr(actual, "message", "") or "").strip()
        if msg:
            partes.append(msg)
        actual = getattr(actual, "cause", None)
    return " | ".join(partes)


def ejecutar_cli(cfg: TaxiPipelineConfig) -> tuple[bool, tuple[str, str], str]:
    """Valida períodos y ejecuta ``nyc_taxi_pipeline`` mediante Dagster."""
    mes_cmp, mes = resolver_meses(cfg)
    job = defs.resolve_job_def("nyc_taxi_pipeline")
    run_config = construir_run_config(cfg)
    with _obtener_dagster_instance(cfg.base_dir) as instance:
        res = job.execute_in_process(
            run_config=run_config,
            instance=instance,
            raise_on_error=False,
        )
        liberar_lock_ejecucion(res.run_id)
        if not res.success:
            errores = [
                _mensaje_error(ev.event_specific_data.error)
                for ev in res.all_events
                if getattr(ev, "is_failure", False)
                and getattr(ev, "event_specific_data", None) is not None
                and getattr(ev.event_specific_data, "error", None) is not None
            ]
            errores = [e for e in errores if e]
            detalle = errores[-1] if errores else f"Ejecución Dagster fallida (run_id={res.run_id})"
            return False, (mes_cmp, mes), detalle
        return True, (mes_cmp, mes), res.run_id


def parsear_args(argv=None) -> TaxiPipelineConfig:
    ap = argparse.ArgumentParser(
        description="Ejecuta el pipeline NYC Taxi oficial a través de Dagster."
    )
    ap.add_argument("--mes", default="2025-03", help="Mes principal en formato AAAA-MM")
    ap.add_argument(
        "--mes-comparacion",
        default="",
        help="Mes de comparación opcional (por defecto mismo mes del año anterior)",
    )
    ap.add_argument(
        "--sin-ingesta",
        action="store_true",
        help="Reutiliza archivos raw ya descargados sin consultar HTTP",
    )
    ap.add_argument(
        "--forzar-dlt",
        action="store_true",
        help="Fuerza la recarga de staging aunque el conteo ya coincida",
    )
    ap.add_argument(
        "--simular-fallo",
        action="store_true",
        help="Dispara un fallo controlado sin modificar datos",
    )
    ap.add_argument("--base-dir", default="data")
    ap.add_argument("--reports-dir", default="reports")
    ap.add_argument("--dbt-dir", default="dbt_project")
    ap.add_argument("--resultados-path", default="RESULTADOS.md")
    args = ap.parse_args(argv)

    return TaxiPipelineConfig(
        mes=args.mes,
        mes_comparacion=args.mes_comparacion,
        base_dir=args.base_dir,
        reports_dir=args.reports_dir,
        dbt_dir=args.dbt_dir,
        resultados_path=args.resultados_path,
        sin_ingesta=args.sin_ingesta,
        forzar_dlt=args.forzar_dlt,
        simular_fallo=args.simular_fallo,
    )


def main(argv=None) -> int:
    try:
        cfg = parsear_args(argv)
        ok, (mes_cmp, mes), info = ejecutar_cli(cfg)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if not ok:
        print(f"ERROR ejecutando pipeline para {mes_cmp} y {mes}: {info}", file=sys.stderr)
        return 1

    print(f"Pipeline Dagster completado OK: {mes_cmp} vs {mes} (run_id={info})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
