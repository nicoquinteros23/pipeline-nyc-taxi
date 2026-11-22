"""Configuración, dbt y control de concurrencia para la orquestación Dagster."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

from dagster import Config, DagsterRunStatus, RunsFilter
from filelock import FileLock

MES_DEFECTO = "2025-03"
PERIODOS_SOPORTADOS: tuple[str, str] = ("2024-03", "2025-03")
_MES_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


class TaxiPipelineConfig(Config):
    """Parámetros ejecutables desde Dagster UI o CLI."""

    mes: str = MES_DEFECTO
    mes_comparacion: str = ""
    base_dir: str = "data"
    reports_dir: str = "reports"
    dbt_dir: str = "dbt_project"
    sin_ingesta: bool = True
    forzar_dlt: bool = False
    simular_fallo: bool = False


def _validar_formato_mes(mes: str) -> str:
    valor = mes.strip()
    if not _MES_RE.fullmatch(valor):
        raise ValueError(f"Mes inválido: {mes!r} (formato esperado AAAA-MM)")
    return valor


def mes_comparacion(mes: str) -> str:
    """Devuelve el mismo mes del año anterior (``'2025-03' -> '2024-03'``)."""
    valor = _validar_formato_mes(mes)
    anio, mm = valor.split("-")
    return f"{int(anio) - 1:04d}-{mm}"


def resolver_meses(config: TaxiPipelineConfig) -> tuple[str, str]:
    """Valida y devuelve ``(mes_comparacion, mes_principal)`` alineados con dbt."""
    mes_principal = _validar_formato_mes(config.mes)
    mes_cmp = (
        _validar_formato_mes(config.mes_comparacion)
        if config.mes_comparacion.strip()
        else mes_comparacion(mes_principal)
    )
    if mes_cmp == mes_principal:
        raise ValueError(f"Los períodos a comparar no pueden ser iguales: {mes_principal}")
    if (mes_cmp, mes_principal) != PERIODOS_SOPORTADOS:
        raise ValueError(
            f"Períodos no soportados por los modelos dbt actuales: {(mes_cmp, mes_principal)}. "
            f"Períodos configurados: {PERIODOS_SOPORTADOS}"
        )
    return (mes_cmp, mes_principal)


def ruta_warehouse(base_dir: Path) -> Path:
    """Devuelve la ruta única a la base DuckDB principal del proyecto."""
    return Path(base_dir) / "warehouse" / "nyc_taxi_dlt.duckdb"


@contextmanager
def adquirir_lock_pipeline(base_dir: Path, timeout: float = 30.0):
    """Serializa escrituras sobre ``nyc_taxi_dlt.duckdb`` y capas compartidas."""
    lock_path = Path(base_dir) / "tmp" / "pipeline_duckdb.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(lock_path), timeout=timeout)
    with lock:
        yield


def verificar_sin_otra_ejecucion_activa(context, job_name: str = "nyc_taxi_pipeline") -> None:
    """Impide iniciar el pipeline si otra ejecución pesada de Dagster sigue activa."""
    actual_run_id = (
        context.run.run_id
        if getattr(context, "run", None) is not None
        else getattr(context, "run_id", None)
    )
    runs = context.instance.get_runs(
        filters=RunsFilter(
            job_name=job_name,
            statuses=[DagsterRunStatus.STARTING, DagsterRunStatus.STARTED],
        )
    )
    activos = [r.run_id for r in runs if r.run_id != actual_run_id]
    if activos:
        raise RuntimeError(
            f"Ya existe una ejecución activa de '{job_name}' ({activos[0]}). "
            "Política de concurrencia: máximo una ejecución pesada por vez."
        )


def _resolver_dbt() -> str:
    """Encuentra el ejecutable ``dbt`` junto al Python activo o en PATH."""
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
    """Ejecuta dbt propagando ``DUCKDB_PATH`` y ``zones_csv_path``."""
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
