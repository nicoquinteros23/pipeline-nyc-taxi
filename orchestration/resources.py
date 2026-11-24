"""Configuración, dbt y control de concurrencia para la orquestación Dagster."""

import atexit
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

from dagster import Config, DagsterRunStatus, RunsFilter
from filelock import FileLock, Timeout

MES_DEFECTO = "2025-03"
_MES_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_ACTIVE_RUN_LOCKS: dict[str, FileLock] = {}


class TaxiPipelineConfig(Config):
    """Parámetros ejecutables desde Dagster UI o CLI."""

    mes: str = MES_DEFECTO
    mes_comparacion: str = ""
    base_dir: str = "data"
    reports_dir: str = "reports"
    dbt_dir: str = "dbt_project"
    resultados_path: str = "RESULTADOS.md"
    sin_ingesta: bool = False
    forzar_dlt: bool = False
    simular_fallo: bool = False


def _validar_formato_mes(mes: str) -> str:
    valor = mes.strip()
    if not _MES_RE.fullmatch(valor):
        raise ValueError(f"Mes inválido: {mes!r} (formato esperado AAAA-MM)")
    anio = int(valor.split("-")[0])
    if anio < 1900:
        raise ValueError(f"Año inválido para cálculo comparativo: {mes!r}")
    return valor


def mes_comparacion(mes: str) -> str:
    """Devuelve el mismo mes del año anterior (``'2025-03' -> '2024-03'``)."""
    valor = _validar_formato_mes(mes)
    anio, mm = valor.split("-")
    anio_ant = int(anio) - 1
    if anio_ant < 1900:
        raise ValueError(f"No hay año anterior válido para {valor!r}")
    return f"{anio_ant:04d}-{mm}"


def resolver_meses(config: TaxiPipelineConfig) -> tuple[str, str]:
    """Valida y devuelve ``(mes_comparacion, mes_principal)`` para cualquier período válido."""
    mes_principal = _validar_formato_mes(config.mes)
    mes_cmp = (
        _validar_formato_mes(config.mes_comparacion)
        if config.mes_comparacion.strip()
        else mes_comparacion(mes_principal)
    )
    if mes_cmp == mes_principal:
        raise ValueError(f"Los períodos a comparar no pueden ser iguales: {mes_principal}")
    return (mes_cmp, mes_principal)


def ruta_warehouse(base_dir: Path) -> Path:
    """Devuelve la ruta única a la base DuckDB principal del proyecto."""
    return Path(base_dir) / "warehouse" / "nyc_taxi_dlt.duckdb"


def obtener_run_id(context) -> str:
    """Obtiene el run_id evitando propiedades deprecadas de Dagster."""
    run = getattr(context, "run", None)
    if run is not None and getattr(run, "run_id", None):
        return str(run.run_id)
    return str(getattr(context, "run_id", "local-run"))


@contextmanager
def adquirir_lock_pipeline(base_dir: Path, timeout: float = 30.0):
    """Serializa accesos de escritura puntuales sobre ``nyc_taxi_dlt.duckdb``."""
    lock_path = Path(base_dir) / "tmp" / "pipeline_duckdb.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(lock_path), timeout=timeout)
    with lock:
        yield


def adquirir_lock_ejecucion(base_dir: Path, run_id: str, timeout: float = 2.0) -> None:
    """Bloquea a nivel de ejecución completa para que CLI y Dagster no se intercalen."""
    if run_id in _ACTIVE_RUN_LOCKS:
        return
    lock_path = Path(base_dir) / "tmp" / "pipeline_run.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(lock_path), timeout=timeout)
    try:
        lock.acquire()
    except Timeout as exc:
        raise RuntimeError(
            f"Ya existe otra ejecución pesada del pipeline en curso (lock en {lock_path})."
        ) from exc
    _ACTIVE_RUN_LOCKS[run_id] = lock


def liberar_lock_ejecucion(run_id: str) -> None:
    """Libera el lock de ejecución completa al finalizar o fallar el run."""
    lock = _ACTIVE_RUN_LOCKS.pop(run_id, None)
    if lock is not None:
        try:
            if lock.is_locked:
                lock.release()
        except Exception:
            pass


@atexit.register
def _liberar_todos_locks_ejecucion() -> None:
    for run_id in list(_ACTIVE_RUN_LOCKS):
        liberar_lock_ejecucion(run_id)


def verificar_sin_otra_ejecucion_activa(context, job_name: str = "nyc_taxi_pipeline") -> None:
    """Impide iniciar el pipeline si otra ejecución pesada de Dagster sigue activa."""
    actual_run_id = obtener_run_id(context)
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
    selected_months: tuple[str, ...] | list[str] | None = None,
) -> None:
    """Ejecuta dbt propagando ``DUCKDB_PATH``, ``zones_csv_path`` y ``selected_months``."""
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
    vars_dict: dict = {}
    if zones_csv_path is not None:
        vars_dict["zones_csv_path"] = Path(zones_csv_path).as_posix()
    if selected_months is not None:
        vars_dict["selected_months"] = list(selected_months)
    if vars_dict:
        cmd.extend(["--vars", json.dumps(vars_dict)])

    env = os.environ.copy()
    if warehouse_path is not None:
        env["DUCKDB_PATH"] = Path(warehouse_path).as_posix()

    proc = subprocess.run(cmd, env=env, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"dbt {subcomando} devolvió código {proc.returncode}")
