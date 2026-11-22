"""Configuración y control de concurrencia para la orquestación Dagster."""

from contextlib import contextmanager
from pathlib import Path

from dagster import Config
from filelock import FileLock

from src.pipeline import mes_comparacion


class TaxiPipelineConfig(Config):
    """Parámetros ejecutables desde la interfaz web de Dagster o CLI."""

    mes: str = "2025-03"
    mes_comparacion: str = ""
    base_dir: str = "data"
    reports_dir: str = "reports"
    dbt_dir: str = "dbt_project"
    sin_ingesta: bool = True
    forzar_dlt: bool = False
    simular_fallo: bool = False


def resolver_meses(config: TaxiPipelineConfig) -> tuple[str, str]:
    """Devuelve ``(mes_comparacion, mes_principal)`` ordenados cronológicamente."""
    mes_cmp = config.mes_comparacion.strip() or mes_comparacion(config.mes)
    return (mes_cmp, config.mes)


def ruta_warehouse(base_dir: Path) -> Path:
    return Path(base_dir) / "warehouse" / "nyc_taxi_dlt.duckdb"


@contextmanager
def adquirir_lock_pipeline(base_dir: Path, timeout: float = 30.0):
    """Evita escrituras concurrentes sobre ``nyc_taxi_dlt.duckdb``."""
    lock_path = Path(base_dir) / "tmp" / "pipeline_duckdb.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(lock_path), timeout=timeout)
    with lock:
        yield
