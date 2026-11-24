"""Capa intermediate en DuckDB (vistas sobre Parquet validados y cuarentena).

Usa rutas POSIX relativas al repositorio cuando ``base_dir`` es relativa
(por defecto ``data``), garantizando portabilidad entre Windows y Docker/Linux.
No duplica datos físicamente en disco.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

MESES_DEFECTO = ("2024-03", "2025-03")
COLUMNAS_VALIDADA_REQ = {"source_file", "file_row_number", "flags"}
COLUMNAS_CUARENTENA_REQ = {"source_file", "file_row_number", "reglas_violadas"}


def _ruta_sql(path: Path) -> str:
    """Devuelve ruta POSIX portable (relativa si el caller pasó ruta relativa)."""
    return path.as_posix()


def _configurar_duckdb(con: duckdb.DuckDBPyConnection, base_dir: Path, memoria: str = "1.5GB") -> None:
    tmp_duck = base_dir / "tmp" / "duckdb"
    tmp_duck.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET memory_limit = '{memoria}'")
    con.execute(f"SET temp_directory = '{_ruta_sql(tmp_duck)}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET threads = 2")


def _verificar_columnas(
    con: duckdb.DuckDBPyConnection, vista: str, requeridas: set[str]
) -> None:
    cols = {fila[0] for fila in con.execute(f"DESCRIBE {vista}").fetchall()}
    faltantes = requeridas - cols
    if faltantes:
        raise ValueError(f"La vista {vista} no tiene las columnas requeridas: {sorted(faltantes)}")


def crear_intermediate(
    meses: tuple[str, ...] | list[str] = MESES_DEFECTO,
    base_dir: Path = Path("data"),
    warehouse_path: Path | None = None,
    memoria: str = "1.5GB",
) -> dict:
    """Crea o reemplaza las vistas del esquema ``intermediate`` de forma idempotente."""
    base_dir = Path(base_dir)
    warehouse = Path(warehouse_path) if warehouse_path is not None else base_dir / "warehouse" / "nyc_taxi_dlt.duckdb"

    if not warehouse.exists():
        raise FileNotFoundError(f"No existe la base DuckDB: {warehouse}")
    if not meses:
        raise ValueError("Debe especificarse al menos un mes")

    meses_ordenados = tuple(meses)
    if len(set(meses_ordenados)) != len(meses_ordenados):
        raise ValueError(f"Hay meses duplicados en intermediate: {meses_ordenados}")

    archivos_mes: list[tuple[str, str, Path, Path]] = []
    for mes in meses_ordenados:
        anio, mm = mes.split("-")
        particion = Path(f"year={anio}") / f"month={int(mm)}"
        validada = base_dir / "validada" / particion / "data.parquet"
        cuarentena = base_dir / "quarantine" / particion / "data.parquet"
        for archivo in (validada, cuarentena):
            if not archivo.is_file():
                raise FileNotFoundError(f"No existe: {archivo}")
        archivos_mes.append((mes, f"{anio}_{mm}", validada, cuarentena))

    resumen_meses: dict[str, dict[str, int]] = {}
    selects_validada: list[str] = []
    selects_cuarentena: list[str] = []

    with duckdb.connect(str(warehouse)) as con:
        _configurar_duckdb(con, base_dir, memoria=memoria)
        con.execute("CREATE SCHEMA IF NOT EXISTS intermediate")
        con.execute("BEGIN TRANSACTION")
        try:
            for mes, sufijo, validada, cuarentena in archivos_mes:
                vista_val = f"intermediate.trips_validated_{sufijo}"
                vista_cua = f"intermediate.trips_quarantine_{sufijo}"

                con.execute(
                    f"""
                    CREATE OR REPLACE VIEW {vista_val} AS
                    SELECT *
                    FROM read_parquet('{_ruta_sql(validada)}', hive_partitioning = false)
                    """
                )
                con.execute(
                    f"""
                    CREATE OR REPLACE VIEW {vista_cua} AS
                    SELECT *
                    FROM read_parquet('{_ruta_sql(cuarentena)}', hive_partitioning = false)
                    """
                )

                _verificar_columnas(con, vista_val, COLUMNAS_VALIDADA_REQ)
                _verificar_columnas(con, vista_cua, COLUMNAS_CUARENTENA_REQ)

                aceptadas = int(con.execute(f"SELECT COUNT(*) FROM {vista_val}").fetchone()[0])
                rechazadas = int(con.execute(f"SELECT COUNT(*) FROM {vista_cua}").fetchone()[0])

                resumen_meses[mes] = {
                    "aceptadas": aceptadas,
                    "cuarentena": rechazadas,
                }
                print(f"{mes}: aceptadas={aceptadas:,}, cuarentena={rechazadas:,}")

                selects_validada.append(
                    f"SELECT *, '{mes}' AS source_month FROM {vista_val}"
                )
                selects_cuarentena.append(
                    f"SELECT *, '{mes}' AS source_month FROM {vista_cua}"
                )

            union_val = "\nUNION ALL BY NAME\n".join(selects_validada)
            union_cua = "\nUNION ALL BY NAME\n".join(selects_cuarentena)

            con.execute(
                f"CREATE OR REPLACE VIEW intermediate.trips_validated AS\n{union_val}"
            )
            con.execute(
                f"CREATE OR REPLACE VIEW intermediate.trips_quarantine AS\n{union_cua}"
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        total_aceptadas = int(
            con.execute("SELECT COUNT(*) FROM intermediate.trips_validated").fetchone()[0]
        )
        total_cuarentena = int(
            con.execute("SELECT COUNT(*) FROM intermediate.trips_quarantine").fetchone()[0]
        )

    suma_aceptadas = sum(v["aceptadas"] for v in resumen_meses.values())
    suma_cuarentena = sum(v["cuarentena"] for v in resumen_meses.values())
    if total_aceptadas != suma_aceptadas:
        raise RuntimeError(
            f"Descuadre en intermediate.trips_validated: {total_aceptadas} != {suma_aceptadas}"
        )
    if total_cuarentena != suma_cuarentena:
        raise RuntimeError(
            f"Descuadre en intermediate.trips_quarantine: {total_cuarentena} != {suma_cuarentena}"
        )

    print(
        f"TOTAL INTERMEDIATE: aceptadas={total_aceptadas:,}, "
        f"cuarentena={total_cuarentena:,}"
    )
    return {
        "meses": resumen_meses,
        "total_aceptadas": total_aceptadas,
        "total_cuarentena": total_cuarentena,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Crea o actualiza las vistas del esquema intermediate en DuckDB"
    )
    ap.add_argument(
        "--meses",
        nargs="+",
        default=list(MESES_DEFECTO),
        help="Meses a incluir, formato AAAA-MM (por defecto: 2024-03 2025-03)",
    )
    args = ap.parse_args(argv)
    try:
        crear_intermediate(meses=tuple(args.meses))
    except Exception as exc:
        print(f"ERROR en intermediate: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
