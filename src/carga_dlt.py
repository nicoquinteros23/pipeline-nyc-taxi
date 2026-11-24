"""Carga idempotente por lotes hacia DuckDB (esquema staging) usando dlt."""

import argparse
import gc
from pathlib import Path
import re
import shutil

import dlt
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from src.esquema import CONTRATO, _mapa_nombres, cargar_contrato, snake

BASE = Path("data/warehouse/nyc_taxi_dlt.duckdb")
_MES_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _conteo_existente(warehouse: Path, schema: str, tabla: str) -> int | None:
    if not warehouse.is_file():
        return None
    with duckdb.connect(
        str(warehouse),
        read_only=True,
        config={"memory_limit": "256MB", "threads": 1},
    ) as con:
        existe = con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [schema, tabla],
        ).fetchone()[0]
        if not existe:
            return None
        return int(con.execute(f'SELECT COUNT(*) FROM {schema}."{tabla}"').fetchone()[0])


def cargar_mes(
    mes: str,
    tamano_lote: int = 5000,
    base_dir: Path = Path("data"),
    warehouse_path: Path | None = None,
    forzar: bool = False,
):
    if tamano_lote <= 0:
        raise ValueError("El tamaño del lote debe ser positivo")
    if not _MES_RE.fullmatch(mes):
        raise ValueError(f"Mes inválido: {mes!r} (formato esperado AAAA-MM)")

    anio, mm = mes.split("-")
    base_dir = Path(base_dir)
    warehouse = (
        Path(warehouse_path)
        if warehouse_path is not None
        else base_dir / "warehouse" / "nyc_taxi_dlt.duckdb"
    )

    archivo = (
        base_dir
        / "raw"
        / "yellow"
        / f"year={anio}"
        / f"month={mm}"
        / f"yellow_tripdata_{mes}.parquet"
    )

    if not archivo.is_file():
        raise FileNotFoundError(f"No existe: {archivo}")

    warehouse.parent.mkdir(parents=True, exist_ok=True)

    nombre_tabla = f"yellow_trips_{anio}_{mm}"
    filas_origen = pq.ParquetFile(archivo).metadata.num_rows

    if not forzar:
        actual = _conteo_existente(warehouse, "staging", nombre_tabla)
        if actual == filas_origen:
            print(
                f"[dlt] Tabla staging.{nombre_tabla} ya cargada con {actual:,} filas "
                "(omitida; usá --forzar para recargar)."
            )
            return

    @dlt.resource(
        name=nombre_tabla,
        write_disposition="replace",
    )
    def viajes():
        parquet = pq.ParquetFile(archivo)

        for lote in parquet.iter_batches(batch_size=tamano_lote):
            yield pa.Table.from_batches([lote])

    tmp_duck = base_dir / "tmp" / "duckdb"
    tmp_duck.mkdir(parents=True, exist_ok=True)
    pipelines_dir = base_dir / "tmp" / "dlt_pipelines"
    pipelines_dir.mkdir(parents=True, exist_ok=True)

    creds = DuckDbCredentials(str(warehouse.resolve()))
    creds.global_config = {
        "memory_limit": "768MB",
        "threads": 2,
        "preserve_insertion_order": False,
        "temp_directory": tmp_duck.resolve().as_posix(),
    }

    pipeline = dlt.pipeline(
        pipeline_name="nyc_taxi_staging",
        pipelines_dir=str(pipelines_dir.resolve()),
        destination=dlt.destinations.duckdb(credentials=creds),
        dataset_name="staging",
    )

    print(f"[dlt] Cargando {archivo}")
    resultado = pipeline.run(viajes())
    print(resultado)
    del pipeline
    gc.collect()
    shutil.rmtree(
        pipelines_dir / "nyc_taxi_staging" / "load" / "loaded",
        ignore_errors=True,
    )

    with duckdb.connect(
        str(warehouse),
        read_only=True,
        config={"memory_limit": "256MB", "threads": 1},
    ) as con:
        cantidad = con.execute(
            f'SELECT COUNT(*) FROM staging."{nombre_tabla}"'
        ).fetchone()[0]

    print(f"[dlt] Tabla: staging.{nombre_tabla}")
    print(f"[dlt] Filas origen: {filas_origen:,}")
    print(f"[dlt] Filas destino: {cantidad:,}")

    if cantidad != filas_origen:
        raise RuntimeError(
            f"Reconciliación fallida: {filas_origen} != {cantidad}"
        )

    print("[dlt] Reconciliación OK")


def actualizar_staging_seleccionado(
    meses: tuple[str, ...] | list[str],
    warehouse_path: Path | None = None,
    contrato_path: Path = CONTRATO,
) -> dict:
    """Actualiza ``staging.yellow_trips_selected`` y ``staging.selected_periods`` de forma atómica.

    Normaliza columnas y tipos contra ``contracts/yellow_schema.yaml``, completa columnas
    opcionales ausentes con ``NULL`` (p. ej. ``cbd_congestion_fee`` en 2024) y conserva intactas
    las tablas históricas mensuales en ``staging``.
    """
    meses_lista = tuple(meses)
    if not meses_lista:
        raise ValueError("Debe especificarse al menos un mes para staging seleccionado")
    if len(set(meses_lista)) != len(meses_lista):
        raise ValueError(f"Hay meses duplicados en la selección: {meses_lista}")
    for m in meses_lista:
        if not _MES_RE.fullmatch(m):
            raise ValueError(f"Mes inválido: {m!r} (formato esperado AAAA-MM)")

    warehouse = Path(warehouse_path) if warehouse_path is not None else BASE
    if not warehouse.is_file():
        raise FileNotFoundError(f"No existe la base DuckDB: {warehouse}")

    contrato = cargar_contrato(contrato_path)
    mapa = _mapa_nombres(contrato)

    selects: list[str] = []
    period_selects: list[str] = []
    conteos: dict[str, int] = {}

    with duckdb.connect(
        str(warehouse),
        config={"memory_limit": "512MB", "threads": 2},
    ) as con:
        for mes in meses_lista:
            anio, mm = mes.split("-")
            tabla = f"yellow_trips_{anio}_{mm}"
            existe = con.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'staging' AND table_name = ?
                """,
                [tabla],
            ).fetchone()[0]
            if not existe:
                raise FileNotFoundError(
                    f"Falta la tabla staging.{tabla} en {warehouse}; ejecutá la carga dlt de {mes}."
                )

            cols_tabla = con.execute(f'DESCRIBE staging."{tabla}"').fetchall()
            presentes: dict[str, tuple[str, str]] = {}
            for col_name, col_type, *_ in cols_tabla:
                destino = mapa.get(col_name) or mapa.get(snake(col_name))
                if destino and destino not in presentes:
                    presentes[destino] = (col_name, col_type)

            proyecciones: list[str] = []
            for c in contrato["columns"]:
                nombre_c = c["name"]
                tipo_c = c["type"]
                if nombre_c in presentes:
                    orig_col, _orig_type = presentes[nombre_c]
                    if "TIMESTAMP" in tipo_c.upper():
                        proyecciones.append(f'"{orig_col}" AS {nombre_c}')
                    else:
                        proyecciones.append(f'CAST("{orig_col}" AS {tipo_c}) AS {nombre_c}')
                elif c.get("nullable", False):
                    proyecciones.append(f"CAST(NULL AS {tipo_c}) AS {nombre_c}")
                else:
                    raise ValueError(
                        f"La tabla staging.{tabla} carece de la columna obligatoria '{nombre_c}'"
                    )

            proyecciones.append(f"'{mes}' AS source_month")
            selects.append(f"SELECT {', '.join(proyecciones)} FROM staging.\"{tabla}\"")
            period_selects.append(f"SELECT '{mes}' AS source_month")
            conteos[mes] = int(
                con.execute(f'SELECT COUNT(*) FROM staging."{tabla}"').fetchone()[0]
            )

        sql_selected = "\nUNION ALL BY NAME\n".join(selects)
        sql_periods = "\nUNION ALL\n".join(period_selects)

        con.execute("CREATE SCHEMA IF NOT EXISTS staging")
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"CREATE OR REPLACE VIEW staging.selected_periods AS\n{sql_periods}")
            con.execute(
                f"CREATE OR REPLACE VIEW staging.yellow_trips_selected AS\n{sql_selected}"
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        total_sel = int(
            con.execute("SELECT COUNT(*) FROM staging.yellow_trips_selected").fetchone()[0]
        )

    esperado = sum(conteos.values())
    if total_sel != esperado:
        raise RuntimeError(
            f"Descuadre en staging.yellow_trips_selected: {total_sel} != {esperado}"
        )

    return {
        "meses": list(meses_lista),
        "conteos": conteos,
        "total": total_sel,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes", default="2025-03")
    parser.add_argument("--lote", type=int, default=5000)
    parser.add_argument(
        "--forzar",
        action="store_true",
        help="Fuerza la recarga aunque la tabla ya coincida",
    )
    args = parser.parse_args()

    cargar_mes(args.mes, args.lote, forzar=args.forzar)
