"""Carga idempotente por lotes hacia DuckDB (esquema staging) usando dlt."""

import argparse
from pathlib import Path

import dlt
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


BASE = Path("data/warehouse/nyc_taxi_dlt.duckdb")


def _conteo_existente(warehouse: Path, schema: str, tabla: str) -> int | None:
    if not warehouse.is_file():
        return None
    with duckdb.connect(str(warehouse), read_only=True) as con:
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

    anio, mm = mes.split("-")
    base_dir = Path(base_dir)
    warehouse = Path(warehouse_path) if warehouse_path is not None else base_dir / "warehouse" / "nyc_taxi_dlt.duckdb"

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

    pipeline = dlt.pipeline(
        pipeline_name="nyc_taxi_staging",
        destination=dlt.destinations.duckdb(
            credentials=str(warehouse.resolve())
        ),
        dataset_name="staging",
    )

    print(f"[dlt] Cargando {archivo}")
    resultado = pipeline.run(viajes())
    print(resultado)

    with duckdb.connect(str(warehouse), read_only=True) as con:
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes", default="2025-03")
    parser.add_argument("--lote", type=int, default=5000)
    parser.add_argument("--forzar", action="store_true", help="Fuerza la recarga aunque la tabla ya coincida")
    args = parser.parse_args()

    cargar_mes(args.mes, args.lote, forzar=args.forzar)
