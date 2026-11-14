import argparse
import shutil
import sys
from pathlib import Path

import duckdb

from src.calidad import _particion, _reemplazar_particion, contar_filas


class ErrorTransformacion(Exception):
    pass


def sql_curada(ruta_validada, ruta_zonas):
    """Filas validadas + columnas derivadas + zonas de origen y destino."""
    val = Path(ruta_validada).as_posix()
    zon = Path(ruta_zonas).as_posix()
    return f"""
WITH base AS (
  SELECT *,
         date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0
           AS trip_duration_min
  FROM read_parquet('{val}')
),
zonas AS (
  SELECT LocationID AS location_id, Borough AS borough, Zone AS zone
  FROM read_csv('{zon}')
)
SELECT b.*,
  CASE WHEN b.trip_duration_min > 0
       THEN b.trip_distance / (b.trip_duration_min / 60.0) END   AS avg_speed_mph,
  CAST(b.tpep_pickup_datetime AS DATE)                           AS pickup_date,
  hour(b.tpep_pickup_datetime)                                   AS pickup_hour,
  isodow(b.tpep_pickup_datetime)                                 AS pickup_dow,
  CASE WHEN b.fare_amount > 0
       THEN 100.0 * b.tip_amount / b.fare_amount END             AS tip_pct,
  zp.borough AS pu_borough, zp.zone AS pu_zone,
  zd.borough AS do_borough, zd.zone AS do_zone
FROM base b
LEFT JOIN zonas zp ON zp.location_id = b.pu_location_id
LEFT JOIN zonas zd ON zd.location_id = b.do_location_id"""


def transformar_mes(mes, base_dir=Path("data"), memoria="4GB"):
    anio, mm = mes.split("-")
    validada = _particion(base_dir / "validada", anio, mm) / "data.parquet"
    zonas = base_dir / "raw" / "zones" / "taxi_zone_lookup.csv"
    if not validada.exists():
        raise ErrorTransformacion(f"No existe {validada}. Corré primero la calidad de {mes}.")
    filas_validadas = contar_filas(validada)

    trabajo = base_dir / "tmp" / f"curada_{mes}"
    shutil.rmtree(trabajo, ignore_errors=True)
    trabajo.mkdir(parents=True)
    salida = trabajo / "data.parquet"

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{memoria}'")
    con.execute(f"SET temp_directory = '{(base_dir / 'tmp' / 'duckdb').as_posix()}'")
    con.execute(f"COPY ({sql_curada(validada, zonas)}) TO '{salida.as_posix()}' "
                f"(FORMAT PARQUET, COMPRESSION ZSTD)")
    sin_zona = con.execute(
        f"SELECT count(*) FROM read_parquet('{salida.as_posix()}') "
        "WHERE pu_zone IS NULL OR do_zone IS NULL").fetchone()[0]
    con.close()

    filas_curadas = contar_filas(trabajo)
    if filas_curadas != filas_validadas:       # un JOIN con claves repetidas duplicaría filas
        shutil.rmtree(trabajo, ignore_errors=True)
        raise ErrorTransformacion(
            f"validadas={filas_validadas} != curadas={filas_curadas}")

    _reemplazar_particion(trabajo, _particion(base_dir / "curated" / "trips", anio, mm))
    return {"mes": mes, "filas": filas_curadas, "sin_zona": sin_zona}


def main():
    ap = argparse.ArgumentParser(description="Capa curada de un mes")
    ap.add_argument("--mes", required=True, help="Formato AAAA-MM")
    args = ap.parse_args()
    try:
        r = transformar_mes(args.mes)
    except Exception as exc:
        print(f"ERROR de transformacion: {exc}", file=sys.stderr)
        return 1
    print(f"{args.mes}: curadas={r['filas']:,} filas sin zona={r['sin_zona']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())