import argparse
import sys
from pathlib import Path

import duckdb

ORDEN = ["dim_zone.sql", "fact_trips.sql", "dim_date.sql", "agg_zone_hour.sql"]


def ruta_warehouse(base_dir=Path("data")):
    return base_dir / "warehouse" / "taxi.duckdb"


def construir_modelo(base_dir=Path("data"), sql_dir=Path("sql/modelo"), memoria="4GB"):
    """Reconstruye el modelo completo (CREATE OR REPLACE): correrlo N veces deja lo mismo."""
    base_dir = Path(base_dir)
    destino = ruta_warehouse(base_dir)
    destino.parent.mkdir(parents=True, exist_ok=True)
    reemplazos = {
        "{CURADA}": (base_dir / "curated" / "trips" / "*" / "*" / "*.parquet").resolve().as_posix(),
        "{ZONAS}": (base_dir / "raw" / "zones" / "taxi_zone_lookup.csv").resolve().as_posix(),
    }
    con = duckdb.connect(str(destino))
    con.execute(f"SET memory_limit = '{memoria}'")
    con.execute(f"SET temp_directory = '{(base_dir / 'tmp' / 'duckdb').resolve().as_posix()}'")
    for nombre in ORDEN:
        sql = (Path(sql_dir) / nombre).read_text(encoding="utf-8")
        for marcador, valor in reemplazos.items():
            sql = sql.replace(marcador, valor)
        con.execute(sql)
    resumen = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
               for t in ("fact_trips", "dim_zone", "dim_date", "agg_zone_hour")}
    con.close()
    return resumen


def main():
    ap = argparse.ArgumentParser(description="Construye el modelo estrella")
    ap.parse_args()
    try:
        r = construir_modelo()
    except Exception as exc:
        print(f"ERROR de modelo: {exc}", file=sys.stderr)
        return 1
    print("Modelo construido: " + ", ".join(f"{t}={n:,}" for t, n in r.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())