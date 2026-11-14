import duckdb
import pytest

from src.calidad import contar_filas, procesar_mes
from src.transformacion import ErrorTransformacion, transformar_mes
from test_calidad_mes import FILAS, preparar


def leer(base, mes_dir):
    ruta = base / "curated" / "trips" / mes_dir / "data.parquet"
    cur = duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{ruta.as_posix()}') ORDER BY file_row_number")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, f)) for f in cur.fetchall()]


def validar_y_transformar(base, tmp_path, mes="2025-03"):
    procesar_mes(mes, base_dir=base, reports_dir=tmp_path / "reports")
    return transformar_mes(mes, base_dir=base)


def test_derivadas_y_zonas(tmp_path):
    base = preparar(tmp_path, FILAS)
    r = validar_y_transformar(base, tmp_path)
    assert r["filas"] == 2 and r["sin_zona"] == 0
    f = leer(base, "year=2025/month=3")[0]          # 3 millas en 20 min, tarifa 15, propina 2
    assert f["trip_duration_min"] == pytest.approx(20.0)
    assert f["avg_speed_mph"] == pytest.approx(9.0)
    assert f["pickup_hour"] == 10
    assert f["pickup_dow"] == 1                      # 2025-03-10 fue lunes (isodow: 1)
    assert str(f["pickup_date"]) == "2025-03-10"
    assert f["tip_pct"] == pytest.approx(100 * 2 / 15)
    assert (f["pu_zone"], f["pu_borough"], f["do_zone"], f["do_borough"]) == ("Z1", "A", "Z2", "B")


def test_tip_pct_es_null_si_la_tarifa_es_cero(tmp_path):
    filas = [dict(FILAS[0], fare_amount=0.0, total_amount=3.0)]
    base = preparar(tmp_path, filas)
    validar_y_transformar(base, tmp_path)
    assert leer(base, "year=2025/month=3")[0]["tip_pct"] is None


def test_reproceso_reemplaza_solo_su_particion(tmp_path):
    base = preparar(tmp_path, FILAS)
    # un segundo mes (2024-03) armado desplazando un año las fechas del primero
    origen = base / "raw" / "yellow" / "year=2025" / "month=03" / "yellow_tripdata_2025-03.parquet"
    destino = base / "raw" / "yellow" / "year=2024" / "month=03"
    destino.mkdir(parents=True)
    duckdb.execute(f"""COPY (SELECT * REPLACE (
        tpep_pickup_datetime - INTERVAL 1 YEAR AS tpep_pickup_datetime,
        tpep_dropoff_datetime - INTERVAL 1 YEAR AS tpep_dropoff_datetime)
        FROM read_parquet('{origen.as_posix()}'))
        TO '{(destino / "yellow_tripdata_2024-03.parquet").as_posix()}' (FORMAT PARQUET)""")
    validar_y_transformar(base, tmp_path, "2025-03")
    validar_y_transformar(base, tmp_path, "2024-03")

    p24 = base / "curated" / "trips" / "year=2024" / "month=3" / "data.parquet"
    p25 = base / "curated" / "trips" / "year=2025" / "month=3" / "data.parquet"
    mtime24, mtime25 = p24.stat().st_mtime_ns, p25.stat().st_mtime_ns
    filas24, filas25 = contar_filas(p24), contar_filas(p25)

    validar_y_transformar(base, tmp_path, "2025-03")  # reproceso de UN solo mes

    assert p24.stat().st_mtime_ns == mtime24          # la otra partición no se tocó
    assert p25.stat().st_mtime_ns != mtime25          # la reprocesada sí se reemplazó
    assert (contar_filas(p24), contar_filas(p25)) == (filas24, filas25)   # y no se duplicó


def test_sin_validada_falla(tmp_path):
    with pytest.raises(ErrorTransformacion):
        transformar_mes("2025-03", base_dir=tmp_path)