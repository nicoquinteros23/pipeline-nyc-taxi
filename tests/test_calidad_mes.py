import duckdb
import pytest

from src import calidad
from src.calidad import ErrorReconciliacion, contar_filas, procesar_mes

TIPOS = {
    "vendor_id": "INTEGER", "tpep_pickup_datetime": "TIMESTAMP",
    "tpep_dropoff_datetime": "TIMESTAMP", "passenger_count": "BIGINT",
    "trip_distance": "DOUBLE", "ratecode_id": "BIGINT", "store_and_fwd_flag": "VARCHAR",
    "pu_location_id": "INTEGER", "do_location_id": "INTEGER", "payment_type": "BIGINT",
    "fare_amount": "DOUBLE", "extra": "DOUBLE", "mta_tax": "DOUBLE", "tip_amount": "DOUBLE",
    "tolls_amount": "DOUBLE", "improvement_surcharge": "DOUBLE", "total_amount": "DOUBLE",
    "congestion_surcharge": "DOUBLE", "airport_fee": "DOUBLE", "cbd_congestion_fee": "DOUBLE",
}


def fila(**cambios):
    base = dict(vendor_id=1, tpep_pickup_datetime="2025-03-10 10:00:00",
                tpep_dropoff_datetime="2025-03-10 10:20:00", passenger_count=1,
                trip_distance=3.0, ratecode_id=1, store_and_fwd_flag="N",
                pu_location_id=1, do_location_id=2, payment_type=1, fare_amount=15.0,
                extra=0.0, mta_tax=0.5, tip_amount=2.0, tolls_amount=0.0,
                improvement_surcharge=1.0, total_amount=20.0, congestion_surcharge=2.5,
                airport_fee=0.0, cbd_congestion_fee=0.75)
    base.update(cambios)
    return base


def preparar(tmp_path, filas):
    """Crea un mes sintético 2025-03 y una tabla de zonas con los ids 1, 2 y 3."""
    base = tmp_path / "data"
    carpeta = base / "raw" / "yellow" / "year=2025" / "month=03"
    carpeta.mkdir(parents=True)
    (base / "raw" / "zones").mkdir(parents=True)
    (base / "raw" / "zones" / "taxi_zone_lookup.csv").write_text(
        "LocationID,Borough,Zone,service_zone\n1,A,Z1,x\n2,B,Z2,x\n3,C,Z3,x\n")
    con = duckdb.connect()
    con.execute("CREATE TABLE s (" + ", ".join(f"{k} {t}" for k, t in TIPOS.items()) + ")")
    con.executemany("INSERT INTO s VALUES (" + ",".join("?" * len(TIPOS)) + ")",
                    [[f[k] for k in TIPOS] for f in filas])
    destino = carpeta / "yellow_tripdata_2025-03.parquet"
    con.execute(f"COPY s TO '{destino.as_posix()}' (FORMAT PARQUET)")
    return base


FILAS = [
    fila(),                                              # 0 buena
    fila(),                                              # 1 duplicado exacto de la 0 -> R08
    fila(trip_distance=0.0),                             # 2 distancia 0 -> R03
    fila(tpep_pickup_datetime="2025-04-01 00:00:00",
         tpep_dropoff_datetime="2025-04-01 00:10:00"),   # 3 fuera de mes -> R01
    fila(passenger_count=0),                             # 4 pasa con advertencia R06
    fila(pu_location_id=999),                            # 5 zona inexistente -> R07
]


def test_procesar_mes_separa_aceptadas_y_cuarentena(tmp_path):
    base = preparar(tmp_path, FILAS)
    rep = procesar_mes("2025-03", base_dir=base, reports_dir=tmp_path / "reports")
    assert rep["filas_leidas"] == 6
    assert rep["aceptadas"] == 2 and rep["cuarentena"] == 4
    assert rep["reconciliacion"] == "ok"
    assert rep["rechazo_por_regla"]["R08"] == 1 and rep["rechazo_por_regla"]["R07"] == 1
    assert rep["advertencias_por_regla"]["R06"] == 1
    cua = duckdb.connect().execute(
        f"SELECT reglas_violadas FROM read_parquet('{(base / 'quarantine').as_posix()}/*/*/*.parquet') "
        "ORDER BY file_row_number").fetchall()
    assert [r[0] for r in cua] == [["R08"], ["R03"], ["R01"], ["R07"]]
    assert (tmp_path / "reports" / "calidad_2025-03.json").exists()
    assert (tmp_path / "reports" / "calidad_2025-03.md").exists()


def test_procesar_mes_es_idempotente(tmp_path):
    base = preparar(tmp_path, FILAS)
    kw = dict(base_dir=base, reports_dir=tmp_path / "reports")
    procesar_mes("2025-03", **kw)
    procesar_mes("2025-03", **kw)
    assert contar_filas(base / "validada") == 2       # no se duplicó al reprocesar
    assert contar_filas(base / "quarantine") == 4
    assert len(list((base / "validada").rglob("*.parquet"))) == 1


def test_si_no_cuadra_falla_y_no_toca_la_particion(tmp_path, monkeypatch):
    base = preparar(tmp_path, FILAS)
    kw = dict(base_dir=base, reports_dir=tmp_path / "reports")
    procesar_mes("2025-03", **kw)
    antes = contar_filas(base / "validada")
    monkeypatch.setattr(calidad, "contar_filas",
                        lambda ruta: 999 if "calidad_" in str(ruta) else contar_filas(ruta))
    with pytest.raises(ErrorReconciliacion):
        procesar_mes("2025-03", **kw)
    assert contar_filas(base / "validada") == antes
    assert "FALLA" in (tmp_path / "reports" / "calidad_2025-03.json").read_text(encoding="utf-8")