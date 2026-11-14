import duckdb
import pytest

from src.modelo import construir_modelo, ruta_warehouse
from src.preguntas import ejecutar
from test_calidad_mes import FILAS, preparar
from test_transformacion import validar_y_transformar


@pytest.fixture
def dos_meses(tmp_path):
    base = preparar(tmp_path, FILAS)
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
    return base


def consultar(base, sql):
    con = duckdb.connect(str(ruta_warehouse(base)), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_modelo_integridad(dos_meses):
    resumen = construir_modelo(dos_meses)
    assert resumen["fact_trips"] == 4 and resumen["dim_zone"] == 3
    # trip_id único
    assert consultar(dos_meses, "SELECT count(*) = count(DISTINCT trip_id) FROM fact_trips") == [(True,)]
    # ningún hecho sin su zona ni su fecha (claves huérfanas)
    assert consultar(dos_meses, """SELECT count(*) FROM fact_trips f
        LEFT JOIN dim_zone z ON z.location_id = f.pu_location_id
        LEFT JOIN dim_zone d ON d.location_id = f.do_location_id
        LEFT JOIN dim_date t ON t.date_key = f.pickup_date_key
        WHERE z.location_id IS NULL OR d.location_id IS NULL OR t.date_key IS NULL""") == [(0,)]
    # la agregada cierra con el hecho
    assert consultar(dos_meses, "SELECT sum(trips), sum(total_revenue) FROM agg_zone_hour") == \
        consultar(dos_meses, "SELECT count(*), sum(total_amount) FROM fact_trips")


def test_modelo_es_idempotente(dos_meses):
    a = construir_modelo(dos_meses)
    b = construir_modelo(dos_meses)
    assert a == b


def test_las_seis_preguntas_generan_resultados(dos_meses, tmp_path):
    construir_modelo(dos_meses)
    salida = ejecutar("2025-03", "2024-03", base_dir=dos_meses, sql_dir="sql",
                      salida=tmp_path / "RESULTADOS.md", interpretaciones=tmp_path / "no_existe.yaml")
    texto = salida.read_text(encoding="utf-8")
    for n in range(1, 7):
        assert f"## Pregunta {n}:" in texto
    assert texto.count("(pendiente)") == 6