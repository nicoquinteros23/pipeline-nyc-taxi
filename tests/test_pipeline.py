import duckdb
import pytest

from src.modelo import ruta_warehouse
from src.pipeline import ErrorPipeline, correr, mes_comparacion
from test_calidad_mes import FILAS, preparar


def preparar_dos_meses(tmp_path):
    base = preparar(tmp_path, FILAS)
    origen = base / "raw" / "yellow" / "year=2025" / "month=03" / "yellow_tripdata_2025-03.parquet"
    destino = base / "raw" / "yellow" / "year=2024" / "month=03"
    destino.mkdir(parents=True)
    duckdb.execute(f"""COPY (SELECT * REPLACE (
        tpep_pickup_datetime - INTERVAL 1 YEAR AS tpep_pickup_datetime,
        tpep_dropoff_datetime - INTERVAL 1 YEAR AS tpep_dropoff_datetime)
        FROM read_parquet('{origen.as_posix()}'))
        TO '{(destino / "yellow_tripdata_2024-03.parquet").as_posix()}' (FORMAT PARQUET)""")
    return base


def filas_fact(base):
    con = duckdb.connect(str(ruta_warehouse(base)), read_only=True)
    try:
        return con.execute("SELECT count(*) FROM fact_trips").fetchone()[0]
    finally:
        con.close()


def test_mes_de_comparacion():
    assert mes_comparacion("2025-03") == "2024-03"
    assert mes_comparacion("2025-12") == "2024-12"


def test_pipeline_completo_y_repetible(tmp_path):
    base = preparar_dos_meses(tmp_path)
    kw = dict(base_dir=base, reports_dir=tmp_path / "reports", sql_dir="sql",
              resultados=tmp_path / "RESULTADOS.md", ingesta=False)
    r1 = correr("2025-03", **kw)
    assert filas_fact(base) == 4
    assert (tmp_path / "RESULTADOS.md").exists()
    assert set(r1["tiempos_por_etapa_s"]) == {"esquema", "calidad", "transformacion", "modelo", "preguntas"}
    assert r1["pico_ram_gb"] > 0 and (tmp_path / "reports" / "ejecucion_2025-03.json").exists()
    correr("2025-03", **kw)                       # segunda corrida: mismo resultado
    assert filas_fact(base) == 4


def test_si_una_etapa_falla_corta_y_la_nombra(tmp_path):
    base = preparar_dos_meses(tmp_path)
    with pytest.raises(ErrorPipeline, match="esquema"):
        correr("2025-06", mes_cmp="2024-06", base_dir=base, reports_dir=tmp_path / "reports",
               sql_dir="sql", resultados=tmp_path / "R.md", ingesta=False)
    assert not (tmp_path / "R.md").exists()       # las etapas siguientes no corrieron