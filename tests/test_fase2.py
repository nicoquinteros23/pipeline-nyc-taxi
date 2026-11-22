from pathlib import Path

import duckdb
import pytest

from src.carga_dlt import actualizar_staging_seleccionado, cargar_mes
from src.intermediate import crear_intermediate
from src.reconciliacion import ErrorReconciliacionFase2, reconciliar_fase2


WAREHOUSE_REAL = Path("data/warehouse/nyc_taxi_dlt.duckdb")


def _crear_parquet(ruta: Path, sql_select: str) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"COPY ({sql_select}) TO '{ruta.as_posix()}' (FORMAT PARQUET)")
    con.close()


def _preparar_base_sintetica(tmp_path: Path, extra_staging_2025: int = 0) -> tuple[Path, Path]:
    base = tmp_path / "data"
    wh = base / "warehouse" / "nyc_taxi_dlt.duckdb"
    wh.parent.mkdir(parents=True, exist_ok=True)

    _crear_parquet(
        base / "validada" / "year=2024" / "month=3" / "data.parquet",
        "SELECT 1 AS id, 'f24.parquet' AS source_file, 0::BIGINT AS file_row_number, []::VARCHAR[] AS flags "
        "UNION ALL SELECT 2, 'f24.parquet', 1::BIGINT, ['R06']::VARCHAR[]",
    )
    _crear_parquet(
        base / "quarantine" / "year=2024" / "month=3" / "data.parquet",
        "SELECT 3 AS id, 'f24.parquet' AS source_file, 2::BIGINT AS file_row_number, ['R03']::VARCHAR[] AS reglas_violadas",
    )
    _crear_parquet(
        base / "validada" / "year=2025" / "month=3" / "data.parquet",
        "SELECT 10 AS id, 'f25.parquet' AS source_file, 0::BIGINT AS file_row_number, []::VARCHAR[] AS flags, 0.75::DOUBLE AS cbd_congestion_fee "
        "UNION ALL SELECT 11, 'f25.parquet', 1::BIGINT, []::VARCHAR[], 0.75::DOUBLE",
    )
    _crear_parquet(
        base / "quarantine" / "year=2025" / "month=3" / "data.parquet",
        "SELECT 12 AS id, 'f25.parquet' AS source_file, 2::BIGINT AS file_row_number, ['R02']::VARCHAR[] AS reglas_violadas",
    )

    with duckdb.connect(str(wh)) as con:
        con.execute("CREATE SCHEMA staging")
        con.execute("CREATE TABLE staging.yellow_trips_2024_03 AS SELECT * FROM range(3)")
        con.execute(f"CREATE TABLE staging.yellow_trips_2025_03 AS SELECT * FROM range({3 + extra_staging_2025})")

    return base, wh


def test_intermediate_crea_vistas_consolidadas_y_es_idempotente(tmp_path):
    base, wh = _preparar_base_sintetica(tmp_path)
    r1 = crear_intermediate(meses=("2024-03", "2025-03"), base_dir=base, warehouse_path=wh)
    r2 = crear_intermediate(meses=("2024-03", "2025-03"), base_dir=base, warehouse_path=wh)

    assert r1 == r2
    assert r1["total_aceptadas"] == 4
    assert r1["total_cuarentena"] == 2

    with duckdb.connect(str(wh), read_only=True) as con:
        meses_val = con.execute(
            "SELECT source_month, COUNT(*) FROM intermediate.trips_validated GROUP BY 1 ORDER BY 1"
        ).fetchall()
        meses_qua = con.execute(
            "SELECT source_month, COUNT(*) FROM intermediate.trips_quarantine GROUP BY 1 ORDER BY 1"
        ).fetchall()
    assert meses_val == [("2024-03", 2), ("2025-03", 2)]
    assert meses_qua == [("2024-03", 1), ("2025-03", 1)]


def test_intermediate_falla_si_faltan_columnas_requeridas(tmp_path):
    base, wh = _preparar_base_sintetica(tmp_path)
    _crear_parquet(
        base / "quarantine" / "year=2025" / "month=3" / "data.parquet",
        "SELECT 12 AS id, 'f25.parquet' AS source_file, 2::BIGINT AS file_row_number",
    )
    with pytest.raises(ValueError, match="reglas_violadas"):
        crear_intermediate(meses=("2024-03", "2025-03"), base_dir=base, warehouse_path=wh)


def test_reconciliacion_fase2_ok_y_falla_ante_descuadre(tmp_path):
    base, wh = _preparar_base_sintetica(tmp_path, extra_staging_2025=0)
    crear_intermediate(meses=("2024-03", "2025-03"), base_dir=base, warehouse_path=wh)
    res = reconciliar_fase2(meses=("2024-03", "2025-03"), warehouse_path=wh)
    assert res["ok"] is True
    assert res["totales"]["staging"] == 6
    assert res["totales"]["aceptadas"] == 4
    assert res["totales"]["cuarentena"] == 2

    base_bad, wh_bad = _preparar_base_sintetica(tmp_path / "bad", extra_staging_2025=1)
    crear_intermediate(meses=("2024-03", "2025-03"), base_dir=base_bad, warehouse_path=wh_bad)
    with pytest.raises(ErrorReconciliacionFase2, match="2025-03"):
        reconciliar_fase2(meses=("2024-03", "2025-03"), warehouse_path=wh_bad)


def test_reconciliacion_fase2_con_marts(tmp_path):
    base, wh = _preparar_base_sintetica(tmp_path)
    crear_intermediate(meses=("2024-03", "2025-03"), base_dir=base, warehouse_path=wh)
    with duckdb.connect(str(wh)) as con:
        con.execute("CREATE SCHEMA marts")
        con.execute("CREATE TABLE marts.dim_zone (location_id INTEGER)")
        con.execute("CREATE TABLE marts.dim_date (date_key INTEGER)")
        con.execute(
            "CREATE VIEW marts.fact_trips AS "
            "SELECT source_file, file_row_number, source_month, 10.0::DOUBLE AS total_amount "
            "FROM intermediate.trips_validated"
        )
        con.execute(
            "CREATE TABLE marts.agg_zone_hour AS "
            "SELECT 1 AS pu_location_id, DATE '2024-03-01' AS pickup_date, 10 AS pickup_hour, "
            "2::BIGINT AS trips, 20.0::DOUBLE AS total_revenue "
            "UNION ALL "
            "SELECT 1 AS pu_location_id, DATE '2025-03-01' AS pickup_date, 10 AS pickup_hour, "
            "2::BIGINT AS trips, 20.0::DOUBLE AS total_revenue"
        )

    res = reconciliar_fase2(meses=("2024-03", "2025-03"), warehouse_path=wh, verificar_marts=True)
    assert res["marts"]["fact_trips"] == 4
    assert res["marts"]["agg_zone_hour_trips"] == 4

    # Si staging.selected_periods no coincide con meses solicitados, falla claramente
    with duckdb.connect(str(wh)) as con:
        con.execute("CREATE OR REPLACE VIEW staging.selected_periods AS SELECT '2024-01' AS source_month")
    with pytest.raises(ErrorReconciliacionFase2, match="selected_periods"):
        reconciliar_fase2(meses=("2024-03", "2025-03"), warehouse_path=wh, verificar_marts=True)


def test_carga_dlt_valida_lote_y_omite_si_ya_coincide(tmp_path, monkeypatch, capsys):
    with pytest.raises(ValueError):
        cargar_mes("2025-03", tamano_lote=0)

    base = tmp_path / "data"
    raw = base / "raw" / "yellow" / "year=2025" / "month=03" / "yellow_tripdata_2025-03.parquet"
    _crear_parquet(raw, "SELECT * FROM range(5)")
    wh = base / "warehouse" / "nyc_taxi_dlt.duckdb"
    wh.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(wh)) as con:
        con.execute("CREATE SCHEMA staging")
        con.execute("CREATE TABLE staging.yellow_trips_2025_03 AS SELECT * FROM range(5)")

    cargar_mes("2025-03", base_dir=base, warehouse_path=wh, forzar=False)
    salida = capsys.readouterr().out
    assert "omitida" in salida


@pytest.mark.skipif(not WAREHOUSE_REAL.is_file(), reason="Requiere data/warehouse/nyc_taxi_dlt.duckdb")
def test_reconciliacion_real_fase2():
    with duckdb.connect(str(WAREHOUSE_REAL), read_only=True) as con:
        meses_activos = tuple(
            sorted(
                r[0]
                for r in con.execute(
                    "SELECT source_month FROM staging.selected_periods"
                ).fetchall()
            )
        )
    assert len(meses_activos) >= 1
    res = reconciliar_fase2(
        meses=meses_activos,
        warehouse_path=WAREHOUSE_REAL,
        verificar_marts=True,
    )
    assert res["ok"] is True
    assert res["totales"]["staging"] == (
        res["totales"]["aceptadas"] + res["totales"]["cuarentena"]
    )
    assert res["marts"]["fact_trips"] == res["totales"]["aceptadas"]
    assert res["marts"]["agg_zone_hour_trips"] == res["totales"]["aceptadas"]
    if meses_activos == ("2024-03", "2025-03"):
        assert res["meses"]["2024-03"] == {
            "staging": 3_582_628,
            "aceptadas": 3_453_562,
            "cuarentena": 129_066,
            "diferencia": 0,
        }
        assert res["meses"]["2025-03"] == {
            "staging": 4_145_257,
            "aceptadas": 3_955_293,
            "cuarentena": 189_964,
            "diferencia": 0,
        }
        assert res["totales"]["staging"] == 7_727_885
        assert res["totales"]["aceptadas"] == 7_408_855
        assert res["totales"]["cuarentena"] == 319_030
