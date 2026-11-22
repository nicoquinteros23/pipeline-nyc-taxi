"""Pruebas unitarias e integración ligera del dashboard Streamlit (Fase 4)."""

from pathlib import Path

import duckdb
import pytest
from streamlit.testing.v1 import AppTest

import dashboard.app as dash_app


def _crear_warehouse_sintetico(db_path: Path) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA marts")
        con.execute(
            """
            CREATE TABLE marts.dim_zone (
                location_id INTEGER,
                borough VARCHAR,
                zone VARCHAR,
                service_zone VARCHAR
            )
            """
        )
        con.execute(
            """
            INSERT INTO marts.dim_zone VALUES
                (132, 'Queens', 'JFK Airport', 'Airports'),
                (138, 'Queens', 'LaGuardia Airport', 'Airports'),
                (161, 'Manhattan', 'Midtown Center', 'Yellow Zone')
            """
        )
        con.execute(
            """
            CREATE TABLE marts.dim_date (
                date_key INTEGER,
                date DATE,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                dow INTEGER,
                dow_name VARCHAR,
                is_weekend BOOLEAN
            )
            """
        )
        con.execute(
            """
            INSERT INTO marts.dim_date VALUES
                (20240315, DATE '2024-03-15', 2024, 3, 15, 5, 'Viernes', false),
                (20250315, DATE '2025-03-15', 2025, 3, 15, 6, 'Sábado', true)
            """
        )
        con.execute(
            """
            CREATE TABLE marts.fact_trips (
                trip_id UBIGINT,
                year INTEGER,
                month INTEGER,
                source_month VARCHAR,
                pu_location_id INTEGER,
                do_location_id INTEGER,
                pickup_date_key INTEGER,
                pickup_date DATE,
                pickup_hour INTEGER,
                pickup_dow INTEGER,
                payment_type BIGINT,
                trip_distance DOUBLE,
                trip_duration_min DOUBLE,
                avg_speed_mph DOUBLE,
                fare_amount DOUBLE,
                tip_amount DOUBLE,
                tip_pct DOUBLE,
                total_amount DOUBLE,
                cbd_congestion_fee DOUBLE,
                flags VARCHAR[],
                pu_borough VARCHAR,
                pu_zone VARCHAR,
                do_borough VARCHAR,
                do_zone VARCHAR
            )
            """
        )
        con.execute(
            """
            INSERT INTO marts.fact_trips
            SELECT
                (i + 1)::UBIGINT AS trip_id,
                2024 AS year,
                3 AS month,
                '2024-03' AS source_month,
                CASE WHEN i % 2 = 0 THEN 161 ELSE 132 END AS pu_location_id,
                161 AS do_location_id,
                20240315 AS pickup_date_key,
                DATE '2024-03-15' AS pickup_date,
                (i % 24) AS pickup_hour,
                5 AS pickup_dow,
                CASE WHEN i % 3 = 0 THEN 2 ELSE 1 END AS payment_type,
                5.0 AS trip_distance,
                15.0 AS trip_duration_min,
                20.0 AS avg_speed_mph,
                20.0 AS fare_amount,
                CASE WHEN i % 3 = 0 THEN 0.0 ELSE 4.0 END AS tip_amount,
                CASE WHEN i % 3 = 0 THEN 0.0 ELSE 20.0 END AS tip_pct,
                CASE WHEN i % 3 = 0 THEN 20.0 ELSE 24.0 END AS total_amount,
                NULL::DOUBLE AS cbd_congestion_fee,
                []::VARCHAR[] AS flags,
                CASE WHEN i % 2 = 0 THEN 'Manhattan' ELSE 'Queens' END AS pu_borough,
                CASE WHEN i % 2 = 0 THEN 'Midtown Center' ELSE 'JFK Airport' END AS pu_zone,
                'Manhattan' AS do_borough,
                'Midtown Center' AS do_zone
            FROM range(2100) t(i)
            """
        )
        con.execute(
            """
            INSERT INTO marts.fact_trips
            SELECT
                (3000 + i)::UBIGINT AS trip_id,
                2025 AS year,
                3 AS month,
                '2025-03' AS source_month,
                CASE WHEN i % 2 = 0 THEN 161 ELSE 138 END AS pu_location_id,
                161 AS do_location_id,
                20250315 AS pickup_date_key,
                DATE '2025-03-15' AS pickup_date,
                (i % 24) AS pickup_hour,
                6 AS pickup_dow,
                1 AS payment_type,
                6.0 AS trip_distance,
                18.0 AS trip_duration_min,
                20.0 AS avg_speed_mph,
                22.0 AS fare_amount,
                4.4 AS tip_amount,
                20.0 AS tip_pct,
                27.15 AS total_amount,
                0.75 AS cbd_congestion_fee,
                ['R06']::VARCHAR[] AS flags,
                CASE WHEN i % 2 = 0 THEN 'Manhattan' ELSE 'Queens' END AS pu_borough,
                CASE WHEN i % 2 = 0 THEN 'Midtown Center' ELSE 'LaGuardia Airport' END AS pu_zone,
                'Manhattan' AS do_borough,
                'Midtown Center' AS do_zone
            FROM range(2100) t(i)
            """
        )
        con.execute(
            """
            CREATE TABLE marts.agg_zone_hour AS
            SELECT
                pu_location_id,
                pickup_date,
                pickup_hour,
                count(*) AS trips,
                sum(total_amount) AS total_revenue,
                avg(trip_duration_min) AS avg_duration_min
            FROM marts.fact_trips
            GROUP BY pu_location_id, pickup_date, pickup_hour
            """
        )
    finally:
        con.close()


def test_resolver_warehouse_y_schema_por_env(monkeypatch):
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.delenv("DUCKDB_SCHEMA", raising=False)
    assert dash_app.resolver_warehouse() == dash_app.DEFAULT_WAREHOUSE
    assert dash_app.resolver_schema() == "marts"

    monkeypatch.setenv("DUCKDB_PATH", "data/warehouse/custom.duckdb")
    monkeypatch.setenv("DUCKDB_SCHEMA", "custom_marts")
    assert dash_app.resolver_warehouse() == dash_app.ROOT / "data/warehouse/custom.duckdb"
    assert dash_app.resolver_schema() == "custom_marts"

    monkeypatch.setenv("DUCKDB_SCHEMA", "marts; DROP TABLE x")
    with pytest.raises(ValueError):
        dash_app.resolver_schema()


def test_dashboard_consultas_sobre_marts_sintetico(monkeypatch, tmp_path):
    db_path = tmp_path / "test_marts.duckdb"
    _crear_warehouse_sintetico(db_path)

    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_SCHEMA", "marts")

    assert dash_app.periodos_disponibles() == [(2025, 3), (2024, 3)]
    df_cnt = dash_app.query_warehouse("SELECT count(*) AS n FROM fact_trips")
    assert int(df_cnt["n"].iloc[0]) == 4200

    for sql_file in sorted((dash_app.SQL_DIR).glob("q*.sql")):
        titulo, _sql, df = dash_app.ejecutar_sql_pregunta(sql_file, 2025, 2024, 3)
        assert titulo
        assert not df.empty, f"{sql_file.name} devolvió DataFrame vacío"


def test_dashboard_manejo_error_base_faltante(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "no_existe.duckdb"))
    monkeypatch.setenv("DUCKDB_SCHEMA", "marts")

    app_path = Path(dash_app.__file__).resolve()
    at = AppTest.from_file(app_path, default_timeout=30).run()
    assert not at.exception
    assert len(at.error) >= 1


@pytest.mark.skipif(
    not Path("data/warehouse/nyc_taxi_dlt.duckdb").is_file(),
    reason="Requiere data/warehouse/nyc_taxi_dlt.duckdb construido localmente.",
)
def test_dashboard_smoke_datos_reales(monkeypatch):
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.delenv("DUCKDB_SCHEMA", raising=False)

    app_path = Path(dash_app.__file__).resolve()
    at = AppTest.from_file(app_path, default_timeout=60).run()
    assert not at.exception
    # El único st.error esperado en la vista inicial es la tarjeta roja explicativa de la regla R01 (rechazo)
    assert all("Por qué se rechaza" in e.value for e in at.error)
    valores_metricas = [m.value for m in at.metric]
    assert "7.408.855" in valores_metricas
    mensajes_ok = " | ".join(s.value for s in at.success)
    assert "único" in mensajes_ok
    assert "claves huérfanas" in mensajes_ok
    assert "cierra exacto" in mensajes_ok
