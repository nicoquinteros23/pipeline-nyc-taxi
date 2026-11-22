"""Pruebas de src/preguntas.py sobre esquema marts de nyc_taxi_dlt.duckdb."""

from pathlib import Path

import duckdb

from src.preguntas import ejecutar


def _crear_marts_prueba(base: Path) -> None:
    wh = base / "warehouse" / "nyc_taxi_dlt.duckdb"
    wh.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(wh)) as con:
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
                (161, 'Manhattan', 'Midtown Center', 'Yellow Zone')
            """
        )
        con.execute(
            """
            CREATE TABLE marts.dim_date (
                date_key INTEGER,
                date DATE,
                year INTEGER,
                month INTEGER
            )
            """
        )
        con.execute(
            """
            INSERT INTO marts.dim_date VALUES
                (20240315, DATE '2024-03-15', 2024, 3),
                (20250315, DATE '2025-03-15', 2025, 3)
            """
        )
        con.execute(
            """
            CREATE TABLE marts.fact_trips (
                trip_id UBIGINT,
                year INTEGER,
                month INTEGER,
                pu_location_id INTEGER,
                do_location_id INTEGER,
                pickup_date_key INTEGER,
                pickup_date DATE,
                pickup_hour INTEGER,
                payment_type BIGINT,
                trip_distance DOUBLE,
                trip_duration_min DOUBLE,
                avg_speed_mph DOUBLE,
                fare_amount DOUBLE,
                tip_amount DOUBLE,
                tip_pct DOUBLE,
                total_amount DOUBLE,
                cbd_congestion_fee DOUBLE
            )
            """
        )
        con.execute(
            """
            INSERT INTO marts.fact_trips
            SELECT
                (i + 1)::UBIGINT,
                2024,
                3,
                161,
                132,
                20240315,
                DATE '2024-03-15',
                10,
                1,
                5.0,
                15.0,
                20.0,
                20.0,
                4.0,
                20.0,
                24.0,
                NULL::DOUBLE
            FROM range(1050) t(i)
            """
        )
        con.execute(
            """
            INSERT INTO marts.fact_trips
            SELECT
                (2000 + i)::UBIGINT,
                2025,
                3,
                161,
                132,
                20250315,
                DATE '2025-03-15',
                10,
                1,
                5.0,
                15.0,
                20.0,
                22.0,
                4.4,
                20.0,
                27.15,
                0.75
            FROM range(1050) t(i)
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
            GROUP BY 1, 2, 3
            """
        )


def test_preguntas_ejecutan_sobre_marts_nyc_taxi_dlt(tmp_path):
    base = tmp_path / "data"
    _crear_marts_prueba(base)
    salida = ejecutar(
        "2025-03",
        "2024-03",
        base_dir=base,
        sql_dir=Path("sql"),
        salida=tmp_path / "RESULTADOS.md",
        interpretaciones=tmp_path / "no_existe.yaml",
    )
    texto = salida.read_text(encoding="utf-8")
    for n in range(1, 7):
        assert f"## Pregunta {n}:" in texto
    assert texto.count("(pendiente)") == 6
