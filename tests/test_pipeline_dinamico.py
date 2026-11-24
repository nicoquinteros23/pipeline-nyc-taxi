"""Pruebas unitarias e integración sintética del pipeline dinámico (MES=AAAA-MM)."""

from pathlib import Path
import subprocess

import duckdb
import pytest

from orchestration.cli import ejecutar_cli, main as cli_main, parsear_args
from orchestration.resources import TaxiPipelineConfig, adquirir_lock_ejecucion, liberar_lock_ejecucion
from src.carga_dlt import actualizar_staging_seleccionado
from src.intermediate import crear_intermediate
from src.preguntas import ejecutar as ejecutar_preguntas


def _escribir_parquet_sintetico(ruta: Path, anio: int, mm: int, con_cbd: bool = True) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    extra_col = ", 0.75::DOUBLE AS cbd_congestion_fee" if con_cbd else ""
    t0 = f"{anio:04d}-{mm:02d}-10 10:00:00"
    t1 = f"{anio:04d}-{mm:02d}-10 10:20:00"
    with duckdb.connect() as con:
        con.execute(
            f"""
            COPY (
              SELECT
                1::INTEGER AS VendorID,
                TIMESTAMP '{t0}' AS tpep_pickup_datetime,
                TIMESTAMP '{t1}' AS tpep_dropoff_datetime,
                1::BIGINT AS passenger_count,
                4.0::DOUBLE AS trip_distance,
                1::BIGINT AS RatecodeID,
                'N'::VARCHAR AS store_and_fwd_flag,
                161::INTEGER AS PULocationID,
                132::INTEGER AS DOLocationID,
                1::BIGINT AS payment_type,
                20.0::DOUBLE AS fare_amount,
                0.5::DOUBLE AS extra,
                0.5::DOUBLE AS mta_tax,
                4.0::DOUBLE AS tip_amount,
                0.0::DOUBLE AS tolls_amount,
                1.0::DOUBLE AS improvement_surcharge,
                26.0::DOUBLE AS total_amount,
                2.5::DOUBLE AS congestion_surcharge,
                0.0::DOUBLE AS Airport_fee
                {extra_col}
              UNION ALL
              SELECT
                1::INTEGER,
                TIMESTAMP '{t0}',
                TIMESTAMP '{t1}',
                1::BIGINT,
                0.0::DOUBLE,
                1::BIGINT,
                'N'::VARCHAR,
                161::INTEGER,
                132::INTEGER,
                1::BIGINT,
                10.0::DOUBLE,
                0.0::DOUBLE,
                0.5::DOUBLE,
                0.0::DOUBLE,
                0.0::DOUBLE,
                1.0::DOUBLE,
                11.5::DOUBLE,
                0.0::DOUBLE,
                0.0::DOUBLE
                {extra_col}
            ) TO '{ruta.as_posix()}' (FORMAT PARQUET)
            """
        )


def _crear_zonas(base_dir: Path) -> None:
    zdir = base_dir / "raw" / "zones"
    zdir.mkdir(parents=True, exist_ok=True)
    (zdir / "taxi_zone_lookup.csv").write_text(
        "LocationID,Borough,Zone,service_zone\n"
        "132,Queens,JFK Airport,Airports\n"
        "161,Manhattan,Midtown Center,Yellow Zone\n",
        encoding="utf-8",
    )


def test_actualizar_staging_seleccionado_dinamico(tmp_path):
    wh = tmp_path / "wh.duckdb"
    with duckdb.connect(str(wh)) as con:
        con.execute("CREATE SCHEMA staging")
        cols_base = """
            vendor_id INTEGER,
            tpep_pickup_datetime TIMESTAMPTZ,
            tpep_dropoff_datetime TIMESTAMPTZ,
            passenger_count BIGINT,
            trip_distance DOUBLE,
            ratecode_id BIGINT,
            store_and_fwd_flag VARCHAR,
            pu_location_id INTEGER,
            do_location_id INTEGER,
            payment_type BIGINT,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            congestion_surcharge DOUBLE,
            airport_fee DOUBLE
        """
        for tbl, dt, cbd in [
            ("yellow_trips_2024_01", "2024-01-10 10:00:00+00", False),
            ("yellow_trips_2025_01", "2025-01-10 10:00:00+00", True),
            ("yellow_trips_2024_03", "2024-03-10 10:00:00+00", False),
            ("yellow_trips_2025_03", "2025-03-10 10:00:00+00", True),
        ]:
            extra_col = ", cbd_congestion_fee DOUBLE" if cbd else ""
            con.execute(f"CREATE TABLE staging.{tbl} ({cols_base}{extra_col})")
            extra_val = ", 0.75" if cbd else ""
            con.execute(
                f"INSERT INTO staging.{tbl} VALUES (1, '{dt}', '{dt}', 1, 3.0, 1, 'N', 161, 132, 1, 15.0, 0, 0.5, 2, 0, 1, 20.0, 2.5, 0{extra_val})"
            )

    res_ene = actualizar_staging_seleccionado(("2024-01", "2025-01"), warehouse_path=wh)
    assert res_ene["total"] == 2
    with duckdb.connect(str(wh), read_only=True) as con:
        meses_sel = sorted(
            r[0] for r in con.execute("SELECT DISTINCT source_month FROM staging.yellow_trips_selected").fetchall()
        )
        cbd_24 = con.execute(
            "SELECT cbd_congestion_fee FROM staging.yellow_trips_selected WHERE source_month = '2024-01'"
        ).fetchone()[0]
        assert meses_sel == ["2024-01", "2025-01"]
        assert cbd_24 is None

    res_mar = actualizar_staging_seleccionado(("2024-03", "2025-03"), warehouse_path=wh)
    assert res_mar["total"] == 2
    with duckdb.connect(str(wh), read_only=True) as con:
        meses_sel = sorted(
            r[0] for r in con.execute("SELECT DISTINCT source_month FROM staging.yellow_trips_selected").fetchall()
        )
        assert meses_sel == ["2024-03", "2025-03"]

    with pytest.raises(FileNotFoundError, match="yellow_trips_2024_02"):
        actualizar_staging_seleccionado(("2024-02", "2025-02"), warehouse_path=wh)


def test_intermediate_cambio_y_rollback_ante_faltante(tmp_path):
    base = tmp_path / "data"
    wh = base / "warehouse" / "nyc_taxi_dlt.duckdb"
    wh.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(wh)) as con:
        for anio, mm in [(2024, 3), (2025, 3), (2024, 1), (2025, 1)]:
            for carpeta, col in [("validada", "flags"), ("quarantine", "reglas_violadas")]:
                p = base / carpeta / f"year={anio}" / f"month={mm}" / "data.parquet"
                p.parent.mkdir(parents=True, exist_ok=True)
                con.execute(
                    f"COPY (SELECT 1 AS id, 'f.parquet' AS source_file, 0::BIGINT AS file_row_number, []::VARCHAR[] AS {col}) "
                    f"TO '{p.as_posix()}' (FORMAT PARQUET)"
                )

    crear_intermediate(meses=("2024-03", "2025-03"), base_dir=base, warehouse_path=wh)
    crear_intermediate(meses=("2024-01", "2025-01"), base_dir=base, warehouse_path=wh)
    with duckdb.connect(str(wh), read_only=True) as con:
        ms = sorted(r[0] for r in con.execute("SELECT DISTINCT source_month FROM intermediate.trips_validated").fetchall())
        assert ms == ["2024-01", "2025-01"]

    # Si falta 2025-02, la vista consolidada de enero no queda rota ni a medias
    with pytest.raises(FileNotFoundError):
        crear_intermediate(meses=("2024-01", "2025-02"), base_dir=base, warehouse_path=wh)
    with duckdb.connect(str(wh), read_only=True) as con:
        ms = sorted(r[0] for r in con.execute("SELECT DISTINCT source_month FROM intermediate.trips_validated").fetchall())
        assert ms == ["2024-01", "2025-01"]


def test_cli_parametros_y_errores(tmp_path):
    cfg = parsear_args(["--mes", "2025-01"])
    assert cfg.mes == "2025-01"
    assert cfg.sin_ingesta is False

    cfg_sin = parsear_args(["--mes", "2025-01", "--sin-ingesta"])
    assert cfg_sin.sin_ingesta is True

    assert cli_main(["--mes", "2025-13"]) == 1
    assert cli_main([
        "--mes", "2025-03",
        "--simular-fallo",
        "--base-dir", str(tmp_path / "data"),
        "--reports-dir", str(tmp_path / "reports"),
        "--resultados-path", str(tmp_path / "RESULTADOS.md"),
    ]) == 1


def test_preguntas_titulo_dinamico_sin_interpretaciones_marzo(tmp_path):
    from tests.test_preguntas import _crear_marts_prueba

    base = tmp_path / "data"
    _crear_marts_prueba(base)
    wh = base / "warehouse" / "nyc_taxi_dlt.duckdb"
    with duckdb.connect(str(wh)) as con:
        con.execute("UPDATE marts.fact_trips SET month = 1, pickup_date = make_date(year, 1, 15)")
        con.execute("UPDATE marts.agg_zone_hour SET pickup_date = make_date(year(pickup_date), 1, 15)")

    salida = tmp_path / "RESULTADOS.md"
    copia = tmp_path / "reports" / "resultados_2025-01.md"
    ejecutar_preguntas("2025-01", "2024-01", base_dir=base, salida=salida, copia_periodo=copia)
    txt = salida.read_text(encoding="utf-8")
    assert "Enero 2025 vs Enero 2024" in txt
    assert "Marzo" not in txt
    assert "interpretación pendiente para este período" in txt
    assert copia.is_file()


def test_e2e_sintetico_otro_anio_2025_06_y_2026_06(tmp_path):
    base = tmp_path / "data"
    reports = tmp_path / "reports"
    resultados = tmp_path / "RESULTADOS.md"
    _crear_zonas(base)
    _escribir_parquet_sintetico(
        base / "raw" / "yellow" / "year=2025" / "month=06" / "yellow_tripdata_2025-06.parquet",
        2025,
        6,
        con_cbd=False,
    )
    _escribir_parquet_sintetico(
        base / "raw" / "yellow" / "year=2026" / "month=06" / "yellow_tripdata_2026-06.parquet",
        2026,
        6,
        con_cbd=True,
    )

    cfg = TaxiPipelineConfig(
        mes="2026-06",
        base_dir=str(base),
        reports_dir=str(reports),
        dbt_dir="dbt_project",
        resultados_path=str(resultados),
        sin_ingesta=True,
    )
    ok, meses, _info = ejecutar_cli(cfg)
    assert ok is True
    assert meses == ("2025-06", "2026-06")

    wh = base / "warehouse" / "nyc_taxi_dlt.duckdb"
    with duckdb.connect(str(wh), read_only=True) as con:
        fact_meses = sorted(
            r[0] for r in con.execute("SELECT DISTINCT source_month FROM marts.fact_trips").fetchall()
        )
        assert fact_meses == ["2025-06", "2026-06"]
        assert con.execute("SELECT COUNT(*) FROM marts.fact_trips").fetchone()[0] == 2
        assert con.execute("SELECT SUM(trips) FROM marts.agg_zone_hour").fetchone()[0] == 2

    res_txt = resultados.read_text(encoding="utf-8")
    assert "Junio 2026 vs Junio 2025" in res_txt
    assert (reports / "resultados_2026-06.md").is_file()

    # Verificar también bloqueo de ejecución concurrente sobre pipeline_run.lock
    adquirir_lock_ejecucion(base, "run-externo", timeout=0.5)
    try:
        ok_conc, _meses_c, err_conc = ejecutar_cli(cfg)
        assert ok_conc is False
        assert "otra ejecución" in err_conc or "en curso" in err_conc
    finally:
        liberar_lock_ejecucion("run-externo")


def test_makefile_dry_run_all():
    p1 = subprocess.run(["make", "-n", "all", "MES=2025-01"], capture_output=True, text=True, check=True)
    assert "orchestration.cli --mes 2025-01" in p1.stdout
    assert "--sin-ingesta" not in p1.stdout

    p2 = subprocess.run(["make", "-n", "all", "MES=2025-01", "SIN_INGESTA=1"], capture_output=True, text=True, check=True)
    assert "orchestration.cli --mes 2025-01 --sin-ingesta" in p2.stdout
