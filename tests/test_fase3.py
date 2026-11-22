import json
from pathlib import Path

import pytest
import yaml
from dagster import DefaultScheduleStatus
from filelock import Timeout
from prometheus_client import generate_latest

from orchestration import defs
from orchestration.resources import TaxiPipelineConfig, adquirir_lock_pipeline, resolver_meses
from src.metrics_exporter import cargar_metricas, construir_registry, registrar_ejecucion_metricas


EXPECTED_ASSETS = [
    "raw_ingesta",
    "esquema_validado",
    "dlt_staging",
    "calidad_cuarentena",
    "intermediate_views",
    "reconciliacion_intermedia",
    "transformacion_curada",
    "dbt_marts",
    "verificacion_final",
]


def test_dagster_definitions_carga_completa():
    asset_keys = [a.key.to_user_string() for a in defs.assets]
    assert asset_keys == EXPECTED_ASSETS

    job = defs.resolve_job_def("nyc_taxi_pipeline")
    assert job.name == "nyc_taxi_pipeline"

    sched = defs.resolve_schedule_def("nyc_taxi_monthly_schedule")
    assert sched.name == "nyc_taxi_monthly_schedule"
    assert sched.default_status == DefaultScheduleStatus.STOPPED


def test_dagster_fallo_controlado_registra_error_sin_tocar_datos(tmp_path):
    job = defs.resolve_job_def("nyc_taxi_pipeline")
    res = job.execute_in_process(
        run_config={
            "ops": {
                k: {"config": {"simular_fallo": True, "reports_dir": str(tmp_path)}}
                for k in EXPECTED_ASSETS
            }
        },
        raise_on_error=False,
    )
    assert res.success is False
    metrics_file = tmp_path / "pipeline_metrics.json"
    assert metrics_file.is_file()
    data = json.loads(metrics_file.read_text(encoding="utf-8"))
    assert data["failures_total"] >= 1
    assert data["last_status"] == 0
    assert "raw_ingesta" in data["last_error"]


def test_metrics_exporter_registro_y_formato_prometheus(tmp_path):
    metrics_file = tmp_path / "pipeline_metrics.json"
    registrar_ejecucion_metricas(
        exito=True,
        duracion_s=42.5,
        mes="2025-03",
        mes_cmp="2024-03",
        pico_ram_gb=1.15,
        tiempos_etapas={"dbt_build": 20.0},
        reconciliacion={
            "meses": {
                "2024-03": {"staging": 100, "aceptadas": 90, "cuarentena": 10, "diferencia": 0},
                "2025-03": {"staging": 200, "aceptadas": 180, "cuarentena": 20, "diferencia": 0},
            },
            "marts": {
                "fact_trips": 270,
                "agg_zone_hour_trips": 270,
                "fact_total_revenue": 1234.56,
            },
        },
        metrics_path=metrics_file,
    )
    registrar_ejecucion_metricas(
        exito=False,
        duracion_s=3.0,
        error="fallo simulado",
        metrics_path=metrics_file,
    )

    payload = generate_latest(
        construir_registry(metrics_path=metrics_file, data_dir=tmp_path, probe_services=False)
    ).decode("utf-8")

    for esperado in (
        "nyc_taxi_pipeline_runs_total 2.0",
        "nyc_taxi_pipeline_success_total 1.0",
        "nyc_taxi_pipeline_failures_total 1.0",
        "nyc_taxi_pipeline_last_status 0.0",
        "nyc_taxi_reconciliation_diff_rows 0.0",
        "nyc_taxi_marts_total_revenue_usd 1234.56",
        "nyc_taxi_system_cpu_percent",
        "nyc_taxi_system_memory_used_bytes",
        "nyc_taxi_disk_free_bytes",
    ):
        assert esperado in payload


def test_archivos_configuracion_fase3_validos():
    dagster_yaml = yaml.safe_load(Path("orchestration/dagster.yaml").read_text(encoding="utf-8"))
    assert dagster_yaml["run_coordinator"]["class"] == "DefaultRunCoordinator"

    prom_cfg = yaml.safe_load(Path("monitoring/prometheus/prometheus.yml").read_text(encoding="utf-8"))
    jobs = {c["job_name"] for c in prom_cfg["scrape_configs"]}
    assert {"prometheus", "nyc_taxi_exporter"}.issubset(jobs)

    ds_cfg = yaml.safe_load(
        Path("monitoring/grafana/provisioning/datasources/datasource.yml").read_text(encoding="utf-8")
    )
    assert ds_cfg["datasources"][0]["uid"] == "prometheus"

    dash = json.loads(
        Path("monitoring/grafana/dashboards/nyc_taxi_monitoring.json").read_text(encoding="utf-8")
    )
    assert dash["title"] == "NYC Taxi — Pipeline Monitoring"
    assert len(dash["panels"]) == 12
    for panel in dash["panels"]:
        assert panel["datasource"]["uid"] == "prometheus"


def test_dagster_dependencias_y_configuracion():
    por_nombre = {a.key.to_user_string(): a for a in defs.assets}
    for idx, nombre in enumerate(EXPECTED_ASSETS):
        deps = {k.to_user_string() for k in por_nombre[nombre].dependency_keys}
        esperado = set() if idx == 0 else {EXPECTED_ASSETS[idx - 1]}
        assert deps == esperado, f"{nombre}: dependencias {deps} != {esperado}"

    cfg = TaxiPipelineConfig()
    assert cfg.mes == "2025-03"
    assert cfg.sin_ingesta is True
    assert cfg.forzar_dlt is False
    assert cfg.simular_fallo is False
    assert resolver_meses(cfg) == ("2024-03", "2025-03")


def test_concurrencia_lock_pipeline_bloquea_segundo_acceso(tmp_path):
    with adquirir_lock_pipeline(tmp_path, timeout=1.0):
        with pytest.raises(Timeout):
            with adquirir_lock_pipeline(tmp_path, timeout=0.1):
                pass


def test_metrics_exporter_deduplica_run_id_y_maneja_archivos_faltantes(tmp_path):
    metrics_file = tmp_path / "pipeline_metrics.json"
    estado_vacio = cargar_metricas(metrics_file)
    assert estado_vacio["runs_total"] == 0
    assert estado_vacio["recorded_run_ids"] == []

    metrics_file.write_text("{json_invalido", encoding="utf-8")
    estado_corrupto = cargar_metricas(metrics_file)
    assert estado_corrupto["runs_total"] == 0
    metrics_file.unlink()

    e1 = registrar_ejecucion_metricas(
        exito=True,
        duracion_s=10.5,
        tiempos_etapas={"raw_ingesta": 1.2},
        metrics_path=metrics_file,
        run_id="run-abc",
    )
    e1_dup = registrar_ejecucion_metricas(
        exito=True,
        duracion_s=10.5,
        metrics_path=metrics_file,
        run_id="run-abc",
    )
    assert e1["runs_total"] == 1
    assert e1_dup["runs_total"] == 1
    assert e1_dup["success_total"] == 1

    e2 = registrar_ejecucion_metricas(
        exito=False,
        duracion_s=2.0,
        error="error simulado",
        metrics_path=metrics_file,
        run_id="run-err",
    )
    e2_dup = registrar_ejecucion_metricas(
        exito=False,
        duracion_s=2.0,
        error="error simulado",
        metrics_path=metrics_file,
        run_id="run-err",
    )
    assert e2["runs_total"] == 2
    assert e2_dup["runs_total"] == 2
    assert e2_dup["failures_total"] == 1
    assert e2_dup["last_run_id"] == "run-err"


def test_docker_compose_y_seguridad_puertos_variables():
    compose = yaml.safe_load(Path("docker-compose.yml").read_text(encoding="utf-8"))
    services = compose["services"]
    for req in ("pipeline", "dagster-webserver", "metrics-exporter", "prometheus", "grafana", "dozzle"):
        assert req in services

    for svc_name, svc in services.items():
        for p in svc.get("ports", []):
            assert str(p).startswith("127.0.0.1:"), f"Puerto expuesto fuera de localhost en {svc_name}: {p}"

    dozzle = services["dozzle"]
    assert "/var/run/docker.sock:/var/run/docker.sock:ro" in dozzle["volumes"]
    assert str(dozzle["environment"]["DOZZLE_ENABLE_ACTIONS"]).lower() == "false"

    dag = services["dagster-webserver"]
    for var in ("DAGSTER_HOME", "DUCKDB_PATH", "DBT_PROFILES_DIR", "PYTHONPATH", "PYTHONDONTWRITEBYTECODE"):
        assert var in dag["environment"]
    assert "./orchestration/dagster.yaml:/app/data/dagster_home/dagster.yaml:ro" in dag["volumes"]
    assert "./src:/app/src:ro" in dag["volumes"]
    assert "./orchestration:/app/orchestration:ro" in dag["volumes"]

    dash_prov = yaml.safe_load(
        Path("monitoring/grafana/provisioning/dashboards/dashboards.yml").read_text(encoding="utf-8")
    )
    assert dash_prov["providers"][0]["options"]["path"] == "/var/lib/grafana/dashboards"
