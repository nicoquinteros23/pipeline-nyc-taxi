import json
from pathlib import Path
from types import SimpleNamespace

from dagster import DagsterRunStatus, RunsFilter
from filelock import Timeout
from prometheus_client import generate_latest
import pytest
import yaml

from orchestration import defs
from orchestration.resources import (
    TaxiPipelineConfig,
    adquirir_lock_ejecucion,
    adquirir_lock_pipeline,
    liberar_lock_ejecucion,
    resolver_meses,
    verificar_sin_otra_ejecucion_activa,
)
from src.metrics_exporter import (
    DockerStatsCache,
    calcular_cpu_percent,
    calcular_memoria_bytes,
    cargar_metricas,
    construir_registry,
    parsear_contenedores_stats,
    registrar_ejecucion_metricas,
)

EXPECTED_ASSETS = [
    "ingesta_raw",
    "carga_dlt_staging",
    "calidad_cuarentena",
    "reconciliacion_staging",
    "dbt_marts",
    "verificacion_final",
    "resultados_analiticos",
]


def test_dagster_definitions_carga_y_dependencias():
    asset_keys = [a.key.to_user_string() for a in defs.assets]
    assert asset_keys == EXPECTED_ASSETS

    por_nombre = {a.key.to_user_string(): a for a in defs.assets}
    for idx, nombre in enumerate(EXPECTED_ASSETS):
        deps = {k.to_user_string() for k in por_nombre[nombre].dependency_keys}
        esperado = set() if idx == 0 else {EXPECTED_ASSETS[idx - 1]}
        assert deps == esperado, f"{nombre}: {deps} != {esperado}"

    job = defs.resolve_job_def("nyc_taxi_pipeline")
    assert job.name == "nyc_taxi_pipeline"
    assert not defs.schedules


def test_configuracion_periodos_unificada():
    assert resolver_meses(TaxiPipelineConfig()) == ("2024-03", "2025-03")
    assert resolver_meses(TaxiPipelineConfig(mes="2025-01")) == ("2024-01", "2025-01")
    assert resolver_meses(TaxiPipelineConfig(mes="2026-06")) == ("2025-06", "2026-06")
    assert resolver_meses(TaxiPipelineConfig(mes="2025-12")) == ("2024-12", "2025-12")
    assert TaxiPipelineConfig().sin_ingesta is False

    for malo in ("2025-00", "2025-13", "2025-1", "texto", "1900-01"):
        with pytest.raises(ValueError):
            resolver_meses(TaxiPipelineConfig(mes=malo))

    with pytest.raises(ValueError, match="iguales"):
        resolver_meses(TaxiPipelineConfig(mes="2025-03", mes_comparacion="2025-03"))


def test_concurrencia_lock_y_ejecucion_activa_bloqueada(tmp_path):
    with adquirir_lock_pipeline(tmp_path, timeout=1.0):
        with pytest.raises(Timeout):
            with adquirir_lock_pipeline(tmp_path, timeout=0.1):
                pass
    with adquirir_lock_pipeline(tmp_path, timeout=0.5):
        pass

    adquirir_lock_ejecucion(tmp_path, "run-a", timeout=0.5)
    try:
        with pytest.raises(RuntimeError, match="en curso"):
            adquirir_lock_ejecucion(tmp_path, "run-b", timeout=0.1)
    finally:
        liberar_lock_ejecucion("run-a")
    # Confirmar liberación tras el bloque try/finally
    adquirir_lock_ejecucion(tmp_path, "run-b", timeout=0.5)
    liberar_lock_ejecucion("run-b")

    class FakeInstance:
        def get_runs(self, filters=None):
            assert isinstance(filters, RunsFilter)
            assert DagsterRunStatus.STARTED in filters.statuses
            return [SimpleNamespace(run_id="run-otro-activo")]

    ctx_otro = SimpleNamespace(instance=FakeInstance(), run_id="run-nuevo")
    with pytest.raises(RuntimeError, match="ejecución activa"):
        verificar_sin_otra_ejecucion_activa(ctx_otro)

    ctx_mismo = SimpleNamespace(instance=FakeInstance(), run_id="run-otro-activo")
    verificar_sin_otra_ejecucion_activa(ctx_mismo)


def test_dagster_fallo_controlado_registra_error_sin_tocar_datos(tmp_path):
    job = defs.resolve_job_def("nyc_taxi_pipeline")
    res = job.execute_in_process(
        run_config={
            "ops": {
                k: {
                    "config": {
                        "simular_fallo": True,
                        "base_dir": str(tmp_path / "data"),
                        "reports_dir": str(tmp_path),
                        "resultados_path": str(tmp_path / "RESULTADOS.md"),
                    }
                }
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
    assert "ingesta_raw" in data["last_error"]


def test_docker_stats_parsers_y_series_por_contenedor(tmp_path):
    stats_dagster = {
        "cpu_stats": {
            "cpu_usage": {"total_usage": 2_500_000_000},
            "system_cpu_usage": 20_000_000_000,
            "online_cpus": 4,
        },
        "precpu_stats": {
            "cpu_usage": {"total_usage": 2_000_000_000},
            "system_cpu_usage": 10_000_000_000,
        },
        "memory_stats": {
            "usage": 400_000_000,
            "limit": 4_000_000_000,
            "stats": {"inactive_file": 50_000_000},
        },
    }
    stats_prom = {
        "cpu_stats": {
            "cpu_usage": {"total_usage": 1_100_000_000},
            "system_cpu_usage": 20_000_000_000,
            "online_cpus": 4,
        },
        "precpu_stats": {
            "cpu_usage": {"total_usage": 1_000_000_000},
            "system_cpu_usage": 10_000_000_000,
        },
        "memory_stats": {
            "usage": 100_000_000,
            "limit": 4_000_000_000,
            "stats": {"inactive_file": 20_000_000},
        },
    }
    cpu_d, sec_d = calcular_cpu_percent(stats_dagster)
    assert cpu_d == 20.0
    assert sec_d == 2.5
    mem_d, lim_d, pct_d = calcular_memoria_bytes(stats_dagster)
    assert (mem_d, lim_d, pct_d) == (350_000_000, 4_000_000_000, 8.75)

    contenedores = [
        {
            "Id": "cid-1",
            "Names": ["/nyc-taxi-dagster"],
            "State": "running",
            "Labels": {"com.docker.compose.service": "dagster-webserver"},
        },
        {
            "Id": "cid-2",
            "Names": ["/nyc-taxi-prometheus"],
            "State": "running",
            "Labels": {"com.docker.compose.service": "prometheus"},
        },
        {
            "Id": "cid-3",
            "Names": ["/nyc-taxi-grafana"],
            "State": "exited",
            "Labels": {"com.docker.compose.service": "grafana"},
        },
    ]
    parsed = parsear_contenedores_stats(
        contenedores, {"cid-1": stats_dagster, "cid-2": stats_prom}
    )
    por_svc = {x["service"]: x for x in parsed}
    assert por_svc["dagster-webserver"]["memory_usage_bytes"] == 350_000_000
    assert por_svc["prometheus"]["memory_usage_bytes"] == 80_000_000
    assert por_svc["dagster-webserver"]["running"] == 1
    assert por_svc["grafana"]["running"] == 0
    assert por_svc["grafana"]["memory_usage_bytes"] == 0

    class FakeCache:
        def snapshot(self):
            return parsed

    metrics_file = tmp_path / "pipeline_metrics.json"
    registrar_ejecucion_metricas(
        exito=True,
        duracion_s=42.5,
        tiempos_etapas={"dbt_marts": 20.0},
        reconciliacion={
            "meses": {
                "2024-03": {"staging": 100, "aceptadas": 90, "cuarentena": 10, "diferencia": 0},
                "2025-03": {"staging": 200, "aceptadas": 180, "cuarentena": 20, "diferencia": 0},
            },
            "marts": {"fact_trips": 270, "agg_zone_hour_trips": 270, "fact_total_revenue": 1234.56},
        },
        metrics_path=metrics_file,
        run_id="run-1",
    )
    payload = generate_latest(
        construir_registry(metrics_path=metrics_file, data_dir=tmp_path, docker_cache=FakeCache())
    ).decode("utf-8")

    for esperado in (
        'nyc_taxi_container_cpu_percent{container="nyc-taxi-dagster",service="dagster-webserver"} 20.0',
        'nyc_taxi_container_memory_usage_bytes{container="nyc-taxi-dagster",service="dagster-webserver"} 3.5e+08',
        'nyc_taxi_container_memory_usage_bytes{container="nyc-taxi-prometheus",service="prometheus"} 8e+07',
        'nyc_taxi_container_running{container="nyc-taxi-grafana",service="grafana"} 0.0',
        "nyc_taxi_pipeline_runs_total 1.0",
        "nyc_taxi_reconciliation_diff_rows 0.0",
    ):
        assert esperado in payload

    # Sin socket disponible no falla ni revienta la aplicación
    cache_sin_socket = DockerStatsCache(socket_path=tmp_path / "no.sock")
    assert cache_sin_socket.refresh_once() == []


def test_metrics_exporter_deduplica_run_id_y_maneja_corrupto(tmp_path):
    metrics_file = tmp_path / "pipeline_metrics.json"
    assert cargar_metricas(metrics_file)["runs_total"] == 0
    metrics_file.write_text("{corrupto", encoding="utf-8")
    assert cargar_metricas(metrics_file)["runs_total"] == 0
    metrics_file.unlink()

    e1 = registrar_ejecucion_metricas(exito=True, duracion_s=5.0, metrics_path=metrics_file, run_id="r1")
    e2 = registrar_ejecucion_metricas(exito=True, duracion_s=5.0, metrics_path=metrics_file, run_id="r1")
    assert e1["runs_total"] == 1 and e2["runs_total"] == 1 and e2["success_total"] == 1


def test_docker_compose_prometheus_grafana_simplificados():
    compose = yaml.safe_load(Path("docker-compose.yml").read_text(encoding="utf-8"))
    services = set(compose["services"].keys())
    assert services == {"dagster-webserver", "metrics-exporter", "prometheus", "grafana", "dozzle"}

    for svc_name, svc in compose["services"].items():
        for p in svc.get("ports", []):
            assert str(p).startswith("127.0.0.1:"), f"Puerto público en {svc_name}: {p}"

    exp = compose["services"]["metrics-exporter"]
    assert "/var/run/docker.sock:/var/run/docker.sock:ro" in exp["volumes"]
    assert compose["services"]["dozzle"]["environment"]["DOZZLE_ENABLE_ACTIONS"] == "false"

    prom_cfg = yaml.safe_load(Path("monitoring/prometheus/prometheus.yml").read_text(encoding="utf-8"))
    assert {"prometheus", "nyc_taxi_exporter"}.issubset({c["job_name"] for c in prom_cfg["scrape_configs"]})

    dash = json.loads(Path("monitoring/grafana/dashboards/nyc_taxi_monitoring.json").read_text(encoding="utf-8"))
    assert len(dash["panels"]) == 12
    exprs = " ".join(t["expr"] for p in dash["panels"] for t in p["targets"])
    assert "nyc_taxi_container_cpu_percent" in exprs
    assert "nyc_taxi_container_memory_usage_bytes" in exprs
