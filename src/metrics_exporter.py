"""Persistencia de métricas de ejecuciones y exporter HTTP para Prometheus.

Expone métricas reales del pipeline NYC Taxi (ejecuciones, duración, pico RAM,
conteos de reconciliación y revenue en marts) junto con métricas reales de CPU,
memoria y disco del entorno donde corre el contenedor/host, más sondas de
disponibilidad a los servicios vecinos (dagster-webserver y dozzle).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psutil
from filelock import FileLock
from prometheus_client import CollectorRegistry, CONTENT_TYPE_LATEST, ProcessCollector, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

METRICS_FILE_DEFAULT = Path("reports/pipeline_metrics.json")
FALLBACK_EJECUCION = Path("reports/ejecucion_fase2_2025-03.json")


def _estado_inicial(fallback_path: Path = FALLBACK_EJECUCION) -> dict:
    """Inicializa el archivo de métricas a partir del último reporte Fase 2 verificado si existe."""
    base = {
        "runs_total": 0,
        "success_total": 0,
        "failures_total": 0,
        "last_duration_seconds": 0.0,
        "last_run_timestamp_seconds": 0.0,
        "last_success_timestamp_seconds": 0.0,
        "last_status": 0,
        "last_mes": "2025-03",
        "last_mes_comparacion": "2024-03",
        "peak_ram_gb": 0.0,
        "stage_durations": {},
        "reconciliacion": {},
        "last_error": "",
        "last_run_id": "",
        "recorded_run_ids": [],
    }
    if fallback_path.is_file():
        try:
            rep = json.loads(fallback_path.read_text(encoding="utf-8"))
            ts = datetime.fromisoformat(rep["fecha_utc"]).timestamp() if rep.get("fecha_utc") else time.time()
            base.update(
                {
                    "runs_total": 1,
                    "success_total": 1,
                    "failures_total": 0,
                    "last_duration_seconds": float(rep.get("tiempo_total_s", 0.0)),
                    "last_run_timestamp_seconds": ts,
                    "last_success_timestamp_seconds": ts,
                    "last_status": 1,
                    "last_mes": rep.get("mes", "2025-03"),
                    "last_mes_comparacion": rep.get("mes_comparacion", "2024-03"),
                    "peak_ram_gb": float(rep.get("pico_ram_gb", 0.0)),
                    "stage_durations": rep.get("tiempos_por_etapa_s", {}),
                    "reconciliacion": rep.get("reconciliacion", {}),
                }
            )
        except Exception:
            pass
    return base


def cargar_metricas(metrics_path: Path = METRICS_FILE_DEFAULT) -> dict:
    metrics_path = Path(metrics_path)
    if metrics_path.is_file():
        try:
            return json.loads(metrics_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    fallback = metrics_path.parent / FALLBACK_EJECUCION.name
    return _estado_inicial(fallback)


def registrar_ejecucion_metricas(
    exito: bool,
    duracion_s: float,
    mes: str = "2025-03",
    mes_cmp: str = "2024-03",
    pico_ram_gb: float = 0.0,
    tiempos_etapas: dict | None = None,
    reconciliacion: dict | None = None,
    error: str = "",
    metrics_path: Path = METRICS_FILE_DEFAULT,
    run_id: str | None = None,
) -> dict:
    """Actualiza atómicamente ``reports/pipeline_metrics.json`` tras una corrida."""
    metrics_path = Path(metrics_path)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = metrics_path.with_suffix(".lock")

    with FileLock(str(lock_path), timeout=15.0):
        estado = cargar_metricas(metrics_path)
        registrados = list(estado.get("recorded_run_ids") or [])
        if run_id and run_id in registrados:
            return estado

        ahora = time.time()

        estado["runs_total"] = int(estado.get("runs_total", 0)) + 1
        if exito:
            estado["success_total"] = int(estado.get("success_total", 0)) + 1
            estado["last_success_timestamp_seconds"] = ahora
            estado["last_status"] = 1
            estado["last_error"] = ""
            if reconciliacion:
                estado["reconciliacion"] = reconciliacion
        else:
            estado["failures_total"] = int(estado.get("failures_total", 0)) + 1
            estado["last_status"] = 0
            estado["last_error"] = str(error)[:500]

        estado["last_duration_seconds"] = round(float(duracion_s), 2)
        estado["last_run_timestamp_seconds"] = ahora
        estado["last_mes"] = mes
        estado["last_mes_comparacion"] = mes_cmp
        if run_id:
            registrados.append(run_id)
            estado["recorded_run_ids"] = registrados[-200:]
            estado["last_run_id"] = run_id
        else:
            estado.setdefault("recorded_run_ids", registrados)
            estado.setdefault("last_run_id", "")
        if pico_ram_gb > 0:
            estado["peak_ram_gb"] = round(float(pico_ram_gb), 3)
        if tiempos_etapas:
            estado["stage_durations"] = dict(tiempos_etapas)

        tmp = metrics_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(estado, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(metrics_path)
        return estado


def _probe_http(url: str, timeout: float = 1.5) -> int:
    if not url:
        return 0
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 1 if 200 <= resp.status < 400 else 0
    except Exception:
        return 0


class NycTaxiCollector:
    """Collector Prometheus personalizado que lee estado real en cada scrape."""

    def __init__(
        self,
        metrics_path: Path = METRICS_FILE_DEFAULT,
        data_dir: Path = Path("data"),
        dagster_url: str | None = None,
        dozzle_url: str | None = None,
        probe_services: bool = True,
    ):
        self.metrics_path = Path(metrics_path)
        self.data_dir = Path(data_dir)
        self.dagster_url = dagster_url if dagster_url is not None else os.environ.get("DAGSTER_URL", "http://dagster-webserver:3000/server_info")
        self.dozzle_url = dozzle_url if dozzle_url is not None else os.environ.get("DOZZLE_URL", "http://dozzle:8080/healthcheck")
        self.probe_services = probe_services
        # Primera lectura para inicializar delta de cpu_percent
        psutil.cpu_percent(interval=None)

    def collect(self):
        estado = cargar_metricas(self.metrics_path)

        runs = CounterMetricFamily(
            "nyc_taxi_pipeline_runs",
            "Cantidad acumulada de ejecuciones del pipeline NYC Taxi",
        )
        runs.add_metric([], int(estado.get("runs_total", 0)))
        yield runs

        succ = CounterMetricFamily(
            "nyc_taxi_pipeline_success",
            "Cantidad acumulada de ejecuciones exitosas del pipeline",
        )
        succ.add_metric([], int(estado.get("success_total", 0)))
        yield succ

        fail = CounterMetricFamily(
            "nyc_taxi_pipeline_failures",
            "Cantidad acumulada de ejecuciones fallidas del pipeline",
        )
        fail.add_metric([], int(estado.get("failures_total", 0)))
        yield fail

        yield GaugeMetricFamily(
            "nyc_taxi_pipeline_duration_seconds",
            "Duración en segundos de la última ejecución del pipeline",
            value=float(estado.get("last_duration_seconds", 0.0)),
        )
        yield GaugeMetricFamily(
            "nyc_taxi_pipeline_last_success_timestamp_seconds",
            "Timestamp Unix del último éxito del pipeline",
            value=float(estado.get("last_success_timestamp_seconds", 0.0)),
        )
        yield GaugeMetricFamily(
            "nyc_taxi_pipeline_last_status",
            "Estado de la última ejecución (1=ok, 0=fallo)",
            value=float(estado.get("last_status", 0)),
        )
        yield GaugeMetricFamily(
            "nyc_taxi_pipeline_peak_ram_bytes",
            "Pico de RAM RSS del proceso en la última ejecución medida (bytes)",
            value=float(estado.get("peak_ram_gb", 0.0)) * 1e9,
        )

        etapas_g = GaugeMetricFamily(
            "nyc_taxi_pipeline_stage_duration_seconds",
            "Duración por etapa en segundos de la última ejecución",
            labels=["stage"],
        )
        for etapa, seg in (estado.get("stage_durations") or {}).items():
            etapas_g.add_metric([str(etapa)], float(seg))
        yield etapas_g

        rows_g = GaugeMetricFamily(
            "nyc_taxi_rows_total",
            "Cantidad de filas verificadas por capa y mes",
            labels=["layer", "month"],
        )
        rec = estado.get("reconciliacion") or {}
        meses_rec = rec.get("meses") or {}
        diff_total = 0
        for m_str, d in meses_rec.items():
            rows_g.add_metric(["staging", str(m_str)], float(d.get("staging", 0)))
            rows_g.add_metric(["accepted", str(m_str)], float(d.get("aceptadas", 0)))
            rows_g.add_metric(["quarantine", str(m_str)], float(d.get("cuarentena", 0)))
            diff_total += abs(int(d.get("diferencia", 0)))

        marts = rec.get("marts") or {}
        if marts:
            rows_g.add_metric(["marts_fact_trips", "all"], float(marts.get("fact_trips", 0)))
            rows_g.add_metric(["marts_agg_zone_hour_trips", "all"], float(marts.get("agg_zone_hour_trips", 0)))
        yield rows_g

        yield GaugeMetricFamily(
            "nyc_taxi_reconciliation_diff_rows",
            "Diferencia absoluta acumulada entre staging y aceptadas+cuarentena",
            value=float(diff_total),
        )
        yield GaugeMetricFamily(
            "nyc_taxi_marts_total_revenue_usd",
            "Revenue total verificado en marts.fact_trips (USD)",
            value=float(marts.get("fact_total_revenue", 0.0)) if marts else 0.0,
        )

        # Métricas reales de CPU, memoria y disco
        vm = psutil.virtual_memory()
        disk_target = self.data_dir if self.data_dir.exists() else Path(".")
        du = shutil.disk_usage(disk_target)

        yield GaugeMetricFamily(
            "nyc_taxi_system_cpu_percent",
            "Porcentaje de uso de CPU observado por el contenedor/host",
            value=float(psutil.cpu_percent(interval=None)),
        )
        yield GaugeMetricFamily(
            "nyc_taxi_system_memory_used_bytes",
            "Memoria RAM en uso observada por psutil (bytes)",
            value=float(vm.used),
        )
        yield GaugeMetricFamily(
            "nyc_taxi_system_memory_available_bytes",
            "Memoria RAM disponible observada por psutil (bytes)",
            value=float(vm.available),
        )
        yield GaugeMetricFamily(
            "nyc_taxi_system_memory_total_bytes",
            "Memoria RAM total observada por psutil (bytes)",
            value=float(vm.total),
        )
        yield GaugeMetricFamily(
            "nyc_taxi_system_memory_percent",
            "Porcentaje de memoria RAM utilizada",
            value=float(vm.percent),
        )
        yield GaugeMetricFamily(
            "nyc_taxi_disk_free_bytes",
            "Espacio libre en disco en el volumen de datos (bytes)",
            value=float(du.free),
        )
        yield GaugeMetricFamily(
            "nyc_taxi_disk_used_percent",
            "Porcentaje de disco utilizado en el volumen de datos",
            value=round(100.0 * du.used / du.total, 2) if du.total else 0.0,
        )

        svc_g = GaugeMetricFamily(
            "nyc_taxi_service_up",
            "Disponibilidad HTTP de servicios auxiliares (1=up, 0=down)",
            labels=["service"],
        )
        svc_g.add_metric(["metrics_exporter"], 1.0)
        if self.probe_services:
            svc_g.add_metric(["dagster_webserver"], float(_probe_http(self.dagster_url)))
            svc_g.add_metric(["dozzle"], float(_probe_http(self.dozzle_url)))
        yield svc_g


def construir_registry(
    metrics_path: Path = METRICS_FILE_DEFAULT,
    data_dir: Path = Path("data"),
    probe_services: bool = True,
) -> CollectorRegistry:
    registry = CollectorRegistry()
    try:
        ProcessCollector(registry=registry)
    except Exception:
        pass
    registry.register(
        NycTaxiCollector(
            metrics_path=metrics_path,
            data_dir=data_dir,
            probe_services=probe_services,
        )
    )
    return registry


def iniciar_exporter(
    port: int = 8000,
    host: str = "0.0.0.0",
    metrics_path: Path = METRICS_FILE_DEFAULT,
    data_dir: Path = Path("data"),
) -> None:
    registry = construir_registry(metrics_path=metrics_path, data_dir=data_dir, probe_services=True)

    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/metrics", "/"):
                payload = generate_latest(registry)
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            elif self.path == "/health":
                body = b"ok\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            # Evita llenar logs con scrapes de Prometheus cada 5 s
            return

    server = ThreadingHTTPServer((host, port), MetricsHandler)
    print(f"[metrics-exporter] Escuchando en http://{host}:{port}/metrics", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Exporter Prometheus de métricas NYC Taxi")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--metrics-file", default=str(METRICS_FILE_DEFAULT))
    args = ap.parse_args(argv)
    iniciar_exporter(port=args.port, host=args.host, metrics_path=Path(args.metrics_file))
    return 0


if __name__ == "__main__":
    sys.exit(main())
