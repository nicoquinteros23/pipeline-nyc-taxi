"""Persistencia de métricas del pipeline y exporter HTTP para Prometheus.

Expone:
1. Métricas del pipeline NYC Taxi (corridas, duración por etapa, reconciliación y revenue).
2. Métricas reales individuales de CPU y RAM por contenedor Docker vía Unix socket
   (``/var/run/docker.sock`` en modo solo lectura, sin exponer Docker por TCP).
3. Espacio en disco del volumen de datos y métricas del proceso exporter (``ProcessCollector``).
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import socket
import sys
import threading
import time
import urllib.parse

from filelock import FileLock
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    ProcessCollector,
    generate_latest,
)
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

METRICS_FILE_DEFAULT = Path("reports/pipeline_metrics.json")
FALLBACK_EJECUCION = Path("reports/ejecucion_fase2_2025-03.json")
DOCKER_SOCKET_DEFAULT = Path("/var/run/docker.sock")


def _estado_inicial(fallback_path: Path = FALLBACK_EJECUCION) -> dict:
    """Inicializa el archivo de métricas a partir del último reporte disponible si existe."""
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
            ts = (
                datetime.fromisoformat(rep["fecha_utc"]).timestamp()
                if rep.get("fecha_utc")
                else time.time()
            )
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


# ---------------------------------------------------------------------------
# Lectura real de CPU y RAM por contenedor vía Docker Engine Unix Socket
# ---------------------------------------------------------------------------


class _UnixHTTPConnection(http.client.HTTPConnection):
    """Cliente HTTP mínimo sobre socket Unix ``/var/run/docker.sock``."""

    def __init__(self, socket_path: str, timeout: float = 5.0):
        super().__init__("localhost", timeout=timeout)
        self.socket_path = socket_path

    def connect(self) -> None:
        if not hasattr(socket, "AF_UNIX"):
            raise RuntimeError("AF_UNIX no está disponible en esta plataforma")
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self.socket_path)
        self.sock = sock


def _docker_get_json(socket_path: Path, path: str, timeout: float = 5.0) -> dict | list:
    conn = _UnixHTTPConnection(str(socket_path), timeout=timeout)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        body = resp.read()
        if resp.status >= 400:
            raise RuntimeError(f"Docker API {path} devolvió HTTP {resp.status}")
        return json.loads(body)
    finally:
        conn.close()


def calcular_cpu_percent(stats: dict) -> tuple[float, float]:
    """Calcula ``(cpu_percent, cpu_seconds_total)`` usando la fórmula estándar de Docker CLI.

    ``cpu_percent`` expresa consumo normalizado donde 100 % equivale a 1 núcleo lógico completo
    (puede superar 100 % si un contenedor usa varios núcleos simultáneamente).
    """
    cpu_stats = stats.get("cpu_stats") or {}
    precpu_stats = stats.get("precpu_stats") or {}
    cpu_usage = cpu_stats.get("cpu_usage") or {}
    precpu_usage = precpu_stats.get("cpu_usage") or {}

    total_usage = int(cpu_usage.get("total_usage") or 0)
    prev_total = int(precpu_usage.get("total_usage") or 0)
    sys_usage = int(cpu_stats.get("system_cpu_usage") or 0)
    prev_sys = int(precpu_stats.get("system_cpu_usage") or 0)

    online_cpus = int(
        cpu_stats.get("online_cpus")
        or len(cpu_usage.get("percpu_usage") or [])
        or 1
    )

    cpu_delta = total_usage - prev_total
    sys_delta = sys_usage - prev_sys
    cpu_pct = 0.0
    if cpu_delta > 0 and sys_delta > 0:
        cpu_pct = round((cpu_delta / sys_delta) * online_cpus * 100.0, 2)

    return cpu_pct, round(total_usage / 1e9, 4)


def calcular_memoria_bytes(stats: dict) -> tuple[int, int, float]:
    """Calcula ``(mem_used_bytes, mem_limit_bytes, mem_percent)`` descontando caché de archivos."""
    ms = stats.get("memory_stats") or {}
    usage = int(ms.get("usage") or 0)
    sub = ms.get("stats") or {}
    cache = int(sub.get("inactive_file", sub.get("cache", 0)) or 0)
    mem_used = max(0, usage - cache)
    mem_limit = int(ms.get("limit") or 0)
    mem_pct = round(100.0 * mem_used / mem_limit, 2) if mem_limit > 0 else 0.0
    return mem_used, mem_limit, mem_pct


def parsear_contenedores_stats(
    contenedores: list[dict], stats_por_id: dict[str, dict]
) -> list[dict]:
    """Transforma respuestas crudas de Docker Engine en métricas limpias por contenedor."""
    resultados: list[dict] = []
    vistos: set[tuple[str, str]] = set()
    for c in contenedores:
        cid = str(c.get("Id") or "")
        nombres = c.get("Names") or [cid[:12]]
        container_name = str(nombres[0]).lstrip("/")
        labels = c.get("Labels") or {}
        service_name = str(labels.get("com.docker.compose.service") or container_name)
        clave = (container_name, service_name)
        if not container_name or clave in vistos:
            continue
        vistos.add(clave)

        running = 1 if str(c.get("State", "")).lower() == "running" else 0
        raw_stats = stats_por_id.get(cid) if running else None
        if raw_stats:
            cpu_pct, cpu_sec = calcular_cpu_percent(raw_stats)
            mem_used, mem_limit, mem_pct = calcular_memoria_bytes(raw_stats)
        else:
            cpu_pct, cpu_sec = 0.0, 0.0
            mem_used, mem_limit, mem_pct = 0, 0, 0.0

        resultados.append(
            {
                "container": container_name,
                "service": service_name,
                "running": running,
                "cpu_percent": cpu_pct,
                "cpu_seconds_total": cpu_sec,
                "memory_usage_bytes": mem_used,
                "memory_limit_bytes": mem_limit,
                "memory_percent": mem_pct,
            }
        )
    resultados.sort(key=lambda d: (d["service"], d["container"]))
    return resultados


class DockerStatsCache:
    """Recolector periódico en segundo plano de estadísticas reales por contenedor Docker."""

    def __init__(
        self,
        socket_path: Path = DOCKER_SOCKET_DEFAULT,
        project: str | None = None,
        interval_s: float = 5.0,
    ):
        self.socket_path = Path(socket_path)
        self.project = (
            project
            if project is not None
            else os.environ.get("DOCKER_COMPOSE_PROJECT", "pipeline-nyc-taxi")
        )
        self.interval_s = float(interval_s)
        self._lock = threading.Lock()
        self._snapshot: list[dict] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def refresh_once(self) -> list[dict]:
        """Consulta una vez a Docker Engine sin lanzar excepciones ante fallos temporales."""
        if not self.socket_path.exists():
            return self.snapshot()
        try:
            filters = json.dumps(
                {"label": [f"com.docker.compose.project={self.project}"]}
            )
            query = urllib.parse.urlencode({"all": "1", "filters": filters})
            contenedores = _docker_get_json(self.socket_path, f"/containers/json?{query}")
            contenedores = [
                c
                for c in contenedores
                if str((c.get("Labels") or {}).get("com.docker.compose.oneoff", "")).lower()
                != "true"
            ]
            corriendo = [
                c for c in contenedores if str(c.get("State", "")).lower() == "running"
            ]
            stats_por_id: dict[str, dict] = {}
            if corriendo:
                def _fetch(cid: str) -> tuple[str, dict | None]:
                    try:
                        return cid, _docker_get_json(
                            self.socket_path,
                            f"/containers/{cid}/stats?stream=false",
                            timeout=6.0,
                        )
                    except Exception:
                        return cid, None

                with ThreadPoolExecutor(max_workers=min(6, len(corriendo))) as pool:
                    for cid, st in pool.map(_fetch, [c["Id"] for c in corriendo]):
                        if st is not None:
                            stats_por_id[cid] = st

            parsed = parsear_contenedores_stats(contenedores, stats_por_id)
            with self._lock:
                self._snapshot = parsed
            return parsed
        except Exception:
            return self.snapshot()

    def snapshot(self) -> list[dict]:
        with self._lock:
            return list(self._snapshot)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.refresh_once()
            self._stop.wait(self.interval_s)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)


# ---------------------------------------------------------------------------
# Collector Prometheus
# ---------------------------------------------------------------------------


class NycTaxiCollector:
    """Collector Prometheus personalizado que lee métricas reales del pipeline y Docker."""

    def __init__(
        self,
        metrics_path: Path = METRICS_FILE_DEFAULT,
        data_dir: Path = Path("data"),
        docker_cache: DockerStatsCache | None = None,
    ):
        self.metrics_path = Path(metrics_path)
        self.data_dir = Path(data_dir)
        self.docker_cache = docker_cache

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

        etapas_g = GaugeMetricFamily(
            "nyc_taxi_pipeline_stage_duration_seconds",
            "Duración por etapa Dagster en segundos de la última ejecución",
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
            rows_g.add_metric(
                ["marts_agg_zone_hour_trips", "all"],
                float(marts.get("agg_zone_hour_trips", 0)),
            )
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

        disk_target = self.data_dir if self.data_dir.exists() else Path(".")
        du = shutil.disk_usage(disk_target)
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

        if self.docker_cache is not None:
            contenedores = self.docker_cache.snapshot()
            cpu_pct_g = GaugeMetricFamily(
                "nyc_taxi_container_cpu_percent",
                "Uso real de CPU del contenedor Docker (% donde 100% = 1 núcleo lógico)",
                labels=["container", "service"],
            )
            cpu_sec_c = CounterMetricFamily(
                "nyc_taxi_container_cpu_usage_seconds",
                "Tiempo acumulado de CPU del contenedor Docker (segundos)",
                labels=["container", "service"],
            )
            mem_used_g = GaugeMetricFamily(
                "nyc_taxi_container_memory_usage_bytes",
                "Memoria RAM real utilizada por el contenedor Docker (bytes, sin caché inactiva)",
                labels=["container", "service"],
            )
            mem_limit_g = GaugeMetricFamily(
                "nyc_taxi_container_memory_limit_bytes",
                "Límite de memoria configurado para el contenedor Docker (bytes)",
                labels=["container", "service"],
            )
            mem_pct_g = GaugeMetricFamily(
                "nyc_taxi_container_memory_percent",
                "Porcentaje de memoria utilizada respecto al límite del contenedor",
                labels=["container", "service"],
            )
            running_g = GaugeMetricFamily(
                "nyc_taxi_container_running",
                "Estado del contenedor Docker (1=running, 0=detenido)",
                labels=["container", "service"],
            )

            for item in contenedores:
                lbls = [item["container"], item["service"]]
                cpu_pct_g.add_metric(lbls, float(item["cpu_percent"]))
                cpu_sec_c.add_metric(lbls, float(item["cpu_seconds_total"]))
                mem_used_g.add_metric(lbls, float(item["memory_usage_bytes"]))
                mem_limit_g.add_metric(lbls, float(item["memory_limit_bytes"]))
                mem_pct_g.add_metric(lbls, float(item["memory_percent"]))
                running_g.add_metric(lbls, float(item["running"]))

            yield cpu_pct_g
            yield cpu_sec_c
            yield mem_used_g
            yield mem_limit_g
            yield mem_pct_g
            yield running_g


def construir_registry(
    metrics_path: Path = METRICS_FILE_DEFAULT,
    data_dir: Path = Path("data"),
    docker_cache: DockerStatsCache | None = None,
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
            docker_cache=docker_cache,
        )
    )
    return registry


def iniciar_exporter(
    port: int = 8000,
    host: str = "0.0.0.0",
    metrics_path: Path = METRICS_FILE_DEFAULT,
    data_dir: Path = Path("data"),
) -> None:
    docker_cache: DockerStatsCache | None = None
    if DOCKER_SOCKET_DEFAULT.exists():
        docker_cache = DockerStatsCache(socket_path=DOCKER_SOCKET_DEFAULT)
        docker_cache.start()

    registry = construir_registry(
        metrics_path=metrics_path,
        data_dir=data_dir,
        docker_cache=docker_cache,
    )

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
            return

    server = ThreadingHTTPServer((host, port), MetricsHandler)
    print(f"[metrics-exporter] Escuchando en http://{host}:{port}/metrics", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if docker_cache is not None:
            docker_cache.stop()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Exporter Prometheus de métricas NYC Taxi y Docker")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--metrics-file", default=str(METRICS_FILE_DEFAULT))
    args = ap.parse_args(argv)
    iniciar_exporter(port=args.port, host=args.host, metrics_path=Path(args.metrics_file))
    return 0


if __name__ == "__main__":
    sys.exit(main())
