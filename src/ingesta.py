import argparse
import csv
import hashlib
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import pyarrow.parquet as pq

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{mes}.parquet"
ZONAS_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
CAMPOS_LOG = ["timestamp_utc", "archivo", "url", "filas", "bytes",
              "sha256", "estado", "accion", "detalle"]

def sha256(ruta, bloque=1 << 20):
    """Hash SHA-256 del archivo, leído de a 1 MB para no cargarlo entero en RAM."""
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        while chunk := f.read(bloque):
            h.update(chunk)
    return h.hexdigest()


def es_transitorio(exc):
    """True si vale la pena reintentar: fallas de red o errores temporales del servidor."""
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        return exc.response.status_code in (429, 500, 502, 503, 504)
    return False


def descargar(url, destino, reintentos=3):
    """Descarga a un .part y lo renombra al terminar. Reintenta con espera creciente."""
    destino = Path(destino)
    espera = 2
    for intento in range(1, reintentos + 1):
        try:
            tmp = destino.with_suffix(destino.suffix + ".part")
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk)
            tmp.replace(destino)
            return
        except Exception as exc:
            if not es_transitorio(exc) or intento == reintentos:
                raise
            print(f"Intento {intento} falló ({exc}). Reintento en {espera}s...")
            time.sleep(espera)
            espera *= 2

def contar_filas_parquet(ruta):
    """Cuenta filas leyendo solo los metadatos del Parquet (instantáneo)."""
    return pq.ParquetFile(ruta).metadata.num_rows


def contar_filas_csv(ruta):
    """Cuenta filas de datos de un CSV (sin la línea de encabezado)."""
    with open(ruta, encoding="utf-8") as f:
        return sum(1 for _ in f) - 1


def ultimo_hash_ok(log, archivo):
    """Hash de la última descarga exitosa de ese archivo, según el log."""
    if not log.exists():
        return None
    with open(log, newline="", encoding="utf-8") as f:
        filas = [r for r in csv.DictReader(f)
                 if r["archivo"] == archivo and r["estado"] == "ok"]
    return filas[-1]["sha256"] if filas else None


def registrar(log, **campos):
    """Agrega una línea al log de control. El log solo crece, nunca se edita."""
    log.parent.mkdir(parents=True, exist_ok=True)
    nuevo = not log.exists()
    campos["timestamp_utc"] = datetime.now(timezone.utc).isoformat()
    with open(log, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS_LOG)
        if nuevo:
            w.writeheader()
        w.writerow({k: campos.get(k, "") for k in CAMPOS_LOG})


def _ingestar_archivo(nombre, url, destino, log, descargar, contar_filas):
    """Núcleo idempotente: descarga solo si falta o si el hash no coincide con el log."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    try:
        previo = ultimo_hash_ok(log, nombre)
        if destino.exists() and previo:
            actual = sha256(destino)
            if actual == previo:
                registrar(log, archivo=nombre, url=url,
                          filas=contar_filas(destino),
                          bytes=destino.stat().st_size, sha256=actual,
                          estado="ok", accion="omitido")
                print(f"{nombre}: ya existe y el hash coincide, no se descarga.")
                return
        descargar(url, destino)
        registrar(log, archivo=nombre, url=url,
                  filas=contar_filas(destino),
                  bytes=destino.stat().st_size, sha256=sha256(destino),
                  estado="ok", accion="descargado")
        print(f"{nombre}: descargado.")
    except Exception as exc:
        registrar(log, archivo=nombre, url=url, estado="error", detalle=str(exc))
        raise


def ingestar(mes, base_dir=Path("data"), descargar=descargar,
             contar_filas=contar_filas_parquet):
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", mes):
        raise ValueError(f"Mes inválido: {mes!r} (formato esperado AAAA-MM)")
    anio, mm = mes.split("-")
    nombre = f"yellow_tripdata_{mes}.parquet"
    destino = base_dir / "raw" / "yellow" / f"year={anio}" / f"month={mm}" / nombre
    log = base_dir / "control" / "ingestion_log.csv"
    _ingestar_archivo(nombre, BASE.format(mes=mes), destino, log,
                      descargar, contar_filas)


def ingestar_zonas(base_dir=Path("data"), descargar=descargar,
                   contar_filas=contar_filas_csv):
    nombre = "taxi_zone_lookup.csv"
    destino = base_dir / "raw" / "zones" / nombre
    log = base_dir / "control" / "ingestion_log.csv"
    _ingestar_archivo(nombre, ZONAS_URL, destino, log, descargar, contar_filas)


def main():
    ap = argparse.ArgumentParser(description="Ingesta de taxis amarillos NYC")
    ap.add_argument("--mes", required=True, help="Formato AAAA-MM, ej: 2025-03")
    args = ap.parse_args()
    try:
        ingestar_zonas()
        ingestar(args.mes)
    except Exception as exc:
        print(f"ERROR de ingesta: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())