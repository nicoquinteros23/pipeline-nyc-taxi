import argparse
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil

from src.calidad import procesar_mes
from src.esquema import validar_mes
from src.ingesta import ingestar, ingestar_zonas
from src.modelo import construir_modelo
from src.preguntas import ejecutar
from src.transformacion import transformar_mes


class ErrorPipeline(Exception):
    pass


def mes_comparacion(mes):
    """Mismo mes del año anterior: '2025-03' -> '2024-03'."""
    anio, mm = mes.split("-")
    return f"{int(anio) - 1}-{mm}"


class _MonitorRam:
    """Muestrea la memoria del proceso cada 0,2 s (DuckDB corre dentro del proceso)."""

    def __init__(self):
        self.pico = 0
        self._stop = threading.Event()
        self._hilo = threading.Thread(target=self._muestrear, daemon=True)

    def _muestrear(self):
        proc = psutil.Process(os.getpid())
        while not self._stop.is_set():
            self.pico = max(self.pico, proc.memory_info().rss)
            time.sleep(0.2)

    def __enter__(self):
        self._hilo.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._hilo.join()


def correr(mes, mes_cmp=None, base_dir=Path("data"), reports_dir=Path("reports"),
           sql_dir=Path("sql"), resultados=Path("RESULTADOS.md"), ingesta=True):
    """Ingesta -> esquema -> calidad -> transformación -> modelo -> preguntas,
    para el mes asignado y su mes de comparación. Si una etapa falla, corta."""
    mes_cmp = mes_cmp or mes_comparacion(mes)
    meses = [mes, mes_cmp]
    tiempos = {}

    def etapa(nombre, funcion):
        t0 = time.perf_counter()
        try:
            funcion()
        except Exception as exc:
            raise ErrorPipeline(f"etapa '{nombre}' falló: {exc}") from exc
        tiempos[nombre] = round(time.perf_counter() - t0, 1)
        print(f"[{nombre}] ok en {tiempos[nombre]} s")

    def _ingesta():
        ingestar_zonas(base_dir)
        for m in meses:
            ingestar(m, base_dir)

    with _MonitorRam() as ram:
        inicio = time.perf_counter()
        if ingesta:
            etapa("ingesta", _ingesta)
        etapa("esquema", lambda: [validar_mes(m, base_dir, reports_dir) for m in meses])
        etapa("calidad", lambda: [procesar_mes(m, base_dir, reports_dir) for m in meses])
        etapa("transformacion", lambda: [transformar_mes(m, base_dir) for m in meses])
        etapa("modelo", lambda: construir_modelo(base_dir, Path(sql_dir) / "modelo"))
        etapa("preguntas", lambda: ejecutar(mes, mes_cmp, base_dir=base_dir, sql_dir=sql_dir,
                                            salida=resultados))
        total = round(time.perf_counter() - inicio, 1)

    resumen = {"mes": mes, "mes_comparacion": mes_cmp,
               "fecha_utc": datetime.now(timezone.utc).isoformat(),
               "tiempo_total_s": total, "tiempos_por_etapa_s": tiempos,
               "pico_ram_gb": round(ram.pico / 1e9, 2)}
    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / f"ejecucion_{mes}.json").write_text(
        json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8")
    return resumen


def main(argv=None):
    ap = argparse.ArgumentParser(description="Pipeline completo NYC Taxi")
    ap.add_argument("--mes", required=True, help="Mes asignado, formato AAAA-MM")
    ap.add_argument("--mes-comparacion", default=None,
                    help="Por defecto, el mismo mes del año anterior")
    args = ap.parse_args(argv)
    try:
        r = correr(args.mes, args.mes_comparacion)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"Pipeline completo en {r['tiempo_total_s']} s | pico de RAM: {r['pico_ram_gb']} GB")
    return 0


if __name__ == "__main__":
    sys.exit(main())