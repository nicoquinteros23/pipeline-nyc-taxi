import time
from datetime import datetime
from pathlib import Path

from src.calidad import contar_filas, procesar_mes
from src.transformacion import transformar_mes

MES = "2025-03"
PARTICIONES = {
    "2024-03": Path("data/curated/trips/year=2024/month=3/data.parquet"),
    "2025-03": Path("data/curated/trips/year=2025/month=3/data.parquet"),
}


def estado():
    for nombre, archivo in PARTICIONES.items():
        modificado = datetime.fromtimestamp(archivo.stat().st_mtime)
        print(f"  {nombre}: {contar_filas(archivo):>10,} filas | modificado {modificado:%H:%M:%S}")


print(f"ANTES de reprocesar {MES}:")
estado()
time.sleep(2)
procesar_mes(MES)
transformar_mes(MES)
print(f"DESPUES de reprocesar {MES}:")
estado()