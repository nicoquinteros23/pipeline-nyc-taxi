import argparse
import sys
from pathlib import Path

import duckdb
import yaml

from src.modelo import ruta_warehouse

SIN_SEPARADOR = {"anio", "hora", "puesto", "tipo_pago"}   # identificadores, no cantidades


def _formato(valor, columna=""):
    if valor is None:
        return "NULL"
    if isinstance(valor, int) and columna not in SIN_SEPARADOR:
        return f"{valor:,}"
    return str(valor)


def _tabla_md(columnas, filas):
    lineas = ["| " + " | ".join(columnas) + " |", "|" + "---|" * len(columnas)]
    lineas += ["| " + " | ".join(_formato(v, c) for v, c in zip(fila, columnas)) + " |"
               for fila in filas]
    return "\n".join(lineas)


def ejecutar(mes, mes_cmp, base_dir=Path("data"), sql_dir=Path("sql"),
             salida=Path("RESULTADOS.md"),
             interpretaciones=Path("docs/interpretaciones.yaml")):
    anio, mm = (int(x) for x in mes.split("-"))
    anio_ant = int(mes_cmp.split("-")[0])
    marcadores = {"{MES}": str(mm), "{ANIO}": str(anio), "{ANIO_ANT}": str(anio_ant)}
    textos = {}
    if Path(interpretaciones).exists():
        textos = yaml.safe_load(Path(interpretaciones).read_text(encoding="utf-8")) or {}

    con = duckdb.connect(str(ruta_warehouse(Path(base_dir))), read_only=True)
    partes = [f"# Resultados: {mes} vs {mes_cmp}", "",
              "Generado automáticamente por `src/preguntas.py` sobre el modelo estrella. "
              "Los números corresponden a viajes **aceptados** (después de la cuarentena).", ""]
    for archivo in sorted(Path(sql_dir).glob("q*.sql")):
        sql = archivo.read_text(encoding="utf-8")
        for marcador, valor in marcadores.items():
            sql = sql.replace(marcador, valor)
        titulo = sql.splitlines()[0].lstrip("- ").strip()
        cur = con.execute(sql)
        columnas = [d[0] for d in cur.description]
        filas = cur.fetchall()
        clave = archivo.stem.split("_")[0]
        partes += [f"## {titulo}", "", f"Consulta: `sql/{archivo.name}`", "",
                   "```sql", sql.strip(), "```", "", _tabla_md(columnas, filas), "",
                   f"**Interpretación:** {textos.get(clave) or '(pendiente)'}", ""]
    con.close()
    Path(salida).write_text("\n".join(partes), encoding="utf-8")
    return Path(salida)


def main():
    ap = argparse.ArgumentParser(description="Responde las 6 preguntas y escribe RESULTADOS.md")
    ap.add_argument("--mes", required=True)
    ap.add_argument("--mes-comparacion", required=True)
    args = ap.parse_args()
    try:
        ruta = ejecutar(args.mes, args.mes_comparacion)
    except Exception as exc:
        print(f"ERROR en las preguntas: {exc}", file=sys.stderr)
        return 1
    print(f"Escrito {ruta}")
    return 0


if __name__ == "__main__":
    sys.exit(main())