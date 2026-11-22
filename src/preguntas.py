import argparse
import sys
from pathlib import Path

import duckdb
import yaml

SIN_SEPARADOR = {"anio", "hora", "puesto", "tipo_pago"}   # identificadores, no cantidades
MESES_ES = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}
PERIODO_INTERPRETACIONES_BASE = ("2024-03", "2025-03")


def ruta_warehouse(base_dir: Path = Path("data")) -> Path:
    return Path(base_dir) / "warehouse" / "nyc_taxi_dlt.duckdb"


def _etiqueta_mes(mes_str: str) -> str:
    anio, mm = (int(x) for x in mes_str.split("-"))
    return f"{MESES_ES[mm]} {anio}"


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


def ejecutar(
    mes,
    mes_cmp=None,
    base_dir=Path("data"),
    sql_dir=Path("sql"),
    salida=Path("RESULTADOS.md"),
    interpretaciones=Path("docs/interpretaciones.yaml"),
    copia_periodo: Path | None = None,
):
    anio, mm = (int(x) for x in mes.split("-"))
    if not mes_cmp:
        mes_cmp = f"{anio - 1:04d}-{mm:02d}"
    anio_ant = int(mes_cmp.split("-")[0])
    marcadores = {"{MES}": str(mm), "{ANIO}": str(anio), "{ANIO_ANT}": str(anio_ant)}

    textos = {}
    if (mes_cmp, mes) == PERIODO_INTERPRETACIONES_BASE and Path(interpretaciones).exists():
        textos = yaml.safe_load(Path(interpretaciones).read_text(encoding="utf-8")) or {}

    etiqueta_actual = _etiqueta_mes(mes)
    etiqueta_ant = _etiqueta_mes(mes_cmp)

    con = duckdb.connect(str(ruta_warehouse(Path(base_dir))), read_only=True)
    try:
        con.execute("SET memory_limit = '1.5GB'")
        con.execute("SET search_path = 'marts,main'")
        partes = [
            f"# Resultados: {etiqueta_actual} vs {etiqueta_ant} ({mes} vs {mes_cmp})",
            "",
            "Generado por `src/preguntas.py` sobre el esquema `marts` de `nyc_taxi_dlt.duckdb`. "
            "Los números corresponden a viajes **aceptados** (después de la cuarentena).",
            "",
        ]
        for archivo in sorted(Path(sql_dir).glob("q*.sql")):
            sql = archivo.read_text(encoding="utf-8")
            for marcador, valor in marcadores.items():
                sql = sql.replace(marcador, valor)
            titulo = sql.splitlines()[0].lstrip("- ").strip()
            cur = con.execute(sql)
            columnas = [d[0] for d in cur.description]
            filas = cur.fetchall()
            clave = archivo.stem.split("_")[0]
            interp = textos.get(clave) or "(interpretación pendiente para este período)"
            partes += [
                f"## {titulo}",
                "",
                f"Consulta: `sql/{archivo.name}`",
                "",
                "```sql",
                sql.strip(),
                "```",
                "",
                _tabla_md(columnas, filas),
                "",
                f"**Interpretación:** {interp}",
                "",
            ]
    finally:
        con.close()

    contenido = "\n".join(partes)
    Path(salida).parent.mkdir(parents=True, exist_ok=True)
    Path(salida).write_text(contenido, encoding="utf-8")
    if copia_periodo is not None:
        Path(copia_periodo).parent.mkdir(parents=True, exist_ok=True)
        Path(copia_periodo).write_text(contenido, encoding="utf-8")
    return Path(salida)


def main():
    ap = argparse.ArgumentParser(description="Responde las 6 preguntas sobre marts y escribe RESULTADOS.md")
    ap.add_argument("--mes", default="2025-03")
    ap.add_argument("--mes-comparacion", default="")
    args = ap.parse_args()
    mes_cmp = args.mes_comparacion.strip() or None
    try:
        ruta = ejecutar(args.mes, mes_cmp)
    except Exception as exc:
        print(f"ERROR en las preguntas: {exc}", file=sys.stderr)
        return 1
    print(f"Escrito {ruta}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
