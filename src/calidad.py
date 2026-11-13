import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import yaml

from src.esquema import CONTRATO, cargar_contrato, sql_mes

REGLAS = Path("contracts/quality_rules.yaml")


class ErrorReconciliacion(Exception):
    pass


def cargar_reglas(ruta=REGLAS):
    with open(ruta, encoding="utf-8") as f:
        reglas = yaml.safe_load(f)
    ids = [r["id"] for r in reglas]
    if len(ids) != len(set(ids)):
        raise ValueError("Hay ids de regla repetidos")
    for r in reglas:
        if r["severidad"] not in ("rechazo", "advertencia"):
            raise ValueError(f"Severidad inválida en {r['id']}: {r['severidad']}")
    return reglas


def limites_mes(mes):
    """'2025-03' -> inicio y fin (exclusivo) del mes, como texto de timestamp."""
    anio, mm = (int(x) for x in mes.split("-"))
    sig_anio, sig_mes = (anio + 1, 1) if mm == 12 else (anio, mm + 1)
    return {"inicio_mes": f"{anio:04d}-{mm:02d}-01 00:00:00",
            "fin_mes": f"{sig_anio:04d}-{sig_mes:02d}-01 00:00:00"}


def _case_reglas(reglas, mes):
    """Expresión SQL que devuelve las reglas violadas de cada fila, ej. 'R02,R03'
    (texto vacío si no viola ninguna). concat_ws ignora los NULL."""
    if not reglas:
        return "''"
    lim = limites_mes(mes)
    casos = [f"CASE WHEN ({r['violacion'].format(**lim)}) THEN '{r['id']}' END"
             for r in reglas]
    return "concat_ws(',',\n    " + ",\n    ".join(casos) + ")"


def reglas_violadas(con, tabla, reglas, mes, id_col="id"):
    """{id_fila: [reglas violadas]} para una tabla. Pensado para tests con datos sintéticos."""
    sql = f"SELECT {id_col}, {_case_reglas(reglas, mes)} FROM {tabla}"
    return {i: r.split(",") for i, r in con.execute(sql).fetchall() if r}


def crear_vista_zonas(con, base_dir=Path("data")):
    """Vista 'zonas' con los ids válidos; la usa la regla R07."""
    ruta = (base_dir / "raw" / "zones" / "taxi_zone_lookup.csv").as_posix()
    con.execute("CREATE OR REPLACE TEMP VIEW zonas AS "
                f"SELECT LocationID AS location_id FROM read_csv('{ruta}')")


def sql_evaluado(sql_origen, contrato, reglas, mes):
    """Una sola pasada: a cada fila le agrega rn (para duplicados), reglas_rechazo y flags."""
    cols = ", ".join(c["name"] for c in contrato["columns"])
    rechazo = [r for r in reglas if r["severidad"] == "rechazo"]
    advert = [r for r in reglas if r["severidad"] == "advertencia"]
    return f"""
WITH base AS (
  SELECT *,
         row_number() OVER (PARTITION BY {cols} ORDER BY file_row_number) AS rn
  FROM ({sql_origen})
)
SELECT *,
  {_case_reglas(rechazo, mes)} AS reglas_rechazo,
  {_case_reglas(advert, mes)} AS flags
FROM base"""


# ---------------------------------------------------------------- bloque 2

def reconciliar(leidas, aceptadas, cuarentena):
    """filas leídas = aceptadas + cuarentena; si no cuadra, el pipeline falla."""
    if leidas != aceptadas + cuarentena:
        raise ErrorReconciliacion(
            f"leidas={leidas} != aceptadas={aceptadas} + cuarentena={cuarentena}")


def contar_filas(ruta):
    """Filas de un archivo Parquet o de todos los que haya en una carpeta (solo metadatos)."""
    ruta = Path(ruta)
    archivos = [ruta] if ruta.is_file() else sorted(ruta.rglob("*.parquet"))
    return sum(pq.ParquetFile(a).metadata.num_rows for a in archivos)


def _conteos(con, reglas, condicion, columna):
    """{id_regla: cantidad de filas que la violan} sobre las filas que cumplen 'condicion'."""
    if not reglas:
        return {}
    sel = ", ".join(
        f"count(*) FILTER (WHERE list_contains(string_split({columna}, ','), '{r['id']}')) AS {r['id']}"
        for r in reglas)
    cur = con.execute(f"SELECT {sel} FROM evaluado WHERE {condicion}")
    return dict(zip([d[0] for d in cur.description], cur.fetchone()))


def _particion(base, anio, mm):
    return base / f"year={anio}" / f"month={int(mm)}"


def _reemplazar_particion(tmp, destino):
    """Borra SOLO la partición destino y mueve la nueva en su lugar."""
    if destino.exists():
        shutil.rmtree(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(tmp), str(destino))


def _escribir_reportes(rep, reports_dir):
    reports_dir.mkdir(parents=True, exist_ok=True)
    with open(reports_dir / f"calidad_{rep['mes']}.json", "w", encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2)

    def tabla(severidad, conteos):
        filas = [f"| {r['id']} | {r['descripcion']} | {r['umbral']} | {conteos[r['id']]:,} |"
                 for r in rep["reglas"] if r["severidad"] == severidad]
        return ["| ID | Regla | Umbral | Filas |", "|---|---|---|---|"] + filas

    md = [
        f"# Reporte de calidad {rep['mes']}", "",
        f"- Archivo: {rep['archivo']}",
        f"- Filas leídas: {rep['filas_leidas']:,}",
        f"- Aceptadas: {rep['aceptadas']:,}",
        f"- Cuarentena: {rep['cuarentena']:,} ({rep['pct_cuarentena']} %)",
        f"- Aceptadas con alguna advertencia: {rep['aceptadas_con_advertencia']:,}",
        f"- Reconciliación (leídas = aceptadas + cuarentena): {rep['reconciliacion']}", "",
        "## Reglas de rechazo (veces que se violó, sobre todas las filas leídas)", "",
        *tabla("rechazo", rep["rechazo_por_regla"]), "",
        f"Una fila puede violar varias reglas, por eso la suma de esta tabla "
        f"({sum(rep['rechazo_por_regla'].values()):,}) puede ser mayor que la cuarentena "
        f"({rep['cuarentena']:,}).", "",
        "## Advertencias (sobre filas aceptadas; no se descartan)", "",
        *tabla("advertencia", rep["advertencias_por_regla"]), "",
    ]
    (reports_dir / f"calidad_{rep['mes']}.md").write_text("\n".join(md), encoding="utf-8")


def procesar_mes(mes, base_dir=Path("data"), reports_dir=Path("reports"),
                 contrato_path=CONTRATO, reglas_path=REGLAS, memoria="4GB"):
    anio, mm = mes.split("-")
    contrato = cargar_contrato(contrato_path)
    reglas = cargar_reglas(reglas_path)
    origen = (base_dir / "raw" / "yellow" / f"year={anio}" / f"month={mm}"
              / f"yellow_tripdata_{mes}.parquet")

    sql_origen = sql_mes(mes, base_dir, reports_dir, contrato_path)   # valida esquema primero
    filas_leidas = contar_filas(origen)                               # fuente independiente

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{memoria}'")
    con.execute(f"SET temp_directory = '{(base_dir / 'tmp' / 'duckdb').as_posix()}'")
    crear_vista_zonas(con, base_dir)
    con.execute("CREATE TEMP TABLE evaluado AS " + sql_evaluado(sql_origen, contrato, reglas, mes))

    cols = ", ".join(c["name"] for c in contrato["columns"])
    trabajo = base_dir / "tmp" / f"calidad_{mes}"
    shutil.rmtree(trabajo, ignore_errors=True)
    tmp_val, tmp_cua = trabajo / "validada", trabajo / "cuarentena"
    tmp_val.mkdir(parents=True)
    tmp_cua.mkdir(parents=True)

    con.execute(f"""COPY (
        SELECT {cols}, source_file, file_row_number,
               CASE WHEN flags = '' THEN []::VARCHAR[] ELSE string_split(flags, ',') END AS flags
        FROM evaluado WHERE reglas_rechazo = '')
        TO '{(tmp_val / 'data.parquet').as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    con.execute(f"""COPY (
        SELECT {cols}, source_file, file_row_number,
               string_split(reglas_rechazo, ',') AS reglas_violadas
        FROM evaluado WHERE reglas_rechazo <> '')
        TO '{(tmp_cua / 'data.parquet').as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)""")

    aceptadas = contar_filas(tmp_val)          # contadas desde lo ESCRITO en disco
    cuarentena = contar_filas(tmp_cua)

    rechazo = [r for r in reglas if r["severidad"] == "rechazo"]
    advert = [r for r in reglas if r["severidad"] == "advertencia"]
    rep = {
        "mes": mes, "archivo": origen.name,
        "generado_utc": datetime.now(timezone.utc).isoformat(),
        "filas_leidas": filas_leidas, "aceptadas": aceptadas, "cuarentena": cuarentena,
        "pct_cuarentena": round(100.0 * cuarentena / filas_leidas, 2) if filas_leidas else 0.0,
        "aceptadas_con_advertencia": con.execute(
            "SELECT count(*) FROM evaluado WHERE reglas_rechazo = '' AND flags <> ''").fetchone()[0],
        "rechazo_por_regla": _conteos(con, rechazo, "TRUE", "reglas_rechazo"),
        "advertencias_por_regla": _conteos(con, advert, "reglas_rechazo = ''", "flags"),
        "nota": "Una fila puede violar varias reglas: la suma de rechazo_por_regla puede superar a cuarentena.",
        "reglas": [{k: r[k] for k in ("id", "descripcion", "severidad", "umbral")} for r in reglas],
    }
    con.close()

    try:
        reconciliar(filas_leidas, aceptadas, cuarentena)
        rep["reconciliacion"] = "ok"
    except ErrorReconciliacion as exc:
        rep["reconciliacion"] = f"FALLA: {exc}"
        _escribir_reportes(rep, reports_dir)          # queda la evidencia del fallo
        shutil.rmtree(trabajo, ignore_errors=True)    # y NO se toca ninguna partición final
        raise

    _reemplazar_particion(tmp_val, _particion(base_dir / "validada", anio, mm))
    _reemplazar_particion(tmp_cua, _particion(base_dir / "quarantine", anio, mm))
    _escribir_reportes(rep, reports_dir)
    shutil.rmtree(trabajo, ignore_errors=True)
    return rep


def main():
    ap = argparse.ArgumentParser(description="Calidad de datos de un mes")
    ap.add_argument("--mes", required=True, help="Formato AAAA-MM")
    args = ap.parse_args()
    try:
        r = procesar_mes(args.mes)
    except Exception as exc:
        print(f"ERROR de calidad: {exc}", file=sys.stderr)
        return 1
    print(f"{args.mes}: leidas={r['filas_leidas']:,} aceptadas={r['aceptadas']:,} "
          f"cuarentena={r['cuarentena']:,} ({r['pct_cuarentena']} %) "
          f"reconciliacion={r['reconciliacion']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())