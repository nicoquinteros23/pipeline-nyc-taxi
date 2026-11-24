import argparse
import json
import re
import sys
from pathlib import Path

import duckdb
import yaml

CONTRATO = Path("contracts/yellow_schema.yaml")

# (tipo real, tipo esperado) que se pueden castear sin perder información
COMPATIBLES = {
    ("SMALLINT", "INTEGER"), ("SMALLINT", "BIGINT"), ("INTEGER", "BIGINT"),
    ("INTEGER", "DOUBLE"), ("BIGINT", "DOUBLE"), ("FLOAT", "DOUBLE"),
}


class ErrorEsquema(Exception):
    pass


def snake(nombre):
    n = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", nombre)
    n = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", n)
    return n.lower()


def cargar_contrato(ruta=CONTRATO):
    with open(ruta, encoding="utf-8") as f:
        return yaml.safe_load(f)


def esquema_real(ruta_parquet):
    """{nombre_original: tipo} leído del archivo, sin cargar los datos."""
    con = duckdb.connect()
    sql = (f"DESCRIBE SELECT * FROM read_parquet("
           f"'{Path(ruta_parquet).as_posix()}', hive_partitioning = false)")
    return {fila[0]: fila[1] for fila in con.execute(sql).fetchall()}


def _mapa_nombres(contrato):
    """Nombre de origen (o su versión snake_case) -> nombre del contrato."""
    mapa = {}
    for c in contrato["columns"]:
        mapa[c["name"]] = c["name"]
        for alias in c.get("aliases", []):
            mapa[alias] = c["name"]
            mapa[snake(alias)] = c["name"]
    return mapa


def comparar(real, contrato):
    mapa = _mapa_nombres(contrato)
    esperado = {c["name"]: c for c in contrato["columns"]}
    presentes, nuevas = {}, []          # presentes: nombre_contrato -> (original, tipo)
    for original, tipo in real.items():
        destino = mapa.get(original) or mapa.get(snake(original))
        if destino:
            presentes[destino] = (original, tipo)
        else:
            nuevas.append(snake(original))
    tipos = {
        n: {"esperado": esperado[n]["type"], "real": t}
        for n, (_, t) in presentes.items() if t != esperado[n]["type"]
    }
    return {
        "faltantes": sorted(set(esperado) - set(presentes)),
        "nuevas": sorted(nuevas),
        "tipos_distintos": tipos,
        "origen_de_cada_columna": {n: o for n, (o, _) in presentes.items()},
    }


def evaluar(resultado, contrato):
    """Aplica la política del contrato. Devuelve (errores, advertencias)."""
    esperado = {c["name"]: c for c in contrato["columns"]}
    errores, advertencias = [], []
    for n in resultado["faltantes"]:
        if esperado[n].get("nullable", False):
            advertencias.append(f"columna faltante, se completa con NULL: {n}")
        else:
            errores.append(f"columna obligatoria faltante: {n}")
    for n in resultado["nuevas"]:
        advertencias.append(f"columna nueva fuera del contrato (no se carga a la curada): {n}")
    for n, d in resultado["tipos_distintos"].items():
        if (d["real"], d["esperado"]) in COMPATIBLES:
            advertencias.append(f"tipo distinto compatible, se castea: {n} {d['real']} -> {d['esperado']}")
        else:
            errores.append(f"tipo incompatible en {n}: real {d['real']}, esperado {d['esperado']}")
    return errores, advertencias


def validar_mes(mes, base_dir=Path("data"), reports_dir=Path("reports"),
                contrato_path=CONTRATO):
    anio, mm = mes.split("-")
    ruta = (base_dir / "raw" / "yellow" / f"year={anio}" / f"month={mm}"
            / f"yellow_tripdata_{mes}.parquet")
    if not ruta.exists():
        raise ErrorEsquema(f"No existe {ruta}. Corré primero la ingesta.")
    contrato = cargar_contrato(contrato_path)
    resultado = comparar(esquema_real(ruta), contrato)
    errores, advertencias = evaluar(resultado, contrato)
    resultado.update({
        "mes": mes, "archivo": ruta.name, "version_contrato": contrato["version"],
        "advertencias": advertencias, "errores": errores,
        "estado": "error" if errores else ("advertencia" if advertencias else "ok"),
    })
    reports_dir.mkdir(parents=True, exist_ok=True)
    with open(reports_dir / f"schema_{mes}.json", "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)
    if errores:
        raise ErrorEsquema("; ".join(errores))
    return resultado


def sql_unificado(ruta_parquet, contrato, origen):
    """SELECT con las columnas del contrato, en orden, más trazabilidad.
    origen: {nombre_contrato: nombre_original_en_el_parquet} (sale de comparar())."""
    partes = []
    for c in contrato["columns"]:
        n, t = c["name"], c["type"]
        if n in origen:
            partes.append(f'CAST("{origen[n]}" AS {t}) AS {n}')
        else:
            partes.append(f"CAST(NULL AS {t}) AS {n}")      # columna que no existía
    cols = ",\n  ".join(partes)
    ruta = Path(ruta_parquet).as_posix()
    return (f"SELECT\n  {cols},\n  '{Path(ruta_parquet).name}' AS source_file,\n"
            f"  file_row_number\n"
            f"FROM read_parquet('{ruta}', file_row_number = true, "
            f"hive_partitioning = false)")


def sql_mes(mes, base_dir=Path("data"), reports_dir=Path("reports"),
            contrato_path=CONTRATO):
    """Valida el esquema del mes (falla si hay errores) y devuelve su SELECT unificado."""
    resultado = validar_mes(mes, base_dir, reports_dir, contrato_path)
    contrato = cargar_contrato(contrato_path)
    anio, mm = mes.split("-")
    ruta = (base_dir / "raw" / "yellow" / f"year={anio}" / f"month={mm}"
            / f"yellow_tripdata_{mes}.parquet")
    return sql_unificado(ruta, contrato, resultado["origen_de_cada_columna"])


def main():
    ap = argparse.ArgumentParser(description="Valida el esquema de un mes contra el contrato")
    ap.add_argument("--mes", required=True, help="Formato AAAA-MM")
    args = ap.parse_args()
    try:
        r = validar_mes(args.mes)
    except Exception as exc:
        print(f"ERROR de esquema: {exc}", file=sys.stderr)
        return 1
    print(f"{args.mes}: estado={r['estado']}")
    for a in r["advertencias"]:
        print(f"  advertencia: {a}")
    return 0


if __name__ == "__main__":
    sys.exit(main())