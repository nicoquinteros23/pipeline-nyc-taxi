from pathlib import Path

import yaml

REGLAS = Path("contracts/quality_rules.yaml")


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