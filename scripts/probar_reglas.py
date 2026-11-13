import duckdb

from src.calidad import cargar_reglas, crear_vista_zonas, sql_evaluado
from src.esquema import cargar_contrato, sql_mes

MES = "2025-03"

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET temp_directory = 'data/tmp'")
crear_vista_zonas(con)

reglas = cargar_reglas()
sql = sql_evaluado(sql_mes(MES), cargar_contrato(), reglas, MES)

cuentas = ",\n  ".join(
    f"count(*) FILTER (WHERE list_contains(string_split("
    f"{'reglas_rechazo' if r['severidad'] == 'rechazo' else 'flags'}, ','), '{r['id']}')) AS {r['id']}"
    for r in reglas)
cur = con.execute(f"""
  SELECT count(*) AS filas,
         count(*) FILTER (WHERE reglas_rechazo <> '') AS con_rechazo,
         {cuentas}
  FROM ({sql})""")
for nombre, valor in zip([d[0] for d in cur.description], cur.fetchone()):
    print(f"{nombre:12} {valor:>10,}")