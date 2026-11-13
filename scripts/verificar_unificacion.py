import duckdb
from src.esquema import sql_mes

con = duckdb.connect()
sql25 = sql_mes("2025-03")
sql24 = sql_mes("2024-03")
con.execute(f"CREATE VIEW unificado AS {sql25} UNION ALL {sql24}")

print("Columnas de la tabla unificada:")
con.sql("DESCRIBE unificado").show(max_rows=40)

print("Filas y cobertura de cbd_congestion_fee por archivo:")
con.sql("""
  SELECT source_file,
         count(*) AS filas,
         count(cbd_congestion_fee) AS cbd_no_nulos
  FROM unificado GROUP BY 1 ORDER BY 1
""").show()