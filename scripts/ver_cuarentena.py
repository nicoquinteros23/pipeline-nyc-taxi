import duckdb

Q = "data/quarantine/year=2025/month=3/data.parquet"

print("Combinaciones de reglas violadas:")
duckdb.sql(f"""
  SELECT reglas_violadas, count(*) AS filas
  FROM read_parquet('{Q}')
  GROUP BY 1 ORDER BY 2 DESC
""").show(max_rows=20)

print("Cinco filas de ejemplo con R09 (velocidad imposible):")
duckdb.sql(f"""
  SELECT file_row_number, reglas_violadas, vendor_id, trip_distance, total_amount
  FROM read_parquet('{Q}')
  WHERE list_contains(reglas_violadas, 'R09') LIMIT 5
""").show()
