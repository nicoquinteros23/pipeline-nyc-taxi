import duckdb

CURADA = "data/curated/trips/*/*/*.parquet"

print("Filas por partición (year y month salen de la ruta, no del archivo):")
duckdb.sql(f"""
  SELECT year, month, count(*) AS filas
  FROM read_parquet('{CURADA}', hive_partitioning = true)
  GROUP BY ALL ORDER BY 1, 2
""").show()

print("Muestra de columnas derivadas y zonas:")
duckdb.sql(f"""
  SELECT trip_duration_min, avg_speed_mph, pickup_date, pickup_hour, pickup_dow,
         tip_pct, pu_zone, pu_borough, do_zone
  FROM read_parquet('{CURADA}', hive_partitioning = true)
  LIMIT 5
""").show()