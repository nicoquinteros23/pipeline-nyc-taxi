from pathlib import Path

import duckdb

from src.modelo import ruta_warehouse

con = duckdb.connect(str(ruta_warehouse(Path("data"))), read_only=True)

print("Tarifa media con y sin las tarifas en cero:")
con.sql("""
  SELECT year AS anio, count(*) AS viajes,
         round(avg(fare_amount), 2) AS tarifa_media,
         round(avg(fare_amount) FILTER (WHERE fare_amount > 0), 2) AS tarifa_media_sin_cero,
         round(100.0 * avg(CAST(fare_amount <= 0 AS INTEGER)), 2) AS pct_tarifa_cero
  FROM fact_trips WHERE month = 3
  GROUP BY year ORDER BY year
""").show()

print("Por tipo de pago:")
con.sql("""
  SELECT year AS anio, payment_type AS tipo_pago, count(*) AS viajes,
         round(100.0 * avg(CAST(fare_amount <= 0 AS INTEGER)), 2) AS pct_tarifa_cero,
         round(avg(fare_amount) FILTER (WHERE fare_amount > 0), 2) AS tarifa_media_sin_cero
  FROM fact_trips WHERE month = 3
  GROUP BY year, payment_type ORDER BY year, payment_type
""").show()