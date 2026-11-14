from pathlib import Path

import duckdb

from src.modelo import ruta_warehouse

con = duckdb.connect(str(ruta_warehouse(Path("data"))), read_only=True)

print("trip_id único (esperado True):")
con.sql("SELECT count(*) = count(DISTINCT trip_id) AS ok FROM fact_trips").show()

print("Hechos sin zona de origen, destino o fecha (esperado 0):")
con.sql("""
  SELECT count(*) AS huerfanos FROM fact_trips f
  LEFT JOIN dim_zone z ON z.location_id = f.pu_location_id
  LEFT JOIN dim_zone d ON d.location_id = f.do_location_id
  LEFT JOIN dim_date t ON t.date_key = f.pickup_date_key
  WHERE z.location_id IS NULL OR d.location_id IS NULL OR t.date_key IS NULL
""").show()

print("La agregada cierra con el hecho (esperado True):")
con.sql("""
  SELECT (SELECT sum(trips) FROM agg_zone_hour) = (SELECT count(*) FROM fact_trips) AS ok
""").show()

print("Nombres exactos de aeropuertos (los usa la pregunta 5):")
con.sql("SELECT location_id, zone FROM dim_zone WHERE zone ILIKE '%airport%' ORDER BY 1").show()