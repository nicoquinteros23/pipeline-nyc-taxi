import duckdb
from pathlib import Path

con = duckdb.connect()
lineas = []

def q(titulo, sql):
    print(f"\n=== {titulo} ===")
    lineas.append(f"\n=== {titulo} ===")
    cur = con.execute(sql)
    cab = " | ".join(c[0] for c in cur.description)
    print(cab); lineas.append(cab)
    for fila in cur.fetchall():
        txt = " | ".join(str(x) for x in fila)
        print(txt); lineas.append(txt)

for etiqueta, f in [("2025-03", "yellow_tripdata_2025-03.parquet"),
                    ("2024-03", "yellow_tripdata_2024-03.parquet")]:
    R = f"read_parquet('data/exploracion/{f}')"
    dur = "date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime)"
    print(f"\n######## {etiqueta} ########"); lineas.append(f"\n######## {etiqueta} ########")

    q("Nulos estructurales por payment_type", f"""
      SELECT payment_type, count(*) AS viajes,
             count(*) FILTER (WHERE passenger_count IS NULL) AS pasajeros_nulo,
             count(*) FILTER (WHERE congestion_surcharge IS NULL) AS congestion_nulo
      FROM {R} GROUP BY 1 ORDER BY 1""")

    q("Bajada igual vs anterior a la subida, por VendorID", f"""
      SELECT VendorID,
             count(*) FILTER (WHERE tpep_dropoff_datetime = tpep_pickup_datetime) AS iguales,
             count(*) FILTER (WHERE tpep_dropoff_datetime < tpep_pickup_datetime) AS anteriores
      FROM {R} GROUP BY 1 ORDER BY 1""")

    q("Tarifa <= 0 por payment_type", f"""
      SELECT payment_type, count(*) AS viajes,
             count(*) FILTER (WHERE fare_amount <= 0) AS tarifa_cero_o_neg
      FROM {R} GROUP BY 1 ORDER BY 1""")

    q("Candidatos a umbral: cuantas filas pasan cada limite", f"""
      SELECT
        count(*) FILTER (WHERE trip_distance > 50)  AS dist_mas_50,
        count(*) FILTER (WHERE trip_distance > 100) AS dist_mas_100,
        count(*) FILTER (WHERE trip_distance > 200) AS dist_mas_200,
        count(*) FILTER (WHERE {dur} > 12*3600)     AS dur_mas_12h,
        count(*) FILTER (WHERE {dur} > 24*3600)     AS dur_mas_24h,
        count(*) FILTER (WHERE {dur} > 0 AND trip_distance > 0
                         AND trip_distance / ({dur}/3600.0) > 60)  AS vel_mas_60,
        count(*) FILTER (WHERE {dur} > 0 AND trip_distance > 0
                         AND trip_distance / ({dur}/3600.0) > 80)  AS vel_mas_80,
        count(*) FILTER (WHERE {dur} > 0 AND trip_distance > 0
                         AND trip_distance / ({dur}/3600.0) > 100) AS vel_mas_100,
        count(*) FILTER (WHERE total_amount > 500)  AS total_mas_500,
        count(*) FILTER (WHERE total_amount > 1000) AS total_mas_1000
      FROM {R}""")

Path("docs/salida_exploracion2.txt").write_text("\n".join(lineas), encoding="utf-8")
print("\nListo: docs/salida_exploracion2.txt")