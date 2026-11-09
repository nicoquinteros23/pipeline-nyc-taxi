import duckdb
from pathlib import Path

D = Path("data/exploracion")
F25 = (D / "yellow_tripdata_2025-03.parquet").as_posix()
F24 = (D / "yellow_tripdata_2024-03.parquet").as_posix()
ZON = (D / "taxi_zone_lookup.csv").as_posix()

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET temp_directory = 'data/tmp'")

lineas = []

def out(texto=""):
    print(texto)
    lineas.append(texto)

def q(titulo, sql):
    out(f"\n=== {titulo} ===")
    cur = con.execute(sql)
    cols = [c[0] for c in cur.description]
    out(" | ".join(cols))
    for fila in cur.fetchall():
        out(" | ".join(str(x) for x in fila))

def explorar(etiqueta, ruta, ini, fin):
    out(f"\n{'#' * 8} {etiqueta} {'#' * 8}")
    R = f"read_parquet('{ruta}')"
    dur = "date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime)"

    q("Filas y rango de fechas de subida", f"""
      SELECT count(*) AS filas,
             min(tpep_pickup_datetime) AS primera_subida,
             max(tpep_pickup_datetime) AS ultima_subida
      FROM {R}""")

    q("Resumen por columna (tipo, min, max, % nulos)", f"""
      SELECT column_name, column_type, min, max, null_percentage
      FROM (SUMMARIZE SELECT * FROM {R})""")

    q("Filas sospechosas (cantidad por condicion)", f"""
      SELECT
        count(*) FILTER (WHERE trip_distance <= 0)       AS distancia_cero_o_neg,
        count(*) FILTER (WHERE total_amount < 0)         AS importe_negativo,
        count(*) FILTER (WHERE fare_amount <= 0)         AS tarifa_cero_o_neg,
        count(*) FILTER (WHERE passenger_count IS NULL)  AS pasajeros_nulo,
        count(*) FILTER (WHERE passenger_count = 0)      AS pasajeros_cero,
        count(*) FILTER (WHERE tpep_dropoff_datetime <= tpep_pickup_datetime)
                                                         AS bajada_no_posterior,
        count(*) FILTER (WHERE tpep_pickup_datetime < TIMESTAMP '{ini}'
                            OR tpep_pickup_datetime >= TIMESTAMP '{fin}')
                                                         AS subida_fuera_de_mes
      FROM {R}""")

    q("Percentiles de distancia (p50, p99, p99.9, p99.99) y maximo", f"""
      SELECT quantile_cont(trip_distance, [0.5, 0.99, 0.999, 0.9999]) AS percentiles,
             max(trip_distance) AS maximo
      FROM {R}""")

    q("Percentiles de duracion en minutos (solo duracion > 0) y maximo", f"""
      SELECT quantile_cont({dur} / 60.0, [0.5, 0.99, 0.999, 0.9999]) AS percentiles,
             max({dur} / 60.0) AS maximo_min
      FROM {R} WHERE {dur} > 0""")

    q("Velocidad promedio en mph (distancia > 0 y duracion > 0)", f"""
      SELECT quantile_cont(trip_distance / ({dur} / 3600.0),
                           [0.5, 0.99, 0.999, 0.9999]) AS percentiles,
             count(*) FILTER (WHERE trip_distance / ({dur} / 3600.0) > 100)
                 AS viajes_mas_de_100_mph
      FROM {R} WHERE {dur} > 0 AND trip_distance > 0""")

    q("Percentiles de total_amount y maximo", f"""
      SELECT quantile_cont(total_amount, [0.5, 0.99, 0.999, 0.9999]) AS percentiles,
             max(total_amount) AS maximo
      FROM {R}""")

    q("Los 5 viajes de mayor distancia", f"""
      SELECT trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime, total_amount
      FROM {R} ORDER BY trip_distance DESC LIMIT 5""")

    q("Las 5 duraciones mas largas (horas)", f"""
      SELECT round({dur} / 3600.0, 1) AS horas, trip_distance, total_amount
      FROM {R} ORDER BY {dur} DESC LIMIT 5""")

    q("Tipos de pago: viajes y propina media", f"""
      SELECT payment_type, count(*) AS viajes, round(avg(tip_amount), 2) AS propina_media
      FROM {R} GROUP BY 1 ORDER BY 1""")

    q("Zonas: ids inexistentes en la tabla de zonas y zonas 264/265", f"""
      SELECT
        count(*) FILTER (WHERE PULocationID NOT IN (SELECT LocationID FROM read_csv('{ZON}'))
                            OR DOLocationID NOT IN (SELECT LocationID FROM read_csv('{ZON}'))
                            OR PULocationID IS NULL OR DOLocationID IS NULL) AS zona_inexistente,
        count(*) FILTER (WHERE PULocationID IN (264, 265)
                            OR DOLocationID IN (264, 265)) AS zona_unknown
      FROM {R}""")

    q("Duplicados exactos (puede tardar)", f"""
      SELECT count(*) AS filas,
             (SELECT count(*) FROM (SELECT DISTINCT * FROM {R})) AS filas_distintas
      FROM {R}""")

explorar("2025-03", F25, "2025-03-01", "2025-04-01")
explorar("2024-03", F24, "2024-03-01", "2024-04-01")

q("Diferencias de esquema 2025 vs 2024 (nombre o tipo)", f"""
  SELECT coalesce(a.column_name, b.column_name) AS columna,
         a.column_type AS tipo_2025, b.column_type AS tipo_2024,
         a.column_name AS nombre_2025, b.column_name AS nombre_2024
  FROM (DESCRIBE SELECT * FROM read_parquet('{F25}')) a
  FULL JOIN (DESCRIBE SELECT * FROM read_parquet('{F24}')) b
    ON lower(a.column_name) = lower(b.column_name)
  WHERE a.column_type IS DISTINCT FROM b.column_type
     OR a.column_name IS DISTINCT FROM b.column_name""")

Path("docs").mkdir(exist_ok=True)
Path("docs/salida_exploracion.txt").write_text("\n".join(lineas), encoding="utf-8")
print("\nListo. Resultado guardado en docs/salida_exploracion.txt")