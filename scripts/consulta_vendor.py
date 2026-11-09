import duckdb

duckdb.sql("""
  SELECT VendorID, count(*) AS viajes,
         count(*) FILTER (WHERE tpep_dropoff_datetime <= tpep_pickup_datetime) AS bajada_no_posterior
  FROM read_parquet('data/exploracion/yellow_tripdata_2025-03.parquet')
  GROUP BY 1 ORDER BY 1
""").show()