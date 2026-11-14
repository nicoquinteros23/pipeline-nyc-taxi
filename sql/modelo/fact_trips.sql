CREATE OR REPLACE VIEW fact_trips AS
SELECT
  hash(source_file, file_row_number)                AS trip_id,
  year, month,
  pu_location_id, do_location_id,
  CAST(strftime(pickup_date, '%Y%m%d') AS INTEGER)  AS pickup_date_key,
  pickup_date, pickup_hour, payment_type,
  trip_distance, trip_duration_min, avg_speed_mph,
  fare_amount, tip_amount, tip_pct, total_amount,
  cbd_congestion_fee, flags
FROM read_parquet('{CURADA}', hive_partitioning = true);