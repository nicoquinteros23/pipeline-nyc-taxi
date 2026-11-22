{{ config(materialized='view') }}

SELECT
  hash(source_file, file_row_number)               AS trip_id,
  year,
  month,
  source_month,
  pu_location_id,
  do_location_id,
  CAST(strftime(pickup_date, '%Y%m%d') AS INTEGER) AS pickup_date_key,
  pickup_date,
  pickup_hour,
  pickup_dow,
  payment_type,
  trip_distance,
  trip_duration_min,
  avg_speed_mph,
  fare_amount,
  tip_amount,
  tip_pct,
  total_amount,
  cbd_congestion_fee,
  flags,
  pu_borough,
  pu_zone,
  do_borough,
  do_zone
FROM {{ ref('int_trips_enriched') }}
