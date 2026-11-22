WITH base AS (
  SELECT *,
         year(tpep_pickup_datetime) AS year,
         month(tpep_pickup_datetime) AS month,
         date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0
           AS trip_duration_min
  FROM {{ source('pipeline_intermediate', 'trips_validated') }}
),
zonas AS (
  SELECT location_id, borough, zone
  FROM {{ ref('stg_taxi_zones') }}
)
SELECT
  b.*,
  CASE WHEN b.trip_duration_min > 0
       THEN b.trip_distance / (b.trip_duration_min / 60.0) END AS avg_speed_mph,
  CAST(b.tpep_pickup_datetime AS DATE)                         AS pickup_date,
  hour(b.tpep_pickup_datetime)                                 AS pickup_hour,
  isodow(b.tpep_pickup_datetime)                               AS pickup_dow,
  CASE WHEN b.fare_amount > 0
       THEN 100.0 * b.tip_amount / b.fare_amount END           AS tip_pct,
  zp.borough AS pu_borough,
  zp.zone    AS pu_zone,
  zd.borough AS do_borough,
  zd.zone    AS do_zone
FROM base b
LEFT JOIN zonas zp ON zp.location_id = b.pu_location_id
LEFT JOIN zonas zd ON zd.location_id = b.do_location_id
