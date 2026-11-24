{{ config(materialized='table') }}

SELECT
  pu_location_id,
  pickup_date,
  pickup_hour,
  count(*)               AS trips,
  sum(total_amount)      AS total_revenue,
  avg(trip_duration_min) AS avg_duration_min
FROM {{ ref('fact_trips') }}
GROUP BY pu_location_id, pickup_date, pickup_hour
