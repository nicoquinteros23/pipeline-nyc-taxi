CREATE OR REPLACE TABLE agg_zone_hour AS
SELECT pu_location_id,
       pickup_date,
       pickup_hour,
       count(*)               AS trips,
       sum(total_amount)      AS total_revenue,
       avg(trip_duration_min) AS avg_duration_min
FROM fact_trips
GROUP BY ALL;