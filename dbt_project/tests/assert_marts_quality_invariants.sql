SELECT
  COUNT(*) AS invalid_rows
FROM {{ ref('fact_trips') }}
WHERE trip_distance <= 0
   OR trip_distance > 100
   OR total_amount < 0
   OR total_amount > 1000
   OR trip_duration_min <= 0
HAVING COUNT(*) > 0
