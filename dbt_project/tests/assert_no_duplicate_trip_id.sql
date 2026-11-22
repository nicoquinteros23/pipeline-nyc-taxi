SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT trip_id) AS distinct_ids
FROM {{ ref('fact_trips') }}
HAVING COUNT(*) != COUNT(DISTINCT trip_id)
