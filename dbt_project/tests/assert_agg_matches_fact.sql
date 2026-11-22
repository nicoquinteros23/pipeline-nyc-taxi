WITH f AS (
  SELECT COUNT(*) AS fact_trips, COALESCE(SUM(total_amount), 0.0) AS fact_rev
  FROM {{ ref('fact_trips') }}
),
a AS (
  SELECT COALESCE(SUM(trips), 0) AS agg_trips, COALESCE(SUM(total_revenue), 0.0) AS agg_rev
  FROM {{ ref('agg_zone_hour') }}
)
SELECT f.fact_trips, a.agg_trips, f.fact_rev, a.agg_rev
FROM f CROSS JOIN a
WHERE f.fact_trips != a.agg_trips
   OR ABS(f.fact_rev - a.agg_rev) >= 0.01
