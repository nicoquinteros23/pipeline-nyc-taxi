WITH a AS (
  SELECT COUNT(*) AS n_fact FROM {{ ref('fact_trips') }}
),
b AS (
  SELECT COUNT(*) AS n_int FROM {{ source('pipeline_intermediate', 'trips_validated') }}
)
SELECT n_fact, n_int
FROM a CROSS JOIN b
WHERE n_fact != n_int
