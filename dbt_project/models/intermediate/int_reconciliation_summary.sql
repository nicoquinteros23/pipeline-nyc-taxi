WITH expected AS (
  SELECT source_month
  FROM {{ source('dlt_staging', 'selected_periods') }}
),
stg AS (
  SELECT source_month, COUNT(*) AS staging_rows
  FROM {{ source('dlt_staging', 'yellow_trips_selected') }}
  GROUP BY source_month
),
acc AS (
  SELECT source_month, COUNT(*) AS accepted_rows
  FROM {{ source('pipeline_intermediate', 'trips_validated') }}
  GROUP BY source_month
),
qua AS (
  SELECT source_month, COUNT(*) AS quarantine_rows
  FROM {{ source('pipeline_intermediate', 'trips_quarantine') }}
  GROUP BY source_month
),
todos_meses AS (
  SELECT source_month FROM expected
  UNION
  SELECT source_month FROM stg
  UNION
  SELECT source_month FROM acc
  UNION
  SELECT source_month FROM qua
)
SELECT
  m.source_month,
  COALESCE(s.staging_rows, 0) AS staging_rows,
  COALESCE(a.accepted_rows, 0) AS accepted_rows,
  COALESCE(q.quarantine_rows, 0) AS quarantine_rows,
  COALESCE(s.staging_rows, 0) - (COALESCE(a.accepted_rows, 0) + COALESCE(q.quarantine_rows, 0)) AS diff_rows
FROM todos_meses m
LEFT JOIN stg s USING (source_month)
LEFT JOIN acc a USING (source_month)
LEFT JOIN qua q USING (source_month)
ORDER BY m.source_month
