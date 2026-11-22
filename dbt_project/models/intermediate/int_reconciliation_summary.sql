WITH stg AS (
  SELECT '2024-03' AS source_month, COUNT(*) AS staging_rows
  FROM {{ source('dlt_staging', 'yellow_trips_2024_03') }}
  UNION ALL
  SELECT '2025-03' AS source_month, COUNT(*) AS staging_rows
  FROM {{ source('dlt_staging', 'yellow_trips_2025_03') }}
),
acc AS (
  SELECT '2024-03' AS source_month, COUNT(*) AS accepted_rows
  FROM {{ source('pipeline_intermediate', 'trips_validated_2024_03') }}
  UNION ALL
  SELECT '2025-03' AS source_month, COUNT(*) AS accepted_rows
  FROM {{ source('pipeline_intermediate', 'trips_validated_2025_03') }}
),
qua AS (
  SELECT '2024-03' AS source_month, COUNT(*) AS quarantine_rows
  FROM {{ source('pipeline_intermediate', 'trips_quarantine_2024_03') }}
  UNION ALL
  SELECT '2025-03' AS source_month, COUNT(*) AS quarantine_rows
  FROM {{ source('pipeline_intermediate', 'trips_quarantine_2025_03') }}
)
SELECT
  s.source_month,
  s.staging_rows,
  a.accepted_rows,
  q.quarantine_rows,
  s.staging_rows - (a.accepted_rows + q.quarantine_rows) AS diff_rows
FROM stg s
JOIN acc a USING (source_month)
JOIN qua q USING (source_month)
ORDER BY s.source_month
