WITH resumen AS (
  SELECT
    SUM(staging_rows) AS tot_staging,
    SUM(accepted_rows) AS tot_accepted,
    SUM(quarantine_rows) AS tot_quarantine,
    SUM(ABS(diff_rows)) AS sum_abs_diff
  FROM {{ ref('int_reconciliation_summary') }}
),
consolidadas AS (
  SELECT
    (SELECT COUNT(*) FROM {{ source('pipeline_intermediate', 'trips_validated') }}) AS val_view_rows,
    (SELECT COUNT(*) FROM {{ source('pipeline_intermediate', 'trips_quarantine') }}) AS qua_view_rows
)
SELECT
  r.tot_staging,
  r.tot_accepted,
  r.tot_quarantine,
  c.val_view_rows,
  c.qua_view_rows
FROM resumen r
CROSS JOIN consolidadas c
WHERE r.sum_abs_diff != 0
   OR r.tot_accepted != c.val_view_rows
   OR r.tot_quarantine != c.qua_view_rows
   OR r.tot_staging != r.tot_accepted + r.tot_quarantine
