{% set sel = var('selected_months', none) %}
WITH expected AS (
  SELECT source_month FROM {{ source('dlt_staging', 'selected_periods') }}
),
{% if sel %}
var_months AS (
  {% for m in sel %}
    SELECT '{{ m }}' AS source_month{% if not loop.last %} UNION ALL {% endif %}
  {% endfor %}
),
{% endif %}
stg_months AS (
  SELECT DISTINCT source_month FROM {{ ref('stg_yellow_trips') }}
),
int_months AS (
  SELECT DISTINCT source_month FROM {{ ref('int_trips_enriched') }}
),
rec_months AS (
  SELECT DISTINCT source_month FROM {{ ref('int_reconciliation_summary') }}
),
fact_months AS (
  SELECT DISTINCT source_month FROM {{ ref('fact_trips') }}
),
agg_months AS (
  SELECT DISTINCT strftime(pickup_date, '%Y-%m') AS source_month FROM {{ ref('agg_zone_hour') }}
)
SELECT 'empty_selected_periods' AS issue, CAST(COUNT(*) AS VARCHAR) AS detail
FROM expected
HAVING COUNT(*) = 0
{% if sel %}
UNION ALL
SELECT 'mismatch_var_vs_expected', COALESCE(v.source_month, e.source_month)
FROM var_months v
FULL OUTER JOIN expected e USING (source_month)
WHERE v.source_month IS NULL OR e.source_month IS NULL
{% endif %}
UNION ALL
SELECT 'mismatch_stg', COALESCE(e.source_month, x.source_month)
FROM expected e FULL OUTER JOIN stg_months x USING (source_month)
WHERE e.source_month IS NULL OR x.source_month IS NULL
UNION ALL
SELECT 'mismatch_int', COALESCE(e.source_month, x.source_month)
FROM expected e FULL OUTER JOIN int_months x USING (source_month)
WHERE e.source_month IS NULL OR x.source_month IS NULL
UNION ALL
SELECT 'mismatch_rec', COALESCE(e.source_month, x.source_month)
FROM expected e FULL OUTER JOIN rec_months x USING (source_month)
WHERE e.source_month IS NULL OR x.source_month IS NULL
UNION ALL
SELECT 'mismatch_fact', COALESCE(e.source_month, x.source_month)
FROM expected e FULL OUTER JOIN fact_months x USING (source_month)
WHERE e.source_month IS NULL OR x.source_month IS NULL
UNION ALL
SELECT 'mismatch_agg', COALESCE(e.source_month, x.source_month)
FROM expected e FULL OUTER JOIN agg_months x USING (source_month)
WHERE e.source_month IS NULL OR x.source_month IS NULL
