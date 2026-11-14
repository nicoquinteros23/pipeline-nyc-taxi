CREATE OR REPLACE TABLE dim_date AS
WITH rango AS (
  SELECT CAST(date_trunc('month', min(pickup_date)) AS DATE) AS d0,
         CAST(date_trunc('month', max(pickup_date)) + INTERVAL 1 MONTH - INTERVAL 1 DAY AS DATE) AS d1
  FROM fact_trips
)
SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
       CAST(d AS DATE)                        AS date,
       year(d)    AS year,
       month(d)   AS month,
       day(d)     AS day,
       isodow(d)  AS dow,
       dayname(d) AS dow_name,
       isodow(d) >= 6 AS is_weekend
FROM rango, generate_series(d0, d1, INTERVAL 1 DAY) AS t(d);