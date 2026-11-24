-- Pregunta 1: Top 10 zonas de origen por viajes y variación contra el mismo mes del año anterior
WITH v AS (
  SELECT year, pu_location_id, count(*) AS viajes
  FROM fact_trips
  WHERE month = {MES} AND year IN ({ANIO}, {ANIO_ANT})
  GROUP BY year, pu_location_id
)
SELECT z.zone AS zona,
       a.viajes AS viajes_actual,
       p.viajes AS viajes_anterior,
       round(100.0 * (a.viajes - p.viajes) / p.viajes, 1) AS variacion_pct
FROM v a
LEFT JOIN v p ON p.pu_location_id = a.pu_location_id AND p.year = {ANIO_ANT}
JOIN dim_zone z ON z.location_id = a.pu_location_id
WHERE a.year = {ANIO}
ORDER BY a.viajes DESC, z.zone
LIMIT 10;