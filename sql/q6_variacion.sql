-- Pregunta 6: Variación de viajes totales y de tarifa media entre los dos meses
WITH m AS (
  SELECT year, count(*) AS viajes, avg(fare_amount) AS tarifa_media
  FROM fact_trips
  WHERE month = {MES} AND year IN ({ANIO}, {ANIO_ANT})
  GROUP BY year
)
SELECT
  max(viajes) FILTER (WHERE year = {ANIO_ANT}) AS viajes_anterior,
  max(viajes) FILTER (WHERE year = {ANIO}) AS viajes_actual,
  round(100.0 * (max(viajes) FILTER (WHERE year = {ANIO}) - max(viajes) FILTER (WHERE year = {ANIO_ANT}))
        / max(viajes) FILTER (WHERE year = {ANIO_ANT}), 2) AS variacion_viajes_pct,
  round(max(tarifa_media) FILTER (WHERE year = {ANIO_ANT}), 2) AS tarifa_media_anterior,
  round(max(tarifa_media) FILTER (WHERE year = {ANIO}), 2) AS tarifa_media_actual,
  round(100.0 * (max(tarifa_media) FILTER (WHERE year = {ANIO}) - max(tarifa_media) FILTER (WHERE year = {ANIO_ANT}))
        / max(tarifa_media) FILTER (WHERE year = {ANIO_ANT}), 2) AS variacion_tarifa_pct
FROM m;