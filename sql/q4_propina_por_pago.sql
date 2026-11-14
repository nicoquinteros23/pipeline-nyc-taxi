-- Pregunta 4: Propina promedio como % de la tarifa por tipo de pago
SELECT year AS anio,
       payment_type AS tipo_pago,
       CASE payment_type
         WHEN 0 THEN 'Flex Fare trip' WHEN 1 THEN 'Credit card' WHEN 2 THEN 'Cash'
         WHEN 3 THEN 'No charge'      WHEN 4 THEN 'Dispute'     WHEN 5 THEN 'Unknown'
         WHEN 6 THEN 'Voided trip'    ELSE 'Otro' END AS descripcion,
       count(*) AS viajes,
       round(avg(tip_pct), 2) AS propina_promedio_pct,
       round(avg(tip_amount), 2) AS propina_promedio_usd
FROM fact_trips
WHERE month = {MES} AND year IN ({ANIO}, {ANIO_ANT})
GROUP BY year, payment_type
ORDER BY year, payment_type;