# Resultados: 2025-03 vs 2024-03

Generado automáticamente por `src/preguntas.py` sobre el modelo estrella. Los números corresponden a viajes **aceptados** (después de la cuarentena).

## Pregunta 1: Top 10 zonas de origen por viajes y variación contra el mismo mes del año anterior

Consulta: `sql/q1_top_zonas.sql`

```sql
-- Pregunta 1: Top 10 zonas de origen por viajes y variación contra el mismo mes del año anterior
WITH v AS (
  SELECT year, pu_location_id, count(*) AS viajes
  FROM fact_trips
  WHERE month = 3 AND year IN (2025, 2024)
  GROUP BY year, pu_location_id
)
SELECT z.zone AS zona,
       a.viajes AS viajes_actual,
       p.viajes AS viajes_anterior,
       round(100.0 * (a.viajes - p.viajes) / p.viajes, 1) AS variacion_pct
FROM v a
LEFT JOIN v p ON p.pu_location_id = a.pu_location_id AND p.year = 2024
JOIN dim_zone z ON z.location_id = a.pu_location_id
WHERE a.year = 2025
ORDER BY a.viajes DESC, z.zone
LIMIT 10;
```

| zona | viajes_actual | viajes_anterior | variacion_pct |
|---|---|---|---|
| Midtown Center | 175,034 | 158,988 | 10.1 |
| Upper East Side South | 165,094 | 152,245 | 8.4 |
| JFK Airport | 148,224 | 149,429 | -0.8 |
| Upper East Side North | 147,813 | 142,716 | 3.6 |
| Times Sq/Theatre District | 130,690 | 118,564 | 10.2 |
| Penn Station/Madison Sq West | 127,849 | 111,780 | 14.4 |
| Midtown East | 123,952 | 120,657 | 2.7 |
| Lincoln Square East | 113,249 | 111,845 | 1.3 |
| Union Sq | 110,036 | 94,612 | 16.3 |
| East Village | 106,631 | 89,492 | 19.2 |

**Interpretación:** De las 10 zonas con más viajes, 9 crecieron entre 1,3 % y 19,2 % respecto de marzo de 2024 y solo JFK Airport bajó (-0,8 %); Midtown Center encabeza con 175.034 viajes y East Village (+19,2 %) y Union Sq (+16,3 %) crecieron más que el total (+14,53 %).

## Pregunta 2: Hora del día con mayor ingreso promedio por viaje (primera fila de cada año; se muestran las 3 primeras)

Consulta: `sql/q2_hora_mayor_ingreso.sql`

```sql
-- Pregunta 2: Hora del día con mayor ingreso promedio por viaje (primera fila de cada año; se muestran las 3 primeras)
WITH h AS (
  SELECT year, pickup_hour,
         count(*) AS viajes,
         round(avg(total_amount), 2) AS ingreso_promedio_por_viaje
  FROM fact_trips
  WHERE month = 3 AND year IN (2025, 2024)
  GROUP BY year, pickup_hour
),
r AS (
  SELECT *, row_number() OVER (PARTITION BY year ORDER BY ingreso_promedio_por_viaje DESC) AS puesto
  FROM h
)
SELECT year AS anio, puesto, pickup_hour AS hora, ingreso_promedio_por_viaje, viajes
FROM r
WHERE puesto <= 3
ORDER BY year, puesto;
```

| anio | puesto | hora | ingreso_promedio_por_viaje | viajes |
|---|---|---|---|---|
| 2024 | 1 | 5 | 35.58 | 21,431 |
| 2024 | 2 | 6 | 31.68 | 47,161 |
| 2024 | 3 | 16 | 30.65 | 209,900 |
| 2025 | 1 | 5 | 33.56 | 31,995 |
| 2025 | 2 | 4 | 30.46 | 31,727 |
| 2025 | 3 | 16 | 30.38 | 221,197 |

**Interpretación:** La hora con mayor ingreso promedio por viaje (promedio de total_amount) son las 5 h en ambos meses (USD 35,58 en 2024 y 33,56 en 2025), pero con pocos viajes (21.431 y 31.995); las 16 h ocupan el tercer puesto en ambos años con unos 210.000 a 221.000 viajes.

## Pregunta 3: % de viajes con cbd_congestion_fee > 0 en el mes asignado y las 5 zonas de origen con mayor proporción (mínimo 1.000 viajes por zona)

Consulta: `sql/q3_cbd_congestion.sql`

```sql
-- Pregunta 3: % de viajes con cbd_congestion_fee > 0 en el mes asignado y las 5 zonas de origen con mayor proporción (mínimo 1.000 viajes por zona)
WITH base AS (
  SELECT pu_location_id, cbd_congestion_fee > 0 AS con_cargo
  FROM fact_trips
  WHERE year = 2025 AND month = 3
),
total_mes AS (
  SELECT count(*) AS viajes_del_mes,
         round(100.0 * avg(CAST(con_cargo AS INTEGER)), 2) AS pct_con_cargo_en_el_mes
  FROM base
),
por_zona AS (
  SELECT z.zone AS zona, count(*) AS viajes,
         round(100.0 * avg(CAST(b.con_cargo AS INTEGER)), 2) AS pct_zona_con_cargo
  FROM base b
  JOIN dim_zone z ON z.location_id = b.pu_location_id
  GROUP BY z.zone
  HAVING count(*) >= 1000
)
SELECT p.zona, p.viajes, p.pct_zona_con_cargo, t.pct_con_cargo_en_el_mes, t.viajes_del_mes
FROM por_zona p CROSS JOIN total_mes t
ORDER BY p.pct_zona_con_cargo DESC, p.zona
LIMIT 5;
```

| zona | viajes | pct_zona_con_cargo | pct_con_cargo_en_el_mes | viajes_del_mes |
|---|---|---|---|---|
| SoHo | 34,502 | 99.83 | 74.25 | 3,955,293 |
| Greenwich Village North | 58,177 | 99.76 | 74.25 | 3,955,293 |
| Greenwich Village South | 64,056 | 99.75 | 74.25 | 3,955,293 |
| Little Italy/NoLiTa | 45,863 | 99.75 | 74.25 | 3,955,293 |
| West Village | 96,003 | 99.75 | 74.25 | 3,955,293 |

**Interpretación:** El 74,25 % de los 3.955.293 viajes de marzo de 2025 tuvo cbd_congestion_fee mayor a 0; las cinco zonas con mayor proporción (SoHo, Greenwich Village North y South, Little Italy/NoLiTa y West Village) son del sur de Manhattan y superan el 99,7 %.

## Pregunta 4: Propina promedio como % de la tarifa por tipo de pago

Consulta: `sql/q4_propina_por_pago.sql`

```sql
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
WHERE month = 3 AND year IN (2025, 2024)
GROUP BY year, payment_type
ORDER BY year, payment_type;
```

| anio | tipo_pago | descripcion | viajes | propina_promedio_pct | propina_promedio_usd |
|---|---|---|---|---|---|
| 2024 | 0 | Flex Fare trip | 379,418 | 3.4 | 0.69 |
| 2024 | 1 | Credit card | 2,575,265 | 25.18 | 4.28 |
| 2024 | 2 | Cash | 457,678 | 0.0 | 0.0 |
| 2024 | 3 | No charge | 12,650 | 0.01 | 0.0 |
| 2024 | 4 | Dispute | 28,551 | 0.19 | 0.02 |
| 2025 | 0 | Flex Fare trip | 854,427 | 1.97 | 0.36 |
| 2025 | 1 | Credit card | 2,663,793 | 26.1 | 4.24 |
| 2025 | 2 | Cash | 377,925 | 0.0 | 0.0 |
| 2025 | 3 | No charge | 14,278 | 0.01 | 0.0 |
| 2025 | 4 | Dispute | 44,870 | 0.05 | 0.01 |

**Interpretación:** La propina promedio es 26,1 % de la tarifa con tarjeta de crédito (25,18 % en 2024); el 0,0 % de efectivo no es interpretable porque el sistema no registra la propina en efectivo, así que refleja una limitación del dato y no que los pasajeros no den propina.

## Pregunta 5: Viajes y tarifa media de los que empiezan o terminan en JFK o LaGuardia

Consulta: `sql/q5_aeropuertos.sql`

```sql
-- Pregunta 5: Viajes y tarifa media de los que empiezan o terminan en JFK o LaGuardia
SELECT f.year AS anio,
       count(*) AS viajes,
       round(avg(f.fare_amount), 2) AS tarifa_media
FROM fact_trips f
JOIN dim_zone o ON o.location_id = f.pu_location_id
JOIN dim_zone d ON d.location_id = f.do_location_id
WHERE f.month = 3 AND f.year IN (2025, 2024)
  AND (o.zone IN ('JFK Airport', 'LaGuardia Airport')
    OR d.zone IN ('JFK Airport', 'LaGuardia Airport'))
GROUP BY f.year
ORDER BY f.year;
```

| anio | viajes | tarifa_media |
|---|---|---|
| 2024 | 320,262 | 55.7 |
| 2025 | 318,293 | 55.29 |

**Interpretación:** Los viajes que empiezan o terminan en JFK o LaGuardia fueron 318.293 en marzo de 2025 (320.262 en 2024, -0,6 %) con una tarifa media de USD 55,29 (55,70 en 2024), unas tres veces la tarifa media general (USD 18,40).

## Pregunta 6: Variación de viajes totales y de tarifa media entre los dos meses

Consulta: `sql/q6_variacion.sql`

```sql
-- Pregunta 6: Variación de viajes totales y de tarifa media entre los dos meses
WITH m AS (
  SELECT year, count(*) AS viajes, avg(fare_amount) AS tarifa_media
  FROM fact_trips
  WHERE month = 3 AND year IN (2025, 2024)
  GROUP BY year
)
SELECT
  max(viajes) FILTER (WHERE year = 2024) AS viajes_anterior,
  max(viajes) FILTER (WHERE year = 2025) AS viajes_actual,
  round(100.0 * (max(viajes) FILTER (WHERE year = 2025) - max(viajes) FILTER (WHERE year = 2024))
        / max(viajes) FILTER (WHERE year = 2024), 2) AS variacion_viajes_pct,
  round(max(tarifa_media) FILTER (WHERE year = 2024), 2) AS tarifa_media_anterior,
  round(max(tarifa_media) FILTER (WHERE year = 2025), 2) AS tarifa_media_actual,
  round(100.0 * (max(tarifa_media) FILTER (WHERE year = 2025) - max(tarifa_media) FILTER (WHERE year = 2024))
        / max(tarifa_media) FILTER (WHERE year = 2024), 2) AS variacion_tarifa_pct
FROM m;
```

| viajes_anterior | viajes_actual | variacion_viajes_pct | tarifa_media_anterior | tarifa_media_actual | variacion_tarifa_pct |
|---|---|---|---|---|---|
| 3,453,562 | 3,955,293 | 14.53 | 19.06 | 18.4 | -3.47 |

**Interpretación:** pendiente
