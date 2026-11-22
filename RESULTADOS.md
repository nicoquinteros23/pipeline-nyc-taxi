# Resultados: 2025-11 vs 2024-11

Generado automáticamente por `src/preguntas.py` sobre el modelo estrella. Los números corresponden a viajes **aceptados** (después de la cuarentena).

## Pregunta 1: Top 10 zonas de origen por viajes y variación contra el mismo mes del año anterior

Consulta: `sql/q1_top_zonas.sql`

```sql
-- Pregunta 1: Top 10 zonas de origen por viajes y variación contra el mismo mes del año anterior
WITH v AS (
  SELECT year, pu_location_id, count(*) AS viajes
  FROM fact_trips
  WHERE month = 11 AND year IN (2025, 2024)
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
| Upper East Side South | 184,881 | 175,997 | 5.0 |
| Midtown Center | 173,177 | 166,730 | 3.9 |
| Upper East Side North | 160,131 | 156,405 | 2.4 |
| JFK Airport | 153,139 | 151,470 | 1.1 |
| Penn Station/Madison Sq West | 123,807 | 116,191 | 6.6 |
| Midtown East | 121,789 | 117,921 | 3.3 |
| Times Sq/Theatre District | 118,050 | 121,191 | -2.6 |
| Lincoln Square East | 117,676 | 118,174 | -0.4 |
| Upper West Side South | 103,461 | 101,956 | 1.5 |
| LaGuardia Airport | 102,447 | 101,697 | 0.7 |

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
  WHERE month = 11 AND year IN (2025, 2024)
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
| 2024 | 1 | 5 | 37.07 | 24,548 |
| 2024 | 2 | 16 | 31.65 | 222,988 |
| 2024 | 3 | 4 | 31.28 | 21,992 |
| 2025 | 1 | 16 | 30.8 | 232,272 |
| 2025 | 2 | 5 | 29.85 | 33,559 |
| 2025 | 3 | 17 | 28.89 | 248,627 |

**Interpretación:** La hora con mayor ingreso promedio por viaje (promedio de total_amount) son las 5 h en ambos meses (USD 35,58 en 2024 y 33,56 en 2025), pero con pocos viajes (21.431 y 31.995); las 16 h ocupan el tercer puesto en ambos años con unos 210.000 a 221.000 viajes.

## Pregunta 3: % de viajes con cbd_congestion_fee > 0 en el mes asignado y las 5 zonas de origen con mayor proporción (mínimo 1.000 viajes por zona)

Consulta: `sql/q3_cbd_congestion.sql`

```sql
-- Pregunta 3: % de viajes con cbd_congestion_fee > 0 en el mes asignado y las 5 zonas de origen con mayor proporción (mínimo 1.000 viajes por zona)
WITH base AS (
  SELECT pu_location_id, cbd_congestion_fee > 0 AS con_cargo
  FROM fact_trips
  WHERE year = 2025 AND month = 11
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
  GROUP BY b.pu_location_id, z.zone
  HAVING count(*) >= 1000
)
SELECT p.zona, p.viajes, p.pct_zona_con_cargo, t.pct_con_cargo_en_el_mes, t.viajes_del_mes
FROM por_zona p CROSS JOIN total_mes t
ORDER BY p.pct_zona_con_cargo DESC, p.zona
LIMIT 5;
```

| zona | viajes | pct_zona_con_cargo | pct_con_cargo_en_el_mes | viajes_del_mes |
|---|---|---|---|---|
| Greenwich Village South | 57,337 | 99.75 | 73.2 | 3,893,040 |
| SoHo | 32,392 | 99.74 | 73.2 | 3,893,040 |
| West Village | 82,974 | 99.65 | 73.2 | 3,893,040 |
| Little Italy/NoLiTa | 42,518 | 99.62 | 73.2 | 3,893,040 |
| Midtown North | 102,308 | 99.58 | 73.2 | 3,893,040 |

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
WHERE month = 11 AND year IN (2025, 2024)
GROUP BY year, payment_type
ORDER BY year, payment_type;
```

| anio | tipo_pago | descripcion | viajes | propina_promedio_pct | propina_promedio_usd |
|---|---|---|---|---|---|
| 2024 | 0 | Flex Fare trip | 346,257 | 3.32 | 0.72 |
| 2024 | 1 | Credit card | 2,696,637 | 26.26 | 4.33 |
| 2024 | 2 | Cash | 425,864 | 0.0 | 0.0 |
| 2024 | 3 | No charge | 14,271 | 0.06 | 0.01 |
| 2024 | 4 | Dispute | 36,894 | 0.06 | 0.02 |
| 2025 | 0 | Flex Fare trip | 872,794 | 1.92 | 0.31 |
| 2025 | 1 | Credit card | 2,628,688 | 24.69 | 4.29 |
| 2025 | 2 | Cash | 347,116 | 0.0 | 0.0 |
| 2025 | 3 | No charge | 11,127 | 0.0 | 0.0 |
| 2025 | 4 | Dispute | 33,315 | 0.0 | 0.0 |

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
WHERE f.month = 11 AND f.year IN (2025, 2024)
  AND (o.zone IN ('JFK Airport', 'LaGuardia Airport')
    OR d.zone IN ('JFK Airport', 'LaGuardia Airport'))
GROUP BY f.year
ORDER BY f.year;
```

| anio | viajes | tarifa_media |
|---|---|---|
| 2024 | 321,952 | 56.88 |
| 2025 | 315,613 | 56.5 |

**Interpretación:** Los viajes que empiezan o terminan en JFK o LaGuardia fueron 318.293 en marzo de 2025 (320.262 en 2024, -0,6 %) con una tarifa media de USD 55,29 (55,70 en 2024), unas tres veces la tarifa media general (USD 18,40).

## Pregunta 6: Variación de viajes totales y de tarifa media entre los dos meses

Consulta: `sql/q6_variacion.sql`

```sql
-- Pregunta 6: Variación de viajes totales y de tarifa media entre los dos meses
WITH m AS (
  SELECT year, count(*) AS viajes, avg(fare_amount) AS tarifa_media
  FROM fact_trips
  WHERE month = 11 AND year IN (2025, 2024)
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
| 3,519,923 | 3,893,040 | 10.6 | 19.67 | 18.11 | -7.95 |

**Interpretación:** Los viajes aceptados subieron 14,53 % (3.453.562 a 3.955.293) y la tarifa media bajó 3,47 % (USD 19,06 a 18,40). Hipótesis: la baja es un efecto de composición y no un abaratamiento: los viajes con tarifa menor o igual a 0 pasaron de 0,43 % a 3,26 % del total, casi todos con payment_type 0 (Flex Fare, que pasó de 379.418 a 854.427 viajes), y la tarifa media de los viajes con tarifa positiva casi no cambió (USD 19,15 a 19,16).
