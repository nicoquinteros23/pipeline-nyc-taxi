# Resultados: Enero 2025 vs Enero 2024 (2025-01 vs 2024-01)

Generado por `src/preguntas.py` sobre el esquema `marts` de `nyc_taxi_dlt.duckdb`. Los números corresponden a viajes **aceptados** (después de la cuarentena).

## Pregunta 1: Top 10 zonas de origen por viajes y variación contra el mismo mes del año anterior

Consulta: `sql/q1_top_zonas.sql`

```sql
-- Pregunta 1: Top 10 zonas de origen por viajes y variación contra el mismo mes del año anterior
WITH v AS (
  SELECT year, pu_location_id, count(*) AS viajes
  FROM fact_trips
  WHERE month = 1 AND year IN (2025, 2024)
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
| Midtown Center | 163,868 | 140,220 | 16.9 |
| Upper East Side South | 159,332 | 140,161 | 13.7 |
| Upper East Side North | 151,619 | 134,029 | 13.1 |
| JFK Airport | 134,810 | 138,349 | -2.6 |
| Times Sq/Theatre District | 120,659 | 102,999 | 17.1 |
| Penn Station/Madison Sq West | 115,021 | 102,166 | 12.6 |
| Midtown East | 113,840 | 104,374 | 9.1 |
| Lincoln Square East | 107,541 | 101,848 | 5.6 |
| Upper West Side South | 93,832 | 86,534 | 8.4 |
| Midtown North | 92,702 | 83,712 | 10.7 |

**Interpretación:** (interpretación pendiente para este período)

## Pregunta 2: Hora del día con mayor ingreso promedio por viaje (primera fila de cada año; se muestran las 3 primeras)

Consulta: `sql/q2_hora_mayor_ingreso.sql`

```sql
-- Pregunta 2: Hora del día con mayor ingreso promedio por viaje (primera fila de cada año; se muestran las 3 primeras)
WITH h AS (
  SELECT year, pickup_hour,
         count(*) AS viajes,
         round(avg(total_amount), 2) AS ingreso_promedio_por_viaje
  FROM fact_trips
  WHERE month = 1 AND year IN (2025, 2024)
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
| 2024 | 1 | 5 | 37.54 | 17,491 |
| 2024 | 2 | 4 | 32.35 | 15,296 |
| 2024 | 3 | 6 | 30.12 | 39,442 |
| 2025 | 1 | 5 | 33.64 | 21,235 |
| 2025 | 2 | 4 | 29.7 | 18,637 |
| 2025 | 3 | 16 | 29.0 | 204,883 |

**Interpretación:** (interpretación pendiente para este período)

## Pregunta 3: % de viajes con cbd_congestion_fee > 0 en el mes asignado y las 5 zonas de origen con mayor proporción (mínimo 1.000 viajes por zona)

Consulta: `sql/q3_cbd_congestion.sql`

```sql
-- Pregunta 3: % de viajes con cbd_congestion_fee > 0 en el mes asignado y las 5 zonas de origen con mayor proporción (mínimo 1.000 viajes por zona)
WITH base AS (
  SELECT pu_location_id, cbd_congestion_fee > 0 AS con_cargo
  FROM fact_trips
  WHERE year = 2025 AND month = 1
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
| Stuy Town/Peter Cooper Village | 5,344 | 90.61 | 65.98 | 3,323,516 |
| Alphabet City | 7,110 | 90.48 | 65.98 | 3,323,516 |
| Union Sq | 92,363 | 90.45 | 65.98 | 3,323,516 |
| Midtown East | 113,840 | 90.38 | 65.98 | 3,323,516 |
| Midtown Center | 163,868 | 89.89 | 65.98 | 3,323,516 |

**Interpretación:** (interpretación pendiente para este período)

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
WHERE month = 1 AND year IN (2025, 2024)
GROUP BY year, payment_type
ORDER BY year, payment_type;
```

| anio | tipo_pago | descripcion | viajes | propina_promedio_pct | propina_promedio_usd |
|---|---|---|---|---|---|
| 2024 | 0 | Flex Fare trip | 117,216 | 9.14 | 1.81 |
| 2024 | 1 | Credit card | 2,297,847 | 26.26 | 4.15 |
| 2024 | 2 | Cash | 422,679 | 0.0 | 0.0 |
| 2024 | 3 | No charge | 10,449 | 0.04 | 0.01 |
| 2024 | 4 | Dispute | 22,785 | 0.03 | 0.0 |
| 2025 | 0 | Flex Fare trip | 484,337 | 2.74 | 0.47 |
| 2025 | 1 | Credit card | 2,423,357 | 26.29 | 4.1 |
| 2025 | 2 | Cash | 368,043 | 0.0 | 0.0 |
| 2025 | 3 | No charge | 12,126 | 0.01 | 0.0 |
| 2025 | 4 | Dispute | 35,653 | 0.03 | 0.01 |

**Interpretación:** (interpretación pendiente para este período)

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
WHERE f.month = 1 AND f.year IN (2025, 2024)
  AND (o.zone IN ('JFK Airport', 'LaGuardia Airport')
    OR d.zone IN ('JFK Airport', 'LaGuardia Airport'))
GROUP BY f.year
ORDER BY f.year;
```

| anio | viajes | tarifa_media |
|---|---|---|
| 2024 | 276,037 | 55.04 |
| 2025 | 270,973 | 54.84 |

**Interpretación:** (interpretación pendiente para este período)

## Pregunta 6: Variación de viajes totales y de tarifa media entre los dos meses

Consulta: `sql/q6_variacion.sql`

```sql
-- Pregunta 6: Variación de viajes totales y de tarifa media entre los dos meses
WITH m AS (
  SELECT year, count(*) AS viajes, avg(fare_amount) AS tarifa_media
  FROM fact_trips
  WHERE month = 1 AND year IN (2025, 2024)
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
| 2,870,976 | 3,323,516 | 15.76 | 18.47 | 17.47 | -5.41 |

**Interpretación:** (interpretación pendiente para este período)
