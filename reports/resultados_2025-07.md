# Resultados: Julio 2025 vs Julio 2024 (2025-07 vs 2024-07)

Generado por `src/preguntas.py` sobre el esquema `marts` de `nyc_taxi_dlt.duckdb`. Los números corresponden a viajes **aceptados** (después de la cuarentena).

## Pregunta 1: Top 10 zonas de origen por viajes y variación contra el mismo mes del año anterior

Consulta: `sql/q1_top_zonas.sql`

```sql
-- Pregunta 1: Top 10 zonas de origen por viajes y variación contra el mismo mes del año anterior
WITH v AS (
  SELECT year, pu_location_id, count(*) AS viajes
  FROM fact_trips
  WHERE month = 7 AND year IN (2025, 2024)
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
| JFK Airport | 167,835 | 171,068 | -1.9 |
| Midtown Center | 165,568 | 152,361 | 8.7 |
| Upper East Side South | 140,124 | 125,889 | 11.3 |
| Penn Station/Madison Sq West | 123,101 | 103,654 | 18.8 |
| Midtown East | 117,688 | 110,866 | 6.2 |
| Times Sq/Theatre District | 117,573 | 104,800 | 12.2 |
| Upper East Side North | 115,559 | 107,795 | 7.2 |
| Murray Hill | 101,798 | 92,694 | 9.8 |
| East Chelsea | 95,931 | 84,688 | 13.3 |
| Lincoln Square East | 94,772 | 86,921 | 9.0 |

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
  WHERE month = 7 AND year IN (2025, 2024)
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
| 2024 | 1 | 5 | 37.43 | 18,807 |
| 2024 | 2 | 4 | 33.01 | 16,297 |
| 2024 | 3 | 16 | 31.69 | 185,139 |
| 2025 | 1 | 5 | 31.72 | 32,031 |
| 2025 | 2 | 16 | 30.97 | 206,033 |
| 2025 | 3 | 0 | 30.24 | 117,115 |

**Interpretación:** (interpretación pendiente para este período)

## Pregunta 3: % de viajes con cbd_congestion_fee > 0 en el mes asignado y las 5 zonas de origen con mayor proporción (mínimo 1.000 viajes por zona)

Consulta: `sql/q3_cbd_congestion.sql`

```sql
-- Pregunta 3: % de viajes con cbd_congestion_fee > 0 en el mes asignado y las 5 zonas de origen con mayor proporción (mínimo 1.000 viajes por zona)
WITH base AS (
  SELECT pu_location_id, cbd_congestion_fee > 0 AS con_cargo
  FROM fact_trips
  WHERE year = 2025 AND month = 7
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
| Greenwich Village South | 48,603 | 99.85 | 74.39 | 3,648,126 |
| SoHo | 30,889 | 99.8 | 74.39 | 3,648,126 |
| West Village | 76,750 | 99.8 | 74.39 | 3,648,126 |
| Battery Park | 2,110 | 99.76 | 74.39 | 3,648,126 |
| Little Italy/NoLiTa | 41,146 | 99.73 | 74.39 | 3,648,126 |

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
WHERE month = 7 AND year IN (2025, 2024)
GROUP BY year, payment_type
ORDER BY year, payment_type;
```

| anio | tipo_pago | descripcion | viajes | propina_promedio_pct | propina_promedio_usd |
|---|---|---|---|---|---|
| 2024 | 0 | Flex Fare trip | 271,191 | 3.53 | 0.74 |
| 2024 | 1 | Credit card | 2,231,295 | 25.1 | 4.34 |
| 2024 | 2 | Cash | 433,566 | 0.0 | 0.0 |
| 2024 | 3 | No charge | 13,152 | 0.03 | 0.01 |
| 2024 | 4 | Dispute | 32,257 | 0.09 | 0.02 |
| 2025 | 0 | Flex Fare trip | 954,423 | 1.32 | 0.26 |
| 2025 | 1 | Credit card | 2,277,639 | 25.13 | 4.33 |
| 2025 | 2 | Cash | 350,555 | 0.0 | 0.0 |
| 2025 | 3 | No charge | 14,090 | 0.04 | 0.01 |
| 2025 | 4 | Dispute | 51,419 | 0.04 | 0.01 |

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
WHERE f.month = 7 AND f.year IN (2025, 2024)
  AND (o.zone IN ('JFK Airport', 'LaGuardia Airport')
    OR d.zone IN ('JFK Airport', 'LaGuardia Airport'))
GROUP BY f.year
ORDER BY f.year;
```

| anio | viajes | tarifa_media |
|---|---|---|
| 2024 | 329,159 | 56.28 |
| 2025 | 317,965 | 56.04 |

**Interpretación:** (interpretación pendiente para este período)

## Pregunta 6: Variación de viajes totales y de tarifa media entre los dos meses

Consulta: `sql/q6_variacion.sql`

```sql
-- Pregunta 6: Variación de viajes totales y de tarifa media entre los dos meses
WITH m AS (
  SELECT year, count(*) AS viajes, avg(fare_amount) AS tarifa_media
  FROM fact_trips
  WHERE month = 7 AND year IN (2025, 2024)
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
| 2,981,461 | 3,648,126 | 22.36 | 20.09 | 19.36 | -3.59 |

**Interpretación:** (interpretación pendiente para este período)
