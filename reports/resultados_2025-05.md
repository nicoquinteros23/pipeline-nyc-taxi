# Resultados: Mayo 2025 vs Mayo 2024 (2025-05 vs 2024-05)

Generado por `src/preguntas.py` sobre el esquema `marts` de `nyc_taxi_dlt.duckdb`. Los números corresponden a viajes **aceptados** (después de la cuarentena).

## Pregunta 1: Top 10 zonas de origen por viajes y variación contra el mismo mes del año anterior

Consulta: `sql/q1_top_zonas.sql`

```sql
-- Pregunta 1: Top 10 zonas de origen por viajes y variación contra el mismo mes del año anterior
WITH v AS (
  SELECT year, pu_location_id, count(*) AS viajes
  FROM fact_trips
  WHERE month = 5 AND year IN (2025, 2024)
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
| Upper East Side South | 196,107 | 180,308 | 8.8 |
| JFK Airport | 176,726 | 157,805 | 12.0 |
| Midtown Center | 174,899 | 171,051 | 2.2 |
| Upper East Side North | 172,121 | 165,816 | 3.8 |
| Lincoln Square East | 129,721 | 122,682 | 5.7 |
| Times Sq/Theatre District | 129,166 | 118,199 | 9.3 |
| Midtown East | 125,584 | 122,917 | 2.2 |
| Penn Station/Madison Sq West | 123,141 | 111,828 | 10.1 |
| LaGuardia Airport | 119,968 | 124,114 | -3.3 |
| Union Sq | 112,687 | 95,317 | 18.2 |

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
  WHERE month = 5 AND year IN (2025, 2024)
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
| 2024 | 1 | 5 | 36.1 | 22,550 |
| 2024 | 2 | 16 | 32.05 | 223,226 |
| 2024 | 3 | 4 | 31.4 | 19,152 |
| 2025 | 1 | 5 | 33.2 | 34,117 |
| 2025 | 2 | 16 | 31.71 | 248,878 |
| 2025 | 3 | 17 | 30.12 | 276,704 |

**Interpretación:** (interpretación pendiente para este período)

## Pregunta 3: % de viajes con cbd_congestion_fee > 0 en el mes asignado y las 5 zonas de origen con mayor proporción (mínimo 1.000 viajes por zona)

Consulta: `sql/q3_cbd_congestion.sql`

```sql
-- Pregunta 3: % de viajes con cbd_congestion_fee > 0 en el mes asignado y las 5 zonas de origen con mayor proporción (mínimo 1.000 viajes por zona)
WITH base AS (
  SELECT pu_location_id, cbd_congestion_fee > 0 AS con_cargo
  FROM fact_trips
  WHERE year = 2025 AND month = 5
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
| SoHo | 37,285 | 99.86 | 73.22 | 4,284,443 |
| Greenwich Village South | 61,901 | 99.84 | 73.22 | 4,284,443 |
| Little Italy/NoLiTa | 47,345 | 99.82 | 73.22 | 4,284,443 |
| West Village | 92,952 | 99.82 | 73.22 | 4,284,443 |
| Greenwich Village North | 60,629 | 99.79 | 73.22 | 4,284,443 |

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
WHERE month = 5 AND year IN (2025, 2024)
GROUP BY year, payment_type
ORDER BY year, payment_type;
```

| anio | tipo_pago | descripcion | viajes | propina_promedio_pct | propina_promedio_usd |
|---|---|---|---|---|---|
| 2024 | 0 | Flex Fare trip | 398,151 | 3.16 | 0.68 |
| 2024 | 1 | Credit card | 2,705,156 | 24.9 | 4.41 |
| 2024 | 2 | Cash | 480,826 | 0.0 | 0.0 |
| 2024 | 3 | No charge | 12,722 | 0.05 | 0.01 |
| 2024 | 4 | Dispute | 31,356 | 0.04 | 0.01 |
| 2025 | 0 | Flex Fare trip | 1,069,371 | 1.7 | 0.32 |
| 2025 | 1 | Credit card | 2,765,046 | 25.36 | 4.47 |
| 2025 | 2 | Cash | 383,435 | 0.0 | 0.0 |
| 2025 | 3 | No charge | 15,242 | 0.11 | 0.03 |
| 2025 | 4 | Dispute | 51,349 | 0.05 | 0.01 |

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
WHERE f.month = 5 AND f.year IN (2025, 2024)
  AND (o.zone IN ('JFK Airport', 'LaGuardia Airport')
    OR d.zone IN ('JFK Airport', 'LaGuardia Airport'))
GROUP BY f.year
ORDER BY f.year;
```

| anio | viajes | tarifa_media |
|---|---|---|
| 2024 | 357,427 | 56.18 |
| 2025 | 372,046 | 57.27 |

**Interpretación:** (interpretación pendiente para este período)

## Pregunta 6: Variación de viajes totales y de tarifa media entre los dos meses

Consulta: `sql/q6_variacion.sql`

```sql
-- Pregunta 6: Variación de viajes totales y de tarifa media entre los dos meses
WITH m AS (
  SELECT year, count(*) AS viajes, avg(fare_amount) AS tarifa_media
  FROM fact_trips
  WHERE month = 5 AND year IN (2025, 2024)
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
| 3,628,211 | 4,284,443 | 18.09 | 20.07 | 19.29 | -3.85 |

**Interpretación:** (interpretación pendiente para este período)
