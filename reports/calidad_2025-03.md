# Reporte de calidad 2025-03

- Archivo: yellow_tripdata_2025-03.parquet
- Filas leídas: 4,145,257
- Aceptadas: 3,955,293
- Cuarentena: 189,964 (4.58 %)
- Aceptadas con alguna advertencia: 899,436
- Reconciliación (leídas = aceptadas + cuarentena): ok

## Reglas de rechazo (veces que se violó, sobre todas las filas leídas)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R01 | Subida fuera del mes del archivo | inicio de mes <= subida < inicio del mes siguiente | 33 |
| R02 | Bajada anterior o igual a la subida | bajada > subida | 22,280 |
| R03 | Distancia menor o igual a 0 | distancia > 0 | 103,722 |
| R04 | Distancia mayor al máximo | distancia <= 100 millas | 214 |
| R05 | Importe total negativo | total_amount >= 0 | 68,686 |
| R07 | Zona de origen o destino inexistente en la tabla de zonas | id presente en taxi_zone_lookup | 0 |
| R08 | Fila duplicada exacta | se conserva la primera ocurrencia | 0 |
| R09 | Velocidad promedio imposible | velocidad <= 100 mph | 961 |
| R11 | Importe total excesivo | total_amount <= 1000 USD | 17 |

Una fila puede violar varias reglas, por eso la suma de esta tabla (195,913) puede ser mayor que la cuarentena (189,964).

## Advertencias (sobre filas aceptadas; no se descartan)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R06 | Pasajeros nulo o 0 | pasajeros >= 1 | 875,764 |
| R10 | Zona Unknown (264 o 265) | n/a | 25,145 |
