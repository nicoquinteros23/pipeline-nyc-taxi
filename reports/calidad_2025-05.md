# Reporte de calidad 2025-05

- Archivo: yellow_tripdata_2025-05.parquet
- Filas leídas: 4,591,845
- Aceptadas: 4,284,443
- Cuarentena: 307,402 (6.69 %)
- Aceptadas con alguna advertencia: 1,120,095
- Reconciliación (leídas = aceptadas + cuarentena): ok

## Reglas de rechazo (veces que se violó, sobre todas las filas leídas)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R01 | Subida fuera del mes del archivo | inicio de mes <= subida < inicio del mes siguiente | 24 |
| R02 | Bajada anterior o igual a la subida | bajada > subida | 64,272 |
| R03 | Distancia menor o igual a 0 | distancia > 0 | 141,121 |
| R04 | Distancia mayor al máximo | distancia <= 100 millas | 370 |
| R05 | Importe total negativo | total_amount >= 0 | 112,544 |
| R07 | Zona de origen o destino inexistente en la tabla de zonas | id presente en taxi_zone_lookup | 0 |
| R08 | Fila duplicada exacta | se conserva la primera ocurrencia | 0 |
| R09 | Velocidad promedio imposible | velocidad <= 100 mph | 1,175 |
| R11 | Importe total excesivo | total_amount <= 1000 USD | 5 |

Una fila puede violar varias reglas, por eso la suma de esta tabla (319,511) puede ser mayor que la cuarentena (307,402).

## Advertencias (sobre filas aceptadas; no se descartan)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R06 | Pasajeros nulo o 0 | pasajeros >= 1 | 1,092,376 |
| R10 | Zona Unknown (264 o 265) | n/a | 30,533 |
