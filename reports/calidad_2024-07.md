# Reporte de calidad 2024-07

- Archivo: yellow_tripdata_2024-07.parquet
- Filas leídas: 3,076,903
- Aceptadas: 2,981,461
- Cuarentena: 95,442 (3.1 %)
- Aceptadas con alguna advertencia: 328,900
- Reconciliación (leídas = aceptadas + cuarentena): ok

## Reglas de rechazo (veces que se violó, sobre todas las filas leídas)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R01 | Subida fuera del mes del archivo | inicio de mes <= subida < inicio del mes siguiente | 47 |
| R02 | Bajada anterior o igual a la subida | bajada > subida | 1,068 |
| R03 | Distancia menor o igual a 0 | distancia > 0 | 48,045 |
| R04 | Distancia mayor al máximo | distancia <= 100 millas | 159 |
| R05 | Importe total negativo | total_amount >= 0 | 50,379 |
| R07 | Zona de origen o destino inexistente en la tabla de zonas | id presente en taxi_zone_lookup | 0 |
| R08 | Fila duplicada exacta | se conserva la primera ocurrencia | 1 |
| R09 | Velocidad promedio imposible | velocidad <= 100 mph | 996 |
| R11 | Importe total excesivo | total_amount <= 1000 USD | 7 |

Una fila puede violar varias reglas, por eso la suma de esta tabla (100,702) puede ser mayor que la cuarentena (95,442).

## Advertencias (sobre filas aceptadas; no se descartan)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R06 | Pasajeros nulo o 0 | pasajeros >= 1 | 298,706 |
| R10 | Zona Unknown (264 o 265) | n/a | 31,565 |
