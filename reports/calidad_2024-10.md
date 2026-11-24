# Reporte de calidad 2024-10

- Archivo: yellow_tripdata_2024-10.parquet
- Filas leídas: 3,833,771
- Aceptadas: 3,694,352
- Cuarentena: 139,419 (3.64 %)
- Aceptadas con alguna advertencia: 424,236
- Reconciliación (leídas = aceptadas + cuarentena): ok

## Reglas de rechazo (veces que se violó, sobre todas las filas leídas)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R01 | Subida fuera del mes del archivo | inicio de mes <= subida < inicio del mes siguiente | 40 |
| R02 | Bajada anterior o igual a la subida | bajada > subida | 1,002 |
| R03 | Distancia menor o igual a 0 | distancia > 0 | 81,639 |
| R04 | Distancia mayor al máximo | distancia <= 100 millas | 129 |
| R05 | Importe total negativo | total_amount >= 0 | 61,426 |
| R07 | Zona de origen o destino inexistente en la tabla de zonas | id presente en taxi_zone_lookup | 0 |
| R08 | Fila duplicada exacta | se conserva la primera ocurrencia | 2 |
| R09 | Velocidad promedio imposible | velocidad <= 100 mph | 1,006 |
| R11 | Importe total excesivo | total_amount <= 1000 USD | 1 |

Una fila puede violar varias reglas, por eso la suma de esta tabla (145,245) puede ser mayor que la cuarentena (139,419).

## Advertencias (sobre filas aceptadas; no se descartan)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R06 | Pasajeros nulo o 0 | pasajeros >= 1 | 392,386 |
| R10 | Zona Unknown (264 o 265) | n/a | 38,344 |
