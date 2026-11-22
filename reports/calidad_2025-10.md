# Reporte de calidad 2025-10

- Archivo: yellow_tripdata_2025-10.parquet
- Filas leídas: 4,428,699
- Aceptadas: 4,136,101
- Cuarentena: 292,598 (6.61 %)
- Aceptadas con alguna advertencia: 922,767
- Reconciliación (leídas = aceptadas + cuarentena): ok

## Reglas de rechazo (veces que se violó, sobre todas las filas leídas)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R01 | Subida fuera del mes del archivo | inicio de mes <= subida < inicio del mes siguiente | 13 |
| R02 | Bajada anterior o igual a la subida | bajada > subida | 67,968 |
| R03 | Distancia menor o igual a 0 | distancia > 0 | 125,599 |
| R04 | Distancia mayor al máximo | distancia <= 100 millas | 229 |
| R05 | Importe total negativo | total_amount >= 0 | 108,735 |
| R07 | Zona de origen o destino inexistente en la tabla de zonas | id presente en taxi_zone_lookup | 0 |
| R08 | Fila duplicada exacta | se conserva la primera ocurrencia | 0 |
| R09 | Velocidad promedio imposible | velocidad <= 100 mph | 1,122 |
| R11 | Importe total excesivo | total_amount <= 1000 USD | 2 |

Una fila puede violar varias reglas, por eso la suma de esta tabla (303,668) puede ser mayor que la cuarentena (292,598).

## Advertencias (sobre filas aceptadas; no se descartan)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R06 | Pasajeros nulo o 0 | pasajeros >= 1 | 896,429 |
| R10 | Zona Unknown (264 o 265) | n/a | 29,873 |
