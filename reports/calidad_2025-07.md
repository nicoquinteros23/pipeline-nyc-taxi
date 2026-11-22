# Reporte de calidad 2025-07

- Archivo: yellow_tripdata_2025-07.parquet
- Filas leídas: 3,898,963
- Aceptadas: 3,648,126
- Cuarentena: 250,837 (6.43 %)
- Aceptadas con alguna advertencia: 999,182
- Reconciliación (leídas = aceptadas + cuarentena): ok

## Reglas de rechazo (veces que se violó, sobre todas las filas leídas)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R01 | Subida fuera del mes del archivo | inicio de mes <= subida < inicio del mes siguiente | 7 |
| R02 | Bajada anterior o igual a la subida | bajada > subida | 56,064 |
| R03 | Distancia menor o igual a 0 | distancia > 0 | 123,774 |
| R04 | Distancia mayor al máximo | distancia <= 100 millas | 274 |
| R05 | Importe total negativo | total_amount >= 0 | 76,455 |
| R07 | Zona de origen o destino inexistente en la tabla de zonas | id presente en taxi_zone_lookup | 0 |
| R08 | Fila duplicada exacta | se conserva la primera ocurrencia | 1 |
| R09 | Velocidad promedio imposible | velocidad <= 100 mph | 947 |
| R11 | Importe total excesivo | total_amount <= 1000 USD | 8 |

Una fila puede violar varias reglas, por eso la suma de esta tabla (257,530) puede ser mayor que la cuarentena (250,837).

## Advertencias (sobre filas aceptadas; no se descartan)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R06 | Pasajeros nulo o 0 | pasajeros >= 1 | 974,142 |
| R10 | Zona Unknown (264 o 265) | n/a | 27,805 |
