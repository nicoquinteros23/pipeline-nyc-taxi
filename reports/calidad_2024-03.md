# Reporte de calidad 2024-03

- Archivo: yellow_tripdata_2024-03.parquet
- Filas leídas: 3,582,628
- Aceptadas: 3,453,562
- Cuarentena: 129,066 (3.6 %)
- Aceptadas con alguna advertencia: 446,612
- Reconciliación (leídas = aceptadas + cuarentena): ok

## Reglas de rechazo (veces que se violó, sobre todas las filas leídas)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R01 | Subida fuera del mes del archivo | inicio de mes <= subida < inicio del mes siguiente | 23 |
| R02 | Bajada anterior o igual a la subida | bajada > subida | 1,128 |
| R03 | Distancia menor o igual a 0 | distancia > 0 | 87,168 |
| R04 | Distancia mayor al máximo | distancia <= 100 millas | 122 |
| R05 | Importe total negativo | total_amount >= 0 | 44,311 |
| R07 | Zona de origen o destino inexistente en la tabla de zonas | id presente en taxi_zone_lookup | 0 |
| R08 | Fila duplicada exacta | se conserva la primera ocurrencia | 0 |
| R09 | Velocidad promedio imposible | velocidad <= 100 mph | 1,178 |
| R11 | Importe total excesivo | total_amount <= 1000 USD | 1 |

Una fila puede violar varias reglas, por eso la suma de esta tabla (133,931) puede ser mayor que la cuarentena (129,066).

## Advertencias (sobre filas aceptadas; no se descartan)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R06 | Pasajeros nulo o 0 | pasajeros >= 1 | 418,029 |
| R10 | Zona Unknown (264 o 265) | n/a | 31,084 |
