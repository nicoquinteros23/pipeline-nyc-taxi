# Reporte de calidad 2024-05

- Archivo: yellow_tripdata_2024-05.parquet
- Filas leídas: 3,723,833
- Aceptadas: 3,628,211
- Cuarentena: 95,622 (2.57 %)
- Aceptadas con alguna advertencia: 468,007
- Reconciliación (leídas = aceptadas + cuarentena): ok

## Reglas de rechazo (veces que se violó, sobre todas las filas leídas)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R01 | Subida fuera del mes del archivo | inicio de mes <= subida < inicio del mes siguiente | 33 |
| R02 | Bajada anterior o igual a la subida | bajada > subida | 1,147 |
| R03 | Distancia menor o igual a 0 | distancia > 0 | 49,791 |
| R04 | Distancia mayor al máximo | distancia <= 100 millas | 154 |
| R05 | Importe total negativo | total_amount >= 0 | 48,992 |
| R07 | Zona de origen o destino inexistente en la tabla de zonas | id presente en taxi_zone_lookup | 0 |
| R08 | Fila duplicada exacta | se conserva la primera ocurrencia | 0 |
| R09 | Velocidad promedio imposible | velocidad <= 100 mph | 1,071 |
| R11 | Importe total excesivo | total_amount <= 1000 USD | 4 |

Una fila puede violar varias reglas, por eso la suma de esta tabla (101,192) puede ser mayor que la cuarentena (95,622).

## Advertencias (sobre filas aceptadas; no se descartan)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R06 | Pasajeros nulo o 0 | pasajeros >= 1 | 435,912 |
| R10 | Zona Unknown (264 o 265) | n/a | 34,365 |
