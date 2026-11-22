# Reporte de calidad 2025-01

- Archivo: yellow_tripdata_2025-01.parquet
- Filas leídas: 3,475,226
- Aceptadas: 3,323,516
- Cuarentena: 151,710 (4.37 %)
- Aceptadas con alguna advertencia: 532,263
- Reconciliación (leídas = aceptadas + cuarentena): ok

## Reglas de rechazo (veces que se violó, sobre todas las filas leídas)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R01 | Subida fuera del mes del archivo | inicio de mes <= subida < inicio del mes siguiente | 22 |
| R02 | Bajada anterior o igual a la subida | bajada > subida | 2,051 |
| R03 | Distancia menor o igual a 0 | distancia > 0 | 90,893 |
| R04 | Distancia mayor al máximo | distancia <= 100 millas | 162 |
| R05 | Importe total negativo | total_amount >= 0 | 63,037 |
| R07 | Zona de origen o destino inexistente en la tabla de zonas | id presente en taxi_zone_lookup | 0 |
| R08 | Fila duplicada exacta | se conserva la primera ocurrencia | 0 |
| R09 | Velocidad promedio imposible | velocidad <= 100 mph | 899 |
| R11 | Importe total excesivo | total_amount <= 1000 USD | 3 |

Una fila puede violar varias reglas, por eso la suma de esta tabla (157,067) puede ser mayor que la cuarentena (151,710).

## Advertencias (sobre filas aceptadas; no se descartan)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R06 | Pasajeros nulo o 0 | pasajeros >= 1 | 507,927 |
| R10 | Zona Unknown (264 o 265) | n/a | 25,518 |
