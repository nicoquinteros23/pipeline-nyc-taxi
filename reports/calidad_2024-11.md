# Reporte de calidad 2024-11

- Archivo: yellow_tripdata_2024-11.parquet
- Filas leídas: 3,646,369
- Aceptadas: 3,519,923
- Cuarentena: 126,446 (3.47 %)
- Aceptadas con alguna advertencia: 402,064
- Reconciliación (leídas = aceptadas + cuarentena): ok

## Reglas de rechazo (veces que se violó, sobre todas las filas leídas)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R01 | Subida fuera del mes del archivo | inicio de mes <= subida < inicio del mes siguiente | 50 |
| R02 | Bajada anterior o igual a la subida | bajada > subida | 1,861 |
| R03 | Distancia menor o igual a 0 | distancia > 0 | 68,315 |
| R04 | Distancia mayor al máximo | distancia <= 100 millas | 148 |
| R05 | Importe total negativo | total_amount >= 0 | 60,563 |
| R07 | Zona de origen o destino inexistente en la tabla de zonas | id presente en taxi_zone_lookup | 0 |
| R08 | Fila duplicada exacta | se conserva la primera ocurrencia | 0 |
| R09 | Velocidad promedio imposible | velocidad <= 100 mph | 910 |
| R11 | Importe total excesivo | total_amount <= 1000 USD | 4 |

Una fila puede violar varias reglas, por eso la suma de esta tabla (131,851) puede ser mayor que la cuarentena (126,446).

## Advertencias (sobre filas aceptadas; no se descartan)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R06 | Pasajeros nulo o 0 | pasajeros >= 1 | 373,403 |
| R10 | Zona Unknown (264 o 265) | n/a | 29,710 |
