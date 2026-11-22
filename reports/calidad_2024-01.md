# Reporte de calidad 2024-01

- Archivo: yellow_tripdata_2024-01.parquet
- Filas leídas: 2,964,624
- Aceptadas: 2,870,976
- Cuarentena: 93,648 (3.16 %)
- Aceptadas con alguna advertencia: 173,984
- Reconciliación (leídas = aceptadas + cuarentena): ok

## Reglas de rechazo (veces que se violó, sobre todas las filas leídas)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R01 | Subida fuera del mes del archivo | inicio de mes <= subida < inicio del mes siguiente | 18 |
| R02 | Bajada anterior o igual a la subida | bajada > subida | 870 |
| R03 | Distancia menor o igual a 0 | distancia > 0 | 60,371 |
| R04 | Distancia mayor al máximo | distancia <= 100 millas | 59 |
| R05 | Importe total negativo | total_amount >= 0 | 35,504 |
| R07 | Zona de origen o destino inexistente en la tabla de zonas | id presente en taxi_zone_lookup | 0 |
| R08 | Fila duplicada exacta | se conserva la primera ocurrencia | 0 |
| R09 | Velocidad promedio imposible | velocidad <= 100 mph | 1,024 |
| R11 | Importe total excesivo | total_amount <= 1000 USD | 7 |

Una fila puede violar varias reglas, por eso la suma de esta tabla (97,853) puede ser mayor que la cuarentena (93,648).

## Advertencias (sobre filas aceptadas; no se descartan)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R06 | Pasajeros nulo o 0 | pasajeros >= 1 | 147,783 |
| R10 | Zona Unknown (264 o 265) | n/a | 27,969 |
