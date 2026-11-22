# Reporte de calidad 2025-11

- Archivo: yellow_tripdata_2025-11.parquet
- Filas leídas: 4,181,444
- Aceptadas: 3,893,040
- Cuarentena: 288,404 (6.9 %)
- Aceptadas con alguna advertencia: 915,729
- Reconciliación (leídas = aceptadas + cuarentena): ok

## Reglas de rechazo (veces que se violó, sobre todas las filas leídas)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R01 | Subida fuera del mes del archivo | inicio de mes <= subida < inicio del mes siguiente | 24 |
| R02 | Bajada anterior o igual a la subida | bajada > subida | 62,120 |
| R03 | Distancia menor o igual a 0 | distancia > 0 | 109,420 |
| R04 | Distancia mayor al máximo | distancia <= 100 millas | 223 |
| R05 | Importe total negativo | total_amount >= 0 | 126,919 |
| R07 | Zona de origen o destino inexistente en la tabla de zonas | id presente en taxi_zone_lookup | 0 |
| R08 | Fila duplicada exacta | se conserva la primera ocurrencia | 0 |
| R09 | Velocidad promedio imposible | velocidad <= 100 mph | 1,001 |
| R11 | Importe total excesivo | total_amount <= 1000 USD | 10 |

Una fila puede violar varias reglas, por eso la suma de esta tabla (299,717) puede ser mayor que la cuarentena (288,404).

## Advertencias (sobre filas aceptadas; no se descartan)

| ID | Regla | Umbral | Filas |
|---|---|---|---|
| R06 | Pasajeros nulo o 0 | pasajeros >= 1 | 891,851 |
| R10 | Zona Unknown (264 o 265) | n/a | 27,684 |
