# Exploración de datos: marzo 2025 vs marzo 2024

Archivos: yellow_tripdata_2025-03.parquet (4.145.257 filas) y
yellow_tripdata_2024-03.parquet (3.582.628 filas), más taxi_zone_lookup.csv.
Generado con scripts/explorar.py y scripts/explorar2.py.

## Esquema
- La única diferencia de columnas es cbd_congestion_fee, que existe solo en 2025
  (0 % de nulos allí). Los nombres y tipos del resto coinciden entre años.
- La columna se llama Airport_fee (con mayúscula) en ambos años.
- Tipos reales: passenger_count, RatecodeID y payment_type son BIGINT;
  VendorID, PULocationID y DOLocationID son INTEGER; las demás numéricas, DOUBLE.
- VendorID llega hasta 7 en 2025 y hasta 6 en 2024 (proveedor nuevo).

## Hallazgos de calidad (cantidad de filas 2025 / 2024)
| Condición | 2025 | 2024 |
|---|---|---|
| Subida fuera del mes | 33 | 23 |
| Bajada anterior o igual a la subida | 22.280 | 1.128 |
| Distancia <= 0 | 103.722 | 87.168 |
| Importe total negativo | 68.686 | 44.311 |
| Tarifa <= 0 | 210.378 | 59.756 |
| Pasajeros nulo | 916.663 | 426.190 |
| Pasajeros = 0 | 22.715 | 40.372 |
| Zona inexistente en la tabla de zonas | 0 | 0 |
| Zonas 264 o 265 (Unknown) | 28.415 | 35.374 |
| Duplicados exactos | 0 | 0 |

## Hechos que explican los números
- Los nulos de passenger_count y congestion_surcharge (también RatecodeID,
  store_and_fwd_flag y Airport_fee) coinciden 1 a 1 con payment_type = 0
  (916.663 filas en 2025, 426.190 en 2024). Son nulos estructurales, no errores
  del conductor. Por eso la regla de pasajeros es advertencia.
- El aumento de bajada <= subida (1.128 a 22.280) se debe al VendorID 7, nuevo en
  2025: 21.481 filas con bajada igual a la subida (96 % del total).
  Viajes totales del VendorID 7:  Viajes totales del VendorID 7: 21.481, y los 21.481 (100 %) tienen bajada igual
  a la subida. El VendorID 6 tiene 334 viajes y 76 (23 %) con bajada no posterior.
  Los VendorID 1 y 2 tienen 630 de 834.394 y 93 de 3.289.048 respectivamente.
- El VendorID 6 concentra las bajadas anteriores a la subida (73 en 2025, 97 en 2024).
- La tarifa <= 0 se concentra en tipos de pago 0, 4, 2 y 3; el tipo 1 (tarjeta)
  casi no tiene (106 en 2025).
- Propina media en efectivo (payment_type = 2): 0,0 en ambos años. El sistema no
  registra la propina en efectivo (respuesta a la pregunta 4).
- Las reglas de zona inexistente y de duplicados exactos no encuentran filas en
  estos meses; se verifican con datos sintéticos en los tests.

## Umbrales elegidos
- Distancia máxima: 100 millas. p99,99 = 58 (2025) y 56 (2024). Hay 214 y 122 filas
  sobre 100 millas, y la mayoría pasa de 200: es una meseta de valores imposibles.
- Velocidad máxima: 100 mph (regla propia). p99,9 = 46 mph; p99,99 > 1.000 mph.
  Filas afectadas: 961 y 1.178.
- Importe total máximo: 1.000 dólares (regla propia). p99,99 = 326 dólares;
  afecta 17 filas en 2025 y 1 en 2024.
- Duración: más de 12 h como advertencia (1.214 y 1.746 filas); más de 24 h solo
  11 y 20 filas.

## Cosas raras encontradas
- Fechas de subida de 2007 y 2002 en los archivos de 2025 y 2024.
- Distancias de hasta 320.136 millas con viajes de 7 minutos.
- fare_amount de -999 y total_amount de -1000 (valores centinela).
- total_amount de hasta 46.269 dólares en 2025.