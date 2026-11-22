# Pipeline batch NYC Taxi (taxi amarillo)

Pipeline reproducible que ingiere, valida, limpia y modela los viajes de taxi amarillo de Nueva York
de dos meses: el mes asignado (**2025-03**) y el mismo mes de 2024 (**2024-03**), junto con la tabla de
zonas de la TLC. Responde seis preguntas de negocio con SQL sobre el modelo (ver `RESULTADOS.md`).

Stack: Python 3.10 o superior (probado con 3.12.10), DuckDB 1.5.6, dlt 1.32.0, dbt-core 1.12.7, dbt-duckdb 1.11.1, PyArrow 26.0.1, PyYAML, requests, psutil, pytest y Docker.

## 1. Cómo instalar y ejecutar

Requisitos: Python 3.10+, Git y conexión a internet la primera vez (se descargan los archivos de la TLC, unas decenas de MB por mes).

```bash
git clone https://github.com/nicoquinteros23/pipeline-nyc-taxi.git
cd pipeline-nyc-taxi
python -m venv .venv
source .venv/bin/activate          # Windows PowerShell: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Comando único (ingesta, esquema, calidad, transformación, modelo y preguntas, para el mes asignado
y su mes de comparación, que es el mismo mes del año anterior):

```bash
make all MES=2025-03
```

Si `make` no está disponible, el equivalente directo es:

```bash
python -m src.pipeline --mes 2025-03
```

Otros comandos:

| Comando | Qué hace |
|---|---|
| `make all MES=2025-03` | Ejecuta el pipeline original (Fase 1) completo |
| `make fase2 MES=2025-03` | Ejecuta el pipeline completo de Fase 2 (`dlt` + calidad + `intermediate` + `dbt build` + reconciliación) |
| `make intermediate` | Reconstruye de forma idempotente las vistas `intermediate` sobre Parquet validados/cuarentena |
| `make reconciliar` | Verifica `staging = aceptadas + cuarentena` y cierre con `marts` de dbt |
| `make dbt-build` | Ejecuta modelos y tests de dbt (`staging`, `intermediate`, `marts`) |
| `make dbt-test` | Ejecuta únicamente los 50 tests de dbt |
| `make test` | Corre los 42 tests automatizados (`pytest`) |
| `make demo-reproceso` | Reprocesa un mes e imprime conteos antes y después (ver sección 3.5) |
| `make clean-data` | Borra las capas derivadas y reportes (no borra `data/raw`) |
| `make clean-all` | Borra **todo** (`data/` completo incluyendo `data/raw` y `reports/`) para descargar y correr desde cero |
| `make dashboard` | Abre el dashboard interactivo opcional (requiere `pip install -r requirements-dashboard.txt`) |

Salidas principales: `RESULTADOS.md`, `reports/` (esquema, calidad y tiempos por mes) y `data/`
(excluido de Git).

## 2. Arquitectura

```mermaid
flowchart LR
  TLC["TLC CloudFront\n(Parquet + Zonas)"] -->|"ingesta.py\nSHA-256 + 3 reintentos"| RAW["Raw\ndata/raw/"]
  TLC -->|registra| LOG["Control\ningestion_log.csv"]
  RAW -->|"esquema.py + calidad.py"| GATE["Validación\ncontrato YAML + 11 reglas\n+ reconciliación"]
  GATE -->|"aceptadas + flags"| VAL["Validada\ndata/validada/"]
  GATE -->|"rechazadas + reglas"| Q["Cuarentena\ndata/quarantine/"]
  GATE -->|reportes| REP["Reportes\nreports/schema_* y calidad_*"]
  VAL -->|"transformacion.py\nderivadas + zonas"| CUR["Curada Hive\nyear=AAAA/month=M"]
  CUR -->|"modelo.py"| DW["Modelo\ndata/warehouse/taxi.duckdb"]
  DW -->|"preguntas.py\nsql/q1..q6"| RES["RESULTADOS.md"]
```

| Capa | Ubicación | Contenido |
|---|---|---|
| Raw | `data/raw/yellow/year=AAAA/month=MM/` y `data/raw/zones/` | Archivos tal como se descargaron. Nunca se modifican. |
| Control | `data/control/ingestion_log.csv` | Historial de descargas: archivo, URL, filas, bytes, hash, fecha-hora y estado. |
| Validada | `data/validada/year=AAAA/month=M/` | Filas aceptadas por las reglas de calidad, con columna `flags` de advertencias. |
| Cuarentena | `data/quarantine/year=AAAA/month=M/` | Filas rechazadas, con archivo de origen, número de fila y reglas violadas. |
| Curada | `data/curated/trips/year=AAAA/month=M/` | Filas validadas más columnas derivadas y zonas. Parquet particionado estilo Hive. |
| Modelo | `data/warehouse/taxi.duckdb` | `fact_trips` (vista sobre la curada), `dim_zone`, `dim_date`, `agg_zone_hour`. |

Cada etapa es un módulo de `src/` ejecutable con `python -m src.<etapa> --mes AAAA-MM`. El runner
`src/pipeline.py` las encadena y corta en la primera que falla, indicando cuál fue y devolviendo código
de salida distinto de 0.

## 3. Decisiones

### 3.1 Herramienta: DuckDB

Cada mes tiene entre 3,5 y 4,1 millones de filas y el pipeline debe correr en 8 GB de RAM. Se eligió
DuckDB porque lee Parquet directamente desde SQL, procesa por bloques, vuelca a disco lo que no entra
(se fija `memory_limit` en 4 GB por etapa) y permite escribir el modelo y las preguntas en SQL. Con
pandas habría que cargar cada mes completo en memoria, y los duplicados y los joins agregan copias
intermedias.

### 3.2 Ingesta

- La descarga se hace por bloques a un archivo `.part` y se renombra al terminar, así un corte nunca
  deja un archivo truncado con el nombre final.
- **Idempotencia:** si el archivo existe y su SHA-256 coincide con el último hash registrado como `ok`
  en el log, no se vuelve a descargar (se registra la acción `omitido`). Si no coincide, se descarga de nuevo.
- El log solo agrega líneas: es un historial de intentos. Lo que no debe duplicarse son los datos.
- Hasta 3 reintentos (4 intentos en total) con espera creciente (2 s, 4 s, 8 s), solo para errores transitorios (fallas de red
  y HTTP 429, 500, 502, 503 y 504). Los errores permanentes fallan de inmediato. El servidor responde
  **403** (no 404) cuando un archivo no existe, por ejemplo un mes no publicado.
- Si la ingesta falla, se registra `estado=error` y el proceso termina con código 1.

### 3.3 Esquema

- El contrato `contracts/yellow_schema.yaml` (versión 1) define 20 columnas con tipo y si admiten nulos.
  Los tipos salen de la exploración de los archivos reales (por ejemplo `passenger_count`, `ratecode_id`
  y `payment_type` son BIGINT).
- Antes de cargar cada mes se compara su esquema contra el contrato y se escribe
  `reports/schema_AAAA-MM.json` con columnas faltantes, nuevas y de tipo distinto.
- Los nombres se normalizan a snake_case (`PULocationID` pasa a `pu_location_id`; `Airport_fee` conserva
  la mayúscula en ambos años y pasa a `airport_fee`).
- **`cbd_congestion_fee` en 2024 es NULL.** El cargo empezó a regir en 2025, así que en 2024 no existía.
  NULL significa "no aplica"; 0 significaría "aplicó y valió cero", lo cual sería falso. Consecuencia:
  SUM y AVG ignoran los NULL, y la pregunta 3 se calcula solo sobre el mes asignado, donde la columna no
  tiene nulos. Alternativa descartada: usar 0 simplificaría las sumas pero mezclaría "no existía" con
  "valió cero".
- **Columna nueva fuera del contrato:** se registra una advertencia en el reporte de esquema, no se carga
  a la capa curada y el archivo raw queda intacto. No se pierde información, el pipeline no se rompe por un
  cambio inofensivo y incorporarla exige un cambio versionado del contrato.
- **Columna obligatoria faltante o tipo incompatible:** el pipeline falla. Un cambio de tipo seguro
  (por ejemplo INTEGER a BIGINT) se castea con advertencia; uno que podría perder información
  (BIGINT a INTEGER) falla.
- Hallazgo: al leer los archivos desde carpetas `year=.../month=...`, DuckDB agregaba por su cuenta las
  columnas `year` y `month`. El comparador de esquema lo detectó como columnas nuevas y se desactivó
  `hive_partitioning` al validar y unificar los archivos raw.

### 3.4 Calidad de datos y cuarentena

Las reglas están en `contracts/quality_rules.yaml`, cada una con id, descripción, severidad, umbral y
justificación. Una sola pasada de SQL calcula, para cada fila, las reglas violadas. **Rechazo** envía la
fila a cuarentena; **advertencia** la deja pasar y la marca en `flags`.

Filas que violan cada regla en 2025-03 (una fila puede violar varias, por eso la suma supera a la
cuarentena):

| ID | Regla | Severidad | Umbral | Filas 2025-03 | Justificación |
|---|---|---|---|---|---|
| R01 | Subida fuera del mes del archivo | Rechazo | inicio de mes <= subida < inicio del mes siguiente | 33 | Contamina conteos y particiones por mes; la exploración encontró subidas de 2007 y 2002. |
| R02 | Bajada anterior o igual a la subida | Rechazo | bajada > subida | 22.280 | Duración no positiva es imposible y rompe duración y velocidad. |
| R03 | Distancia menor o igual a 0 | Rechazo | distancia > 0 | 103.722 | Sin distancia no hay viaje utilizable y se rompe la velocidad. |
| R04 | Distancia mayor al máximo | Rechazo | distancia <= 100 millas | 214 | El percentil 99,99 es 58 mi; entre 100 y 200 mi casi no hay viajes y el máximo llega a 320.136 mi. |
| R05 | Importe total negativo | Rechazo | total_amount >= 0 | 68.686 | Reembolsos o disputas no representan el ingreso de un viaje. |
| R06 | Pasajeros nulo o 0 | Advertencia | pasajeros >= 1 | 939.378 | Los nulos son estructurales (ver abajo). Rechazarlos descartaría viajes válidos. |
| R07 | Zona inexistente en la tabla de zonas | Rechazo | id presente en `taxi_zone_lookup` | 0 | Sin zona no se puede enriquecer ni unir al modelo. |
| R08 | Fila duplicada exacta | Rechazo | se conserva la primera ocurrencia | 0 | Duplicar infla viajes e ingresos. |
| R09 | Velocidad promedio imposible (regla propia) | Rechazo | velocidad <= 100 mph | 961 | El percentil 99,9 es 46 mph y el 99,99 supera 1.000 mph. |
| R10 | Zona Unknown, ids 264 o 265 | Advertencia | n/a | 28.415 | Existe en la tabla de zonas pero no informa ubicación; se conserva y se marca. |
| R11 | Importe total excesivo (regla propia) | Rechazo | total_amount <= 1.000 USD | 17 | El percentil 99,99 es 326 USD y el máximo llega a 46.269 USD. |

Los conteos de R06 y R10 son sobre todas las filas leídas; el reporte por mes
(`reports/calidad_AAAA-MM.md`) informa también cuántas filas aceptadas pasaron con cada advertencia.
R07 y R08 no encuentran filas en estos meses; se prueban con datos sintéticos en los tests.

Resultado por mes:

| Mes | Filas leídas | Aceptadas | Cuarentena | % cuarentena |
|---|---|---|---|---|
| 2025-03 | 4.145.257 | 3.955.293 | 189.964 | 4,58 % |
| 2024-03 | 3.582.628 | 3.453.562 | 129.066 | 3,60 % |

**Hallazgos de la exploración que sustentan las decisiones:**

- **Nulos estructurales:** los nulos de `passenger_count`, `ratecode_id`, `store_and_fwd_flag`,
  `congestion_surcharge` y `Airport_fee` coinciden uno a uno con `payment_type = 0` (916.663 filas en 2025
  y 426.190 en 2024). No son errores de carga: por eso R06 es advertencia y no rechazo.
- **VendorID 7:** es un proveedor nuevo en 2025. Sus 21.481 viajes tienen la bajada igual a la subida, así
  que R02 los deja fuera de 2025: son el 96 % de las 22.280 filas con bajada no posterior de ese mes
  (contra 1.128 en 2024). Se
  mantuvo R02 como rechazo porque el criterio se aplica igual a todos los proveedores; el efecto
  (0,5 % de los viajes de 2025) se reconoce como limitación.
- **Propina en efectivo:** la propina media con `payment_type = 2` es 0,0 en ambos años (ver pregunta 4).

**Reconciliación.** `filas leídas = aceptadas + cuarentena`. Las filas leídas salen de los metadatos del
archivo Parquet original, y las otras dos se cuentan sobre los archivos **escritos**, no sobre la tabla
en memoria. Todo se escribe primero en una carpeta temporal y solo si la cuenta cierra se reemplazan las
particiones finales; si no cierra, el pipeline falla, deja el motivo en el reporte y no toca nada de lo
que ya estaba guardado.

### 3.5 Transformación y almacenamiento

- Columnas derivadas: `trip_duration_min`, `avg_speed_mph` (NULL si la duración es 0), `pickup_date`,
  `pickup_hour`, `pickup_dow` (`isodow`: 1 es lunes y 7 es domingo) y `tip_pct` (NULL si la tarifa es 0).
- Enriquecimiento con la tabla de zonas mediante `LEFT JOIN`: `pu_borough`, `pu_zone`, `do_borough`,
  `do_zone`. Se compara el conteo antes y después del join para detectar claves repetidas.
- Parquet particionado `year=AAAA/month=M`. `year` y `month` salen de la ruta y no se guardan dentro
  del archivo. Se escribe la partición directamente y no con `COPY ... PARTITION_BY`, porque esa opción
  no borra los archivos previos de una partición existente y el reproceso no sería idempotente.

**Reproceso de una sola partición** (`make demo-reproceso`, mes reprocesado 2025-03):

| Momento | year=2024/month=3 | year=2025/month=3 |
|---|---|---|
| Antes de reprocesar | 3.453.562 filas, modificada 14:09:16 | 3.955.293 filas, modificada 14:09:06 |
| Después de reprocesar | 3.453.562 filas, modificada 14:09:16 (sin cambios) | 3.955.293 filas, modificada 14:13:00 (reemplazada) |

### 3.6 Modelo estrella

| Tabla | Granularidad (una fila representa...) | Filas |
|---|---|---|
| `fact_trips` | un viaje aceptado de taxi amarillo | 7.408.855 |
| `dim_zone` | una zona de taxi (un `location_id`) | 265 |
| `dim_date` | un día calendario | 396 |
| `agg_zone_hour` | una combinación de zona de origen, fecha y hora de subida | 205.179 |

```mermaid
erDiagram
  dim_zone ||--o{ fact_trips : "pu_location_id (origen)"
  dim_zone ||--o{ fact_trips : "do_location_id (destino)"
  dim_date ||--o{ fact_trips : "pickup_date_key"
  fact_trips ||--o{ agg_zone_hour : "agrega por zona, fecha y hora"
```

- `fact_trips` es una vista sobre la capa curada: no duplica datos y refleja siempre lo que hay en disco.
- `trip_id` es `hash(source_file, file_row_number)`: estable entre corridas y único (se verifica con
  `scripts/verificar_modelo.py`).
- `dim_zone` se usa dos veces desde la tabla de hechos (zona de origen y de destino): es una dimensión con rol.
- `dim_date` es un calendario continuo desde el primer día del primer mes hasta el último del último mes.
- El modelo se reconstruye completo con `CREATE OR REPLACE`, así que correrlo N veces deja lo mismo.
- Las preguntas usan solo viajes aceptados. Una consulta por archivo en `sql/`; resultados e
  interpretaciones en `RESULTADOS.md` (las interpretaciones viven en `docs/interpretaciones.yaml`).

## 4. Tests

`make test` corre 36 tests automatizados. Los tres que pide el enunciado:

- **Regla de calidad sobre datos sintéticos:** `tests/test_calidad.py` (una prueba por regla, incluidos
  los límites del mes, los nulos y la división por cero de la velocidad).
- **Idempotencia de la ingesta:** `tests/test_ingesta.py` (la segunda corrida no descarga; un archivo
  alterado se vuelve a descargar), sin acceso a internet gracias a que la función de descarga se inyecta.
- **Reconciliación de conteos:** `tests/test_reconciliacion.py` y `tests/test_calidad_mes.py`, que
  también verifica que, si no cuadra, falla sin tocar las particiones existentes.

El resto cubre reintentos y errores permanentes, el comparador de esquema, la transformación, el
reproceso de una sola partición, el modelo y el pipeline completo.

## 5. Tiempo de ejecución y pico de RAM

Medido por `src/pipeline.py` (guarda `reports/ejecucion_AAAA-MM.json`). `make all MES=2025-03`, con los
archivos ya descargados:

| Etapa | Segundos |
|---|---|
| ingesta | 0,2 |
| esquema | 0,1 |
| calidad (dos meses) | 24,7 |
| transformación (dos meses) | 7,5 |
| modelo | 0,5 |
| preguntas | 1,1 |
| **Total** | **34,1** |

**Pico de RAM del proceso: 1,91 GB** (muestreado cada 0,2 s; DuckDB corre dentro del proceso).
La primera ejecución desde cero (incluyendo la descarga de los tres archivos desde la TLC) tardó
**44,1 s** en total (**20,2 s** de ingesta, pico de RAM **2,16 GB**).

Máquina de medición: AMD Ryzen 3 7320U with Radeon Graphics, 4 nucleos

## 6. Limitaciones conocidas

- **VendorID 7 queda fuera de 2025** por R02 (21.481 viajes, 0,5 % del mes). La variación de viajes entre
  meses está levemente subestimada por esa razón.
- Los **umbrales** (100 mi, 100 mph, 1.000 USD) son fijos y se justificaron con los dos meses explorados;
  otro mes podría pedir ajustarlos.
- Solo se detectan **duplicados exactos**; no los casi iguales.
- La tabla `agg_zone_hour` se reconstruye completa en cada corrida. Con muchos meses convendría
  hacerla incremental por partición.
- El hash local se compara contra el hash que registró el propio pipeline, no contra el servidor (que no
  publica uno). Protege contra archivos corruptos o alterados después de la descarga, pero no detecta que
  la TLC reemplace un archivo remoto por otro con el mismo nombre.
- Un archivo que existe pero no figura en el log se vuelve a descargar, porque no hay hash de referencia.
- Los análisis del modelo muestran dónde cambian los datos, no por qué: por ejemplo, el crecimiento de
  los viajes con `payment_type = 0` (pregunta 6) no se puede explicar solo con estos datos.
- Las descripciones de los tipos de pago salen del diccionario de datos de la TLC.
- El pico de RAM se muestrea cada 0,2 s: un pico más breve podría no registrarse.
- En Windows, un proyecto dentro de una carpeta sincronizada (OneDrive) puede dar errores de archivo
  bloqueado al mover carpetas; conviene pausar la sincronización o usar una ruta fuera de OneDrive.
- El pipeline procesa los meses en forma secuencial y en un solo proceso.
- La base heredada `data/warehouse/taxi.duckdb` (Fase 1) fue creada originalmente con DuckDB 2.0.0 (formato de almacenamiento v69) y se conserva sin modificar; el entorno actual de Fase 2 fija `duckdb==1.5.6` por compatibilidad con `dlt` y `dbt-duckdb`, por lo que trabaja íntegramente sobre `data/warehouse/nyc_taxi_dlt.duckdb`.

## 7. Estructura del repositorio

```
README.md  RESULTADOS.md  Makefile  requirements.txt  requirements-dashboard.txt  Dockerfile  docker-compose.yml  .gitignore  .dockerignore
contracts/   yellow_schema.yaml (contrato de esquema), quality_rules.yaml (reglas de calidad)
src/         ingesta, esquema, calidad, transformacion, modelo, preguntas, pipeline, carga_dlt, intermediate, reconciliacion, pipeline_fase2
dbt_project/ dbt_project.yml, profiles.yml, macros/, models/{staging,intermediate,marts}, tests/
sql/         modelo/ (dim_zone, fact_trips, dim_date, agg_zone_hour) y q1..q6 (una consulta por archivo)
tests/       42 tests (36 originales + 6 de Fase 2 en test_fase2.py)
scripts/     exploración de datos, verificaciones y demo de reproceso
dashboard/   app.py (panel opcional Streamlit + Plotly de solo consulta sobre DuckDB)
docs/        exploracion.md (hallazgos de los datos) e interpretaciones.yaml
```

## 8. Fase 2: Integración dlt + DuckDB + Intermediate + dbt + Docker

### 8.1 Arquitectura y flujo de datos

```mermaid
flowchart LR
  RAW["Raw Parquet\ndata/raw/"] -->|"src/carga_dlt.py\n(lotes Arrow 5.000)"| STG["DuckDB staging\nyellow_trips_2024_03\nyellow_trips_2025_03"]
  RAW -->|"src/calidad.py\n11 reglas + reconciliación"| VAL["Parquet Validada & Cuarentena\ndata/validada/ · data/quarantine/"]
  VAL -->|"src/intermediate.py\n(rutas relativas portables)"| INT["DuckDB intermediate\ntrips_validated · trips_quarantine"]
  STG --> REC["src/reconciliacion.py\nSTAGING = ACEPTADAS + CUARENTENA"]
  INT --> REC
  INT -->|"dbt views"| DBT_INT["dbt_staging & intermediate\nint_trips_enriched\nint_reconciliation_summary"]
  DBT_INT -->|"dbt build"| MARTS["dbt marts\nfact_trips (VIEW)\ndim_zone · dim_date · agg_zone_hour"]
```

- **Base principal de Fase 2:** `data/warehouse/nyc_taxi_dlt.duckdb` (conserva `data/warehouse/taxi.duckdb` intacta).
- **Ingesta con dlt (`src/carga_dlt.py`):** lee Parquet con `pyarrow.parquet.ParquetFile.iter_batches(batch_size=5000)`, convierte a `pyarrow.Table` y carga a `staging.yellow_trips_{AAAA}_{MM}` con `write_disposition="replace"`. Si la tabla ya existe con el conteo exacto del Parquet, omite la recarga salvo `--forzar`.
- **Capa `intermediate` portable (`src/intermediate.py`):** crea vistas sobre `data/validada/...` y `data/quarantine/...` usando rutas POSIX relativas (sin `.resolve()`), evitando duplicar los 7,4M de registros en disco y asegurando compatibilidad entre Windows y Docker (`/app`).
- **Capas dbt (`dbt_project/`):**
  - `dbt_staging` (`view`): `stg_yellow_trips_2024_03` (`cbd_congestion_fee = NULL`), `stg_yellow_trips_2025_03`, `stg_yellow_trips`, `stg_taxi_zones`.
  - `intermediate` (`view`): `int_trips_enriched` (reutiliza exactamente las derivadas y joins de `src/transformacion.py`) e `int_reconciliation_summary`.
  - `marts`: `fact_trips` (`view` para no duplicar 7,4M filas en disco), `dim_zone` (`table`), `dim_date` (`table`) y `agg_zone_hour` (`table`).

### 8.2 Cómo ejecutar Fase 2, dbt y reconciliación

```bash
# Pipeline integrado completo (ingesta -> esquema -> dlt -> calidad -> intermediate -> reconciliación -> transformación -> dbt build -> verificación final)
make fase2 MES=2025-03
# o directo: python -m src.pipeline_fase2 --mes 2025-03

# Etapas individuales
make intermediate                                            # crea/actualiza vistas intermediate
make dbt-build                                               # ejecuta 10 modelos + 50 tests dbt
make dbt-test                                                # ejecuta solo los 50 tests dbt
make reconciliar                                             # verifica staging = aceptadas + cuarentena y marts
python -m pytest -q                                          # 42 tests Python (36 Fase 1 + 6 Fase 2)
```

### 8.3 Ejecución con Docker

```bash
# Construir imagen (incluye Python 3.12, dlt, DuckDB 1.5.6 y dbt-duckdb)
docker compose build

# Ejecutar reconciliación, dbt y tests sobre los volúmenes montados (./data, ./reports, ./RESULTADOS.md)
docker compose run --rm --entrypoint python pipeline -m src.reconciliacion --con-marts
docker compose run --rm --entrypoint dbt pipeline build --project-dir dbt_project --profiles-dir dbt_project
docker compose run --rm --entrypoint python pipeline -m pytest -q

# Ejecutar pipeline completo de Fase 2 dentro del contenedor (usando raw ya descargados)
docker compose run --rm --entrypoint python pipeline -m src.pipeline_fase2 --mes 2025-03 --sin-ingesta
```

### 8.4 Resultados verificados de Fase 2

| Mes | `staging` (dlt) | `aceptadas` (`intermediate`) | `cuarentena` (`intermediate`) | Diferencia |
|---|---:|---:|---:|---:|
| `2024-03` | 3.582.628 | 3.453.562 | 129.066 | 0 |
| `2025-03` | 4.145.257 | 3.955.293 | 189.964 | 0 |
| **TOTAL** | **7.727.885** | **7.408.855** | **319.030** | **0** |

- `marts.fact_trips` (VIEW): **7.408.855** filas · `marts.dim_zone`: **265** · `marts.dim_date`: **396** · `marts.agg_zone_hour`: **205.179** (suma de `trips` = **7.408.855**, `total_revenue` = **203.263.790,71 USD**).
- `dbt build`: **60/60 OK** (10 modelos + 50 tests de integridad, unicidad, relaciones y reconciliación).
- `python -m src.pipeline_fase2 --mes 2025-03`: **66,1 s** en total con pico de RAM de **1,16 GB** (dentro del límite de 8 GB).

### 8.5 Resumen técnico para defensa oral (Fase 2)

1. **Separación de responsabilidades por capa:** `dlt` resuelve la carga incremental/idempotente por lotes Arrow a DuckDB (`staging`); el motor de calidad original (`src/calidad.py`) mantiene exactamente las 11 reglas SQL con `file_row_number` y separa aceptadas y cuarentena en Parquet sin alterar resultados; `src/intermediate.py` expone ambos resultados como vistas portables con rutas relativas; y `dbt` construye las vistas enriquecidas y el modelo estrella en `marts` con 50 pruebas automatizadas.
2. **Eficiencia en 8 GB de RAM y disco acotado:** `dlt` procesa de a 5.000 filas; DuckDB y dbt operan con `memory_limit = '1.5GB'`, `threads = 2`, `preserve_insertion_order = false` y `temp_directory` en disco; `fact_trips` e `int_trips_enriched` son vistas para no duplicar 7,4 millones de registros en disco.
3. **Preparación para Fase 3 (Dagster + Observabilidad):** cada etapa en `src/pipeline_fase2.py` está desacoplada con entradas/salidas explícitas y métricas JSON por corrida (`reports/ejecucion_fase2_*.json`), lista para modelarse como software-defined assets en Dagster junto con Prometheus, Grafana y Dozzle en `docker-compose.yml`.

## 9. Declaración de uso de IA

Usé CLAUDE en el desarrollo para: entender los conceptos (Parquet,
idempotencia, particionado, modelo dimensional, calidad de datos), armar el plan de trabajo, redactar
código base de los módulos y de los tests, interpretar mensajes de error y revisar resultados, y
Antigravity para la auditoría final contra la consigna, correcciones menores y verificación de la
ejecución limpia. Yo definí las decisiones del pipeline a partir de mi propia exploración de los datos
(umbrales, severidades, tratamiento de nulos, NULL en `cbd_congestion_fee`), ejecuté y verifiqué cada
etapa en mi máquina, corregí los errores que aparecieron y redacté las interpretaciones de `RESULTADOS.md`.