# Guía Integral de Defensa Oral y Práctica — `pipeline-nyc-taxi`

Esta guía reúne la explicación completa del proyecto, el flujo de datos de punta a punta, la referencia código por código, los conceptos teóricos exigidos, un banco de 32 preguntas de defensa con respaldo en el código real y 10 ejercicios prácticos de estudio (sin aplicar sobre el repositorio).

---

## Parte A — Explicación del Proyecto

### 1. Qué problema resuelve
Construye una plataforma **batch de ingeniería de datos reproducible, idempotente, observable y verificable** capaz de ingerir, validar, limpiar, modelar y analizar millones de viajes de taxis amarillos de Nueva York en una computadora personal restringida (**AMD Ryzen 3 7320U, 8 GB de RAM, espacio en disco acotado, Windows + Docker Desktop**), evitando cargar datasets completos en memoria Python y garantizando integridad numérica exacta entre origen y modelo analítico.

### 2. Qué datos utiliza
- **Dataset principal:** registros públicos de viajes de taxi amarillo de la *NYC Taxi & Limousine Commission* (**NYC TLC**):
  - `yellow_tripdata_2024-03.parquet` (marzo 2024: **3.582.628 filas**).
  - `yellow_tripdata_2025-03.parquet` (marzo 2025: **4.145.257 filas**).
- **Tabla maestra de referencia:** `taxi_zone_lookup.csv` (**265 zonas** con `LocationID`, `Borough`, `Zone`, `service_zone`).
- **Base de trabajo actual (Fases 2–4):** `data/warehouse/nyc_taxi_dlt.duckdb` (conserva intacta `data/warehouse/taxi.duckdb`, heredada de Fase 1 en formato DuckDB v69/2.0.0).

### 3. Por qué se eligieron esos datos
1. **Volumen real no trivial (~7,73 millones de registros en dos meses):** obliga a diseñar para streaming/lotes, volcado a disco y vistas en lugar de cargar todo con `pandas`.
2. **Anomalías reales de datos:** contiene duraciones nulas/negativas (`VendorID = 7`), distancias absurdas (hasta `320.136` millas), tarifas negativas (`-999`), fechas fuera de mes (2002/2007) y nulos estructurales (`payment_type = 0`).
3. **Evolución de esquema real entre años:** la columna `cbd_congestion_fee` (peaje de congestión del centro de Manhattan) aparece recién en 2025 y no existía en 2024.

### 4. Qué hace el pipeline
Ejecuta un flujo desacoplado de **9 etapas** (orquestables por CLI en `src/pipeline_fase2.py` o por Dagster en `orchestration/assets.py`):
1. **Ingesta idempotente** de Parquet raw y CSV de zonas con hash SHA-256 y reintentos (`src/ingesta.py`).
2. **Validación de esquema** contra `contracts/yellow_schema.yaml` (`src/esquema.py`).
3. **Carga por lotes Arrow hacia `staging`** en DuckDB usando `dlt` (`src/carga_dlt.py`).
4. **Calidad y cuarentena** con las 11 reglas de `contracts/quality_rules.yaml`, reconciliación y escritura atómica por partición (`src/calidad.py`).
5. **Vistas `intermediate` portables** con rutas relativas POSIX sobre Parquet validados y cuarentena (`src/intermediate.py`).
6. **Reconciliación intermedia** (`staging = aceptadas + cuarentena`) en solo lectura (`src/reconciliacion.py`).
7. **Transformación curada** Parquet Hive (`year=AAAA/month=M`) con columnas derivadas y enriquecimiento de zonas (`src/transformacion.py`).
8. **Transformaciones y tests `dbt`** (`dbt_staging` → `intermediate` → `marts` + 50 tests) en `dbt_project/`.
9. **Verificación final integral** incluyendo reconciliación contra `marts` y actualización de métricas para observabilidad.

### 5. Cuáles son sus resultados
- **Reconciliación integral exacta:**
  - `2024-03`: **3.582.628** `staging` = **3.453.562** `aceptadas` + **129.066** `cuarentena` (`diferencia = 0`).
  - `2025-03`: **4.145.257** `staging` = **3.955.293** `aceptadas` + **189.964** `cuarentena` (`diferencia = 0`).
  - **Total:** **7.727.885** `staging` = **7.408.855** `aceptadas` + **319.030** `cuarentena` (`diferencia = 0`).
- **Modelo estrella (`marts` en `nyc_taxi_dlt.duckdb`):**
  - `marts.fact_trips` (VIEW): **7.408.855** filas.
  - `marts.dim_zone` (TABLE): **265** zonas.
  - `marts.dim_date` (TABLE): **396** días calendario.
  - `marts.agg_zone_hour` (TABLE): **205.179** agregados, cuya suma iguala exactamente **7.408.855** viajes y **203.263.790,71 USD** de revenue.
- **Hallazgos analíticos (`RESULTADOS.md` / `docs/interpretaciones.yaml`):**
  - **Q1:** Midtown Center lidera con `175.034` viajes; 9 de las 10 zonas principales crecieron respecto a 2024 (solo JFK bajó `-0,8 %`).
  - **Q2:** La hora de mayor ingreso medio por viaje es las `5 h` (`USD 35,58` en 2024; `USD 33,56` en 2025).
  - **Q3:** El `74,25 %` de los viajes de `2025-03` tuvo `cbd_congestion_fee > 0`, superando el `99,7 %` en el sur de Manhattan (SoHo `99,83 %`).
  - **Q4:** La propina con tarjeta fue `26,1 %` (`25,18 %` en 2024); el `0,0 %` en efectivo refleja que el taxímetro no registra propinas en efectivo.
  - **Q5:** Aeropuertos JFK/LaGuardia suman `318.293` viajes en `2025-03` con tarifa media `USD 55,29` (~3× la media general `USD 18,40`).
  - **Q6:** Los viajes crecieron `+14,53 %` y la tarifa media total bajó `-3,47 %` por efecto composición (`payment_type = 0` / Flex Fare subió de `379.418` a `854.427`), manteniéndose casi estable la tarifa media positiva (`USD 19,15` vs `USD 19,16`).

### 6. Cómo está organizado
- `contracts/`: contratos declarativos YAML de esquema y reglas de calidad.
- `src/`: módulos Python desacoplados para ingesta, esquema, calidad, dlt, intermediate, transformación, modelo, preguntas, reconciliación, orquestación CLI y exporter de métricas.
- `orchestration/`: definición de assets, job, hooks, locks de concurrencia y schedule de Dagster.
- `dbt_project/`: proyecto dbt con capas `staging`, `intermediate`, `marts` y tests singulares/esquema.
- `monitoring/`: configuración de Prometheus y provisioning/dashboard JSON de Grafana.
- `sql/`: DDL de referencia (Fase 1) y las 6 consultas SQL analíticas `q1..q6`.
- `dashboard/`: aplicación interactiva Streamlit + Plotly (`app.py`) conectada en `READ_ONLY` a `marts`.
- `tests/`: suite `pytest` organizada por fase y componente.
- `reports/`, `docs/`, `scripts/`: artefactos de ejecución/auditoría, documentación y scripts de exploración/verificación.

### 7. Qué herramientas utiliza y qué aporta cada una
| Herramienta | Qué aporta en este proyecto |
|---|---|
| **Python 3.12 + PyArrow** | Pegamento del pipeline, lectura/escritura de Parquet por lotes (`iter_batches`) y lectura instantánea de metadatos (`num_rows`) sin cargar millones de filas en RAM. |
| **DuckDB 1.5.6** | Motor SQL columnar embebido que consulta Parquet directamente, limita RAM (`memory_limit = '1.5GB'`), vuelca a disco (`temp_directory`) y aloja el data warehouse (`nyc_taxi_dlt.duckdb`). |
| **dlt 1.32.0** | Ingesta estructurada e idempotente por lotes Arrow hacia `staging.yellow_trips_*` con `write_disposition="replace"` y verificación de conteos. |
| **dbt-core / dbt-duckdb** | Transformaciones SQL modulares (`source()`, `ref()`), materializaciones diferenciadas (`view` vs `table`), documentación y **50 pruebas automatizadas** (60 items en `dbt build`). |
| **Dagster 1.13.31** | Orquestador basado en **9 Software-Defined Assets**, configuración tipada (`TaxiPipelineConfig`), ejecución serial (`in_process_executor`), hooks de éxito/fallo y UI web con logs por paso. |
| **filelock** | Exclusión mutua (`data/tmp/pipeline_duckdb.lock` y `reports/pipeline_metrics.lock`) para evitar corrupción o locks conflictivos en DuckDB y métricas. |
| **Prometheus + psutil** | Recolección cada `5 s` de métricas reales de negocio, reconciliación, duración por etapa, CPU, RAM, disco y disponibilidad HTTP desde `src/metrics_exporter.py`. |
| **Grafana 11.3.0** | Dashboard provisionado (`NYC Taxi — Pipeline Monitoring`, 12 paneles) conectado al datasource `prometheus` con persistencia en volumen Docker. |
| **Dozzle v8.8.1** | Visor web ligero de logs en tiempo real sobre `/var/run/docker.sock:ro` con `DOZZLE_ENABLE_ACTIONS=false`. |
| **Docker & Compose** | Empaquetado reproducible de la plataforma (`dagster-webserver`, `metrics-exporter`, `prometheus`, `grafana`, `dozzle`, y perfil `cli`) con puertos acotados a `127.0.0.1`. |
| **Streamlit + Plotly** | Dashboard analítico local (`READ_ONLY`) para demostrar reglas, cuarentena, integridad del modelo estrella y las 6 preguntas de negocio en la defensa. |

---

## Parte B — Flujo Completo de Información (Punta a Punta)

```text
[1. CloudFront TLC]
   yellow_tripdata_YYYY-MM.parquet + taxi_zone_lookup.csv
        │  src/ingesta.py :: ingestar() / ingestar_zonas()
        │  (descarga a .part -> SHA-256 -> append data/control/ingestion_log.csv)
        ▼
[2. Capa Raw]  data/raw/yellow/year=AAAA/month=MM/*.parquet  +  data/raw/zones/taxi_zone_lookup.csv
        │
        ├──► [3. Contrato de Esquema]  src/esquema.py :: validar_mes() / sql_mes()
        │       Compara vs contracts/yellow_schema.yaml -> escribe reports/schema_AAAA-MM.json
        │       (NO altera raw; completa columnas nullable faltantes con CAST(NULL AS tipo))
        │
        ├──► [4. Staging dlt]  src/carga_dlt.py :: cargar_mes()
        │       Lee Parquet en lotes Arrow de 5.000 filas (ParquetFile.iter_batches)
        │       Si COUNT(*) ya coincide y no hay --forzar -> omite recarga
        │       Carga en DuckDB -> staging.yellow_trips_2024_03 (3.582.628) y staging.yellow_trips_2025_03 (4.145.257)
        │
        └──► [5. Calidad & Cuarentena]  src/calidad.py :: procesar_mes()
                Evalúa 11 reglas SQL de contracts/quality_rules.yaml en data/tmp/calidad_*/work.duckdb
                (Pre-filtro hash para R08 + memory_limit='1.5GB' + threads=2 + spill a disco)
                Reconcilia filas_leidas == aceptadas + cuarentena ANTES de mover particiones
                Escribe reports/calidad_AAAA-MM.{json,md}
                   ├──► data/validada/year=AAAA/month=M/data.parquet   (3.453.562 + 3.955.293 = 7.408.855)
                   └──► data/quarantine/year=AAAA/month=M/data.parquet (129.066 + 189.964 = 319.030)
                            │
                            ├──► [6. Intermediate Portable]  src/intermediate.py :: crear_intermediate()
                            │       Crea VIEWs con rutas relativas POSIX (sin duplicar bytes):
                            │       - intermediate.trips_validated_2024_03 / _2025_03 / trips_validated
                            │       - intermediate.trips_quarantine_2024_03 / _2025_03 / trips_quarantine
                            │
                            ├──► [7. Reconciliación]  src/reconciliacion.py :: reconciliar_fase2()
                            │       Abre nyc_taxi_dlt.duckdb en read_only=True
                            │       Verifica staging == aceptadas + cuarentena (7.727.885 == 7.408.855 + 319.030)
                            │
                            ├──► [8. Capa Curada Hive]  src/transformacion.py :: transformar_mes()
                            │       Genera data/curated/trips/year=AAAA/month=M/data.parquet (ZSTD)
                            │
                            └──► [9. dbt Build]  src/pipeline_fase2.py :: ejecutar_dbt("build")
                                    dbt_project/models/:
                                    - dbt_staging (VIEW): stg_yellow_trips_2024_03 (cbd NULL), _2025_03, stg_yellow_trips, stg_taxi_zones
                                    - intermediate (VIEW): int_trips_enriched, int_reconciliation_summary
                                    - marts: fact_trips (VIEW 7.408.855), dim_zone (TABLE 265), dim_date (TABLE 396), agg_zone_hour (TABLE 205.179)
                                    + 50 tests (schema.yml + 5 tests singulares en dbt_project/tests/)
                                            │
                                            ├──► [10a. Dashboard Analítico] dashboard/app.py
                                            │       DuckDB read_only=True, search_path='marts,main'
                                            │       Consulta fact_trips, dim_zone, dim_date, agg_zone_hour y sql/q1..q6
                                            │
                                            └──► [10b. Orquestación & Observabilidad]
                                                    orchestration/jobs.py hooks (success_hook / failure_hook)
                                                       ──► src/metrics_exporter.py :: registrar_ejecucion_metricas()
                                                           (FileLock + deduplicación por run_id -> reports/pipeline_metrics.json)
                                                       ──► metrics-exporter :8000/metrics (NycTaxiCollector + ProcessCollector + psutil + HTTP probes)
                                                       ──► Prometheus :9090 (scrape cada 5s)
                                                       ──► Grafana :3001 (12 paneles en vivo)
                                                       ──► Dozzle :8080 (logs stdout/stderr vía /var/run/docker.sock:ro)
```

---

## Parte C — Explicación Detallada del Código por Módulo

### 1. `src/ingesta.py`
- **Responsabilidad:** descarga streaming tolerante a fallos e idempotente por SHA-256 de Parquet mensuales y `taxi_zone_lookup.csv`.
- **Funciones principales:** `sha256(ruta, bloque=1<<20)`, `es_transitorio(exc)`, `descargar(url, destino, reintentos=3)`, `ultimo_hash_ok(log, archivo)`, `registrar(log, **campos)`, `ingestar(mes, base_dir, descargar, contar_filas)`, `ingestar_zonas(...)`.
- **Qué devuelve:** `None` (imprime estado y devuelve código `0`/`1` en `main()`).
- **Dependencias:** `requests`, `pyarrow.parquet`, `hashlib`, `csv`.
- **Lee / Escribe:** lee/escribe `data/control/ingestion_log.csv` (append-only); descarga vía `.part` y renombra a `data/raw/yellow/year=AAAA/month=MM/yellow_tripdata_AAAA-MM.parquet` y `data/raw/zones/taxi_zone_lookup.csv`.
- **Errores típicos:** `ValueError` (formato de mes distinto a `AAAA-MM`), `requests.HTTPError` `403` (archivo inexistente en TLC, falla sin reintentar), errores transitorios (`429`, `500`, `502`, `503`, `504`, timeout/conexión → 3 reintentos de `2s, 4s, 8s`).
- **Ejecución / Modificación:** `python -m src.ingesta --mes 2025-03`. Para cambiar fuente o reintentos, editar constantes `BASE`, `ZONAS_URL` o parámetros de `descargar()`.

### 2. `src/esquema.py`
- **Responsabilidad:** comparar esquema físico de cada Parquet contra `contracts/yellow_schema.yaml` y generar el `SELECT` unificado con tipos normalizados y columnas de trazabilidad.
- **Funciones principales:** `snake(nombre)`, `cargar_contrato(ruta)`, `esquema_real(ruta_parquet)`, `comparar(real, contrato)`, `evaluar(resultado, contrato)`, `validar_mes(mes, base_dir, reports_dir, contrato_path)`, `sql_unificado(...)`, `sql_mes(...)`.
- **Qué devuelve:** `validar_mes()` devuelve dict con `faltantes`, `nuevas`, `tipos_distintos`, `origen_de_cada_columna`, `estado`; `sql_mes()` devuelve string SQL con `file_row_number` y `CAST(...)`.
- **Dependencias:** `duckdb`, `pyyaml`.
- **Lee / Escribe:** lee `contracts/yellow_schema.yaml` y Parquet en `data/raw/`; escribe `reports/schema_AAAA-MM.json`.
- **Errores típicos:** `ErrorEsquema` si falta el Parquet raw, falta una columna `nullable: false` o un tipo cambia de forma incompatible (ej. `BIGINT` → `INTEGER`).
- **Ejecución / Modificación:** `python -m src.esquema --mes 2025-03`. Para agregar una columna oficial al contrato, actualizar `contracts/yellow_schema.yaml` (si cambia `version`, los tests unitarios y reportes lo reflejan).

### 3. `src/carga_dlt.py`
- **Responsabilidad:** carga incremental/idempotente por lotes Arrow (`batch_size=5000`) hacia el esquema `staging` de `nyc_taxi_dlt.duckdb`.
- **Funciones principales:** `_conteo_existente(warehouse, schema, tabla)`, `cargar_mes(mes, tamano_lote=5000, base_dir=Path("data"), warehouse_path=None, forzar=False)`.
- **Qué devuelve:** `None` (imprime resumen y valida reconciliación `cantidad == filas_origen`).
- **Dependencias:** `dlt`, `duckdb`, `pyarrow`, `pyarrow.parquet`.
- **Lee / Escribe:** lee Parquet `data/raw/...`; escribe tablas `staging.yellow_trips_{anio}_{mm}` en `data/warehouse/nyc_taxi_dlt.duckdb`.
- **Errores típicos:** `ValueError` (`tamano_lote <= 0`), `FileNotFoundError` si falta el Parquet raw, `RuntimeError` si tras `pipeline.run()` el conteo en DuckDB no iguala `metadata.num_rows`.
- **Ejecución / Modificación:** `python -m src.carga_dlt --mes 2025-03 [--lote 5000] [--forzar]`.

### 4. `src/calidad.py`
- **Responsabilidad:** evaluar en SQL las 11 reglas de `contracts/quality_rules.yaml`, detectar duplicados exactos con bajo consumo de RAM, separar filas aceptadas/cuarentena y exigir reconciliación antes de reemplazar particiones.
- **Funciones principales:** `cargar_reglas(ruta)`, `limites_mes(mes)`, `reglas_violadas(con, tabla, reglas, mes)`, `crear_vista_zonas(con, base_dir)`, `sql_evaluado(...)`, `reconciliar(leidas, aceptadas, cuarentena)`, `contar_filas(ruta)`, `procesar_mes(mes, base_dir, reports_dir, contrato_path, reglas_path, memoria="1.5GB")`.
- **Qué devuelve:** dict `rep` con `filas_leidas`, `aceptadas`, `cuarentena`, `pct_cuarentena`, `aceptadas_con_advertencia`, `rechazo_por_regla`, `advertencias_por_regla`, `reconciliacion`.
- **Dependencias:** `duckdb`, `pyarrow.parquet`, `pyyaml`, `src.esquema`.
- **Lee / Escribe:** lee `contracts/quality_rules.yaml`, `data/raw/...`, `taxi_zone_lookup.csv`; usa `data/tmp/calidad_{mes}/work.duckdb` + `data/tmp/duckdb`; escribe `data/validada/year=AAAA/month=M/data.parquet`, `data/quarantine/year=AAAA/month=M/data.parquet` y `reports/calidad_AAAA-MM.{json,md}`.
- **Errores típicos:** `ErrorReconciliacion` si `leidas != aceptadas + cuarentena` (deja intactas las particiones finales y escribe la evidencia del fallo); OOM si se eleva demasiado `memoria` o se quita el pre-filtro por hash de `R08`.
- **Ejecución / Modificación:** `python -m src.calidad --mes 2025-03`. Reglas y umbrales se modifican declarativamente en `contracts/quality_rules.yaml`.

### 5. `src/intermediate.py`
- **Responsabilidad:** crear/reemplazar vistas SQL en el esquema `intermediate` sobre los Parquet validados y de cuarentena sin duplicación física en disco.
- **Funciones principales:** `_ruta_sql(path)`, `_configurar_duckdb(con, base_dir, memoria)`, `_verificar_columnas(...)`, `crear_intermediate(meses=("2024-03","2025-03"), base_dir=Path("data"), warehouse_path=None, memoria="1.5GB")`.
- **Qué devuelve:** dict con `meses`, `total_aceptadas`, `total_cuarentena`.
- **Dependencias:** `duckdb`.
- **Lee / Escribe:** lee `data/validada/...` y `data/quarantine/...`; crea vistas `intermediate.trips_validated_{AAAA}_{MM}`, `intermediate.trips_quarantine_{AAAA}_{MM}`, `intermediate.trips_validated` y `intermediate.trips_quarantine` en `nyc_taxi_dlt.duckdb`.
- **Errores típicos:** `FileNotFoundError` si falta el warehouse o un Parquet mensual; `ValueError` si falta `source_file`/`file_row_number`/`flags`/`reglas_violadas`; `RuntimeError` si la vista consolidada descuadra contra la suma mensual.
- **Ejecución / Modificación:** `python -m src.intermediate [--meses 2024-03 2025-03]`. Mantiene rutas relativas POSIX cuando `base_dir=Path("data")` para compatibilidad Windows ↔ Docker (`/app`).

### 6. `src/reconciliacion.py`
- **Responsabilidad:** auditar en modo `READ_ONLY` el cuadre `staging == aceptadas + cuarentena` por mes, vistas consolidadas y (con `verificar_marts=True`) `marts.fact_trips` y `marts.agg_zone_hour` (conteo y revenue).
- **Funciones principales:** `reconciliar_fase2(meses=("2024-03","2025-03"), warehouse_path=Path("data/warehouse/nyc_taxi_dlt.duckdb"), verificar_marts=False)`, `imprimir_resumen(res)`.
- **Qué devuelve:** dict `resultado` con `meses`, `totales`, `marts`, `ok`, `errores`.
- **Dependencias:** `duckdb`.
- **Lee / Escribe:** solo lectura sobre `nyc_taxi_dlt.duckdb`. No escribe archivos.
- **Errores típicos:** `FileNotFoundError` si falta DuckDB; `ErrorReconciliacionFase2` si falta un objeto esperado, si `diferencia != 0` o si `agg_zone_hour` difiere de `fact_trips` (`>= 0.01` en revenue o distinto conteo).
- **Ejecución / Modificación:** `python -m src.reconciliacion --con-marts`.

### 7. `src/transformacion.py`
- **Responsabilidad:** generar la capa curada Parquet Hive (`data/curated/trips/year=AAAA/month=M/data.parquet`) añadiendo `trip_duration_min`, `avg_speed_mph`, `pickup_date`, `pickup_hour`, `pickup_dow`, `tip_pct` y zonas (`pu_*`, `do_*`), controlando que el `LEFT JOIN` no multiplique filas.
- **Funciones principales:** `sql_curada(ruta_validada, ruta_zonas)`, `transformar_mes(mes, base_dir=Path("data"), memoria="4GB")`.
- **Qué devuelve:** dict `{"mes": mes, "filas": filas_curadas, "sin_zona": sin_zona}`.
- **Dependencias:** `duckdb`, `src.calidad`.
- **Lee / Escribe:** lee `data/validada/...` y `taxi_zone_lookup.csv`; escribe `data/curated/trips/year=AAAA/month=M/data.parquet`.
- **Errores típicos:** `ErrorTransformacion` si falta la partición validada o si `filas_curadas != filas_validadas` (por ejemplo, por duplicados en la tabla de zonas).

### 8. `src/modelo.py` y `src/preguntas.py` (Fase 1 heredada)
- **`src/modelo.py`:** `construir_modelo(base_dir, sql_dir, memoria)` ejecuta los 4 SQL en `sql/modelo/` sobre `data/warehouse/taxi.duckdb`. Mantenido intacto para compatibilidad con tests y CLI `make all`.
- **`src/preguntas.py`:** `ejecutar(mes, mes_cmp, base_dir, sql_dir, salida, interpretaciones)` reemplaza `{MES}`, `{ANIO}`, `{ANIO_ANT}` en `sql/q1..q6` y genera `RESULTADOS.md`.

### 9. `src/pipeline.py` y `src/pipeline_fase2.py`
- **`src/pipeline.py`:** orquestador CLI Fase 1 (`correr()`), define `mes_comparacion()`, `ErrorPipeline` y `_MonitorRam` (muestrea RSS con `psutil` cada `0.2 s`).
- **`src/pipeline_fase2.py`:** orquestador CLI integrado de Fase 2 (`ejecutar_dbt()`, `correr_fase2()`). Invoca `dbt build` como subproceso propagando `DUCKDB_PATH` y `--vars {"zones_csv_path": ...}`, escribe `reports/ejecucion_fase2_{mes}.json` y registra métricas en `reports/pipeline_metrics.json` tanto en éxito como en fallo.
- **Ejecución:** `python -m src.pipeline_fase2 --mes 2025-03 [--sin-ingesta] [--forzar-dlt]`.

### 10. `src/metrics_exporter.py`
- **Responsabilidad:** persistir atómicamente el estado acumulado de ejecuciones (`reports/pipeline_metrics.json` con `FileLock` y deduplicación por `run_id`) y servir `/metrics` y `/health` para Prometheus.
- **Funciones/Clases principales:** `cargar_metricas(metrics_path)`, `registrar_ejecucion_metricas(exito, duracion_s, ..., metrics_path, run_id=None)`, `NycTaxiCollector`, `construir_registry(...)`, `iniciar_exporter(port=8000, host="0.0.0.0", ...)`.
- **Dependencias:** `prometheus_client` (incluyendo `ProcessCollector`), `psutil`, `filelock`, `urllib.request`, `http.server`.
- **Lee / Escribe:** `registrar_ejecucion_metricas()` escribe `reports/pipeline_metrics.json` (vía `.json.tmp` + `FileLock .lock`). `NycTaxiCollector.collect()` **solo lee** `reports/pipeline_metrics.json` (con fallback a `ejecucion_fase2_2025-03.json`), consulta `psutil` y `shutil.disk_usage`, y sondea `DAGSTER_URL` y `DOZZLE_URL`.
- **Ejecución:** `python -m src.metrics_exporter --port 8000` (en Docker como servicio `metrics-exporter`).

### 11. Paquete `orchestration/` (Dagster)
- **`orchestration/resources.py`:** `TaxiPipelineConfig` (`mes="2025-03"`, `mes_comparacion=""`, `base_dir="data"`, `reports_dir="reports"`, `dbt_dir="dbt_project"`, `sin_ingesta=True`, `forzar_dlt=False`, `simular_fallo=False`), `resolver_meses()`, `ruta_warehouse()`, y `@contextmanager adquirir_lock_pipeline(base_dir, timeout=30.0)` con `FileLock("data/tmp/pipeline_duckdb.lock")`.
- **`orchestration/assets.py`:** define los 9 `@asset` en serie (`ALL_ASSETS`): `raw_ingesta` → `esquema_validado` → `dlt_staging` → `calidad_cuarentena` → `intermediate_views` → `reconciliacion_intermedia` → `transformacion_curada` → `dbt_marts` → `verificacion_final`. Todas las etapas que escriben o usan DuckDB/temporales (`dlt_staging`, `calidad_cuarentena`, `intermediate_views`, `reconciliacion_intermedia`, `transformacion_curada`, `dbt_marts`, `verificacion_final`) adquieren `adquirir_lock_pipeline(base_dir)`.
- **`orchestration/jobs.py`:** define `nyc_taxi_pipeline = define_asset_job(...)` con `executor_def=in_process_executor` y hooks `registrar_exito_pipeline` / `registrar_fallo_pipeline`. Los hooks calculan duración total y por paso (`get_run_step_stats`), reconcilian bajo lock en caso de éxito y llaman `registrar_ejecucion_metricas(..., run_id=context.run_id)` para garantizar que cada ejecución se registre una sola vez.
- **`orchestration/schedules.py`:** define `nyc_taxi_monthly_schedule` (`0 6 5 * *`, `DefaultScheduleStatus.STOPPED`).
- **`orchestration/definitions.py` / `__init__.py`:** exportan `defs = Definitions(...)`.
- **`orchestration/dagster.yaml`:** desactiva telemetría y fija `DefaultRunCoordinator` para que `dagster-webserver` ejecute runs inmediatamente sin requerir `dagster-daemon`.

### 12. Proyecto `dbt_project/`
- **`dbt_project.yml` & `profiles.yml`:** perfil `nyc_taxi_dbt`, ruta dinámica vía `env_var('DUCKDB_PATH', 'data/warehouse/nyc_taxi_dlt.duckdb')`, `threads: 2`, `memory_limit: "1.5GB"`, `preserve_insertion_order: false`, `temp_directory: "data/tmp/duckdb"`.
- **`macros/generate_schema_name.sql`:** escribe los esquemas exactos `dbt_staging`, `intermediate`, `marts` (sin prefijo `main_`).
- **Modelos:**
  - `staging/`: `stg_yellow_trips_2024_03` (`cbd_congestion_fee = CAST(NULL AS DOUBLE)`), `stg_yellow_trips_2025_03`, `stg_yellow_trips` (`UNION ALL BY NAME`), `stg_taxi_zones`.
  - `intermediate/`: `int_trips_enriched` (derivadas + joins sobre `intermediate.trips_validated`), `int_reconciliation_summary` (cuadre por mes).
  - `marts/`: `fact_trips` (`materialized='view'`, `hash(source_file, file_row_number) AS trip_id`), `dim_zone` (`table`, 265 filas), `dim_date` (`table`, 396 días), `agg_zone_hour` (`table`, 205.179 filas).
- **Tests (`dbt_project/tests/` + `schema.yml`):** 45 tests genéricos (`not_null`, `unique`, `accepted_values`, `relationships`) + 5 tests singulares (`assert_agg_matches_fact`, `assert_fact_matches_intermediate`, `assert_marts_quality_invariants`, `assert_no_duplicate_trip_id`, `assert_reconciliation_staging_intermediate`) = **50 tests dbt** + **10 modelos** = **60 ejecuciones en `dbt build`**.

### 13. `dashboard/app.py` (Streamlit + Plotly)
- **Responsabilidad:** interfaz analítica interactiva en modo **solo lectura** (`read_only=True`, `memory_limit='1.5GB'`, `threads=2`) organizada en 4 pestañas (*Ingesta y Esquema*, *Calidad, Reglas y Cuarentena*, *Modelo Estrella*, *Preguntas de Negocio Q1–Q6*).
- **Adaptación Fase 4:** `resolver_warehouse()` y `resolver_schema()` leen `DUCKDB_PATH` (default `data/warehouse/nyc_taxi_dlt.duckdb`) y `DUCKDB_SCHEMA` (default `marts`), ejecutando `SET search_path = '<schema>,main'` antes de cada consulta sin modificar nombres de tablas ni SQL `q1..q6`. Prioriza `reports/ejecucion_fase2_*.json` y resume `reports/pipeline_metrics.json`.
- **Ejecución:** `make dashboard` → `http://127.0.0.1:8501`.

### 14. `docker-compose.yml` y `monitoring/`
- **Servicios:** `dagster-webserver` (`127.0.0.1:3000`), `metrics-exporter` (`127.0.0.1:8000`), `prometheus` (`127.0.0.1:9090`), `grafana` (`127.0.0.1:3001`), `dozzle` (`127.0.0.1:8080`) y `pipeline` (perfil `cli`).
- **`monitoring/prometheus/prometheus.yml`:** `scrape_interval: 5s` sobre `localhost:9090` (`prometheus`) y `metrics-exporter:8000` (`nyc_taxi_exporter`).
- **`monitoring/grafana/`:** datasource `Prometheus` (`uid: prometheus`, `http://prometheus:9090`) y dashboard `nyc_taxi_monitoring.json` con **12 paneles** (estado, contadores de corridas, duración total, diferencia de reconciliación, revenue marts, disco libre, disponibilidad servicios, filas por capa, CPU %, RAM entorno/pico, RSS contenedores, duración por etapa).

---

## Parte D — Conceptos Teóricos (con Ejemplos Reales del Proyecto)

1. **ETL y ELT:** *Extract-Transform-Load* (transformar antes de cargar al destino) vs *Extract-Load-Transform* (cargar datos crudos/validados primero al warehouse y transformar allí con SQL). En este proyecto coexisten ambas ideas: `src/ingesta.py` + `src/carga_dlt.py` hacen *E+L* hacia `staging` de DuckDB, y `dbt_project/` aplica *T* en SQL dentro de DuckDB (`dbt_staging` → `intermediate` → `marts`).
2. **Pipeline de datos:** secuencia encadenada y automatizable de etapas con contratos de entrada/salida y corte ante error. Ejemplo: `correr_fase2()` en `src/pipeline_fase2.py` y el grafo de 9 assets en `orchestration/assets.py`.
3. **Batch processing:** procesamiento periódico por lotes cerrados (ej. mes completo terminado) optimizado para alto rendimiento en lecturas secuenciales, a diferencia del streaming evento por evento. Aquí cada mes (`2024-03`, `2025-03`) se procesa como lote mensual completo.
4. **Parquet:** formato de almacenamiento **columnar comprimido** con estadísticas min/max y metadatos de cantidad de filas en el footer. Permite que `contar_filas_parquet()` en `src/ingesta.py` cuente 4,14 millones de filas en milisegundos sin leer las filas, y que DuckDB lea solo las columnas necesarias.
5. **DuckDB:** sistema de gestión de bases de datos analíticas (OLAP) **embebido y columnar**, optimizado para procesar millones de filas en una máquina local mediante ejecución vectorizada y volcado a disco (`SET temp_directory`, `memory_limit='1.5GB'`). Aloja `data/warehouse/nyc_taxi_dlt.duckdb`.
6. **SQL:** lenguaje declarativo para consultar, filtrar y transformar conjuntos de datos. Toda la lógica de reglas (`contracts/quality_rules.yaml`), enriquecimiento (`dbt_project/models/`), reconciliación (`src/reconciliacion.py`) y negocio (`sql/q1..q6`) se expresa en SQL estándar/DuckDB.
7. **dlt (data load tool):** librería Python para extracción y carga declarativa que maneja esquemas, batching e idempotencia. En `src/carga_dlt.py`, `@dlt.resource(write_disposition="replace")` carga lotes PyArrow de `5.000` filas hacia `staging.yellow_trips_*`.
8. **dbt (data build tool):** herramienta para modelar datos en SQL mediante `SELECT`, resolviendo dependencias (`source()`, `ref()`), materializaciones (`view`/`table`) y pruebas de datos. En `dbt_project/` construye las capas analíticas y ejecuta 50 tests.
9. **Staging:** primera capa dentro del warehouse, fiel al origen y con mínima normalización de tipos. Ejemplos: `staging.yellow_trips_2024_03` (de dlt) y `dbt_staging.stg_yellow_trips_2024_03` / `stg_yellow_trips` (que alinea `cbd_congestion_fee = CAST(NULL AS DOUBLE)` para 2024).
10. **Intermediate:** capa intermedia donde se aplican reglas estructurales, uniones, enriquecimientos o resúmenes antes de exponer al consumo final. En este repo: `intermediate.trips_validated`, `intermediate.trips_quarantine` (vistas de `src/intermediate.py`), `int_trips_enriched` e `int_reconciliation_summary` (dbt).
11. **Marts:** capa final orientada a consumo analítico/negocio organizada por dominios o modelos dimensionales. En `dbt_project/models/marts/`: esquema `marts` con `fact_trips`, `dim_zone`, `dim_date` y `agg_zone_hour`.
12. **Modelo estrella (Star Schema):** diseño dimensional compuesto por una tabla central de hechos rodeada de tablas de dimensiones descriptivas desnormalizadas. Aquí `marts.fact_trips` se une con `marts.dim_zone` (origen y destino) y `marts.dim_date`.
13. **Tabla de hechos (Fact Table):** tabla que registra eventos medibles o transacciones con métricas numéricas y claves foráneas hacia dimensiones. `marts.fact_trips` tiene granularidad **1 fila = 1 viaje aceptado** (`7.408.855` filas), con métricas como `trip_distance`, `fare_amount`, `total_amount` y `tip_pct`.
14. **Dimensiones:** tablas de contexto descriptivo usadas para filtrar y agrupar hechos. `marts.dim_zone` describe las 265 zonas (jugando **doble rol**: `pu_location_id` y `do_location_id`) y `marts.dim_date` provee el calendario continuo de 396 días.
15. **Idempotencia:** propiedad por la cual ejecutar una operación 1 o $N$ veces produce exactamente el mismo estado final sin duplicar datos ni efectos laterales indebidos. Ejemplos: SHA-256 en `src/ingesta.py`, omisión por conteo en `src/carga_dlt.py`, reemplazo atómico de partición en `src/calidad.py`, `CREATE OR REPLACE VIEW` en `src/intermediate.py` y deduplicación por `run_id` en `src/metrics_exporter.py`.
16. **Reconciliación:** comprobación cuantitativa que verifica que ningún registro se perdió ni se duplicó entre capas. Ejemplos: `reconciliar()` en `src/calidad.py` (`leidas == aceptadas + cuarentena`) y `reconciliar_fase2()` (`7.727.885 == 7.408.855 + 319.030`, más cierre de viajes y `203.263.790,71 USD` entre `fact_trips` y `agg_zone_hour`).
17. **Calidad de datos:** conjunto de validaciones explícitas sobre completitud, validez temporal, rango numérico, integridad referencial y unicidad. Definida declarativamente en `contracts/quality_rules.yaml` (`R01`–`R11`) y complementada con 50 tests en `dbt_project/`.
18. **Cuarentena:** mecanismo para aislar registros inválidos sin perder trazabilidad ni contaminar los análisis finales, ni detener el resto del lote. En `src/calidad.py`, las filas que violan al menos una regla de `severidad: rechazo` van a `data/quarantine/...` conservando `source_file`, `file_row_number` y el arreglo `reglas_violadas`.
19. **Orquestación:** coordinación automática de tareas dependientes, paso de parámetros, reintentos, manejo de fallos, locks y captura centralizada de logs/eventos. Implementada en `orchestration/` con **Dagster**.
20. **DAG (Directed Acyclic Graph):** grafo dirigido sin ciclos donde los nodos representan tareas/activos y las aristas representan dependencias de ejecución. En Dagster (`orchestration/assets.py`), `raw_ingesta → ... → verificacion_final` forma un DAG lineal de 9 nodos.
21. **Assets (Software-Defined Assets):** abstracción declarativa de Dagster centrada en **qué conjunto de datos/artefacto debe existir** y de qué otros depende, en lugar de enfocarse solo en scripts imperativos. Cada `@asset` en `orchestration/assets.py` declara `deps=[...]` y metadatos observables.
22. **Docker:** plataforma de virtualización ligera a nivel de sistema operativo que empaqueta aplicación + dependencias + runtime en imágenes inmutables y reproducibles. Definido en `Dockerfile` (`python:3.12-slim` → imagen `nyc-taxi-platform:fase3`).
23. **Contenedores:** instancias en ejecución aisladas creadas a partir de imágenes Docker que comparten kernel Linux (en Docker Desktop sobre WSL2) pero aíslan procesos, red y sistema de archivos. Ejemplo: `nyc-taxi-dagster`, `nyc-taxi-exporter`, `nyc-taxi-prometheus`, `nyc-taxi-grafana`, `nyc-taxi-dozzle`.
24. **Docker Compose:** herramienta declarativa (`docker-compose.yml`) para definir, conectar en red, montar volúmenes y gestionar múltiples contenedores como un único stack (`make up`, `make ps`, `make down`).
25. **Prometheus:** base de datos de series temporales y sistema de monitoreo basado en modelo *pull* (scrape HTTP periódico). En `monitoring/prometheus/prometheus.yml` recolecta métricas cada `5 s` desde `metrics-exporter:8000/metrics` y desde sí mismo.
26. **Métricas:** mediciones numéricas agregadas en el tiempo (contadores acumulativos *Counter* como `nyc_taxi_pipeline_runs_total`, medidores instantáneos *Gauge* como `nyc_taxi_pipeline_last_status` o `nyc_taxi_system_memory_used_bytes`). Se diferencian de los logs por su bajo costo de almacenamiento y facilidad para graficar/alertar.
27. **Grafana:** plataforma de visualización y tableros operacionales que consulta fuentes como Prometheus vía PromQL. El dashboard `monitoring/grafana/dashboards/nyc_taxi_monitoring.json` renderiza 12 paneles provisionados automáticamente al levantar Docker.
28. **Logs:** registros cronológicos discretos (texto/eventos en `stdout`/`stderr` o archivos) útiles para diagnosticar exactamente qué ocurrió en una etapa. Ejemplos: `print()` y `context.log.info()` de Dagster, logs de `dbt build`, y `data/control/ingestion_log.csv`.
29. **Dozzle:** visor web en tiempo real y de bajo consumo (`~10 MiB` RAM) que lee la API de Docker vía `/var/run/docker.sock:ro` para filtrar e inspeccionar `stdout`/`stderr` de todos los contenedores sin necesidad de entrar por `docker logs`.

---

## Parte E — Banco de 32 Preguntas Posibles del Profesor (con Respuestas Verificadas en el Código)

1. **¿Por qué elegiste DuckDB en lugar de pandas o PostgreSQL?**
   - DuckDB es embebido (sin servidor ni contenedor pesado adicional), lee Parquet columnar directamente desde SQL, vuelca a disco cuando supera `memory_limit='1.5GB'` (`SET temp_directory`) y procesa 7,73M de filas dentro de 8 GB de RAM (`~1,16–1,54 GB` de pico RSS). Con `pandas` cargar 4,14M filas + joins y deduplicación agotaría la RAM.
2. **¿Por qué usaste dlt?**
   - `src/carga_dlt.py` usa `dlt` para desacoplar la ingesta hacia el warehouse con esquema explícito (`staging`), carga incremental/por lotes PyArrow de `5.000` filas (`ParquetFile.iter_batches`) y manejo automático de metadatos dlt sin saturar RAM.
3. **¿Qué diferencia hay entre dlt y dbt en este proyecto?**
   - **dlt** realiza *Extract & Load* hacia `staging.yellow_trips_*` leyendo los Parquet crudos en Python (`src/carga_dlt.py`). **dbt** realiza *Transform* dentro de DuckDB en SQL (`dbt_project/models/`), construyendo `dbt_staging`, `intermediate` y `marts` y validando los modelos con 50 tests.
4. **¿Qué hace Dagster y por qué no bastaba solo el Makefile?**
   - Dagster (`orchestration/`) modela cada etapa como un `@asset` con dependencias explícitas, UI web (`:3000`), configuración tipada por ejecución (`TaxiPipelineConfig`), detección automática de fallos en cualquier paso, logs estructurados por asset y hooks (`@success_hook`, `@failure_hook`) que alimentan a Prometheus.
5. **¿Qué es un DAG y cómo se materializa aquí?**
   - Es un grafo dirigido acíclico. En `orchestration/assets.py`, cada asset declara `deps=[anterior]`, formando un DAG lineal de 9 assets desde `raw_ingesta` hasta `verificacion_final` (`ALL_ASSETS`), compilado en el job `nyc_taxi_pipeline` (`orchestration/jobs.py`).
6. **¿Cómo se ejecuta un asset o el job completo en Dagster?**
   - Al usar `orchestration/dagster.yaml` con `DefaultRunCoordinator` y `executor_def=in_process_executor`, `dagster-webserver` (`127.0.0.1:3000`) ejecuta las materializaciones en secuencia dentro de un mismo proceso, compartiendo un `FileLock` (`adquirir_lock_pipeline`) en las etapas que usan DuckDB o escriben capas en `data/`.
7. **¿Qué pasa si falla una etapa del pipeline?**
   - En CLI (`src/pipeline_fase2.py`), el wrapper `etapa()` captura la excepción, lanza `ErrorPipeline` cortando inmediatamente para no propagar datos corruptos, y el `except` llama `registrar_ejecucion_metricas(exito=False, ...)`. En Dagster, el asset que falla marca el run como `FAILURE`, detiene los assets dependientes y dispara `@failure_hook registrar_fallo_pipeline`, incrementando `nyc_taxi_pipeline_failures_total` y dejando intactos datos y `marts` previos. En `src/calidad.py`, además, los reemplazos son atómicos y solo ocurren **después** de pasar `reconciliar()`.
8. **¿Cómo detectás duplicados y por qué usás hash antes del `ROW_NUMBER()`?**
   - Regla `R08` en `src/calidad.py::sql_evaluado()`. Hacer `row_number() OVER (PARTITION BY 20 columnas)` directo sobre 4,14M filas en Windows/8 GB RAM disparaba *Out of Memory*. La solución calcula primero `_row_hash = hash(20 columnas)`, agrupa hashes con `count(*) > 1` (`dup_hashes`) y solo calcula la ventana `ROW_NUMBER()` sobre candidatos con hash repetido (`WHERE _row_hash IN ...`), reduciendo drásticamente el consumo de memoria sin perder exactitud (conserva `rn = 1` y rechaza `rn > 1`).
9. **¿Qué es la cuarentena y qué columnas guarda?**
   - Es el almacenamiento separado de registros rechazados (`data/quarantine/year=AAAA/month=M/data.parquet` y vista `intermediate.trips_quarantine`) para auditoría y trazabilidad. Conserva las 20 columnas del contrato más `source_file`, `file_row_number` y la lista `reglas_violadas` (ej. `['R02', 'R09']`).
10. **¿Qué significa idempotencia y dónde está garantizada?**
    - Repetir una corrida deja exactamente los mismos datos sin duplicar filas. Se garantiza en: 1) `src/ingesta.py` (compara SHA-256 contra `ingestion_log.csv` y omite descarga), 2) `src/carga_dlt.py` (verifica conteo exacto y omite recarga; o usa `write_disposition="replace"`), 3) `src/calidad.py` y `src/transformacion.py` (`_reemplazar_particion()` borra y reemplaza solo la partición del mes), 4) `src/intermediate.py` y `dbt` (`CREATE OR REPLACE`), 5) `src/metrics_exporter.py` (deduplicación por `run_id`).
11. **¿Cómo comprobás la reconciliación de principio a fin?**
    - En `src/calidad.py::reconciliar()`: compara `metadata.num_rows` del Parquet original contra las filas **escritas** en `tmp_val` y `tmp_cua`. En `src/reconciliacion.py::reconciliar_fase2()` (modo `read_only=True`): verifica por mes y total `staging == aceptadas + cuarentena` (`7.727.885 == 7.408.855 + 319.030`), vistas consolidadas, `marts.fact_trips == total_aceptadas`, `SUM(agg_zone_hour.trips) == fact_trips` y diferencia de revenue `< 0.01 USD` (`203.263.790,71`). Además `dbt test` ejecuta `assert_reconciliation_staging_intermediate.sql` y `assert_agg_matches_fact.sql`.
12. **¿Por qué usás vistas en `intermediate`, `dbt_staging` y `marts.fact_trips`?**
    - Porque materializar como tablas los ~7,41 millones de filas en cada capa duplicaría gigabytes innecesarios en un disco muy ajustado (`~2,75 GB` libres) y consumiría mucha más RAM/tiempo. Al usar `VIEW`, DuckDB evalúa bajo demanda y solo materializa como `TABLE` las tablas livianas/agregadas (`dim_zone` 265, `dim_date` 396, `agg_zone_hour` 205.179).
13. **¿Por qué usás rutas relativas POSIX en `src/intermediate.py` en vez de `.resolve()`?**
    - Porque una ruta absoluta de Windows (`C:/Users/...`) falla dentro del contenedor Linux (`/app/data/...`). Al pasar `base_dir=Path("data")` y convertir con `.as_posix()`, las vistas apuntan a `'data/validada/...'` y funcionan idénticamente en PowerShell y en Docker Compose (ambos tienen `./` o `/app` como directorio de trabajo).
14. **¿Cómo evitás consumir demasiada RAM?**
    - Lotes Arrow de `5.000` filas en `dlt`; `SET memory_limit = '1.5GB'`, `SET threads = 2`, `SET preserve_insertion_order = false` y `SET temp_directory = 'data/tmp/duckdb'` tanto en Python (`src/calidad.py`, `src/intermediate.py`, `src/reconciliacion.py`) como en `dbt_project/profiles.yml`; pre-filtro por hash en `R08`; ejecución serial en Dagster (`in_process_executor` + `FileLock`); y vistas SQL en `marts.fact_trips`.
15. **¿Cómo protegés a DuckDB contra escrituras concurrentes?**
    - DuckDB permite múltiples lectores `read_only=True` simultáneos (si no hay escritor activo) pero solo **un escritor**. En Dagster (`orchestration/resources.py` y `assets.py`), `adquirir_lock_pipeline(base_dir)` usa `FileLock("data/tmp/pipeline_duckdb.lock")` para serializar todas las etapas de escritura/temporales (`dlt_staging`, `calidad_cuarentena`, `intermediate_views`, `reconciliacion_intermedia`, `transformacion_curada`, `dbt_marts`, `verificacion_final` y el hook de éxito), mientras que `metrics-exporter` solo lee `reports/pipeline_metrics.json` sin abrir DuckDB y Streamlit/reconciliación abren en `read_only=True`.
16. **¿Qué pasa si dos hooks o procesos intentan escribir `pipeline_metrics.json` a la vez?**
    - `registrar_ejecucion_metricas()` en `src/metrics_exporter.py` protege la lectura-modificación-escritura atómica (`tmp.replace()`) con un `FileLock("reports/pipeline_metrics.lock")` y deduplica mediante `recorded_run_ids` (guardando los últimos 200 `run_id`), evitando doble conteo ante reintentos o llamadas repetidas para el mismo `run_id`.
17. **¿Qué monitorea Prometheus exactamente?**
    - Scrapea cada `5 s` al target propio (`prometheus` en `:9090`) y a `nyc_taxi_exporter` (`metrics-exporter:8000/metrics`). Este expone contadores (`runs_total`, `success_total`, `failures_total`), gauges del pipeline (`last_status`, `duration_seconds`, `peak_ram_bytes`, `stage_duration_seconds`, `rows_total`, `reconciliation_diff_rows`, `marts_total_revenue_usd`), métricas de recursos (`system_cpu_percent`, `system_memory_*`, `disk_free_bytes`, `disk_used_percent`), `process_resident_memory_bytes` / `process_cpu_seconds_total` (`ProcessCollector`) y sondas HTTP `nyc_taxi_service_up`.
18. **¿Cómo obtiene Grafana sus datos y por qué no se pierden si se reinicia?**
    - Grafana usa provisioning declarativo (`monitoring/grafana/provisioning/datasources/datasource.yml` con `uid: prometheus` apuntando a `http://prometheus:9090` y `dashboards.yml` leyendo `/var/lib/grafana/dashboards/nyc_taxi_monitoring.json`), y persiste sus metadatos de usuario/estado en el volumen Docker `grafana_data`, mientras Prometheus persiste sus series 7 días en `prometheus_data`.
19. **¿Qué muestra Dozzle y cómo está asegurado?**
    - Muestra en tiempo real (`127.0.0.1:8080`) los logs `stdout`/`stderr` de los 5 contenedores (`nyc-taxi-dagster`, `nyc-taxi-exporter`, `nyc-taxi-prometheus`, `nyc-taxi-grafana`, `nyc-taxi-dozzle`). Como los jobs de Dagster corren en proceso dentro de `dagster-webserver`, los logs detallados del pipeline aparecen en `nyc-taxi-dagster`. Está limitado con `/var/run/docker.sock:ro` (solo lectura) y `DOZZLE_ENABLE_ACTIONS="false"` (sin shell ni botones de reinicio).
20. **¿Qué sucede si Docker Desktop se reinicia?**
    - Los datos en `./data/` y `./reports/` están en bind mounts del host Windows, y las series/configuración en los volúmenes nombrados `prometheus_data` y `grafana_data`. Los 5 servicios tienen `restart: unless-stopped`, `dagster.yaml`, `src/` y `orchestration/` montados en `:ro`, de modo que la plataforma vuelve a levantarse sin perder historial ni métricas.
21. **¿Qué pasa si un archivo de la TLC viene con columnas nuevas?**
    - `src/esquema.py::comparar()` detecta la columna en `nuevas`, `evaluar()` registra un estado `"advertencia"` en `reports/schema_AAAA-MM.json`, y `sql_unificado()` selecciona explícitamente únicamente las 20 columnas declaradas en `contracts/yellow_schema.yaml`. Así el archivo raw queda intacto, el pipeline no falla y la capa curada mantiene esquema estable.
22. **¿Por qué `cbd_congestion_fee` en 2024 es `NULL` y no `0`?**
    - Porque ese cargo comenzó a regir en 2025; en marzo de 2024 no existía. Usar `0` falsearía que aplicó con monto cero y distorsionaría promedios; usar `NULL` (`CAST(NULL AS DOUBLE)` en `src/esquema.py` y `dbt_project/models/staging/stg_yellow_trips_2024_03.sql`) significa "no aplica / no existía".
23. **¿Qué ocurrió con `VendorID = 7` en marzo de 2025?**
    - La exploración (`docs/exploracion.md`) descubrió un proveedor nuevo en 2025 (`VendorID = 7`) cuyos `21.481` viajes (`100 %`) tienen `tpep_dropoff_datetime == tpep_pickup_datetime`. La regla `R02` (`bajada > subida`) los envía a cuarentena (`96 %` de los `22.280` rechazos por `R02`). Se mantuvo el rechazo uniforme entre proveedores y se documentó este impacto (`0,5 %` del mes) como limitación conocida.
24. **¿Qué significa el efecto composición detectado en Q6 con `payment_type = 0`?**
    - Entre 2024-03 y 2025-03 la tarifa media total cayó `-3,47 %` (`USD 19,06 → 18,40`), pero la tarifa media de viajes con `fare_amount > 0` casi no varió (`USD 19,15 → 19,16`). Lo que cambió fue la proporción de viajes con tarifa `<= 0` (`0,43 % → 3,26 %`), concentrados casi todos en `payment_type = 0` (*Flex Fare trip*, que creció de `379.418` a `854.427` viajes y también coincide con los nulos estructurales de `passenger_count`).
25. **¿Por qué `R07` y `R08` aparecen en 0 en 2024-03 y 2025-03 pero siguen implementadas?**
    - Porque un control preventivo no se elimina solo porque el lote actual venga limpio. Su funcionamiento está demostrado con datos sintéticos en `tests/test_calidad.py` y `tests/test_calidad_mes.py`.
26. **¿Cómo se relaciona `src/transformacion.py` con `dbt_project/models/intermediate/int_trips_enriched.sql`?**
    - `int_trips_enriched.sql` replica exactamente las mismas fórmulas derivadas (`trip_duration_min`, `avg_speed_mph`, `pickup_date`, `pickup_hour`, `pickup_dow`, `tip_pct`) y los `LEFT JOIN` a zonas (`stg_taxi_zones`) directamente sobre `intermediate.trips_validated` en DuckDB, permitiendo que `dbt` gobierne la construcción de `marts` en SQL manteniendo equivalencia con Fase 1.
27. **¿Por qué el dashboard Streamlit (`dashboard/app.py`) apuntaba originalmente a `taxi.duckdb` y cómo se resolvió en Fase 4?**
    - `taxi.duckdb` quedó en formato DuckDB v69 (creado en Fase 1 con DuckDB 2.0.0), ilegible para `duckdb==1.5.6` requerido por `dlt` y `dbt-duckdb`. Como `marts` en `nyc_taxi_dlt.duckdb` expone exactamente las mismas 4 relaciones y columnas (`fact_trips`, `dim_zone`, `dim_date`, `agg_zone_hour`), se parametrizó `dashboard/app.py` con `DUCKDB_PATH` (default `data/warehouse/nyc_taxi_dlt.duckdb`) y `DUCKDB_SCHEMA` (default `marts`, vía `SET search_path = 'marts,main'`), manteniendo intacto `taxi.duckdb` y las SQL `q1..q6`.
28. **¿Por qué no se agregó un contenedor Docker exclusivo para Streamlit?**
    - Con `2,75 GB` libres en disco y `~0,70–0,77 GB` de RAM disponible en reposo, construir una imagen con `streamlit`, `plotly` y `pandas` sumaría cientos de MB en disco más caché de build, poniendo en riesgo el espacio de spill de DuckDB (`data/tmp/duckdb`). Como `.venv` ya tiene `streamlit` y `plotly` instalados y `.dockerignore` excluye `dashboard/`, ejecutar `make dashboard` localmente en `READ_ONLY` es la opción segura y reversible.
29. **¿Cómo agregás otro mes al pipeline?**
    - Se agrega el mes en ejecución CLI (`--mes AAAA-MM`), se verifica su esquema, se carga a `staging` con `dlt`, se ejecuta `calidad` e `intermediate` (`--meses ...`), se añaden la fuente/modelo `stg_yellow_trips_AAAA_MM.sql` + `UNION ALL` en `stg_yellow_trips.sql` y en `int_reconciliation_summary.sql`, y se corre `dbt build` + `reconciliacion`. (Ver Ejercicio 10 en Parte F).
30. **¿Cómo agregás una regla de calidad nueva?**
    - Se añade una entrada YAML única en `contracts/quality_rules.yaml` con `id`, `descripcion`, `severidad` (`rechazo` o `advertencia`), `umbral`, `justificacion` y expresión SQL `violacion`; `src/calidad.py` la compila automáticamente en `_case_reglas()`, y luego se agrega su test unitario en `tests/test_calidad.py`.
31. **¿Cómo agregás una métrica Prometheus y un panel en Grafana?**
    - Se agrega la métrica (`GaugeMetricFamily` o `CounterMetricFamily`) en `NycTaxiCollector.collect()` dentro de `src/metrics_exporter.py` (verificando en `/metrics` y `tests/test_fase3.py`), y se añade un nuevo objeto de panel con `datasource.uid: "prometheus"` y consulta PromQL en `monitoring/grafana/dashboards/nyc_taxi_monitoring.json` reiniciando o esperando el recargado de provisioning en Grafana.
32. **¿Cómo probás que el pipeline entero funciona sin romper ni reprocesar datos?**
    - Corriendo `pytest -v` (54 tests unitarios y de integración), `dbt build` (60/60 modelos + tests), `python -m src.reconciliacion --con-marts`, consultando Prometheus/Grafana/Dozzle, y lanzando un fallo controlado en Dagster con `simular_fallo: true` (tarda segundos y prueba detección + hooks + logs sin tocar datos reales).

---

## Parte F — 10 Ejercicios Prácticos de Estudio (Ordenados por Dificultad)

> **Importante:** Estos ejercicios son **únicamente material de práctica y estudio para la defensa**. No están aplicados al repositorio para preservar intactos los datos y resultados entregados.

### Ejercicio 1 — Cambiar un mensaje de log en un asset de Dagster (Muy Fácil)
- **Archivos involucrados:** `orchestration/assets.py`.
- **Conceptos necesarios:** logging estructurado en Dagster (`context.log.info`), `stdout` capturado en Dozzle (`nyc-taxi-dagster`).
- **Pasos:**
  1. Abrir `orchestration/assets.py`, función `raw_ingesta`.
  2. Modificar el mensaje de `context.log.info(...)` agregando un dato útil (por ejemplo `config.mes`).
  3. Reiniciar o usar `./orchestration:/app/orchestration:ro` montado y lanzar una corrida rápida desde Dagster UI (`:3000`).
- **Verificación:** abrir la pestaña *Step Logs* de `raw_ingesta` en Dagster UI y verificar la misma línea en Dozzle (`:8080`, contenedor `nyc-taxi-dagster`).
- **Errores frecuentes:** usar f-strings con variables inexistentes o olvidar recargar definiciones en Dagster UI (*Reload definitions*).

### Ejercicio 2 — Agregar una columna booleana derivada (`es_fin_de_semana`) en la capa curada y dbt (Fácil)
- **Archivos involucrados:** `src/transformacion.py`, `dbt_project/models/intermediate/int_trips_enriched.sql`, `dbt_project/models/marts/fact_trips.sql`.
- **Conceptos necesarios:** expresiones SQL DuckDB (`isodow`), vistas `intermediate`, propagación de columnas hacia `fact_trips`.
- **Pasos:**
  1. En `src/transformacion.py::sql_curada()`, agregar `(isodow(b.tpep_pickup_datetime) >= 6) AS es_fin_de_semana`.
  2. En `dbt_project/models/intermediate/int_trips_enriched.sql`, agregar la misma expresión en el `SELECT`.
  3. Exponer `es_fin_de_semana` en `dbt_project/models/marts/fact_trips.sql`.
- **Verificación:** ejecutar `dbt build --project-dir dbt_project --profiles-dir dbt_project` y consultar `SELECT es_fin_de_semana, COUNT(*) FROM marts.fact_trips GROUP BY 1`.
- **Errores frecuentes:** agregar la columna solo en `src/transformacion.py` y olvidar `dbt` (como `fact_trips` de Fase 2 lee de `intermediate`, dbt no vería columnas añadidas solo en `curated/`).

### Ejercicio 3 — Modificar el umbral de una regla de calidad (Fácil)
- **Archivos involucrados:** `contracts/quality_rules.yaml`, `tests/test_calidad.py`.
- **Conceptos necesarios:** reglas declarativas YAML, impacto en tasa de cuarentena, reconciliación invariante (`leidas = aceptadas + cuarentena`).
- **Pasos:**
  1. Editar `R04` en `contracts/quality_rules.yaml` cambiando `100` por `80` tanto en `umbral` como en `violacion` (`trip_distance > 80`).
  2. Ajustar o añadir un caso borde (`trip_distance = 90.0`) en `tests/test_calidad.py`.
- **Verificación:** correr `pytest tests/test_calidad.py` y comprobar sobre una copia temporal (no sobre datos de entrega) cómo sube cuarentena y baja aceptadas manteniendo `diferencia = 0`.
- **Errores frecuentes:** cambiar el texto `umbral` sin cambiar la expresión SQL `violacion`.

### Ejercicio 4 — Agregar un nuevo test unitario en `pytest` (Fácil–Medio)
- **Archivos involucrados:** `tests/test_fase3.py` (o `tests/test_esquema.py`).
- **Conceptos necesarios:** `pytest`, fixtures `tmp_path` y `monkeypatch`, aislamiento sin tocar datos reales.
- **Pasos:**
  1. Escribir `test_metrics_exporter_respeta_limite_200_run_ids(tmp_path)` en `tests/test_fase3.py`.
  2. Invocar `registrar_ejecucion_metricas(..., run_id=f"r-{i}", metrics_path=tmp_path/"m.json")` 210 veces.
  3. Validar `len(res["recorded_run_ids"]) == 200` y `res["runs_total"] == 210`.
- **Verificación:** `.\.venv\Scripts\python.exe -m pytest tests/test_fase3.py -v`.
- **Errores frecuentes:** olvidar pasar `metrics_path=tmp_path/...`, contaminando el archivo real `reports/pipeline_metrics.json`.

### Ejercicio 5 — Crear una nueva consulta analítica `q7_velocidad_por_hora.sql` e integrarla (Medio)
- **Archivos involucrados:** `sql/q7_velocidad_por_hora.sql`, `docs/interpretaciones.yaml`, `dashboard/app.py`.
- **Conceptos necesarios:** SQL analítico sobre `marts.fact_trips`, agregaciones con `FILTER`, placeholder `{MES}/{ANIO}/{ANIO_ANT}`.
- **Pasos:**
  1. Crear `sql/q7_velocidad_por_hora.sql` con comentario título en la línea 1 (`-- Pregunta 7: ...`) calculando velocidad media (`avg_speed_mph`) y cantidad de viajes por `pickup_hour`.
  2. Agregar entrada `q7` en `docs/interpretaciones.yaml`.
  3. Añadir el selector `"Q7 Velocidad"` y su bloque en `dashboard/app.py::render_tab_preguntas()`.
- **Verificación:** correr `python -m src.preguntas --mes 2025-03 --mes-comparacion 2024-03` (o en un `tmp_path` en tests) y verificar `Q7` en `dashboard/app.py`.
- **Errores frecuentes:** omitir el comentario título en línea 1 (`ejecutar_sql_pregunta` usa línea 1 como título) o usar tablas que no están en `marts`.

### Ejercicio 6 — Modificar un modelo dbt (`agg_zone_hour.sql`) y su contrato `schema.yml` (Medio)
- **Archivos involucrados:** `dbt_project/models/marts/agg_zone_hour.sql`, `dbt_project/models/marts/schema.yml`, `dbt_project/tests/assert_agg_matches_fact.sql`.
- **Conceptos necesarios:** `ref()`, materialización `table` en dbt, tests genéricos y singulares.
- **Pasos:**
  1. En `agg_zone_hour.sql`, sumar columna `sum(tip_amount) AS total_tips`.
  2. En `dbt_project/models/marts/schema.yml`, añadir `total_tips` con test `not_null`.
  3. Ampliar `dbt_project/tests/assert_agg_matches_fact.sql` para comprobar que `ABS(SUM(agg.total_tips) - SUM(fact.tip_amount)) < 0.01`.
- **Verificación:** `dbt build --project-dir dbt_project --profiles-dir dbt_project`.
- **Errores frecuentes:** cambiar nombre de columnas existentes (`trips`, `total_revenue`) rompiendo `src/reconciliacion.py` y `dashboard/app.py`.

### Ejercicio 7 — Agregar un nuevo asset Dagster al DAG (Medio–Alto)
- **Archivos involucrados:** `orchestration/assets.py`, `orchestration/jobs.py`, `tests/test_fase3.py`.
- **Conceptos necesarios:** `@asset(deps=[...])`, ejecución serial `AssetSelection.all()`, metadatos `Output` y hooks.
- **Pasos:**
  1. Crear `@asset(deps=[verificacion_final], group_name="reportes")` llamado `reporte_preguntas` que ejecute un resumen read-only de `sql/q6_variacion.sql` sobre `nyc_taxi_dlt.duckdb`.
  2. Incluir `reporte_preguntas` al final de `ALL_ASSETS` en `orchestration/assets.py`.
  3. Ajustar `registrar_exito_pipeline` en `orchestration/jobs.py` si el último asset deja de ser `verificacion_final`, y actualizar `EXPECTED_ASSETS` en `tests/test_fase3.py`.
- **Verificación:** cargar UI de Dagster (`:3000`) comprobando 10 nodos en el Asset Graph y correr `pytest tests/test_fase3.py`.
- **Errores frecuentes:** olvidar actualizar la condición `if context.op.name != "..."` en `registrar_exito_pipeline`, impidiendo que el hook registre el éxito al finalizar el job.

### Ejercicio 8 — Agregar una nueva métrica en Prometheus Exporter (Medio–Alto)
- **Archivos involucrados:** `src/metrics_exporter.py`, `tests/test_fase3.py`.
- **Conceptos necesarios:** `GaugeMetricFamily`, protocolo OpenMetrics/Prometheus `/metrics`, collector en vivo sin caché obsoleto.
- **Pasos:**
  1. En `NycTaxiCollector.collect()`, calcular la tasa de cuarentena global (%) a partir de `estado["reconciliacion"]["totales"]` (`cuarentena / staging * 100`).
  2. Emitir `GaugeMetricFamily("nyc_taxi_quarantine_ratio_percent", "...", value=ratio)`.
  3. Validar su presencia en `test_metrics_exporter_registro_y_formato_prometheus`.
- **Verificación:** reiniciar `metrics-exporter` (`docker compose up -d --no-build metrics-exporter`), consultar `http://127.0.0.1:8000/metrics` y buscar `nyc_taxi_quarantine_ratio_percent` en `http://127.0.0.1:9090`.
- **Errores frecuentes:** dividir por cero cuando `pipeline_metrics.json` todavía no tiene datos (`staging == 0`), o nombrar mal un `Counter` quitando `_total` implícito.

### Ejercicio 9 — Agregar o modificar un panel en Grafana (Alto)
- **Archivos involucrados:** `monitoring/grafana/dashboards/nyc_taxi_monitoring.json`, `tests/test_fase3.py`.
- **Conceptos necesarios:** PromQL, JSON de dashboard Grafana (`gridPos`, `targets`, `datasource.uid: "prometheus"`), provisioning desde repositorio.
- **Pasos:**
  1. Añadir un panel ID `13` en `panels` de `nyc_taxi_monitoring.json` graficando la métrica del Ejercicio 8 (`expr: nyc_taxi_quarantine_ratio_percent`) con `"unit": "percent"` y `gridPos` debajo de los paneles existentes (`y: 22`).
  2. Ajustar `len(dash["panels"]) == 13` en `tests/test_fase3.py`.
- **Verificación:** `docker compose restart grafana` y abrir `http://127.0.0.1:3001` confirmando que el nuevo panel renderiza datos de Prometheus y sobrevive reinicios.
- **Errores frecuentes:** usar `uid` distinto de `"prometheus"`, superponer coordenadas `gridPos`, o introducir sintaxis JSON inválida (coma final).

### Ejercicio 10 — Incorporar un nuevo mes (por ejemplo `2025-04`) de extremo a extremo (Alto)
- **Archivos involucrados:** `dbt_project/models/sources.yml`, `dbt_project/models/staging/stg_yellow_trips_2025_04.sql` (nuevo), `dbt_project/models/staging/stg_yellow_trips.sql`, `dbt_project/models/intermediate/int_reconciliation_summary.sql`, `orchestration/assets.py`.
- **Conceptos necesarios:** particionado mensual, `UNION ALL BY NAME`, evolución de fuentes dbt, escalabilidad de memoria/disco.
- **Pasos:**
  1. Antes de descargar, verificar que haya espacio libre suficiente en disco (cada mes crudo + validado/cuarentena/curado + staging en DuckDB consume cientos de MB).
  2. Ejecutar `python -m src.ingesta --mes 2025-04`, `esquema`, `carga_dlt --mes 2025-04` y `calidad --mes 2025-04`.
  3. Actualizar `src/intermediate.py` pasando `--meses 2024-03 2025-03 2025-04`.
  4. Registrar `yellow_trips_2025_04` y `trips_validated_2025_04`/`trips_quarantine_2025_04` en `dbt_project/models/sources.yml`; crear `stg_yellow_trips_2025_04.sql`; agregar el `UNION ALL BY NAME` en `stg_yellow_trips.sql` y las filas de `2025-04` en `int_reconciliation_summary.sql`.
- **Verificación:** correr `dbt build`, `python -m src.reconciliacion --meses 2024-03 2025-03 2025-04 --con-marts` y validar cuadre `staging == aceptadas + cuarentena`.
- **Errores frecuentes:** olvidar verificar el espacio en disco antes de bajar/cargar en DuckDB, o no actualizar `staging/schema.yml` (`accepted_values` de `source_month`).
