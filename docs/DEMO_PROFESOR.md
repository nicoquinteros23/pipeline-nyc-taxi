# Guion de Demostración en Vivo para el Profesor (5–10 minutos)

Este guion está diseñado para ejecutarse en **Windows PowerShell** usando el entorno virtual `.venv` y los **5 contenedores Docker** de observabilidad/orquestación en `127.0.0.1`.

---

## 1. Checklist Previo (antes de empezar la defensa)

1. Confirmar que **Docker Desktop** esté iniciado y en estado *Running*.
2. Abrir una terminal PowerShell en la raíz del proyecto (`pipeline-nyc-taxi`).
3. Verificar estado de contenedores, espacio en disco y memoria disponible:

```powershell
docker compose ps
.\.venv\Scripts\python.exe -c "import shutil, psutil; du=shutil.disk_usage('.'); vm=psutil.virtual_memory(); print(f'Disco libre: {du.free/1e9:.2f} GB | RAM libre: {vm.available/1e9:.2f}/{vm.total/1e9:.2f} GB')"
```

Si algún contenedor no está arriba (`Up (healthy)`), levantarlos sin reconstruir imágenes:

```powershell
docker compose up -d dagster-webserver metrics-exporter prometheus grafana dozzle
docker compose ps
```

4. Tener abiertas en el navegador estas **URLs locales verificadas**:
   - **Dagster UI:** `http://127.0.0.1:3000`
   - **Prometheus:** `http://127.0.0.1:9090`
   - **Grafana:** `http://127.0.0.1:3001` (usuario `admin`, contraseña `admin`)
   - **Dozzle (logs):** `http://127.0.0.1:8080`
   - **Dashboard Streamlit (opcional bajo demanda):** iniciar en otra pestaña con `make dashboard` → abrir `http://127.0.0.1:8501` (cerrar con `Ctrl+C` cuando termines de mostrarlo para liberar RAM).

---

## 2. Guion Paso a Paso (Modalidad Rápida: ~7 minutos sin reprocesar 7,7M filas)

> **Idea clave para la defensa:** no hace falta esperar 3–4 minutos reprocesando los 7,73 millones de registros en vivo para demostrar que todo funciona. Podés verificar los datos ya materializados en segundos, lanzar un **fallo controlado** en Dagster (tarda ~5 s) para demostrar orquestación/hooks/logs/alertado en tiempo real, y recorrer Prometheus, Grafana, Dozzle y Streamlit. *(Si el profesor pide ver la corrida completa real, ver Sección 3).*

### Minuto 0:00 – 1:30 | Arquitectura, Salud del Stack y Tests Automatizados
- **En terminal PowerShell:**
  ```powershell
  docker compose ps
  .\.venv\Scripts\python.exe -m src.reconciliacion --con-marts
  .\.venv\Scripts\dbt.exe test --project-dir dbt_project --profiles-dir dbt_project
  ```
- **Qué explicar:**
  - Mostrar los 5 contenedores saludables escuchando solo en `127.0.0.1` (`dagster-webserver`, `metrics-exporter`, `prometheus`, `grafana`, `dozzle`).
  - Señalar que `src/reconciliacion.py` consulta `data/warehouse/nyc_taxi_dlt.duckdb` en `READ_ONLY` y confirma el cuadre exacto:
    - `2024-03`: `3,582,628 = 3,453,562 + 129,066`
    - `2025-03`: `4,145,257 = 3,955,293 + 189,964`
    - `TOTAL`: `7,727,885 = 7,408,855 + 319,030`
    - `MARTS`: `fact_trips = 7,408,855 == agg_zone_hour_trips = 7,408,855 | revenue = 203,263,790.71`.
  - Señalar que `dbt test` pasa **50/50** pruebas sobre esquemas `dbt_staging`, `intermediate` y `marts` (y `pytest` pasa **54/54** tests unitarios/integración).

### Minuto 1:30 – 3:30 | Dagster UI (`http://127.0.0.1:3000`): DAG + Detección de Fallos Controlados
- **Qué abrir:**
  1. **Assets → View global asset lineage:** mostrar los **9 Software-Defined Assets** encadenados desde `raw_ingesta` hasta `verificacion_final`.
  2. **Jobs → `nyc_taxi_pipeline` → Launchpad:** explicar `TaxiPipelineConfig` (`mes: "2025-03"`, `sin_ingesta: true`, `forzar_dlt: false`, `simular_fallo: false`).
  3. **Demostración en vivo sin alterar datos (`~5 s`):** cambiar `simular_fallo: true` en `ops.raw_ingesta.config` y hacer clic en **Launch Run**.
- **Resultado esperado:**
  - El run pasa a estado **`FAILURE`** en `raw_ingesta`; los 8 assets dependientes no se ejecutan (`skipped`).
  - Los datos en `data/` y `marts` permanecen intactos.
  - En los **Step Logs** de `raw_ingesta` figura el `RuntimeError` controlado y el `@failure_hook` registra el fallo en `reports/pipeline_metrics.json` (con deduplicación por `run_id` y `FileLock`).

### Minuto 3:30 – 4:30 | Dozzle (`http://127.0.0.1:8080`): Logs en Tiempo Real
- **Qué abrir:** elegir el contenedor **`nyc-taxi-dagster`** en el menú izquierdo de Dozzle.
- **Qué explicar:**
  - Mostrar los logs reales del job ejecutado recién en Dagster.
  - Señalar la arquitectura de seguridad: `docker.sock` montado en solo lectura (`:ro`) y `DOZZLE_ENABLE_ACTIONS=false` (no hay shell remota ni reinicio destructivo desde el navegador).

### Minuto 4:30 – 6:30 | Prometheus (`http://127.0.0.1:9090`) y Grafana (`http://127.0.0.1:3001`)
- **En Prometheus (`http://127.0.0.1:9090`):**
  1. Ir a **Status → Targets:** confirmar `prometheus` y `nyc_taxi_exporter` en **`UP`** (`scrape_interval: 5s`).
  2. En **Graph**, ejecutar consultas rápidas:
     - `nyc_taxi_reconciliation_diff_rows` (resultado esperado: `0`).
     - `nyc_taxi_pipeline_runs_total` y `nyc_taxi_pipeline_failures_total` (se observa el incremento inmediato por el fallo controlado recién disparado).
     - `nyc_taxi_marts_total_revenue_usd` (`203263790.71`).
- **En Grafana (`http://127.0.0.1:3001`, login `admin` / `admin`):**
  - Abrir **Dashboards → NYC Taxi → `NYC Taxi — Pipeline Monitoring`**.
  - Recorrer los **12 paneles**:
    - Fila superior: *Estado última corrida* (se habrá actualizado a rojo `FALLO` tras la demo controlada — o verde `OK` tras corrida exitosa), *Ejecuciones (Totales / OK / Fallos)*, *Duración última corrida*, *Diferencia Reconciliación (`0`)*, *Revenue Total Marts (`$203M`)* y *Espacio libre en disco*.
    - Fila media: *Disponibilidad de Servicios* (scrape + HTTP Dagster/Dozzle) y *Filas verificadas por Capa y Mes*.
    - Fila inferior: *Consumo de CPU (%)*, *Uso de Memoria RAM*, *Memoria RSS de Contenedores Instrumentados* (`prometheus` y `nyc_taxi_exporter`), y *Duración por Etapa (última corrida)*.

### Minuto 6:30 – 8:30 | Dashboard Analítico Streamlit (`http://127.0.0.1:8501`)
- **En una terminal PowerShell:**
  ```powershell
  .\.venv\Scripts\python.exe -m streamlit run dashboard/app.py --server.address 127.0.0.1 --server.port 8501
  ```
- **Qué mostrar en las 4 pestañas:**
  1. **Ingesta y Esquema:** métricas de última ejecución (`ejecucion_fase2_2025-03.json`), log append-only (`ingestion_log.csv` con eventos `omitido` por SHA-256), y contraste de esquema `2025-03` vs `2024-03` (`cbd_congestion_fee = NULL` en 2024).
  2. **Calidad, Reglas y Cuarentena:** cuadre de `3,58M` / `4,14M` filas, muestra bajo demanda con `LIMIT` sobre Parquet de cuarentena y advertencias `R06` / `R10` sobre `fact_trips`.
  3. **Modelo Estrella:** conteos en vivo de `fact_trips` (`7.408.855`), `dim_zone` (`265`), `dim_date` (`396`), `agg_zone_hour` (`205.179`), checks de integridad (`0` huérfanos, `trip_id` único) y explorador por zona/hora (`JFK Airport`) que consulta solo la tabla agregada `agg_zone_hour`.
  4. **Preguntas Q1–Q6:** mostrar Q3 (cargo de congestión `74,25 %` global, `99,83 %` en SoHo) y Q6 (efecto composición por `payment_type = 0`).

---

## 3. Opcional: Si el Profesor Pide Ver una Corrida Real Completa (3–4 minutos)
- En Dagster UI → **Jobs → `nyc_taxi_pipeline` → Launchpad**, dejar la configuración por defecto (`simular_fallo: false`, `sin_ingesta: true`, `forzar_dlt: false`) y lanzar el run.
- Durante la ejecución (~200–240 s):
  - Mostrar en **Dagster UI** los 9 pasos avanzando en serie.
  - Mostrar en **Dozzle** (`nyc-taxi-dagster`) la salida en vivo de `calidad`, `reconciliacion` y `dbt build`.
- Al finalizar (`SUCCESS`):
  - En **Grafana** (recarga automática cada `5 s`): el estado vuelve a verde **`OK`**, `Success` suma `+1`, y el panel **Duración por Etapa** muestra los tiempos exactos de los 9 pasos de Dagster.
  - En **Reconciliación** (`python -m src.reconciliacion --con-marts`): los conteos finales son **idénticos** (prueba práctica de idempotencia).

---

## 4. Plan de Recuperación ante Fallos Comunes Durante la Demo

| Síntoma | Causa probable | Acción de recuperación inmediata |
|---|---|---|
| Un servicio en `:3000`, `:9090`, `:3001` u `:8080` no responde | Contenedor detenido o reiniciando | `docker compose ps` y `docker compose up -d <servicio>` |
| Run en Dagster queda en `QUEUED` | Falta montaje de `orchestration/dagster.yaml` | Verificar `docker compose config` y recrear con `docker compose up -d --no-build dagster-webserver` |
| Error `Conflicting lock` en DuckDB | Se intentó correr CLI (`make fase2` o `dbt`) mientras Dagster ejecutaba un run | Esperar a que termine el run en curso (el dashboard Streamlit usa `read_only=True` y no bloquea) |
| Grafana muestra "No data" recién iniciado | Prometheus aún no completó el primer scrape (`5 s`) | Esperar 5–10 segundos y presionar el botón de refresco en Grafana |
| Streamlit arroja falta de RAM o lentitud junto a corrida pesada | Ambos consumen RAM en los 8 GB totales | Cerrar Streamlit (`Ctrl+C`) antes de lanzar una corrida completa en Dagster; volver a abrirlo al terminar |
| Error de puerto ocupado (`Bind for 127.0.0.1:8501 failed`) | Otra instancia previa de Streamlit sigue viva | `Get-NetTCPConnection -LocalPort 8501` para ubicar el PID y detenerlo, o usar `--server.port 8502` |
