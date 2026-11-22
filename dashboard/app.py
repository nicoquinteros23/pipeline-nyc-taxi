"""Dashboard interactivo (Streamlit + Plotly) para la defensa del pipeline NYC Taxi.

Se alimenta exclusivamente mediante consultas bajo demanda contra DuckDB (READ_ONLY),
los contratos YAML, los reportes JSON/CSV y muestras acotadas (LIMIT) de cuarentena,
sin cargar el dataset completo en memoria.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yaml

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
WAREHOUSE = DATA_DIR / "warehouse" / "taxi.duckdb"
LOG_INGESTA = DATA_DIR / "control" / "ingestion_log.csv"
REPORTS_DIR = ROOT / "reports"
CONTRACTS_DIR = ROOT / "contracts"
SQL_DIR = ROOT / "sql"
INTERPRETACIONES_PATH = ROOT / "docs" / "interpretaciones.yaml"

COLOR_ACTUAL = "#F59E0B"   # amarillo taxi NYC
COLOR_ANTERIOR = "#64748B" # gris pizarra para año comparación
COLOR_ROJO = "#EF4444"
COLOR_VERDE = "#10B981"

st.set_page_config(
    page_title="Pipeline NYC Taxi — Panel de Defensa",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Utilidades de acceso a datos (siempre bajo demanda y sin bloquear taxi.duckdb)
# ---------------------------------------------------------------------------

def _fmt_int(n: int | float | None) -> str:
    if n is None or pd.isna(n):
        return "—"
    return f"{int(round(n)):,}".replace(",", ".")


def _fmt_float(n: float | None, dec: int = 2) -> str:
    if n is None or pd.isna(n):
        return "—"
    return f"{n:,.{dec}f}".replace(",", "_").replace(".", ",").replace("_", ".")


@st.cache_data(show_spinner=False)
def query_warehouse(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """Ejecuta una consulta agregada o acotada sobre taxi.duckdb en modo READ_ONLY."""
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    try:
        con.execute("SET memory_limit = '2GB'")
        cur = con.execute(sql, list(params)) if params is not None else con.execute(sql)
        return cur.df()
    finally:
        con.close()


@st.cache_data(show_spinner=False)
def query_parquet(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """Ejecuta una consulta acotada sobre archivos Parquet (p. ej. cuarentena)."""
    con = duckdb.connect()
    try:
        con.execute("SET memory_limit = '2GB'")
        cur = con.execute(sql, list(params)) if params is not None else con.execute(sql)
        return cur.df()
    finally:
        con.close()


@st.cache_data(show_spinner=False)
def cargar_yaml(ruta: str):
    p = Path(ruta)
    if not p.exists():
        return None
    return yaml.safe_load(p.read_text(encoding="utf-8"))


@st.cache_data(show_spinner=False)
def cargar_json(ruta: str):
    p = Path(ruta)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


@st.cache_data(show_spinner=False)
def cargar_log_ingesta() -> pd.DataFrame:
    if not LOG_INGESTA.exists():
        return pd.DataFrame()
    df = pd.read_csv(LOG_INGESTA)
    if "timestamp_utc" in df.columns:
        df = df.sort_values("timestamp_utc", ascending=False).reset_index(drop=True)
    return df


@st.cache_data(show_spinner=False)
def periodos_disponibles() -> list[tuple[int, int]]:
    """Lista de (year, month) presentes en fact_trips, ordenados del más reciente al más antiguo."""
    df = query_warehouse("SELECT DISTINCT year, month FROM fact_trips ORDER BY year DESC, month DESC")
    return [(int(r.year), int(r.month)) for r in df.itertuples(index=False)]


def ejecutar_sql_pregunta(archivo_sql: Path, anio: int, anio_ant: int, mes: int) -> tuple[str, str, pd.DataFrame]:
    raw_sql = archivo_sql.read_text(encoding="utf-8")
    sql = (
        raw_sql.replace("{ANIO}", str(anio))
        .replace("{ANIO_ANT}", str(anio_ant))
        .replace("{MES}", str(mes))
    )
    titulo = sql.splitlines()[0].lstrip("- ").strip()
    df = query_warehouse(sql)
    return titulo, sql.strip(), df


# ---------------------------------------------------------------------------
# Pestaña 1: Ingesta y Esquema
# ---------------------------------------------------------------------------

def render_tab_ingesta_esquema(mes_str: str, mes_cmp_str: str):
    st.subheader("Ejecución del pipeline, ingesta idempotente y contrato de esquema")

    # 1. Métricas de última corrida
    ejec = cargar_json(str(REPORTS_DIR / f"ejecucion_{mes_str}.json"))
    if ejec:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tiempo total (última corrida)", f"{_fmt_float(ejec['tiempo_total_s'], 1)} s")
        c2.metric("Pico de RAM del proceso", f"{_fmt_float(ejec['pico_ram_gb'], 2)} GB", "Límite examen: 8 GB")
        c3.metric("Período ejecutado", f"{ejec['mes']} vs {ejec['mes_comparacion']}")
        c4.metric("Fecha corrida (UTC)", ejec["fecha_utc"][:19].replace("T", " "))

        tiempos = ejec.get("tiempos_por_etapa_s", {})
        if tiempos:
            df_t = pd.DataFrame(
                {"etapa": list(tiempos.keys()), "segundos": list(tiempos.values())}
            )
            fig_t = px.bar(
                df_t,
                x="segundos",
                y="etapa",
                orientation="h",
                text="segundos",
                title="Tiempo por etapa del pipeline (segundos)",
                color_discrete_sequence=[COLOR_ACTUAL],
            )
            fig_t.update_traces(texttemplate="%{text:.1f} s", textposition="outside")
            fig_t.update_layout(
                yaxis={"categoryorder": "array", "categoryarray": list(reversed(df_t["etapa"].tolist()))},
                xaxis_title="Segundos",
                yaxis_title="",
                height=290,
                margin=dict(l=10, r=40, t=40, b=10),
            )
            st.plotly_chart(fig_t)
    else:
        st.info(f"No se encontró `reports/ejecucion_{mes_str}.json`. Ejecutá `make all MES={mes_str}`.")

    st.divider()

    # 2. Log de ingesta
    st.markdown("### Log de control de ingesta (`data/control/ingestion_log.csv`)")
    st.caption(
        "El log es append-only (solo agrega filas). La idempotencia evita volver a descargar cuando "
        "el archivo existe y su SHA-256 coincide con el último registro `ok`. Ante fallas transitorias "
        "hace hasta 3 reintentos con espera creciente (2 s, 4 s, 8 s) y escribe primero en `.part`."
    )
    df_log = cargar_log_ingesta()
    if df_log.empty:
        st.warning("No se encontró `data/control/ingestion_log.csv`.")
    else:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Eventos registrados", _fmt_int(len(df_log)))
        k2.metric("Descargas efectivas", _fmt_int((df_log["accion"] == "descargado").sum()))
        k3.metric("Omitidos por hash (idempotencia)", _fmt_int((df_log["accion"] == "omitido").sum()))
        k4.metric("Errores registrados", _fmt_int((df_log["estado"] == "error").sum()))

        col_f1, col_f2 = st.columns(2)
        archivos = ["Todos"] + sorted(df_log["archivo"].dropna().unique().tolist())
        sel_arch = col_f1.selectbox("Filtrar por archivo", archivos, key="log_arch")
        estados = ["Todos"] + sorted(df_log["estado"].dropna().unique().tolist())
        sel_est = col_f2.selectbox("Filtrar por estado", estados, key="log_est")

        vista = df_log.copy()
        if sel_arch != "Todos":
            vista = vista[vista["archivo"] == sel_arch]
        if sel_est != "Todos":
            vista = vista[vista["estado"] == sel_est]

        if "sha256" in vista.columns:
            vista["sha256_corto"] = vista["sha256"].fillna("").astype(str).str.slice(0, 14) + "…"
            vista.loc[vista["sha256"].isna() | (vista["sha256"] == ""), "sha256_corto"] = ""

        cols_ver = [
            c for c in ["timestamp_utc", "archivo", "estado", "accion", "filas", "bytes", "sha256_corto", "detalle"]
            if c in vista.columns
        ]
        st.dataframe(vista[cols_ver], hide_index=True)

    st.divider()

    # 3. Validación de esquema y evolución entre años
    st.markdown("### Contrato de esquema y evolución entre años (`contracts/yellow_schema.yaml`)")
    sch_act = cargar_json(str(REPORTS_DIR / f"schema_{mes_str}.json"))
    sch_ant = cargar_json(str(REPORTS_DIR / f"schema_{mes_cmp_str}.json"))

    col_s1, col_s2 = st.columns(2)
    for col, m_label, sch in [(col_s1, mes_str, sch_act), (col_s2, mes_cmp_str, sch_ant)]:
        with col:
            st.markdown(f"#### Mes `{m_label}`")
            if not sch:
                st.info(f"Sin reporte `reports/schema_{m_label}.json`.")
                continue
            estado = sch.get("estado", "desconocido")
            if estado == "ok":
                st.success(f"Estado: **{estado.upper()}** — contrato v{sch.get('version_contrato')}")
            elif estado == "advertencia":
                st.warning(f"Estado: **{estado.upper()}** — contrato v{sch.get('version_contrato')}")
            else:
                st.error(f"Estado: **{estado.upper()}** — contrato v{sch.get('version_contrato')}")

            st.write(f"- **Columnas faltantes:** `{sch.get('faltantes') or 'ninguna'}`")
            st.write(f"- **Columnas nuevas:** `{sch.get('nuevas') or 'ninguna'}`")
            st.write(f"- **Tipos distintos:** `{sch.get('tipos_distintos') or 'ninguno'}`")
            for adv in sch.get("advertencias", []):
                st.caption(f"⚠️ {adv}")

    st.info(
        "**Decisiones clave de esquema:**\n"
        "- **`cbd_congestion_fee` en 2024 = `NULL` (no `0`):** el cargo de congestión empezó a regir en 2025; "
        "`NULL` expresa *no existía / no aplica*, mientras que `0` falsearía que existía y valió cero.\n"
        "- **Columna nueva fuera del contrato:** se advierte en `reports/schema_*.json`, no se propaga a la capa curada "
        "y el archivo raw queda intacto (incorporarla exige versionar el contrato YAML)."
    )

    contrato = cargar_yaml(str(CONTRACTS_DIR / "yellow_schema.yaml"))
    if contrato:
        with st.expander("Ver contrato completo (`contracts/yellow_schema.yaml` — 20 columnas y política)"):
            st.dataframe(pd.DataFrame(contrato["columns"]), hide_index=True)
            if "policy" in contrato:
                pol_df = pd.DataFrame(
                    [{"caso": k, "accion": v} for k, v in contrato["policy"].items()]
                )
                st.markdown("**Política declarada:**")
                st.dataframe(pol_df, hide_index=True)


# ---------------------------------------------------------------------------
# Pestaña 2: Calidad, Reglas y Cuarentena
# ---------------------------------------------------------------------------

def render_tab_calidad(anio: int, anio_ant: int, mes_num: int, mes_str: str, mes_cmp_str: str):
    st.subheader("Calidad de datos, reglas explícitas, cuarentena y reconciliación")

    rep_act = cargar_json(str(REPORTS_DIR / f"calidad_{mes_str}.json"))
    rep_ant = cargar_json(str(REPORTS_DIR / f"calidad_{mes_cmp_str}.json"))
    reglas = cargar_yaml(str(CONTRACTS_DIR / "quality_rules.yaml")) or []
    reglas_por_id = {r["id"]: r for r in reglas}

    if not rep_act and not rep_ant:
        st.warning("No se encontraron reportes de calidad en `reports/calidad_*.json`.")
        return

    # 1. Reconciliación lado a lado
    st.markdown("### Reconciliación de conteos (`filas leídas = aceptadas + cuarentena`)")
    col_r1, col_r2 = st.columns(2)
    for col, label, rep in [(col_r1, mes_str, rep_act), (col_r2, mes_cmp_str, rep_ant)]:
        with col:
            st.markdown(f"#### {label}")
            if not rep:
                st.info(f"Sin reporte para {label}.")
                continue
            m1, m2, m3 = st.columns(3)
            m1.metric("Leídas (raw)", _fmt_int(rep["filas_leidas"]))
            m2.metric("Aceptadas", _fmt_int(rep["aceptadas"]))
            m3.metric("Cuarentena", _fmt_int(rep["cuarentena"]), f"{_fmt_float(rep['pct_cuarentena'], 2)} %")
            cierra = rep["filas_leidas"] == rep["aceptadas"] + rep["cuarentena"]
            if cierra and rep.get("reconciliacion") == "ok":
                st.success(
                    f"Reconciliación OK: {_fmt_int(rep['aceptadas'])} + {_fmt_int(rep['cuarentena'])} = "
                    f"{_fmt_int(rep['filas_leidas'])} · Aceptadas con advertencia: {_fmt_int(rep['aceptadas_con_advertencia'])}"
                )
            else:
                st.error(f"Reconciliación: {rep.get('reconciliacion')}")

    st.divider()

    # 2. Gráfico comparativo de reglas (año actual vs anterior)
    st.markdown(f"### Impacto por regla: `{mes_str}` vs `{mes_cmp_str}`")
    col_ctrl1, col_ctrl2 = st.columns([3, 1])
    filtro_sev = col_ctrl1.radio(
        "Mostrar reglas",
        ["Rechazo (van a cuarentena)", "Advertencia (se marcan en flags)", "Todas (R01–R11)"],
        horizontal=True,
    )
    escala_log = col_ctrl2.checkbox("Escala logarítmica", value=False)

    filas_graf = []
    for r in reglas:
        rid = r["id"]
        sev = r["severidad"]
        if filtro_sev.startswith("Rechazo") and sev != "rechazo":
            continue
        if filtro_sev.startswith("Advertencia") and sev != "advertencia":
            continue
        clave = "rechazo_por_regla" if sev == "rechazo" else "advertencias_por_regla"
        c_act = rep_act[clave].get(rid, 0) if rep_act else 0
        c_ant = rep_ant[clave].get(rid, 0) if rep_ant else 0
        etiqueta = f"{rid}: {r['descripcion']}"
        filas_graf.append({"regla": etiqueta, "id": rid, "severidad": sev, "mes": mes_cmp_str, "filas": c_ant})
        filas_graf.append({"regla": etiqueta, "id": rid, "severidad": sev, "mes": mes_str, "filas": c_act})

    df_graf = pd.DataFrame(filas_graf)
    if not df_graf.empty:
        fig = px.bar(
            df_graf,
            x="id",
            y="filas",
            color="mes",
            barmode="group",
            hover_data=["regla", "severidad"],
            color_discrete_map={mes_str: COLOR_ACTUAL, mes_cmp_str: COLOR_ANTERIOR},
            log_y=escala_log,
            title=f"Filas que violan cada regla ({mes_str} vs {mes_cmp_str})",
        )
        fig.update_layout(
            xaxis_title="Regla",
            yaxis_title="Filas" + (" (escala log)" if escala_log else ""),
            legend_title="Período",
            height=380,
            margin=dict(l=10, r=10, t=45, b=10),
        )
        st.plotly_chart(fig)
        st.caption(
            "Nota: una misma fila puede violar más de una regla de rechazo, por eso la suma de rechazos "
            "por regla supera a la cantidad única de filas en cuarentena. Las advertencias se reportan sobre filas aceptadas."
        )

    st.divider()

    # 3. Explicador interactivo de reglas + inspección en vivo (LIMIT)
    st.markdown("### Explicador de reglas y muestra real en vivo (consulta bajo demanda)")
    opciones = [f"{r['id']} — {r['descripcion']} ({r['severidad']})" for r in reglas]
    sel_opt = st.selectbox("Elegí una regla para ver el porqué, el SQL y ejemplos reales", opciones)
    rid_sel = sel_opt.split(" — ")[0]
    regla = reglas_por_id[rid_sel]
    sev_sel = regla["severidad"]
    clave_dic = "rechazo_por_regla" if sev_sel == "rechazo" else "advertencias_por_regla"
    cnt_act = rep_act[clave_dic].get(rid_sel, 0) if rep_act else 0
    cnt_ant = rep_ant[clave_dic].get(rid_sel, 0) if rep_ant else 0

    f1, f2, f3, f4 = st.columns(4)
    f1.metric("Severidad", sev_sel.upper())
    f2.metric("Umbral", regla["umbral"])
    f3.metric(f"Filas en {mes_str}", _fmt_int(cnt_act))
    f4.metric(f"Filas en {mes_cmp_str}", _fmt_int(cnt_ant))

    if sev_sel == "rechazo":
        st.error(f"**Por qué se rechaza ({rid_sel}):** {regla['justificacion']}")
    else:
        st.warning(f"**Por qué es advertencia y NO rechazo ({rid_sel}):** {regla['justificacion']}")

    with st.expander("Ver expresión SQL de evaluación definida en `contracts/quality_rules.yaml`"):
        st.code(regla["violacion"], language="sql")

    # Muestra en vivo acotada con LIMIT
    c_m1, c_m2 = st.columns([2, 1])
    mes_inspeccion = c_m1.radio(
        "Período a inspeccionar",
        [mes_str, mes_cmp_str],
        horizontal=True,
        key="cal_mes_insp",
    )
    limite = c_m2.select_slider("Cantidad de filas de ejemplo (LIMIT)", options=[5, 10, 20, 50], value=10)
    y_insp, m_insp = (int(x) for x in mes_inspeccion.split("-"))

    if sev_sel == "rechazo":
        q_path = DATA_DIR / "quarantine" / f"year={y_insp}" / f"month={m_insp}" / "data.parquet"
        if not q_path.exists():
            st.info(f"No existe el archivo de cuarentena `{q_path.relative_to(ROOT)}`.")
        else:
            sql_muestra = (
                "SELECT file_row_number, reglas_violadas, vendor_id, "
                "tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, "
                "fare_amount, total_amount, pu_location_id, do_location_id, "
                "passenger_count, payment_type, source_file\n"
                f"FROM read_parquet('{q_path.as_posix()}')\n"
                "WHERE list_contains(reglas_violadas, ?)\n"
                "LIMIT ?"
            )
            df_muestra = query_parquet(sql_muestra, (rid_sel, limite))
            if df_muestra.empty:
                st.info(
                    f"La regla **{rid_sel}** tiene **0 casos** en `{mes_inspeccion}`. "
                    "Su funcionamiento está verificado con datos sintéticos en `tests/test_calidad.py` y `tests/test_calidad_mes.py`."
                )
            else:
                st.caption(f"Mostrando {len(df_muestra)} filas reales de cuarentena que violaron `{rid_sel}` en `{mes_inspeccion}`:")
                st.dataframe(df_muestra, hide_index=True)
            with st.expander("Ver SQL ejecutado sobre cuarentena"):
                st.code(sql_muestra.replace("?", f"'{rid_sel}'", 1).replace("?", str(limite), 1), language="sql")
    else:
        sql_muestra = (
            "SELECT trip_id, flags, pickup_date, pickup_hour, payment_type, "
            "pu_location_id, do_location_id, trip_distance, fare_amount, total_amount\n"
            "FROM fact_trips\n"
            "WHERE year = ? AND month = ? AND list_contains(flags, ?)\n"
            "LIMIT ?"
        )
        df_muestra = query_warehouse(sql_muestra, (y_insp, m_insp, rid_sel, limite))
        if df_muestra.empty:
            st.info(f"No se encontraron filas aceptadas con flag `{rid_sel}` en `{mes_inspeccion}`.")
        else:
            st.caption(
                f"Mostrando {len(df_muestra)} filas **aceptadas** en `fact_trips` marcadas con flag `{rid_sel}` "
                f"en `{mes_inspeccion}` (se conservan sin descartar):"
            )
            st.dataframe(df_muestra, hide_index=True)
        with st.expander("Ver SQL ejecutado sobre `fact_trips`"):
            st.code(
                sql_muestra.replace("?", str(y_insp), 1)
                .replace("?", str(m_insp), 1)
                .replace("?", f"'{rid_sel}'", 1)
                .replace("?", str(limite), 1),
                language="sql",
            )

    with st.expander("Ver catálogo completo de las 11 reglas (`contracts/quality_rules.yaml`)"):
        df_cat = pd.DataFrame(
            [
                {
                    "ID": r["id"],
                    "Regla": r["descripcion"],
                    "Severidad": r["severidad"],
                    "Umbral": r["umbral"],
                    f"Filas {mes_str}": (rep_act["rechazo_por_regla"] if r["severidad"] == "rechazo" else rep_act["advertencias_por_regla"]).get(r["id"], 0) if rep_act else None,
                    f"Filas {mes_cmp_str}": (rep_ant["rechazo_por_regla"] if r["severidad"] == "rechazo" else rep_ant["advertencias_por_regla"]).get(r["id"], 0) if rep_ant else None,
                    "Justificación": r["justificacion"],
                }
                for r in reglas
            ]
        )
        st.dataframe(df_cat, hide_index=True)


# ---------------------------------------------------------------------------
# Pestaña 3: Modelo Estrella y Explorador Agregado
# ---------------------------------------------------------------------------

def render_tab_modelo(anio: int, anio_ant: int, mes_num: int, mes_str: str, mes_cmp_str: str):
    st.subheader("Modelo estrella (`data/warehouse/taxi.duckdb`) y verificación de integridad")

    # 1. Conteos vivos y granularidades
    conteos = {}
    for tabla in ("fact_trips", "dim_zone", "dim_date", "agg_zone_hour"):
        conteos[tabla] = int(query_warehouse(f"SELECT count(*) AS n FROM {tabla}")["n"].iloc[0])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("fact_trips (VIEW)", _fmt_int(conteos["fact_trips"]))
    c2.metric("dim_zone (TABLE)", _fmt_int(conteos["dim_zone"]))
    c3.metric("dim_date (TABLE)", _fmt_int(conteos["dim_date"]))
    c4.metric("agg_zone_hour (TABLE)", _fmt_int(conteos["agg_zone_hour"]))

    df_gran = pd.DataFrame(
        [
            {"Tabla": "fact_trips", "Tipo": "VIEW sobre Parquet Hive", "Granularidad (1 fila = ...)": "Un viaje aceptado de taxi amarillo", "Filas actuales": conteos["fact_trips"]},
            {"Tabla": "dim_zone", "Tipo": "TABLE (rol origen y destino)", "Granularidad (1 fila = ...)": "Una zona oficial de taxi (`location_id`)", "Filas actuales": conteos["dim_zone"]},
            {"Tabla": "dim_date", "Tipo": "TABLE (calendario continuo)", "Granularidad (1 fila = ...)": "Un día calendario", "Filas actuales": conteos["dim_date"]},
            {"Tabla": "agg_zone_hour", "Tipo": "TABLE pre-agregada", "Granularidad (1 fila = ...)": "Una combinación de zona origen, fecha y hora", "Filas actuales": conteos["agg_zone_hour"]},
        ]
    )
    st.dataframe(df_gran, hide_index=True)

    # 2. Chequeos de integridad en vivo
    st.markdown("### Verificación de integridad referencial y cuadre en vivo")
    check_pk = bool(
        query_warehouse("SELECT count(*) = count(DISTINCT trip_id) AS ok FROM fact_trips")["ok"].iloc[0]
    )
    check_huerf = int(
        query_warehouse(
            """
            SELECT count(*) AS n
            FROM fact_trips f
            LEFT JOIN dim_zone o ON o.location_id = f.pu_location_id
            LEFT JOIN dim_zone d ON d.location_id = f.do_location_id
            LEFT JOIN dim_date t ON t.date_key = f.pickup_date_key
            WHERE o.location_id IS NULL OR d.location_id IS NULL OR t.date_key IS NULL
            """
        )["n"].iloc[0]
    )
    agg_check = query_warehouse(
        """
        SELECT
            (SELECT sum(trips) FROM agg_zone_hour) AS agg_trips,
            (SELECT count(*) FROM fact_trips) AS fact_trips,
            (SELECT sum(total_revenue) FROM agg_zone_hour) AS agg_rev,
            (SELECT sum(total_amount) FROM fact_trips) AS fact_rev
        """
    ).iloc[0]
    check_agg = (
        int(agg_check["agg_trips"]) == int(agg_check["fact_trips"])
        and abs(float(agg_check["agg_rev"]) - float(agg_check["fact_rev"])) < 1e-2
    )

    i1, i2, i3 = st.columns(3)
    if check_pk:
        i1.success("✅ `trip_id` único (sin duplicados)")
    else:
        i1.error("❌ `trip_id` tiene duplicados")
    if check_huerf == 0:
        i2.success("✅ 0 claves huérfanas (`dim_zone` y `dim_date`)")
    else:
        i2.error(f"❌ {check_huerf} claves huérfanas")
    if check_agg:
        i3.success("✅ `agg_zone_hour` cierra exacto con `fact_trips`")
    else:
        i3.error("❌ Descuadre entre `agg_zone_hour` y `fact_trips`")

    st.divider()

    # 3. Explorador interactivo sobre agg_zone_hour (bajo demanda: máx 48 filas)
    st.markdown(f"### Explorador por zona y hora sobre `agg_zone_hour` (`{mes_str}` vs `{mes_cmp_str}`)")
    st.caption(
        "Esta vista consulta **únicamente la tabla agregada `agg_zone_hour`** (205k filas pre-agregadas en lugar "
        "de escanear 7,4M de viajes en `fact_trips`), devolviendo como máximo 48 filas (24 horas × 2 años)."
    )

    df_zonas = query_warehouse(
        "SELECT location_id, zone || ' (' || borough || ')' AS etiqueta FROM dim_zone ORDER BY zone, location_id"
    )
    mapa_zonas = {"Todas las zonas": None}
    for row in df_zonas.itertuples(index=False):
        mapa_zonas[row.etiqueta] = int(row.location_id)

    sel_zona_lbl = st.selectbox("Elegí una zona de origen", list(mapa_zonas.keys()))
    sel_zona_id = mapa_zonas[sel_zona_lbl]

    if sel_zona_id is None:
        sql_agg = """
SELECT year(pickup_date) AS anio,
       pickup_hour AS hora,
       sum(trips) AS viajes,
       round(sum(total_revenue) / sum(trips), 2) AS ingreso_promedio,
       round(sum(avg_duration_min * trips) / sum(trips), 1) AS duracion_media_min
FROM agg_zone_hour
WHERE month(pickup_date) = ? AND year(pickup_date) IN (?, ?)
GROUP BY year(pickup_date), pickup_hour
ORDER BY anio, hora
""".strip()
        df_h = query_warehouse(sql_agg, (mes_num, anio, anio_ant))
    else:
        sql_agg = """
SELECT year(pickup_date) AS anio,
       pickup_hour AS hora,
       sum(trips) AS viajes,
       round(sum(total_revenue) / sum(trips), 2) AS ingreso_promedio,
       round(sum(avg_duration_min * trips) / sum(trips), 1) AS duracion_media_min
FROM agg_zone_hour
WHERE month(pickup_date) = ? AND year(pickup_date) IN (?, ?)
  AND pu_location_id = ?
GROUP BY year(pickup_date), pickup_hour
ORDER BY anio, hora
""".strip()
        df_h = query_warehouse(sql_agg, (mes_num, anio, anio_ant, sel_zona_id))

    if df_h.empty:
        st.info("No hay viajes registrados para esa selección.")
    else:
        df_h["anio_str"] = df_h["anio"].astype(str)
        cmap = {str(anio): COLOR_ACTUAL, str(anio_ant): COLOR_ANTERIOR}
        g1, g2 = st.columns(2)
        with g1:
            fig_v = px.line(
                df_h,
                x="hora",
                y="viajes",
                color="anio_str",
                markers=True,
                hover_data=["duracion_media_min"],
                color_discrete_map=cmap,
                title=f"Viajes por hora — {sel_zona_lbl}",
            )
            fig_v.update_layout(xaxis=dict(dtick=2), xaxis_title="Hora del día", yaxis_title="Viajes", legend_title="Año", height=340)
            st.plotly_chart(fig_v)
        with g2:
            fig_r = px.line(
                df_h,
                x="hora",
                y="ingreso_promedio",
                color="anio_str",
                markers=True,
                hover_data=["viajes", "duracion_media_min"],
                color_discrete_map=cmap,
                title=f"Ingreso promedio por viaje (USD) — {sel_zona_lbl}",
            )
            fig_r.update_layout(xaxis=dict(dtick=2), xaxis_title="Hora del día", yaxis_title="USD / viaje", legend_title="Año", height=340)
            st.plotly_chart(fig_r)

    with st.expander("Ver SQL ejecutado sobre `agg_zone_hour`"):
        st.code(sql_agg, language="sql")


# ---------------------------------------------------------------------------
# Pestaña 4: Preguntas de Negocio (Q1 a Q6)
# ---------------------------------------------------------------------------

def render_tab_preguntas(anio: int, anio_ant: int, mes_num: int, mes_str: str, mes_cmp_str: str):
    st.subheader(f"Preguntas de negocio sobre el modelo estrella ({mes_str} vs {mes_cmp_str})")
    st.caption(
        "Cada bloque ejecuta bajo demanda su archivo `sql/q*.sql` contra `taxi.duckdb` (`READ_ONLY`) "
        f"reemplazando `{{ANIO}}={anio}`, `{{ANIO_ANT}}={anio_ant}` y `{{MES}}={mes_num}`."
    )

    interpretaciones = cargar_yaml(str(INTERPRETACIONES_PATH)) or {}
    usar_interp_guardadas = mes_str == "2025-03" and mes_cmp_str == "2024-03"

    selector = st.radio(
        "Ir a pregunta",
        ["Todas (Q1–Q6)", "Q1 Top zonas", "Q2 Hora ingreso", "Q3 Congestión CBD", "Q4 Propinas", "Q5 Aeropuertos", "Q6 Variación"],
        horizontal=True,
    )

    def mostrar(clave: str) -> bool:
        return selector.startswith("Todas") or selector.upper().startswith(clave.upper())

    def bloque_interp_y_sql(clave: str, archivo: Path, sql: str):
        if usar_interp_guardadas and clave in interpretaciones:
            st.info(f"**Interpretación:** {interpretaciones[clave]}")
        with st.expander(f"Ver consulta SQL ejecutada (`sql/{archivo.name}`)"):
            st.code(sql, language="sql")

    cmap = {str(anio): COLOR_ACTUAL, str(anio_ant): COLOR_ANTERIOR}

    # --- Q1 ---
    if mostrar("q1"):
        p = SQL_DIR / "q1_top_zonas.sql"
        titulo, sql, df = ejecutar_sql_pregunta(p, anio, anio_ant, mes_num)
        st.markdown(f"### {titulo}")
        if not df.empty:
            df_plot = df.iloc[::-1].copy()
            fig = go.Figure()
            fig.add_bar(
                y=df_plot["zona"],
                x=df_plot["viajes_anterior"],
                name=str(anio_ant),
                orientation="h",
                marker_color=COLOR_ANTERIOR,
            )
            fig.add_bar(
                y=df_plot["zona"],
                x=df_plot["viajes_actual"],
                name=str(anio),
                orientation="h",
                marker_color=COLOR_ACTUAL,
                text=[f"{v:+.1f}%" if pd.notna(v) else "" for v in df_plot["variacion_pct"]],
                textposition="outside",
            )
            fig.update_layout(
                barmode="group",
                xaxis_title="Viajes aceptados",
                yaxis_title="",
                legend_title="Año",
                height=440,
                margin=dict(l=10, r=60, t=20, b=10),
            )
            st.plotly_chart(fig)
            st.dataframe(df, hide_index=True)
        bloque_interp_y_sql("q1", p, sql)
        st.divider()

    # --- Q2 ---
    if mostrar("q2"):
        p = SQL_DIR / "q2_hora_mayor_ingreso.sql"
        titulo, sql, df = ejecutar_sql_pregunta(p, anio, anio_ant, mes_num)
        st.markdown(f"### {titulo}")
        if not df.empty:
            df_plot = df.copy()
            df_plot["anio_str"] = df_plot["anio"].astype(str)
            df_plot["hora_lbl"] = df_plot["hora"].astype(str) + " h"
            fig = px.bar(
                df_plot,
                x="hora_lbl",
                y="ingreso_promedio_por_viaje",
                color="anio_str",
                barmode="group",
                text="ingreso_promedio_por_viaje",
                hover_data=["puesto", "viajes"],
                color_discrete_map=cmap,
                title=f"Top 3 horas con mayor ingreso promedio por viaje ({anio_ant} vs {anio})",
            )
            fig.update_traces(texttemplate="USD %{text:.2f}", textposition="outside")
            fig.update_layout(xaxis_title="Hora de subida", yaxis_title="Ingreso promedio (USD)", legend_title="Año", height=360)
            st.plotly_chart(fig)
            st.dataframe(df, hide_index=True)
        bloque_interp_y_sql("q2", p, sql)
        st.divider()

    # --- Q3 ---
    if mostrar("q3"):
        p = SQL_DIR / "q3_cbd_congestion.sql"
        titulo, sql, df = ejecutar_sql_pregunta(p, anio, anio_ant, mes_num)
        st.markdown(f"### {titulo}")
        if df.empty or pd.isna(df["pct_con_cargo_en_el_mes"].iloc[0]):
            st.info(f"En `{mes_str}` no hay viajes con `cbd_congestion_fee > 0` (el cargo comenzó a regir en 2025).")
        else:
            pct_mes = float(df["pct_con_cargo_en_el_mes"].iloc[0])
            viajes_mes = int(df["viajes_del_mes"].iloc[0])
            k1, k2 = st.columns(2)
            k1.metric(f"% de viajes con cbd_congestion_fee > 0 ({mes_str})", f"{_fmt_float(pct_mes, 2)} %")
            k2.metric(f"Total de viajes aceptados en {mes_str}", _fmt_int(viajes_mes))

            df_plot = df.iloc[::-1].copy()
            fig = px.bar(
                df_plot,
                x="pct_zona_con_cargo",
                y="zona",
                orientation="h",
                text="pct_zona_con_cargo",
                hover_data=["viajes"],
                color_discrete_sequence=[COLOR_ACTUAL],
                title=f"Top 5 zonas de origen con mayor proporción de viajes con cargo CBD ({mes_str})",
            )
            fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
            fig.update_layout(xaxis=dict(range=[0, 108]), xaxis_title="% viajes con cargo CBD", yaxis_title="", height=310)
            st.plotly_chart(fig)
            st.dataframe(df, hide_index=True)
        bloque_interp_y_sql("q3", p, sql)
        st.divider()

    # --- Q4 ---
    if mostrar("q4"):
        p = SQL_DIR / "q4_propina_por_pago.sql"
        titulo, sql, df = ejecutar_sql_pregunta(p, anio, anio_ant, mes_num)
        st.markdown(f"### {titulo}")
        if not df.empty:
            df_plot = df.copy()
            df_plot["anio_str"] = df_plot["anio"].astype(str)
            df_plot["pago_lbl"] = df_plot["tipo_pago"].astype(str) + " · " + df_plot["descripcion"].astype(str)
            fig = px.bar(
                df_plot,
                x="pago_lbl",
                y="propina_promedio_pct",
                color="anio_str",
                barmode="group",
                text="propina_promedio_pct",
                hover_data=["viajes", "propina_promedio_usd"],
                color_discrete_map=cmap,
                title=f"Propina promedio como % de la tarifa por tipo de pago ({anio_ant} vs {anio})",
            )
            fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
            fig.update_layout(xaxis_title="Tipo de pago", yaxis_title="Propina promedio (% tarifa)", legend_title="Año", height=380)
            st.plotly_chart(fig)
            st.warning(
                "**Por qué `2 · Cash` (0,0 %) no es interpretable:** el taxímetro / sistema de la TLC solo registra "
                "propinas abonadas con tarjeta; el efectivo no se carga, por lo que `0,0 %` refleja ausencia de registro "
                "y no comportamiento real de los pasajeros."
            )
            st.dataframe(df, hide_index=True)
        bloque_interp_y_sql("q4", p, sql)
        st.divider()

    # --- Q5 ---
    if mostrar("q5"):
        p = SQL_DIR / "q5_aeropuertos.sql"
        titulo, sql, df = ejecutar_sql_pregunta(p, anio, anio_ant, mes_num)
        st.markdown(f"### {titulo}")
        if not df.empty:
            df_plot = df.copy()
            df_plot["anio_str"] = df_plot["anio"].astype(str)
            g1, g2 = st.columns(2)
            with g1:
                fig1 = px.bar(
                    df_plot, x="anio_str", y="viajes", color="anio_str", text="viajes",
                    color_discrete_map=cmap, title="Viajes desde/hacia JFK o LaGuardia",
                )
                fig1.update_traces(texttemplate="%{text:,}", textposition="outside")
                fig1.update_layout(showlegend=False, xaxis_title="Año", yaxis_title="Viajes", height=320)
                st.plotly_chart(fig1)
            with g2:
                fig2 = px.bar(
                    df_plot, x="anio_str", y="tarifa_media", color="anio_str", text="tarifa_media",
                    color_discrete_map=cmap, title="Tarifa media JFK / LaGuardia (USD)",
                )
                fig2.update_traces(texttemplate="USD %{text:.2f}", textposition="outside")
                fig2.update_layout(showlegend=False, xaxis_title="Año", yaxis_title="Tarifa media (USD)", height=320)
                st.plotly_chart(fig2)
            st.dataframe(df, hide_index=True)
        bloque_interp_y_sql("q5", p, sql)
        st.divider()

    # --- Q6 ---
    if mostrar("q6"):
        p = SQL_DIR / "q6_variacion.sql"
        titulo, sql, df = ejecutar_sql_pregunta(p, anio, anio_ant, mes_num)
        st.markdown(f"### {titulo}")
        if not df.empty:
            row = df.iloc[0]
            k1, k2, k3, k4 = st.columns(4)
            k1.metric(f"Viajes {mes_cmp_str}", _fmt_int(row["viajes_anterior"]))
            k2.metric(
                f"Viajes {mes_str}",
                _fmt_int(row["viajes_actual"]),
                f"{row['variacion_viajes_pct']:+.2f} %" if pd.notna(row["variacion_viajes_pct"]) else None,
            )
            k3.metric(f"Tarifa media {mes_cmp_str}", f"USD {_fmt_float(row['tarifa_media_anterior'], 2)}")
            k4.metric(
                f"Tarifa media {mes_str}",
                f"USD {_fmt_float(row['tarifa_media_actual'], 2)}",
                f"{row['variacion_tarifa_pct']:+.2f} %" if pd.notna(row["variacion_tarifa_pct"]) else None,
            )
            st.dataframe(df, hide_index=True)

            with st.expander("Evidencia en vivo de la hipótesis de Q6 (efecto composición Flex Fare `payment_type = 0`)"):
                sql_hip = """
SELECT year AS anio,
       count(*) AS viajes,
       round(avg(fare_amount), 2) AS tarifa_media,
       round(avg(fare_amount) FILTER (WHERE fare_amount > 0), 2) AS tarifa_media_positiva,
       round(100.0 * avg(CAST(fare_amount <= 0 AS INTEGER)), 2) AS pct_tarifa_menor_igual_cero
FROM fact_trips
WHERE month = ? AND year IN (?, ?)
GROUP BY year
ORDER BY year
""".strip()
                df_hip = query_warehouse(sql_hip, (mes_num, anio, anio_ant))
                st.dataframe(df_hip, hide_index=True)
                st.code(sql_hip, language="sql")
        bloque_interp_y_sql("q6", p, sql)


# ---------------------------------------------------------------------------
# Punto de entrada principal
# ---------------------------------------------------------------------------

def main():
    st.title("🚕 Pipeline NYC Taxi — Panel de Exploración y Defensa")

    if not WAREHOUSE.exists():
        st.error(
            "No se encontró `data/warehouse/taxi.duckdb`. "
            "Corré primero `make all MES=2025-03` (o `python -m src.pipeline --mes 2025-03`) para construir el modelo."
        )
        return

    periodos = periodos_disponibles()
    if not periodos:
        st.warning("La tabla `fact_trips` no contiene datos.")
        return

    conjunto = set(periodos)
    # Priorizamos períodos que tienen su año anterior disponible (ej. 2025-03 -> 2024-03)
    con_par = [(y, m) for (y, m) in periodos if (y - 1, m) in conjunto]
    opciones_p = con_par if con_par else periodos

    with st.sidebar:
        st.header("Filtros de consulta")
        sel_idx = st.selectbox(
            "Período principal (compara auto contra año anterior)",
            range(len(opciones_p)),
            format_func=lambda i: f"{opciones_p[i][0]:04d}-{opciones_p[i][1]:02d}  (vs {opciones_p[i][0]-1:04d}-{opciones_p[i][1]:02d})",
        )
        anio, mes_num = opciones_p[sel_idx]
        anio_ant = anio - 1
        mes_str = f"{anio:04d}-{mes_num:02d}"
        mes_cmp_str = f"{anio_ant:04d}-{mes_num:02d}"

        st.success(f"Consultando: **{mes_str}** vs **{mes_cmp_str}**")
        if (anio_ant, mes_num) not in conjunto:
            st.warning(f"No hay partición cargada para `{mes_cmp_str}` en `taxi.duckdb`.")

        if st.button("🔄 Recargar datos", width="stretch"):
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.caption(
            "**Modo de operación:**\n"
            "- DuckDB `READ_ONLY` (no bloquea re-ejecuciones de `make all`).\n"
            "- Consultas agregadas y `LIMIT` bajo demanda (nunca carga el dataset completo en RAM)."
        )

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "📥 Ingesta y Esquema",
            "🛡️ Calidad, Reglas y Cuarentena",
            "⭐ Modelo Estrella",
            "📊 Preguntas de Negocio (Q1–Q6)",
        ]
    )

    with tab1:
        render_tab_ingesta_esquema(mes_str, mes_cmp_str)
    with tab2:
        render_tab_calidad(anio, anio_ant, mes_num, mes_str, mes_cmp_str)
    with tab3:
        render_tab_modelo(anio, anio_ant, mes_num, mes_str, mes_cmp_str)
    with tab4:
        render_tab_preguntas(anio, anio_ant, mes_num, mes_str, mes_cmp_str)


if __name__ == "__main__":
    main()
