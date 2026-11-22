"""Reconciliación integral de conteos para la Fase 2 (staging = aceptadas + cuarentena).

Consulta en modo solo lectura los datos reales cargados en ``nyc_taxi_dlt.duckdb``
(esquemas ``staging``, ``intermediate`` y, opcionalmente, ``marts`` de dbt).
No modifica datos y puede ejecutarse repetidamente.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

MESES_DEFECTO = ("2024-03", "2025-03")
WAREHOUSE_DEFECTO = Path("data/warehouse/nyc_taxi_dlt.duckdb")


class ErrorReconciliacionFase2(Exception):
    """Se lanza cuando los conteos entre capas no reconcilian exactamente."""


def _contar(con: duckdb.DuckDBPyConnection, objeto: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {objeto}").fetchone()[0])


def _existe_objeto(con: duckdb.DuckDBPyConnection, schema: str, nombre: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, nombre],
    ).fetchone()
    return bool(row and row[0] > 0)


def reconciliar_fase2(
    meses: tuple[str, ...] | list[str] = MESES_DEFECTO,
    warehouse_path: Path = WAREHOUSE_DEFECTO,
    verificar_marts: bool = False,
) -> dict:
    """Verifica ``staging == aceptadas + cuarentena`` por mes y totales consolidados.

    Si ``verificar_marts=True``, además valida que ``marts.fact_trips`` iguale a
    ``intermediate.trips_validated`` y que ``marts.agg_zone_hour`` cierre con
    ``marts.fact_trips``.
    """
    warehouse = Path(warehouse_path)
    if not warehouse.is_file():
        raise FileNotFoundError(f"No existe la base DuckDB: {warehouse}")
    if not meses:
        raise ValueError("Debe especificarse al menos un mes para reconciliar")

    errores: list[str] = []
    por_mes: dict[str, dict[str, int]] = {}
    marts_resumen: dict | None = None

    with duckdb.connect(str(warehouse), read_only=True) as con:
        con.execute("SET memory_limit = '1.5GB'")
        con.execute("SET preserve_insertion_order = false")
        con.execute("SET threads = 2")

        for mes in meses:
            anio, mm = mes.split("-")
            sufijo = f"{anio}_{mm}"

            staging_tbl = f"staging.yellow_trips_{sufijo}"
            val_view = f"intermediate.trips_validated_{sufijo}"
            cua_view = f"intermediate.trips_quarantine_{sufijo}"

            for sch, name in (
                ("staging", f"yellow_trips_{sufijo}"),
                ("intermediate", f"trips_validated_{sufijo}"),
                ("intermediate", f"trips_quarantine_{sufijo}"),
            ):
                if not _existe_objeto(con, sch, name):
                    raise ErrorReconciliacionFase2(
                        f"Falta el objeto requerido {sch}.{name} en {warehouse}"
                    )

            staging_n = _contar(con, staging_tbl)
            aceptadas_n = _contar(con, val_view)
            cuarentena_n = _contar(con, cua_view)
            diferencia = staging_n - (aceptadas_n + cuarentena_n)

            por_mes[mes] = {
                "staging": staging_n,
                "aceptadas": aceptadas_n,
                "cuarentena": cuarentena_n,
                "diferencia": diferencia,
            }

            if diferencia != 0:
                errores.append(
                    f"{mes}: staging ({staging_n:,}) != aceptadas ({aceptadas_n:,}) + "
                    f"cuarentena ({cuarentena_n:,}) [dif={diferencia:+,}]"
                )

        total_staging = sum(v["staging"] for v in por_mes.values())
        total_aceptadas = sum(v["aceptadas"] for v in por_mes.values())
        total_cuarentena = sum(v["cuarentena"] for v in por_mes.values())

        if total_staging != total_aceptadas + total_cuarentena:
            errores.append(
                f"TOTAL: staging ({total_staging:,}) != aceptadas ({total_aceptadas:,}) + "
                f"cuarentena ({total_cuarentena:,})"
            )

        consolidada_val = _contar(con, "intermediate.trips_validated")
        if consolidada_val != total_aceptadas:
            errores.append(
                f"intermediate.trips_validated ({consolidada_val:,}) != "
                f"suma mensual de aceptadas ({total_aceptadas:,})"
            )

        consolidada_cua = None
        if _existe_objeto(con, "intermediate", "trips_quarantine"):
            consolidada_cua = _contar(con, "intermediate.trips_quarantine")
            if consolidada_cua != total_cuarentena:
                errores.append(
                    f"intermediate.trips_quarantine ({consolidada_cua:,}) != "
                    f"suma mensual de cuarentena ({total_cuarentena:,})"
                )

        if verificar_marts:
            for tbl in ("fact_trips", "dim_zone", "dim_date", "agg_zone_hour"):
                if not _existe_objeto(con, "marts", tbl):
                    raise ErrorReconciliacionFase2(
                        f"Falta el modelo dbt marts.{tbl} en {warehouse}"
                    )

            fact_n = _contar(con, "marts.fact_trips")
            agg_trips, agg_rev = con.execute(
                "SELECT COALESCE(SUM(trips), 0), COALESCE(SUM(total_revenue), 0.0) "
                "FROM marts.agg_zone_hour"
            ).fetchone()
            fact_rev = float(
                con.execute(
                    "SELECT COALESCE(SUM(total_amount), 0.0) FROM marts.fact_trips"
                ).fetchone()[0]
            )
            agg_trips = int(agg_trips)
            agg_rev = float(agg_rev)

            marts_resumen = {
                "fact_trips": fact_n,
                "agg_zone_hour_trips": agg_trips,
                "fact_total_revenue": round(fact_rev, 2),
                "agg_total_revenue": round(agg_rev, 2),
            }

            if fact_n != total_aceptadas:
                errores.append(
                    f"marts.fact_trips ({fact_n:,}) != total aceptadas ({total_aceptadas:,})"
                )
            if agg_trips != fact_n:
                errores.append(
                    f"SUM(marts.agg_zone_hour.trips) ({agg_trips:,}) != "
                    f"marts.fact_trips ({fact_n:,})"
                )
            if abs(agg_rev - fact_rev) >= 0.01:
                errores.append(
                    f"SUM(marts.agg_zone_hour.total_revenue) ({agg_rev:.2f}) != "
                    f"SUM(marts.fact_trips.total_amount) ({fact_rev:.2f})"
                )

    resultado = {
        "meses": por_mes,
        "totales": {
            "staging": total_staging,
            "aceptadas": total_aceptadas,
            "cuarentena": total_cuarentena,
            "intermediate_trips_validated": consolidada_val,
            "intermediate_trips_quarantine": consolidada_cua,
        },
        "marts": marts_resumen,
        "ok": len(errores) == 0,
        "errores": errores,
    }

    if errores:
        raise ErrorReconciliacionFase2("; ".join(errores))

    return resultado


def imprimir_resumen(res: dict) -> None:
    print("=== RECONCILIACION INTEGRAL FASE 2 ===")
    for mes, d in res["meses"].items():
        print(
            f"  {mes}: staging={d['staging']:,} = "
            f"aceptadas={d['aceptadas']:,} + cuarentena={d['cuarentena']:,} (OK)"
        )
    t = res["totales"]
    print(
        f"  TOTAL:   staging={t['staging']:,} = "
        f"aceptadas={t['aceptadas']:,} + cuarentena={t['cuarentena']:,} (OK)"
    )
    if res["marts"] is not None:
        m = res["marts"]
        print(
            f"  MARTS:   fact_trips={m['fact_trips']:,} == "
            f"agg_zone_hour_trips={m['agg_zone_hour_trips']:,} | "
            f"revenue={m['fact_total_revenue']:,.2f} (OK)"
        )


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Reconciliación integral entre staging (dlt), intermediate y marts (dbt)"
    )
    ap.add_argument(
        "--meses",
        nargs="+",
        default=list(MESES_DEFECTO),
        help="Meses a reconciliar, formato AAAA-MM (por defecto: 2024-03 2025-03)",
    )
    ap.add_argument(
        "--warehouse",
        default=str(WAREHOUSE_DEFECTO),
        help="Ruta a la base DuckDB principal",
    )
    ap.add_argument(
        "--con-marts",
        action="store_true",
        help="Verificar además cierre con marts.fact_trips y marts.agg_zone_hour",
    )
    args = ap.parse_args(argv)
    try:
        res = reconciliar_fase2(
            meses=tuple(args.meses),
            warehouse_path=Path(args.warehouse),
            verificar_marts=args.con_marts,
        )
    except Exception as exc:
        print(f"ERROR DE RECONCILIACION: {exc}", file=sys.stderr)
        return 1
    imprimir_resumen(res)
    return 0


if __name__ == "__main__":
    sys.exit(main())
