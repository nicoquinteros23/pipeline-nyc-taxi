import duckdb

from src.esquema import comparar, evaluar, sql_unificado

CONTRATO = {"columns": [
    {"name": "a", "type": "INTEGER", "nullable": False},
    {"name": "b", "type": "DOUBLE", "nullable": True, "aliases": ["B_Col"]},
]}


def test_columna_nueva_es_advertencia():
    res = comparar({"a": "INTEGER", "b": "DOUBLE", "extra_col": "VARCHAR"}, CONTRATO)
    errores, advertencias = evaluar(res, CONTRATO)
    assert res["nuevas"] == ["extra_col"]
    assert errores == [] and len(advertencias) == 1


def test_columna_obligatoria_faltante_es_error():
    res = comparar({"b": "DOUBLE"}, CONTRATO)
    errores, _ = evaluar(res, CONTRATO)
    assert res["faltantes"] == ["a"]
    assert len(errores) == 1


def test_columna_nullable_faltante_es_advertencia():
    res = comparar({"a": "INTEGER"}, CONTRATO)
    errores, advertencias = evaluar(res, CONTRATO)
    assert errores == [] and len(advertencias) == 1


def test_alias_resuelve_el_nombre():
    res = comparar({"a": "INTEGER", "B_Col": "DOUBLE"}, CONTRATO)
    assert res["faltantes"] == [] and res["nuevas"] == []
    assert res["origen_de_cada_columna"]["b"] == "B_Col"


def test_tipo_compatible_y_tipo_incompatible():
    ok = comparar({"a": "SMALLINT", "b": "DOUBLE"}, CONTRATO)
    assert evaluar(ok, CONTRATO)[0] == []                  # SMALLINT -> INTEGER es seguro
    mal = comparar({"a": "BIGINT", "b": "DOUBLE"}, CONTRATO)
    assert len(evaluar(mal, CONTRATO)[0]) == 1             # BIGINT -> INTEGER podría truncar


def test_unificado_completa_con_null_la_columna_ausente(tmp_path):
    parquet = tmp_path / "mes.parquet"
    duckdb.execute(f"COPY (SELECT 1::INTEGER AS a) TO '{parquet.as_posix()}' (FORMAT PARQUET)")
    sql = sql_unificado(parquet, CONTRATO, {"a": "a"})     # 'b' no existe en el archivo
    fila = duckdb.execute(f"SELECT a, b, source_file FROM ({sql})").fetchall()
    assert fila == [(1, None, "mes.parquet")]