import duckdb
import pytest

from src.calidad import cargar_reglas, reglas_violadas

MES = "2025-03"


@pytest.fixture(scope="module")
def reglas():
    return {r["id"]: r for r in cargar_reglas()}


@pytest.fixture
def con():
    return duckdb.connect()


def violadas(con, reglas, tabla, ids):
    return reglas_violadas(con, tabla, [reglas[i] for i in ids], MES)


def test_r01_fecha_fuera_de_mes(con, reglas):
    con.execute("""CREATE TABLE t AS SELECT * FROM (VALUES
        (1, TIMESTAMP '2025-03-15 10:00:00'),
        (2, TIMESTAMP '2025-04-01 00:00:00'),
        (3, TIMESTAMP '2025-02-28 23:59:59'),
        (4, NULL::TIMESTAMP)
      ) v(id, tpep_pickup_datetime)""")
    # el límite superior es exclusivo y un nulo cuenta como violación
    assert violadas(con, reglas, "t", ["R01"]) == {2: ["R01"], 3: ["R01"], 4: ["R01"]}


def test_r02_bajada_anterior_o_igual(con, reglas):
    con.execute("""CREATE TABLE t AS SELECT * FROM (VALUES
        (1, TIMESTAMP '2025-03-01 10:00:00', TIMESTAMP '2025-03-01 10:10:00'),
        (2, TIMESTAMP '2025-03-01 10:00:00', TIMESTAMP '2025-03-01 09:00:00'),
        (3, TIMESTAMP '2025-03-01 10:00:00', TIMESTAMP '2025-03-01 10:00:00')
      ) v(id, tpep_pickup_datetime, tpep_dropoff_datetime)""")
    assert violadas(con, reglas, "t", ["R02"]) == {2: ["R02"], 3: ["R02"]}


def test_r03_y_r04_distancia(con, reglas):
    con.execute("""CREATE TABLE t AS SELECT * FROM (VALUES
        (1, 5.0), (2, 0.0), (3, -1.0), (4, 150.0), (5, NULL::DOUBLE)
      ) v(id, trip_distance)""")
    assert violadas(con, reglas, "t", ["R03"]) == {2: ["R03"], 3: ["R03"], 5: ["R03"]}
    assert violadas(con, reglas, "t", ["R04"]) == {4: ["R04"]}


def test_r05_y_r11_importe(con, reglas):
    con.execute("""CREATE TABLE t AS SELECT * FROM (VALUES
        (1, 20.0), (2, -5.0), (3, 1500.0), (4, NULL::DOUBLE)
      ) v(id, total_amount)""")
    assert violadas(con, reglas, "t", ["R05"]) == {2: ["R05"], 4: ["R05"]}
    assert violadas(con, reglas, "t", ["R11"]) == {3: ["R11"]}


def test_r06_pasajeros_es_advertencia_y_marca_nulos(con, reglas):
    con.execute("""CREATE TABLE t AS SELECT * FROM (VALUES
        (1, 2), (2, 0), (3, NULL::BIGINT)
      ) v(id, passenger_count)""")
    assert reglas["R06"]["severidad"] == "advertencia"
    assert violadas(con, reglas, "t", ["R06"]) == {2: ["R06"], 3: ["R06"]}


def test_r07_zona_inexistente(con, reglas):
    con.execute("CREATE TABLE zonas AS SELECT * FROM (VALUES (1), (2)) v(location_id)")
    con.execute("""CREATE TABLE t AS SELECT * FROM (VALUES
        (1, 1, 2), (2, 1, 999), (3, NULL::INTEGER, 2)
      ) v(id, pu_location_id, do_location_id)""")
    assert violadas(con, reglas, "t", ["R07"]) == {2: ["R07"], 3: ["R07"]}


def test_r08_duplicado_exacto(con, reglas):
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 1), (2, 2), (3, 1)) v(id, rn)")
    assert violadas(con, reglas, "t", ["R08"]) == {2: ["R08"]}   # solo la copia, no la primera


def test_r09_velocidad_imposible_sin_dividir_por_cero(con, reglas):
    con.execute("""CREATE TABLE t AS SELECT * FROM (VALUES
        (1, 3.0,  TIMESTAMP '2025-03-01 10:00:00', TIMESTAMP '2025-03-01 10:10:00'),
        (2, 1.0,  TIMESTAMP '2025-03-01 10:00:00', TIMESTAMP '2025-03-01 10:01:00'),
        (3, 50.0, TIMESTAMP '2025-03-01 10:00:00', TIMESTAMP '2025-03-01 10:10:00'),
        (4, 5.0,  TIMESTAMP '2025-03-01 10:00:00', TIMESTAMP '2025-03-01 10:00:00')
      ) v(id, trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime)""")
    # 18 mph y 60 mph pasan; 300 mph viola; duración 0 no explota (la cubre R02)
    assert violadas(con, reglas, "t", ["R09"]) == {3: ["R09"]}


def test_r10_zona_unknown(con, reglas):
    con.execute("""CREATE TABLE t AS SELECT * FROM (VALUES
        (1, 10, 20), (2, 264, 20), (3, 10, 265)
      ) v(id, pu_location_id, do_location_id)""")
    assert violadas(con, reglas, "t", ["R10"]) == {2: ["R10"], 3: ["R10"]}