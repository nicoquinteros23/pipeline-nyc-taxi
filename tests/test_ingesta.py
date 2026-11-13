import csv

import pytest
import requests

from src import ingesta


def leer_log(base):
    with open(base / "control" / "ingestion_log.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_ingesta_idempotente(tmp_path):
    llamadas = []

    def descarga_falsa(url, destino):
        llamadas.append(url)
        destino.write_bytes(b"contenido fijo")

    args = dict(base_dir=tmp_path, descargar=descarga_falsa, contar_filas=lambda p: 3)
    ingesta.ingestar("2025-03", **args)
    ingesta.ingestar("2025-03", **args)

    assert len(llamadas) == 1                       # la segunda vez NO descarga
    assert [f["accion"] for f in leer_log(tmp_path)] == ["descargado", "omitido"]


def test_redescarga_si_el_archivo_cambia(tmp_path):
    llamadas = []

    def descarga_falsa(url, destino):
        llamadas.append(url)
        destino.write_bytes(b"contenido fijo")

    args = dict(base_dir=tmp_path, descargar=descarga_falsa, contar_filas=lambda p: 3)
    ingesta.ingestar("2025-03", **args)
    archivo = (tmp_path / "raw" / "yellow" / "year=2025" / "month=03"
               / "yellow_tripdata_2025-03.parquet")
    archivo.write_bytes(b"corrupto")                 # simula un archivo dañado
    ingesta.ingestar("2025-03", **args)

    assert len(llamadas) == 2                       # el hash no coincidió: se bajó de nuevo


def test_error_se_registra_y_se_propaga(tmp_path):
    def descarga_que_falla(url, destino):
        raise RuntimeError("sin red")

    with pytest.raises(RuntimeError):
        ingesta.ingestar("2025-03", base_dir=tmp_path,
                         descargar=descarga_que_falla, contar_filas=lambda p: 0)

    filas = leer_log(tmp_path)
    assert len(filas) == 1 and filas[0]["estado"] == "error"


def test_mes_invalido(tmp_path):
    with pytest.raises(ValueError):
        ingesta.ingestar("marzo", base_dir=tmp_path)


def test_reintentos_con_espera_creciente(tmp_path, monkeypatch):
    esperas, intentos = [], []
    monkeypatch.setattr(ingesta.time, "sleep", esperas.append)   # no esperamos de verdad

    def get_falso(*args, **kwargs):
        intentos.append(1)
        raise requests.ConnectionError("sin red")

    monkeypatch.setattr(ingesta.requests, "get", get_falso)
    with pytest.raises(requests.ConnectionError):
        ingesta.descargar("http://ejemplo", tmp_path / "a.csv")

    assert len(intentos) == 3                       # hasta 3 intentos
    assert esperas == [2, 4]                        # espera creciente
    assert not list(tmp_path.glob("*.part"))        # sin archivos a medias


def test_error_permanente_no_se_reintenta(tmp_path, monkeypatch):
    intentos = []

    class Respuesta:
        status_code = 403

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def raise_for_status(self):
            raise requests.HTTPError("403", response=self)

    def get_falso(*args, **kwargs):
        intentos.append(1)
        return Respuesta()

    monkeypatch.setattr(ingesta.requests, "get", get_falso)
    with pytest.raises(requests.HTTPError):
        ingesta.descargar("http://ejemplo", tmp_path / "a.csv")

    assert len(intentos) == 1                       # falló rápido, sin reintentar