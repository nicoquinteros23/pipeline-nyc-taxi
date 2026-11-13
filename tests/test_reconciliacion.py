import pytest

from src.calidad import ErrorReconciliacion, reconciliar


def test_reconciliacion_ok():
    reconciliar(leidas=10, aceptadas=7, cuarentena=3)


def test_reconciliacion_falla():
    with pytest.raises(ErrorReconciliacion):
        reconciliar(leidas=10, aceptadas=6, cuarentena=3)