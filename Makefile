PY  ?= python
MES ?= 2025-03

.PHONY: all install test demo-reproceso clean-data

all:
	$(PY) -m src.pipeline --mes $(MES)

install:
	$(PY) -m pip install -r requirements.txt

test:
	$(PY) -m pytest -q

demo-reproceso:
	$(PY) -m scripts.demo_reproceso

clean-data:
	$(PY) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ['data/curated','data/validada','data/quarantine','data/warehouse','data/control','data/tmp','reports']]"
