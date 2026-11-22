PY  ?= python
MES ?= 2025-03

.PHONY: all install test demo-reproceso clean-data clean-all dashboard intermediate reconciliar dbt-build dbt-test fase2

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

clean-all:
	$(PY) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ['data','reports']]"

dashboard:
	$(PY) -m streamlit run dashboard/app.py

intermediate:
	$(PY) -m src.intermediate

reconciliar:
	$(PY) -m src.reconciliacion --con-marts

dbt-build:
	dbt build --project-dir dbt_project --profiles-dir dbt_project

dbt-test:
	dbt test --project-dir dbt_project --profiles-dir dbt_project

fase2:
	$(PY) -m src.pipeline_fase2 --mes $(MES)
