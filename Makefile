PY  ?= python
MES ?= 2025-03

.PHONY: install up down ps dagster-run dbt-build dbt-test reconciliar preguntas test clean-data clean-all

install:
	$(PY) -m pip install -r requirements.txt

up:
	docker compose up -d dagster-webserver metrics-exporter prometheus grafana dozzle

down:
	docker compose down

ps:
	docker compose ps

dagster-run:
	$(PY) -c "from orchestration import defs; job = defs.resolve_job_def('nyc_taxi_pipeline'); assets = [a.key.to_user_string() for a in defs.assets]; res = job.execute_in_process(run_config={'ops': {k: {'config': {'mes': '$(MES)', 'sin_ingesta': True}} for k in assets}}); assert res.success"

dbt-build:
	dbt build --project-dir dbt_project --profiles-dir dbt_project

dbt-test:
	dbt test --project-dir dbt_project --profiles-dir dbt_project

reconciliar:
	$(PY) -m src.reconciliacion --con-marts

preguntas:
	$(PY) -m src.preguntas --mes $(MES) --mes-comparacion 2024-03

test:
	$(PY) -m pytest -q

clean-data:
	$(PY) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ['data/validada','data/quarantine','data/warehouse','data/control','data/tmp','reports']]"

clean-all:
	$(PY) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ['data','reports']]"
