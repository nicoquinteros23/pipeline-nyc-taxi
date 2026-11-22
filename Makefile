PY  ?= python
ifneq ("$(wildcard .venv/Scripts/python.exe)","")
  PY := .venv\Scripts\python.exe
  DBT := .venv\Scripts\dbt.exe
else ifneq ("$(wildcard .venv/bin/python)","")
  PY := .venv/bin/python
  DBT := .venv/bin/dbt
else
  PY ?= python
  DBT ?= dbt
endif

MES ?= 2025-03
SIN_INGESTA ?= 0
FORZAR_DLT ?= 0

CLI_FLAGS :=
ifeq ($(SIN_INGESTA),1)
  CLI_FLAGS += --sin-ingesta
endif
ifeq ($(FORZAR_DLT),1)
  CLI_FLAGS += --forzar-dlt
endif

.PHONY: all install up down ps dagster-run dbt-build dbt-test reconciliar preguntas test clean-data clean-all

all:
	$(PY) -m orchestration.cli --mes $(MES) $(CLI_FLAGS)

install:
	$(PY) -m pip install -r requirements.txt

up:
	docker compose up -d dagster-webserver metrics-exporter prometheus grafana dozzle

down:
	docker compose down

ps:
	docker compose ps

dagster-run:
	$(PY) -m orchestration.cli --mes $(MES) --sin-ingesta

dbt-build:
	$(DBT) build --project-dir dbt_project --profiles-dir dbt_project

dbt-test:
	$(DBT) test --project-dir dbt_project --profiles-dir dbt_project

reconciliar:
	$(PY) -m src.reconciliacion --mes $(MES) --con-marts

preguntas:
	$(PY) -m src.preguntas --mes $(MES)

test:
	$(PY) -m pytest -q

clean-data:
	$(PY) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ['data/validada','data/quarantine','data/warehouse','data/control','data/tmp','reports']]"

clean-all:
	$(PY) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ['data','reports']]"
