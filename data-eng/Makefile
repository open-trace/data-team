.PHONY: help docker-up docker-setup local-airflow-up dbt-deps dbt-run-raw-dev terraform-fmt terraform-validate airflow-compile ingest-install ingest-auth ingest-list ingest-dry ingest ingest-test dbt-landing dbt-dev-landing

SOURCE ?= hdx_wfp_prices
ARGS ?=

help:
	@echo "OpenTrace monorepo — common commands"
	@echo "  make docker-up          Start Postgres (root docker-compose.yml)"
	@echo "  make docker-setup       Populate local DB from BigQuery (profile setup)"
	@echo "  make local-airflow-up   Local Airflow (docker-compose.local.yml from data-eng/)"
	@echo "  make dbt-deps           dbt deps (run from repo root)"
	@echo "  make dbt-run-raw-dev    dbt run --target raw_dev (requires BQ auth)"
	@echo "  make terraform-fmt      terraform fmt on data-eng/infra"
	@echo "  make terraform-validate terraform validate (data-eng/infra/environments/dev)"
	@echo "  make airflow-compile    compileall Airflow DAG Python files"
	@echo "  make ingest-install     pip install -e ingestion[dev] (package under ingestion/package/)"
	@echo "  make ingest-auth        python -m ingestion check-auth"
	@echo "  make ingest-list        list ingestion sources"
	@echo "  make ingest-dry         dry-run SOURCE (default hdx_wfp_prices)"
	@echo "  make ingest             run SOURCE (or: python ingestion/scripts/run_source.py …)"
	@echo "  make ingest-test        pytest ingestion/tests"
	@echo "  notebooks:              ingestion/notebooks/ (try loads here first)"
	@echo "  make dbt-landing        dbt build from landing SoT (not landing_dev)"

docker-up:
	docker compose up -d

docker-setup:
	docker compose --profile setup up

dbt-deps:
	cd dbt && dbt deps

dbt-run-raw-dev:
	cd dbt && dbt run --target raw_dev

terraform-fmt:
	terraform fmt -recursive infra || true

terraform-validate:
	cd infra/environments/dev && terraform init -backend=false -input=false && terraform validate

local-airflow-up:
	docker compose -f docker-compose.local.yml up -d

docker-up:
	cd .. && docker compose up -d

docker-setup:
	cd .. && docker compose --profile setup up

airflow-compile:
	python -m compileall -q airflow/dags

ingest-install:
	pip install -e ingestion[dev]

ingest-auth:
	python -m ingestion check-auth

ingest-list:
	python -m ingestion list

ingest-dry:
	python -m ingestion run $(SOURCE) --dry-run --limit 500 $(ARGS)

ingest:
	python -m ingestion run $(SOURCE) $(ARGS)

ingest-test:
	pytest ingestion/tests -q

# Transforms always read durable landing (SoT). Ingest sandbox is landing_dev via `make ingest`.
dbt-landing:
	BQ_DATASET_LANDING=landing dbt build --select source:landing+ --target raw_dev

# Alias kept so old muscle memory still hits the correct SoT (not landing_dev).
dbt-dev-landing: dbt-landing

