.PHONY: help docker-up docker-setup local-airflow-up dbt-deps dbt-run-raw-dev terraform-fmt terraform-validate airflow-compile airbyte-check

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
	@echo "  make airbyte-check      py_compile airbyte helper script"

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

airbyte-check:
	python -m py_compile airbyte/scripts/trigger_sync.py
