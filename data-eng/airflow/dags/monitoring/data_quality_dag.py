"""Data quality / freshness checks — wire to dbt tests, Great Expectations, or custom sensors."""

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="data_quality",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["monitoring", "quality"],
):
    EmptyOperator(task_id="quality_checks_placeholder")
