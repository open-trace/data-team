"""Run dbt in Composer — point bash_command or KubernetesPodOperator at your dbt project path."""

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dbt_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["transformations", "dbt"],
):
    dbt_run = EmptyOperator(task_id="dbt_run")
    dbt_test = EmptyOperator(task_id="dbt_test")
    dbt_run >> dbt_test
