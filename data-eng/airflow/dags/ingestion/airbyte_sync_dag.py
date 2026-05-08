"""Trigger Airbyte connection sync(s), wait for job completion, then trigger dbt DAG.

**Config (one of):**

- Env `AIRBYTE_SYNC_CONNECTIONS` — JSON list of connection UUIDs
- Env `AIRBYTE_CONNECTION_ID` — single UUID
- Airflow Variables `AIRBYTE_SYNC_CONNECTIONS` or `AIRBYTE_CONNECTION_ID`

**Env:** `AIRBYTE_URL`, optional `AIRBYTE_CLIENT_TOKEN`, optional `AIRBYTE_JOB_TIMEOUT_SEC` (default 7200).
"""

from __future__ import annotations

import json
import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from common.airbyte_client import extract_job_id, poll_job, trigger_connection_sync


def _connection_ids() -> list[str]:
    raw = os.environ.get("AIRBYTE_SYNC_CONNECTIONS", "").strip()
    if raw:
        return [str(x).strip() for x in json.loads(raw) if str(x).strip()]
    v = Variable.get("AIRBYTE_SYNC_CONNECTIONS", default_var="[]")
    ids = json.loads(v) if isinstance(v, str) else v
    if ids:
        return [str(x).strip() for x in ids if str(x).strip()]
    single = os.environ.get("AIRBYTE_CONNECTION_ID", "").strip() or str(
        Variable.get("AIRBYTE_CONNECTION_ID", default_var="")
    ).strip()
    if single:
        return [single]
    raise ValueError(
        "Set AIRBYTE_SYNC_CONNECTIONS (JSON list) or AIRBYTE_CONNECTION_ID (env or Airflow Variable)."
    )


def trigger_syncs(**context):
    ti = context["ti"]
    jobs: list[dict] = []
    for cid in _connection_ids():
        resp = trigger_connection_sync(cid)
        jid = extract_job_id(resp)
        if jid is None:
            raise RuntimeError(f"No job id in sync response for {cid}: {resp!r}")
        jobs.append({"connection_id": cid, "job_id": jid})
    ti.xcom_push(key="sync_jobs", value=jobs)
    return jobs


def wait_for_jobs(**context):
    ti = context["ti"]
    jobs = ti.xcom_pull(task_ids="trigger_airbyte_syncs", key="sync_jobs")
    if not jobs:
        jobs = ti.xcom_pull(task_ids="trigger_airbyte_syncs")
    if not jobs:
        raise RuntimeError("No sync_jobs in XCom; trigger task failed?")
    timeout = float(os.environ.get("AIRBYTE_JOB_TIMEOUT_SEC", "7200"))
    for j in jobs:
        poll_job(int(j["job_id"]), max_wait_sec=timeout)


with DAG(
    dag_id="airbyte_sync",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ingestion", "airbyte"],
    doc_md=__doc__,
) as dag:
    trigger_airbyte_syncs = PythonOperator(
        task_id="trigger_airbyte_syncs",
        python_callable=trigger_syncs,
    )
    wait_airbyte_jobs = PythonOperator(
        task_id="wait_airbyte_jobs",
        python_callable=wait_for_jobs,
    )
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_pipeline",
        trigger_dag_id="dbt_pipeline",
        wait_for_completion=False,
        reset_dag_run=False,
    )

    trigger_airbyte_syncs >> wait_airbyte_jobs >> trigger_dbt
