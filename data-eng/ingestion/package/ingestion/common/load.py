"""Load jobs, metadata columns, replace_partition and merge modes."""

from __future__ import annotations

import hashlib
import json
import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Protocol

import pandas as pd
from google.cloud import bigquery

from ingestion.common.bq import ensure_table, schema_from_dataframe, table_ref
from ingestion.common.config import Settings


class BQClientLike(Protocol):
    def load_table_from_dataframe(self, dataframe, destination, job_config=None, **kwargs): ...
    def query(self, query, job_config=None, **kwargs): ...
    def delete_table(self, table, not_found_ok=False): ...
    def update_table(self, table, fields): ...
    def get_table(self, table): ...
    def insert_rows_json(self, table, json_rows, **kwargs): ...  # must never be used for data


METADATA_COLS = (
    "_source",
    "_run_id",
    "_ingested_at",
    "_ingest_date",
    "_source_url",
    "_record_hash",
)


@dataclass
class LoadResult:
    rows: int
    mode: str
    destination: str
    staging: str | None = None
    merge_sql: str | None = None
    dropped_staging: bool = False


@dataclass
class FakeLoadJob:
    destination: Any
    job_config: Any
    done: bool = True

    def result(self) -> FakeLoadJob:
        return self


@dataclass
class FakeQueryJob:
    sql: str
    rows: list[Any] = field(default_factory=list)

    def result(self) -> FakeQueryJob:
        return self


@dataclass
class FakeTable:
    table_id: str
    expires: Any = None
    project: str = "p"
    dataset_id: str = "d"


class FakeBQClient:
    """In-memory client for offline tests."""

    def __init__(self) -> None:
        self.load_calls: list[dict[str, Any]] = []
        self.query_calls: list[str] = []
        self.deleted: list[str] = []
        self.tables: dict[str, pd.DataFrame] = {}
        self.updated: list[Any] = []
        self.insert_rows_json_calls: list[Any] = []

    def load_table_from_dataframe(self, dataframe, destination, job_config=None, **kwargs):
        dest = str(destination)
        cfg = job_config
        write_disp = getattr(cfg, "write_disposition", None) if cfg else None
        self.load_calls.append(
            {
                "destination": dest,
                "write_disposition": write_disp,
                "rows": len(dataframe),
                "dataframe": dataframe.copy(),
            }
        )
        key = dest.split("$")[0]
        if write_disp == bigquery.WriteDisposition.WRITE_TRUNCATE or dest not in self.tables:
            self.tables[key] = dataframe.copy()
            if "$" in dest:
                self.tables[dest] = dataframe.copy()
        else:
            self.tables[key] = pd.concat([self.tables.get(key, pd.DataFrame()), dataframe])
        return FakeLoadJob(destination=destination, job_config=job_config)

    def query(self, query, job_config=None, **kwargs):
        self.query_calls.append(query)
        # Simulate MERGE: copy staging into target keyed by _record_hash
        if "MERGE" in query.upper():
            # Best-effort: leave tables as-is; tests inspect SQL
            pass
        return FakeQueryJob(sql=query)

    def delete_table(self, table, not_found_ok=False):
        self.deleted.append(str(table))
        self.tables.pop(str(table), None)

    def get_table(self, table):
        return FakeTable(table_id=str(table))

    def update_table(self, table, fields):
        self.updated.append((table, fields))
        return table

    def create_table(self, table, exists_ok=False):
        return table

    def insert_rows_json(self, table, json_rows, **kwargs):
        self.insert_rows_json_calls.append((table, json_rows))
        raise AssertionError("insert_rows_json must not be used")


def new_run_id() -> str:
    return str(uuid.uuid4())


def record_hash(row: pd.Series, key_cols: list[str]) -> str:
    parts = []
    for col in key_cols:
        val = row[col] if col in row.index else ""
        if pd.isna(val):
            val = ""
        parts.append(f"{col}={val}")
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def add_metadata(
    df: pd.DataFrame,
    *,
    source: str,
    run_id: str,
    source_url: str,
    key_cols: list[str],
    ingest_date: date | None = None,
) -> pd.DataFrame:
    out = df.copy()
    now = datetime.now(timezone.utc)
    d = ingest_date or now.date()
    out["_source"] = source
    out["_run_id"] = run_id
    out["_ingested_at"] = now
    out["_ingest_date"] = d
    out["_source_url"] = source_url
    if key_cols:
        out["_record_hash"] = out.apply(lambda r: record_hash(r, key_cols), axis=1)
    else:
        out["_record_hash"] = out.apply(
            lambda r: hashlib.sha256(
                json.dumps(r.to_dict(), default=str, sort_keys=True).encode()
            ).hexdigest(),
            axis=1,
        )
    return out


def git_sha() -> str | None:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return out.strip()
    except Exception:
        return None


def load_dataframe(
    client: Any,
    settings: Settings,
    df: pd.DataFrame,
    *,
    table: str,
    mode: str,
    run_id: str,
) -> LoadResult:
    if mode == "replace_partition":
        return _load_replace_partition(client, settings, df, table=table, run_id=run_id)
    if mode == "merge":
        return _load_merge(client, settings, df, table=table, run_id=run_id)
    raise ValueError(f"Unknown load mode: {mode}")


def _load_replace_partition(
    client: Any,
    settings: Settings,
    df: pd.DataFrame,
    *,
    table: str,
    run_id: str,
) -> LoadResult:
    ingest_date = df["_ingest_date"].iloc[0]
    if isinstance(ingest_date, datetime):
        ingest_date = ingest_date.date()
    elif not isinstance(ingest_date, date):
        ingest_date = pd.to_datetime(ingest_date).date()
    decorator = ingest_date.strftime("%Y%m%d")
    dest = f"{table_ref(settings, table)}${decorator}"
    schema = schema_from_dataframe(df)
    ensure_table(client, settings, table, schema)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )
    client.load_table_from_dataframe(df, dest, job_config=job_config).result()
    return LoadResult(rows=len(df), mode="replace_partition", destination=dest)


def _load_merge(
    client: Any,
    settings: Settings,
    df: pd.DataFrame,
    *,
    table: str,
    run_id: str,
) -> LoadResult:
    staging = f"_stg_{table}_{run_id.replace('-', '')[:12]}"
    staging_ref = table_ref(settings, staging)
    target_ref = table_ref(settings, table)
    schema = schema_from_dataframe(df)
    ensure_table(client, settings, table, schema)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )
    client.load_table_from_dataframe(df, staging_ref, job_config=job_config).result()
    try:
        stg_table = client.get_table(staging_ref)
        stg_table.expires = datetime.now(timezone.utc).replace(
            hour=23, minute=59, second=59
        ) + pd.Timedelta(days=1)
        client.update_table(stg_table, ["expires"])
    except Exception:
        pass

    cols = list(df.columns)
    update_set = ", ".join(f"T.`{c}` = S.`{c}`" for c in cols if c != "_record_hash")
    insert_cols = ", ".join(f"`{c}`" for c in cols)
    insert_vals = ", ".join(f"S.`{c}`" for c in cols)
    merge_sql = f"""
MERGE `{target_ref}` T
USING `{staging_ref}` S
ON T._record_hash = S._record_hash
WHEN MATCHED THEN UPDATE SET {update_set}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
""".strip()
    client.query(merge_sql).result()
    client.delete_table(staging_ref, not_found_ok=True)
    return LoadResult(
        rows=len(df),
        mode="merge",
        destination=target_ref,
        staging=staging_ref,
        merge_sql=merge_sql,
        dropped_staging=True,
    )


def write_run_log(
    client: Any,
    settings: Settings,
    *,
    run_id: str,
    source: str,
    rows: int,
    status: str,
    started: datetime,
    ended: datetime,
    principal: str,
    error: str | None = None,
) -> None:
    table = "_ingest_runs"
    ref = table_ref(settings, table)
    row = {
        "run_id": run_id,
        "source": source,
        "target": settings.target_dataset,
        "rows": rows,
        "status": status,
        "started": started.isoformat(),
        "ended": ended.isoformat(),
        "principal": principal,
        "git_sha": git_sha(),
        "error": error,
    }
    df = pd.DataFrame([row])
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    try:
        client.load_table_from_dataframe(df, ref, job_config=job_config).result()
    except Exception:
        # Best-effort: create then append
        schema = [
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("target", "STRING"),
            bigquery.SchemaField("rows", "INT64"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("started", "STRING"),
            bigquery.SchemaField("ended", "STRING"),
            bigquery.SchemaField("principal", "STRING"),
            bigquery.SchemaField("git_sha", "STRING"),
            bigquery.SchemaField("error", "STRING"),
        ]
        ensure_table(client, settings, table, schema, partition_field=None)  # type: ignore[arg-type]
        client.load_table_from_dataframe(df, ref, job_config=job_config).result()


def load_quarantine(
    client: Any,
    settings: Settings,
    quarantine: pd.DataFrame,
    *,
    table: str,
) -> None:
    if quarantine.empty:
        return
    qtable = f"{table}__quarantine"
    ref = table_ref(settings, qtable)
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    client.load_table_from_dataframe(quarantine, ref, job_config=job_config).result()
