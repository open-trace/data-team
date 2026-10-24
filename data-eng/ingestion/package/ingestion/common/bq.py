"""BigQuery client helpers. Never creates datasets."""

from __future__ import annotations

from typing import Any

from google.cloud import bigquery

from ingestion.common.auth import get_credentials
from ingestion.common.config import Settings


def get_client(settings: Settings, client: bigquery.Client | None = None) -> bigquery.Client:
    if client is not None:
        return client
    creds, project = get_credentials(settings)
    return bigquery.Client(
        project=project or settings.project,
        credentials=creds,
        location=settings.location,
    )


def table_ref(settings: Settings, table: str) -> str:
    return f"{settings.project}.{settings.target_dataset}.{table}"


def ensure_table(
    client: bigquery.Client,
    settings: Settings,
    table: str,
    schema: list[bigquery.SchemaField],
    *,
    partition_field: str | None = "_ingest_date",
    cluster_fields: list[str] | None = None,
) -> bigquery.Table:
    """Create table if missing. Does not create datasets."""
    ref = table_ref(settings, table)
    try:
        return client.get_table(ref)
    except Exception:
        pass
    tbl = bigquery.Table(ref, schema=schema)
    if partition_field:
        tbl.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
        tbl.clustering_fields = cluster_fields or ["_source"]
    return client.create_table(tbl)


def write_test(client: bigquery.Client, settings: Settings) -> str:
    """Create and drop a temp table to verify write access."""
    name = f"_ingest_auth_test_{settings.target_dataset}"
    ref = table_ref(settings, name)
    schema = [bigquery.SchemaField("ok", "BOOL")]
    tbl = bigquery.Table(ref, schema=schema)
    client.create_table(tbl, exists_ok=True)
    client.delete_table(ref, not_found_ok=True)
    return ref


def schema_from_dataframe(df: Any) -> list[bigquery.SchemaField]:
    """Map pandas dtypes to BQ schema fields (best-effort)."""
    import pandas as pd

    fields: list[bigquery.SchemaField] = []
    for col, dtype in df.dtypes.items():
        name = str(col)
        if pd.api.types.is_bool_dtype(dtype):
            bq_type = "BOOL"
        elif pd.api.types.is_integer_dtype(dtype):
            bq_type = "INT64"
        elif pd.api.types.is_float_dtype(dtype):
            bq_type = "FLOAT64"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            bq_type = "TIMESTAMP"
        elif str(dtype) == "object" and name.endswith("_payload"):
            bq_type = "JSON"
        else:
            bq_type = "STRING"
        if name == "_ingest_date":
            bq_type = "DATE"
        if name == "_ingested_at":
            bq_type = "TIMESTAMP"
        fields.append(bigquery.SchemaField(name, bq_type))
    return fields
