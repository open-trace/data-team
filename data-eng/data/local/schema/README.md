# Local schema DDL

PostgreSQL DDL for the local partition database. Directories match BigQuery dataset ids:

| Directory | BigQuery / local schema |
|-----------|-------------------------|
| `landing/` | `landing` — transform source of truth |
| `raw_dev/` | `raw_dev` |
| `staging_dev/` | `staging_dev` |
| `intermediate_dev/` | `intermediate_dev` |
| `mart_dev/` | `mart_dev` |

Generate from BigQuery:

```bash
python data/local/scripts/bq_schema_to_local_pg.py
```

Do not use bronze/silver/gold folder names. Ingest sandbox `landing_dev` is not mirrored here by default.
