# Data Pipelines (Bronze → Silver → Gold databases)

Notebooks and SQL used to **prototype and design** the medallion ETL. These are the source of truth for logic that will run in **BigQuery** against the **bronze, silver, and gold databases** (scheduled via Composer/Cloud Scheduler).

## Layout

| Layer   | Purpose |
|--------|---------|
| `ingestion/` | Scripts/notebooks for pulling data into the pipeline (e.g. from APIs, GCS, or BQ partitions). Writes into the **bronze database**. |
| `bronze/`    | **Bronze database** logic: raw, immutable. One-to-one or partitioned loads from source. |
| `silver/`    | **Silver database** logic: cleaned, normalized, deduplicated. Business-level entities. |
| `gold/`      | **Gold database** logic: aggregated, ML-ready, analytics-ready. Feeds the **feature store**. |

## Usage

- Develop and test in notebooks here (local or against the local partition DB).
- Export or translate logic to SQL in `data/sql/bronze_to_silver/` and `data/sql/silver_to_gold/` for BigQuery execution (reading/writing the bronze, silver, gold databases).
- Infra (Composer DAGs, Terraform) in `infra/` references these assets.

## Conventions

- One notebook (or SQL file) per major flow or partition. Organize by **database** (bronze/silver/gold).
- Document partition keys and incremental strategy in the notebook or a README in the layer folder.
