# Data

Structured assets for ingestion, SQL transforms, validation, and the **local partition database**. Aligns with the **bronze, silver, and gold databases** and the **feature store** in BigQuery, and with `data-pipelines/` notebooks.

## Layout

- **`ingestion/`** — Configs and scripts per source (weather, market, satellite). Feeds the **bronze database**.
- **`sql/`** — BigQuery-oriented SQL that operates on **databases** (not ad-hoc tables):
  - `bronze_to_silver/` — Transforms from the bronze database to the silver database.
  - `silver_to_gold/` — Transforms from the silver database to the gold database (analytics & ML).
- **`validation/`** — Data quality checks (expectations, unit tests on schema/sample data).
- **`local/`** — Local DB for a partition of BQ data: schema, sync scripts, and `.env.example`. Used to curate ETL in the IDE.

## BigQuery schema catalog (aligned schema)

To keep dbt, local DDL, and ingestion aligned with BigQuery, the repo can hold a **single schema snapshot** of all pipeline datasets (landing, bronze, silver, gold):

- **Script:** `data/local/scripts/bq_schema_catalog.py` — Connects to BigQuery (using `BQ_PROJECT` and `BQ_DATASET_*` from `data/local/.env`), lists tables in each dataset, and dumps each table’s column name/type/mode.
- **Outputs (in `docs/`):**
  - **`docs/bq_schema_catalog.json`** — Machine-readable catalog: `project`, `datasets` (landing, bronze, silver, gold), each with `tables` and `columns`.
  - **`docs/BIGQUERY_SCHEMA.md`** — Human-readable summary (tables and columns per dataset).

Run from repo root (after setting `BQ_PROJECT` and credentials in `data/local/.env`):

```bash
python data/local/scripts/bq_schema_catalog.py
```

Use the generated catalog to curate dbt `sources.yml`, local `data/local/schema/` DDL, and ingestion config so they match the live BQ schema. Re-run the script whenever BigQuery schema changes.

**dbt sources:** To keep `dbt/models/sources.yml` in sync with BQ without hand-maintaining tables, run the catalog script above, then:

```bash
python data/local/scripts/generate_dbt_sources.py
```

Or refresh the catalog and generate sources in one go: `python data/local/scripts/generate_dbt_sources.py --refresh`.

## Flow

1. **Engineers** ingest data on GCP → **bronze database** (and beyond) in **BigQuery**.
2. **Analysts** do EDA and produce **reports**; the team decides how to shape the pipeline.
3. A **partition** of the data is synced into the **local database** (`data/local/`). Pipeline developers use it to curate ETL (notebooks + SQL) locally, then promote logic to `data/sql/` for BigQuery.
4. `sql/bronze_to_silver/` and `sql/silver_to_gold/` run in BigQuery against the bronze/silver/gold **databases** (manually or via Composer).
5. Validation runs in CI or in Composer after pipeline steps.
