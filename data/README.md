# Data

Structured assets for ingestion, SQL transforms, validation, and the **local partition database**. Aligns with the **bronze, silver, and gold databases** and the **feature store** in BigQuery, and with `data-pipelines/` notebooks.

## Layout

- **`ingestion/`** — Configs and scripts per source (weather, market, satellite). Feeds the **bronze database**.
- **`sql/`** — BigQuery-oriented SQL that operates on **databases** (not ad-hoc tables):
  - `bronze_to_silver/` — Transforms from the bronze database to the silver database.
  - `silver_to_gold/` — Transforms from the silver database to the gold database (analytics & ML).
- **`validation/`** — Data quality checks (expectations, unit tests on schema/sample data).
- **`local/`** — Local DB for a partition of BQ data: schema, sync scripts, and `.env.example`. Used to curate ETL in the IDE.

## Flow

1. **Engineers** ingest data on GCP → **bronze database** (and beyond) in **BigQuery**.
2. **Analysts** do EDA and produce **reports**; the team decides how to shape the pipeline.
3. A **partition** of the data is synced into the **local database** (`data/local/`). Pipeline developers use it to curate ETL (notebooks + SQL) locally, then promote logic to `data/sql/` for BigQuery.
4. `sql/bronze_to_silver/` and `sql/silver_to_gold/` run in BigQuery against the bronze/silver/gold **databases** (manually or via Composer).
5. Validation runs in CI or in Composer after pipeline steps.
