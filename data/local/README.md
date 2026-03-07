# Local partition database (PostgreSQL)

A **local PostgreSQL database** holds a **partition** (or sample) of the data that lives in the **bronze database** (and optionally silver/gold) in BigQuery. It is used to **curate and test** the ETL pipeline (bronze → silver → gold) in the IDE without running on full production data.

We use **PostgreSQL** so the team’s local environment is close to BigQuery (full SQL, multiple connections, same mental model). SQLite remains supported if you prefer a file-based DB.

## Team collaboration

- **Recommended: one database per developer**  
  Each person runs their own PostgreSQL instance (e.g. via Docker or a local install) and sets `LOCAL_DB_URL` in their own `.env` to point at their own DB (e.g. `datateam_local` or `datateam_alice`). No shared state, no conflicts.

- **Optional: shared dev server**  
  If you use a single shared Postgres server for the team, give each developer a **separate database** (e.g. `datateam_alice`, `datateam_bob`) or a **separate schema** inside one database. Never share one database for concurrent writes.

- **What to commit**  
  Schema DDL in `schema/`, sync scripts, and this README are in git. Each developer’s `.env` and the actual data in their local DB are **not** committed.

So: collaboration is smooth as long as everyone has their **own** database (or schema); the repo only holds code and schema.

## For team members (shared repo)

When this repo is shared with the team, **everyone gets the same local DB setup**:

- **Same schema:** The table list (`scripts/bronze_tables.txt`) and schema script (`bq_schema_to_local_pg.py`) are in the repo. When any team member runs the scripts, they create the **same tables** (same columns and types) in their local Postgres, using the same BigQuery project/dataset.
- **Same workflow:** Everyone uses the same Docker Compose, same scripts, and the same `.env.example` as a template. Run `docker compose up -d`, create your DB, copy `.env.example` to `.env` and add your own BigQuery credentials and `LOCAL_DB_URL`, then run `bash data/local/scripts/populate_local_db.sh`.
- **Your own data:** Each person’s local DB is **their own copy**. The rows in it are whatever they synced from BigQuery when they ran the sync. To get similar data across the team, use the same `BQ_PARTITION_LIMIT` and filters in `.env` (or document the recommended values in the repo).

So: same structure and process for everyone; each team member syncs their own data into their own database.

## Layout

| Path | Purpose |
|------|---------|
| **`schema/`** | DDL for the local DB (PostgreSQL and optional SQLite). Mirror bronze/silver/gold **database** structures. |
| **`scripts/`** | Scripts to sync a partition from a BigQuery **database** into the local DB (e.g. `bq_partition_to_local.py`). **`engine_connector.py`** is the single entry point for all DB connections: use it when reading, writing, or creating tables/datasets. |
| **`.env.example`** | Example env vars for BQ and **PostgreSQL** `LOCAL_DB_URL` (do not commit `.env`). |

## Creating bronze tables from BigQuery

To recreate the bronze tables (from the list in `scripts/bronze_tables.txt`) on your local PostgreSQL database:

1. **Set env:** `BQ_PROJECT`, `BQ_DATASET`, and optionally `LOCAL_DB_URL` (to create tables directly on your local DB).
2. **Generate DDL and create tables:**  
   From repo root:
   ```bash
   python data/local/scripts/bq_schema_to_local_pg.py
   ```
   This fetches each table’s schema from BigQuery, writes one `.sql` file per table under `schema/bronze/`, and (if `LOCAL_DB_URL` is set) runs the DDL so the tables exist locally. Use `--write-only` to only write `.sql` files, or `--execute-only` to run existing `.sql` files against the local DB without calling BigQuery.
3. **Load data:** Sync a partition into each table with the existing script (per table) or the batch helper:
   ```bash
   python data/local/scripts/bq_partition_to_local.py --table <table_id> --target-table <table_id>
   # Or sync all listed bronze tables:
   python data/local/scripts/sync_all_bronze_tables.py
   ```
   Use `BQ_PARTITION_LIMIT` or `--limit` to control how many rows are pulled per table.

**One-shot (from repo root):** If `data/local/.env` is configured, you can run:
   ```bash
   bash data/local/scripts/populate_local_db.sh
   ```
   This loads `.env`, creates all bronze tables from BigQuery schemas, then syncs data into them.

## Usage

1. **Start PostgreSQL**  
   Use the project’s Docker Compose (see repo root `docker-compose.yml`) or your own local Postgres. Create a database for yourself (e.g. `datateam_local`).

2. **Configure**  
   Copy `.env.example` to `.env`. Set `BQ_PROJECT`, `BQ_DATASET`, and the table/partition you want. Set **`LOCAL_DB_URL`** to your Postgres URL, e.g.  
   `postgresql://postgres:postgres@localhost:5432/datateam_local`  
   Do not commit `.env` or credentials.

3. **Sync a partition**  
   From repo root:
   ```bash
   python data/local/scripts/bq_partition_to_local.py
   ```
   This queries BigQuery (with your partition filter) and loads the result into your local Postgres. Run again to refresh.

4. **Curate ETL**  
   Use the local DB in notebooks under `data-pipelines/` (bronze, silver, gold) or in Python: connect with the same `LOCAL_DB_URL` and develop transforms. When logic is ready, translate it to `data/sql/bronze_to_silver/` and `data/sql/silver_to_gold/` for BigQuery.

## Database options

- **PostgreSQL (recommended):** Set `LOCAL_DB_URL` in `.env`. Requires `psycopg2-binary` and `sqlalchemy` (in `requirements.txt`). Use one DB per developer (or one schema per developer on a shared server).
- **SQLite:** Leave `LOCAL_DB_URL` unset and set `LOCAL_DB_PATH` (e.g. `data/local/local.db`) if you prefer a file-based DB.

## Conventions

- Do **not** commit `.env` or the local DB data (credentials and DB contents are in `.gitignore` or live only on your machine).
- Keep **schema** in `schema/` so everyone can create the same structure.
- Document which partition (e.g. date range) the team is using in a small README or in the script’s default query.
