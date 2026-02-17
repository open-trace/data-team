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

## Layout

| Path | Purpose |
|------|---------|
| **`schema/`** | DDL for the local DB (PostgreSQL and optional SQLite). Mirror bronze/silver/gold **database** structures. |
| **`scripts/`** | Scripts to sync a partition from a BigQuery **database** into the local DB (e.g. `bq_partition_to_local.py`). |
| **`.env.example`** | Example env vars for BQ and **PostgreSQL** `LOCAL_DB_URL` (do not commit `.env`). |

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
