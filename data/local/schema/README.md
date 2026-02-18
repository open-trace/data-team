# Local DB schema

DDL for the local partition database (PostgreSQL recommended). Schema here should mirror (or simplify) the **bronze, silver, and gold databases** in BigQuery so that ETL developed locally can be promoted to `data/sql/` with minimal changes.

- **Bronze tables:** The `bronze/` directory holds one `.sql` file per bronze table. These can be **generated** by the schema-discovery script: run `python data/local/scripts/bq_schema_to_local_pg.py` (with `BQ_PROJECT`, `BQ_DATASET` set). The script fetches BigQuery schemas and writes PostgreSQL DDL to `bronze/*.sql`. You can then run those files against your local DB (e.g. `psql -f ...`) or run the script with `LOCAL_DB_URL` set to create tables directly.
- **PostgreSQL (other):** use `*_pg.sql` (e.g. `bronze_example_pg.sql`) for hand-maintained DDL. Run against your local DB (e.g. `psql` or your IDE).
- **SQLite:** use `bronze_example.sql` if you use SQLite instead of Postgres.
- Use one `.sql` file per layer or entity. After changing schema, re-run the partition sync script or re-create the DB and load again.
