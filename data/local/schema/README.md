# Local DB schema

DDL for the local partition database (PostgreSQL recommended). Schema here should mirror (or simplify) the **bronze, silver, and gold databases** in BigQuery so that ETL developed locally can be promoted to `data/sql/` with minimal changes.

- **PostgreSQL:** use `*_pg.sql` (e.g. `bronze_example_pg.sql`). Run against your local DB (e.g. `psql` or your IDE).
- **SQLite:** use `bronze_example.sql` if you use SQLite instead of Postgres.
- Use one `.sql` file per layer or entity. After changing schema, re-run the partition sync script or re-create the DB and load again.
