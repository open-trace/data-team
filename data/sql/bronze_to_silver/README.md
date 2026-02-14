# Bronze → Silver SQL

BigQuery SQL that reads from the **bronze database** and writes into the **silver database** (cleaned, normalized).

- Organize by domain or flow; target objects live in the silver **database**.
- Use partitioning and incremental logic where applicable.
- These scripts are run by Composer or manually in BQ; design is prototyped in `data-pipelines/silver/` notebooks.
