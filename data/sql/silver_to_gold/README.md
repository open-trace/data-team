# Silver → Gold SQL

BigQuery SQL that reads from the **silver database** and writes into the **gold database** (aggregations, ML-ready, analytics-ready). Gold feeds the **feature store**.

- Organize by domain or use case; target objects live in the gold **database**.
- Design is prototyped in `data-pipelines/gold/` notebooks.
