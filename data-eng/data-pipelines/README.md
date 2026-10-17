# Data Pipelines (design notebooks)

Notebooks used to **prototype** ETL. Production transforms run in **dbt** (transform-v2). Runtime loads use [`../ingestion/`](../ingestion/) → `landing_dev` / `raw_dev`.

## Layer folders

| Folder | Purpose |
|--------|---------|
| `raw_dev/` | Design for **raw_dev** |
| `staging_dev/` | Design for **staging_dev** |
| `intermediate_dev/` | Design for **intermediate_dev** |
| `mart_dev/` | Design for **mart_dev** |

Promote logic into `dbt/models/`. Do not treat notebooks as production.
