---
status: accepted
---

# Python ingest only (Airbyte retired from active path)

Airbyte was the documented load path into BigQuery `landing`, but the working stack is the repo package `data-eng/ingestion/` writing `landing_dev` (and, under transform-v2, `raw_dev`). Keeping both stories caused engineers to follow dead compose/registry docs. We retire Airbyte from the active tree (`archive/airbyte/`) and document Python ingest as the only load path. Composer DAGs that called Airbyte are legacy until replaced by ingest jobs.
