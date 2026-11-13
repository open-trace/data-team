---
status: accepted
---

# Transform-v2 ownership: ingest writes landing_dev and raw_dev

We cut pipeline version **transform-v2** (`dbt_project` 2.x, git tag `transform-v2.*`) with this contract: **ingestion** writes `landing_dev` and `raw_dev`; **transformation** treats `raw_dev` as the primordial source through `mart_dev`. Alternatives were (a) dbt owning landing→raw forever, or (b) a parallel `dbt-v2/` tree. We rejected (b) for drift and kept one `dbt/` tree; v1 is recoverable via tag `transform-v1.0.0`. Soft cutover leaves existing model SQL unchanged at 2.0.0.
