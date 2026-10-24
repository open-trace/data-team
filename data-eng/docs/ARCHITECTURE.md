# Architecture (active pointer)

**Canonical narrative:** [`../README.md`](../README.md) and [`../CONTEXT.md`](../CONTEXT.md).

**Transform-v2:** Ingestion writes `landing_dev` and `raw_dev`. dbt treats `raw_dev` as the primordial source through `mart_dev`. Python `ingestion/` only — Airbyte is archived.

**ADRs:** [`adr/`](./adr/).

**Historical docs** (bronze / Airbyte era): [`archive/`](./archive/).
