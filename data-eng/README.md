# Data engineering (`data-eng/`)

OpenTrace analytics engineering: **Python ingestion** into BigQuery, then **dbt** transforms. ML/RAG lives under `ml-eng/` (or repo `ml/`), not here.

## Transform-v2 contract (current)

| Owner | Datasets | Role |
| ----- | -------- | ---- |
| **Ingestion** (`ingestion/`) | `landing_dev`, `raw_dev` | Writes both |
| **Transformation** (`dbt/`) | `raw_dev` → `staging_dev` → `intermediate_dev` → `mart_dev` | `raw_dev` is the **primordial source** |

```text
External sources → ingestion → landing_dev → raw_dev
                                      ↓
                    dbt: raw_dev → staging_dev → intermediate_dev → mart_dev
```

- **Airbyte is not used.** Legacy Airbyte config (if present) lives under `archive/airbyte/` only.
- **Soft cutover:** existing dbt SQL (including transitional `models/landing` and `models/raw_dev` runners) is unchanged at transform-v2.0.0. New loads should go through `ingestion/` into `landing_dev` / `raw_dev`.
- Dataset name `landing` (no `_dev`) may still appear in older dbt sources; it is not the primary ingest target.

### Pipeline versions

| Version | Meaning |
| ------- | ------- |
| `transform-v1.0.0` | Git tag baseline before this contract |
| `transform-v2.0.0`+ | Current contract; see `version` in `dbt/dbt_project.yml` |

Tag releases as `transform-vX.Y.Z`. Bump patch for docs/fixes, minor for additive models/tests, major only if mart/source contracts break. Rollback = checkout the tag and re-run dbt.

Glossary: [`CONTEXT.md`](./CONTEXT.md). ADRs: [`docs/adr/`](./docs/adr/).

## Layout

| Path | Role |
| ---- | ---- |
| **`ingestion/`** | Load → `landing_dev` and (contract) `raw_dev` |
| **`dbt/`** | Transforms; hand sources in `models/`; generated catalog in `catalog/` |
| **`data/local/`** | Local Postgres partition sync (path may move to `local/` later) |
| **`infra/`** | Terraform + ingest SA setup |
| **`ci/`** | data-eng CI notes |
| **`docs/`** | Architecture + analyst mart guides |
| **`docs/archive/`** | Bronze / Airbyte-era guides |
| **`archive/`** | Removed active paths (e.g. Airbyte tree) |
| **`scripts/`** | Dictionary / guide generators |
| **`notebooks/`**, **`data-pipelines/`** | Scratch / design only — not production |

## Day-to-day

```bash
cd data-eng
make ingest-install && make ingest-auth
make ingest-dry SOURCE=hdx_wfp_prices
make ingest SOURCE=hdx_wfp_prices ARGS="--limit 1000"

# dbt (from data-eng/dbt)
dbt run --target staging_dev
dbt run --target mart_dev
```

Regenerate the BigQuery source catalog (not hand-edited):

```bash
python data/local/scripts/generate_dbt_sources.py --refresh
# writes dbt/catalog/sources.generated.yml (gitignored)
```

See [`ingestion/README.md`](./ingestion/README.md) and [`dbt/README.md`](./dbt/README.md).
