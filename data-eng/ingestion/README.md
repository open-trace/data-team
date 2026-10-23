# Ingestion

Load external data into BigQuery **`landing_dev`** and **`raw_dev`** (transform-v2).

## New engineer (ingest a dataset)

A→Z clone → first load: [`notebooks/ONBOARDING.md`](./notebooks/ONBOARDING.md) (also in [`notebooks/README.md`](./notebooks/README.md)).

## Simple workflow

1. **Notebook** — try a load under [`notebooks/`](./notebooks/) (copy the World Bank Data360 HCP example; see onboarding).
2. **Script** — reuse with [`scripts/run_source.py`](./scripts/run_source.py) or Make targets below.
3. **Package** — shared CLI/sources under [`package/`](./package/) (Stage 2 after review; not the first step).

```text
notebook / script  →  package (python -m ingestion)  →  landing_dev / raw_dev
                                                              ↓
                                                         dbt → mart_dev
```

## Setup (once per machine)

1. Admin: [`../infra/ingestion/setup.sh`](../infra/ingestion/setup.sh) for the shared `ingest-dev` key (outside the repo).
2. From `data-eng/`:

```bash
cp ingestion/.env.example ingestion/.env
# set GOOGLE_APPLICATION_CREDENTIALS to the key path
pip install -e "./ingestion[dev]"
make ingest-auth
```

## Commands

```bash
cd data-eng

make ingest-list
make ingest-dry SOURCE=hdx_wfp_prices
make ingest SOURCE=hdx_wfp_prices ARGS="--limit 1000"

# equivalent script form
python ingestion/scripts/run_source.py hdx_wfp_prices --dry-run --limit 500
python -m ingestion run hdx_wfp_prices --limit 1000
```

Cursor launch configs: [`../.vscode/launch.json`](../.vscode/launch.json).

## Layout

| Path | Role |
| ---- | ---- |
| `notebooks/` | Discovery and load try-outs |
| `scripts/` | Reusable runners (`run_source.py`, `make_fixtures.py`) |
| `package/ingestion/` | Installable CLI + sources (internals) |
| `tests/` | Pytest |

Design notes: [INGESTION_PLAN.md](./INGESTION_PLAN.md). Glossary: [`../CONTEXT.md`](../CONTEXT.md).
