# OpenTrace ingestion

Repo-based BigQuery ingestion. Under **transform-v2**, this module **writes `landing_dev` and `raw_dev`**.

- **`landing_dev`** — default load target (sandbox; partitions expire; shared SA key).
- **`raw_dev`** — ingest-owned source-aligned dataset; **primordial source for dbt**.

Transformation (`dbt/`) reads `raw_dev` through `mart_dev`. It does not own writing raw long-term. Existing dbt models under `models/landing` and `models/raw_dev` may still materialize tables during soft cutover — leave them alone unless migrating a table into this package.

Design record: [INGESTION_PLAN.md](./INGESTION_PLAN.md). ADRs: [`../docs/adr/`](../docs/adr/). Glossary: [`../CONTEXT.md`](../CONTEXT.md).

## Folder ownership

| Path | Role |
|------|------|
| **`data-eng/ingestion/`** (this package) | Writes `landing_dev` / `raw_dev` |
| `data-eng/dbt/` | Transforms: primordial `raw_dev` → `staging_dev` → `intermediate_dev` → `mart_dev` |
| `data-eng/data-pipelines/` | Design notebooks only |
| `data-eng/archive/airbyte/` | Retired Airbyte config (not active) |

## Team onboarding (shared SA key)

### Admin (once)

1. Run [`../infra/ingestion/setup.sh`](../infra/ingestion/setup.sh) — creates `ingest-dev`, grants write on **`landing_dev`** (extend IAM for `raw_dev` when ingest jobs write raw), creates a key file **outside** the repo.
2. Share the key path securely with the team (not via git). Rotate on departure / every 90 days.

### Each engineer

1. Place the shared key at e.g. `~/.config/opentrace/ingest-dev.json` (**never** inside the repo).
2. From `data-eng/`:

```bash
cp ingestion/.env.example ingestion/.env
# Edit .env: set GOOGLE_APPLICATION_CREDENTIALS to your key path
make ingest-install
make ingest-auth
make ingest-dry SOURCE=hdx_wfp_prices
make ingest SOURCE=hdx_wfp_prices ARGS="--limit 1000"
```

Or use Cursor launch configs in [`../.vscode/launch.json`](../.vscode/launch.json).

## Commands

```bash
cd data-eng/ingestion
pip install -e ".[dev]"

python -m ingestion list
python -m ingestion check-auth
python -m ingestion check-auth --write-test
python -m ingestion run hdx_wfp_prices --dry-run --limit 200
python -m ingestion run hdx_wfp_prices --limit 1000
```

Target precedence: `--target` > `INGEST_TARGET_DATASET` > `landing_dev`.  
`--target landing` fails locally unless `CI=true` and `INGEST_ALLOW_PROD=1`.

Offline: `INGEST_OFFLINE_FIXTURES=1` reads `tests/fixtures/`.

## Add a new source

1. Copy `ingestion/sources/_template/` → `ingestion/sources/<name>/`.
2. Edit `source.yaml`; implement `extract` / `transform`.
3. Prefer writing through to **`raw_dev`** when the source is ready for transforms; use `landing_dev` for sandbox iteration.
4. Register tables in `dbt/models/sources_ingestion.yml` as needed; regenerate `dbt/catalog/sources.generated.yml` via `generate_dbt_sources.py`.
5. Do **not** hand-edit the generated catalog.

## Safety

- Never commit `.env`, key JSON, or `.out/`.
- `GOOGLE_APPLICATION_CREDENTIALS` must point **outside** the repo.
- Ingest owns `landing_dev` + `raw_dev`. No Airbyte.
