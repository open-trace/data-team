# Engineer guide: ingest a new dataset (A→Z)

Assumes you just cloned `data-team`. Goal: land data in **`landing_dev`** with **two files** (notebook + MD). Full dump by default; pilot only if you opt in.

Also mirrored in [`README.md`](./README.md). PDF: [`ONBOARDING.pdf`](./ONBOARDING.pdf).

---

### A. Clone and open

```text
git clone <repo>
# Open the data-team folder in Cursor
```

### B. Create / activate `data-eng` venv

```powershell
cd data-eng
python -m venv venv
.\venv\Scripts\Activate.ps1
```

Prefer this interpreter for notebooks when Cursor lets you pick it.

### C. Install the ingestion package

```powershell
pip install -e "./ingestion[dev]"
```

### D. Configure `.env`

```powershell
copy ingestion\.env.example ingestion\.env
```

Edit `ingestion/.env`:

- `GCP_PROJECT`, `BQ_LOCATION=europe-west3`
- `INGEST_TARGET_DATASET=landing_dev`
- `GOOGLE_APPLICATION_CREDENTIALS` = path to the shared SA JSON **outside** the repo  
  (never under `data-team/`)
- Leave `INGEST_IMPERSONATE_SA` unset for key-only auth

### E. Place the SA key outside the repo

Example:

`C:\Users\<you>\.config\opentrace\ingest-dev.json`

### F. Check auth

```powershell
cd ingestion
python -m ingestion check-auth
```

Expect `kind: key_file` and `target: landing_dev`.

### G. Pick a full name

Pattern:

`{organization}_{product_or_platform}_{dataset_full_name}`

Example: `world_bank_data360_harmonized_country_poverty`

Use that same string for notebook, MD, and (later) package/table.

### H. Copy an example as a template

Use:

- Notebook: [`world_bank_data360_harmonized_country_poverty_landing_dev.ipynb`](./world_bank_data360_harmonized_country_poverty_landing_dev.ipynb)
- MD: [`world_bank_data360_harmonized_country_poverty.md`](./world_bank_data360_harmonized_country_poverty.md)
- Bootstrap: keep using [`_bootstrap.py`](./_bootstrap.py) (don’t reinvent)

Rename copies to your full name, e.g.:

- `…/<your_full_name>_landing_dev.ipynb`
- `…/<your_full_name>.md`

### I. Write the companion MD first (or in parallel)

Must include:

- What the source/dataset is and **grain** (what one row means)
- Column dictionary (business + `_` metadata)
- How extract works (URL, paging, auth quirks)
- Full dump is default; how to set `PILOT = True`
- How to verify in BigQuery

### J. Notebook setup cell

- Call `_bootstrap` so `import ingestion` works even on the wrong kernel
- `get_settings(cli_target="landing_dev")` + `ensure_auth_safe`
- `TABLE` / `SOURCE_NAME` = your full name
- `PILOT = False` by default

### K. Extract (full by default)

- Pull the **whole** dataset (page if the API caps page size)
- Only if exploring: `PILOT = True` with a small filter — not the permanent default

### L. Transform / normalize

- Flat columns suitable for landing
- Coerce types; keep stable identity keys for hashing

### M. Add ingest metadata

Use shared helpers:

```python
add_metadata(df, source=SOURCE_NAME, run_id=run_id, source_url=..., key_cols=[...])
```

### N. Preview locally (optional)

Write parquet under `ingestion/.out/` before BQ.

### O. Load to BigQuery

```python
load_dataframe(client, settings, ready, table=TABLE, mode="replace_partition", run_id=run_id)
```

Use the package load job path — **not** `insert_rows_json`.

### P. Verify in BigQuery

- Table: `landing_dev.<your_full_name>`
- Row count ≈ source full count (if `PILOT=False`)
- Spot-check keys, nulls, sample values

### Q. Review

Someone reviews **notebook + MD + `landing_dev` table**. Do not jump to prod yet.

### R. (Later) Stage 2 — package

Only after review:

- Add `package/ingestion/sources/<your_full_name>/` (`source.yaml`, `extract.py`, `transform.py`)
- Same full name; full dump default
- Small fixture + test
- Add to launch.json pick list

### S. Package dry-run

```powershell
cd data-eng\ingestion
$env:INGEST_OFFLINE_FIXTURES=1
python -m ingestion run <your_full_name> --dry-run
```

### T. Package live to `landing_dev` (optional while validating CLI)

```powershell
python -m ingestion run <your_full_name>
# or from data-eng: make ingest SOURCE=<your_full_name>
```

### U. Prod `landing` only via CI

Needs `INGEST_ALLOW_PROD=1` and `CI=true`. Do not set that for local day-to-day work.

### V. What you should **not** do

- Put SA keys in the repo
- Start with package/CLI into `landing`
- Use short cryptic names as the primary identity
- Make a pilot slice the package default
- Hand-roll BQ streaming inserts for landing loads

### W. Reference example

Working end-to-end pattern:

`ingestion/notebooks/world_bank_data360_harmonized_country_poverty_*`

### X. Layout reminder

| Path | Role |
| ---- | ---- |
| `data-eng/ingestion/notebooks/` | Stage 1 (your two files) |
| `data-eng/ingestion/package/` | Shared helpers + Stage 2 sources |
| `data-eng/ingestion/.env` | Local config (not committed secrets) |

### Y. Mental model

```text
clone → env + SA key → notebook + MD → landing_dev (full)
                              ↓ review
                    package source → landing (CI)
```

### Z. Done when

- Full (or intentional pilot) data is in `landing_dev.<full_name>`
- MD explains data + ingest concepts
- Review passed; Stage 2 only if/when promoting
