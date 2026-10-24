# World Bank Data360 Harmonized Country Poverty — companion guide

Upstream API database: **WB_HCP**. Landing identity (full name): **`world_bank_data360_harmonized_country_poverty`**.

Companion notebook: [`world_bank_data360_harmonized_country_poverty_landing_dev.ipynb`](./world_bank_data360_harmonized_country_poverty_landing_dev.ipynb)

---

## Conventions (all ingest work)

1. **Full dataset by default** for package/CLI and for notebook loads. Pilot slices are **opt-in** on the notebook only (`PILOT = True`).
2. **Full names** for source folder, CLI name, BigQuery table, notebook, and MD:  
   `{organization}_{product_or_platform}_{dataset_full_name}`  
   Example: `world_bank_data360_harmonized_country_poverty` (not `wb_hcp_data360`).

---

## Concepts

### What this dataset is

**Harmonized Country Poverty (HCP)** on World Bank **Data360** — SDMX-style observations (indicator × geography × breakdowns × time).

API UI: [Data360 WB_HCP](https://data360.worldbank.org/en/api?datasetid=WB_HCP#/).  
Endpoint: `https://data360api.worldbank.org/data360/data`.

### Grain

One landing row = one observation keyed by:

`database_id` + `indicator` + `ref_area` + `sex` + `age` + `time_period`

### Full dump size

- `DATABASE_ID=WB_HCP` alone → about **616k** observations.
- API returns **1000** rows per page; paginate with **`skip` + `top`** (not `offset`/`limit`).

### Ingestion concepts

| Concept | How |
| ------- | --- |
| Stage 1 | Notebook → `landing_dev` (review) |
| Stage 2 | Package CLI → `landing` after review (CI guards) |
| Default extract | Full DB: `DATABASE_ID=WB_HCP` + skip/top pages |
| Notebook pilot | `PILOT = True` → one indicator, three countries, years ≥ 2020 |
| Load | `replace_partition` load job — not `insert_rows_json` |
| Auth | SA key via `GOOGLE_APPLICATION_CREDENTIALS` (outside repo) |

---

## Landing table schema

**Table:** `landing_dev.world_bank_data360_harmonized_country_poverty` (Stage 1) / `landing.…` (Stage 2)

### Business columns

| Column | Required | Description |
| ------ | -------- | ----------- |
| `database_id` | yes | Upstream DB (`WB_HCP`) |
| `indicator` | yes | Indicator code |
| `ref_area` | yes | ISO3 area |
| `sex` / `age` / `urbanisation` | no | Breakdowns (`_T`, `_Z`, …) |
| `time_period` | yes | Period label (year) |
| `time_period_date` | no | Parsed year → Jan 1 |
| `freq` | no | Frequency |
| `obs_value` | yes | Numeric value |
| `unit_measure` / `obs_status` / `data_source` | no | Units / status / provenance |

### Metadata columns

`_source`, `_run_id`, `_ingested_at`, `_ingest_date`, `_source_url`, `_record_hash`

---

## Lifecycle

1. **Stage 1:** notebook + this MD → `landing_dev` (full dump unless `PILOT=True`).
2. **Stage 2:** package `world_bank_data360_harmonized_country_poverty` → `landing`.

### Stage 1 setup

```bash
cd data-eng
pip install -e "./ingestion[dev]"   # optional if using notebook bootstrap + data-eng/venv
```

`.env`: project, `BQ_LOCATION=europe-west3`, SA key path outside the repo.

### Verify Stage 1

- `PILOT=False`: row count near API `count` (~616k).
- `PILOT=True`: small slice for KEN/NGA/GHA + one indicator.

### Stage 2

```bash
cd data-eng/ingestion
set INGEST_OFFLINE_FIXTURES=1
python -m ingestion run world_bank_data360_harmonized_country_poverty --dry-run

# Prod (CI): INGEST_ALLOW_PROD=1 and CI=true
python -m ingestion run world_bank_data360_harmonized_country_poverty --target landing
```
