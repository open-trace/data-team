# HDX WFP food prices (Nigeria) — companion guide

Upstream file: **`wfp_food_prices_nga.csv`** on Humanitarian Data Exchange (HDX).  
Landing identity (full name): **`hdx_wfp_food_prices_nigeria`**.

Companion notebook: [`hdx_wfp_food_prices_nigeria_landing_dev.ipynb`](./hdx_wfp_food_prices_nigeria_landing_dev.ipynb)

---

## Concepts

### What this dataset is

WFP market food prices for **Nigeria**, published via HDX. One CSV snapshot of retail (and related) prices by market, commodity, date, and unit.

Dataset / resource on HDX (download URL used by extract):

`https://data.humdata.org/dataset/42db041f-7aaf-4ab4-961f-2a12096861e7/resource/12b51155-0cd3-4806-9924-61ede4077591/download/wfp_food_prices_nga.csv`

### Grain

One landing row ≈ one price observation keyed by:

`date` + `market_id` + `commodity_id` + `pricetype` + `unit`

### HXL header

The CSV has a second header row of HXL tags (`#date`, `#loc+market`, …). Ingest **skips** that row before `read_csv`.

### Ingestion concepts

| Concept | How |
| ------- | --- |
| Stage 1 | Notebook → `landing_dev.hdx_wfp_food_prices_nigeria` |
| Stage 2 | Existing package CLI `hdx_wfp_prices` → table `hdx_wfp_food_prices` (after review / CI for `landing`) |
| Default extract | Full CSV download |
| Notebook pilot | `PILOT = True` → first `PILOT_MAX_ROWS` (default 500) after HXL skip |
| Load | `replace_partition` load job — not `insert_rows_json` |
| Auth | SA key via `GOOGLE_APPLICATION_CREDENTIALS` (outside repo) |

---

## Landing table schema (Stage 1)

**Table:** `landing_dev.hdx_wfp_food_prices_nigeria`

### Business columns (from CSV)

| Column | Description |
| ------ | ----------- |
| `date` | Observation date |
| `market_id` / `market` | Market id and name |
| `commodity_id` / `commodity` | Commodity id and name |
| `pricetype` | e.g. Retail |
| `unit` | Unit of measure |
| `price` | Price in local currency |
| `currency` | e.g. NGN |
| `usdprice` | Price in USD (when present) |
| `latitude` / `longitude` | Market coordinates |

### Metadata columns

`_source`, `_run_id`, `_ingested_at`, `_ingest_date`, `_source_url`, `_record_hash`

---

## Lifecycle

1. **Stage 1:** run the notebook (`PILOT=False` for full file) → review `landing_dev`.
2. **Stage 2:** package already at [`../package/ingestion/sources/hdx_wfp_prices/`](../package/ingestion/sources/hdx_wfp_prices/) — CLI name `hdx_wfp_prices`, BQ table `hdx_wfp_food_prices`.

### Stage 1 verify

- Full mode: row count matches the CSV (minus HXL row).
- Pilot: ≤ `PILOT_MAX_ROWS`.
- Spot-check `date`, `market`, `commodity`, `price`, `currency`.

### Stage 2 CLI

```bash
cd data-eng/ingestion
python -m ingestion run hdx_wfp_prices --dry-run
python -m ingestion run hdx_wfp_prices
# make ingest SOURCE=hdx_wfp_prices
```
