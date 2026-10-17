# Ontology v3 — Ask ADZA integration

Offline agricultural ontology (`ml-eng/opentrace_ontology/`) wired into Ask ADZA behind feature flags that all default **OFF**. This PR does **not** enable routing/filter/BQ/ACF in production; it lands the package, adapter, seams, and offline acceptance suite.

## Vernacular enrichment (amended)

| Metric | Baseline (post africa_core) | After amendments |
|--------|----------------------------|------------------|
| Non-eng `preferred` Latin-looking | ~50% | **0%** (demote to `scientific`, keep language; **0 rows deleted**) |
| Alias row count | 322,818 | **322,820** (+2 QID-only Wikidata) |
| Priority vernacular gaps | 0 | **0** |
| `review_status=unreviewed` (African Wikidata) | — | **420** |
| sqlite size | 56.48 MB | **57.54 MB** (+1.9%, gate &lt;25%) |
| Matcher cold-start | 0.30 s | **0.30 s** (gate &lt;1.0 s) |
| `plan_query` warm p95 | 0.13 ms | **0.12 ms** |
| Value-map coverage | 5358 / 0 unmapped / 25 allowlisted | unchanged |

Rules: demote-only Latin cleanup; Wikidata join **only** via `ontology_codes.scheme=wikidata` QID; `SOURCE_RANK` curated &gt; repo &gt; faostat &gt; wikidata &gt; agrovoc &gt; generated; short locals (`ji`/`oka`/`ata`) need `lang=`. Package tests: **98 passed, 1 skipped**.

## Deploy (Railway, no Git LFS)

- **`ml-eng/opentrace_ontology/seeds/ontology_aliases.csv.gz`** is the package form (~1.9 MB); plain `ontology_aliases.csv` is not committed under ml-eng.
- **`data-eng/dbt/seeds/ontology/ontology_aliases.csv`** stays plain CSV for dbt; `sync_dbt_seeds.py --check` compares **decompressed** SHA256.
- Other seeds remain plain `.csv` in both trees. No Git LFS.
- Generated `ml-eng/opentrace_ontology/data/` (sqlite, matcher keys, reports) is **not** committed; Docker `RUN` bakes sqlite from seeds, then removes `seeds/`, `tests/`, `bench/`.
- All ontology feature flags default **OFF**. After deploy, confirm Railway logs contain `ontology ok`.

## Summary

- Package lives under **`ml-eng/opentrace_ontology/`** only (not the repo-root `opentrace_ontology/` bundle).
- RAG adapter: `ml-eng/ml/rag/chatbot/ontology_routing.py` — sole import site for `plan_query`.
- Seams: Qdrant `ontology_ids` filter + cascade drop, BQ value-map literals + `validate_literals`, profile country → `clean_profile_country` / `sanitize_profile_country`, ACF `source_basis` cap, optional ingest tagging + backfill script.
- Offline acceptance: `ml-eng/ml/rag/tests/chatbot/test_ontology_v3_acceptance.py` (§11 a–f).

## Flags (all default OFF)

| Flag | Default | Effect |
|------|---------|--------|
| `ADZA_ONTOLOGY_ROUTING` | `0` | Call `plan_query` once per turn; cache on state |
| `ADZA_QDRANT_ONTOLOGY_FILTER` | `0` | `MatchAny` on payload `ontology_ids` |
| `ADZA_ONTOLOGY_QUERY_VIEW` | `0` | Extra Qdrant query view for `qdrant_only` subjects |
| `ADZA_ONTOLOGY_TAGGING` | `0` | Tag new chunks at ingest with `ontology_ids` |
| `ADZA_ONTOLOGY_BQ_VALUES` | `0` | Pass value-map literals into NL2SQL (`ALLOWED FILTER LITERALS`) |
| `ADZA_ONTOLOGY_BQ_STRICT` | `0` | Reject SQL literals not in the value map |
| `ADZA_ONTOLOGY_SKIP_BQ` | `0` | Skip BQ when plan says `run_bigquery` is false |
| `ADZA_ACF_SOURCE_BASIS_CAP` | `0` | Cap ACF band by cited source kinds / `source_basis` |
| `ADZA_SINGLE_COUNTRY_POLICY` | `0` | Hard Farmers geo override (pre-existing; stays off) |

Documented in `ml-eng/deploy/PRODUCTION_ENV.md` and `ml-eng/opentrace_ontology/README.md`.

Profile country cleaning (ISO2/name → African ISO3, drop non-African) runs at the API / `geo_policy` boundary **regardless** of the ontology routing flags.

## Rollout order

Enable only after payload KEYWORD indexes exist and a dry-run backfill has tagged collections:

1. `ADZA_ONTOLOGY_ROUTING=1` — plan once per turn (safe; no retrieval change until other flags on)
2. After backfill + indexes: `ADZA_QDRANT_ONTOLOGY_FILTER=1`, then `ADZA_ONTOLOGY_QUERY_VIEW=1`; keep `ADZA_ONTOLOGY_TAGGING=1` for new ingest
3. `ADZA_ONTOLOGY_BQ_VALUES=1` (optional `ADZA_ONTOLOGY_BQ_STRICT=1`, `ADZA_ONTOLOGY_SKIP_BQ=1`)
4. `ADZA_ACF_SOURCE_BASIS_CAP=1`

Backfill (rotated Qdrant key; dry-run first):

```bash
cd ml-eng
PYTHONPATH=. python -m ml.rag.scripts.backfill_ontology_tags --collection news_data --dry-run
```

## Path drift vs prompt / plan

| Prompt / plan expectation | Actual in this tree |
|---------------------------|---------------------|
| Package / seeds / build under `ml-eng/opentrace_ontology/` | Confirmed; **do not** import or commit repo-root `opentrace_ontology/` |
| Ingestion tagging test | `ml-eng/ml/rag/tests/text_processors/test_ingestion_ontology_tags.py` (not under `chatbot/`) |
| Cascade ontology drop | Covered in `ml-eng/ml/rag/tests/chatbot/test_vector_cascade.py` (`test_cascade_second_attempt_drops_ontology_ids`) |
| Makefile `make ontology` | Paths assume cwd = **repo root** (`PYTHONPATH=ml-eng python ml-eng/opentrace_ontology/...`); for local pytest use `cd ml-eng` + `PYTHONPATH=.` |
| Coverage report | Written under `ml-eng/opentrace_ontology/data/reports/` by `check_value_index_coverage.py` |
| Acceptance geography `["NGA"]` | Asserted via `plan_query` / `ontology_plan_for_state` → `countries` / `african_countries`; query decomposer still surfaces display name `"Nigeria"` until ISO3 normalize at retrieval |

## Offline acceptance (§11)

| Case | Covered by |
|------|------------|
| (a) casssava Nigeria, Farmers, Albania profile | `test_a_cassava_nigeria_farmers_profile_albania` |
| (b) mushroom Ghana → documents_only | `test_b_mushroom_ghana_documents_only` |
| (c) zorblax Togo → web_fallback | `test_c_zorblax_togo_web_fallback` |
| (d) maize prices this year + profile | `test_d_maize_prices_this_year_profile_defaults` |
| (e) all flags off | `test_e_all_flags_off_returns_none` |
| (f) latency + coverage gate | `test_f_plan_query_warm_p95_and_coverage` |

Warm `plan_query` budget: prefer p95 &lt; 1 ms; Windows allowance 5 ms in the acceptance test.

## Test commands and captured output

### §10 pytest suite (2026-10-16, Windows)

```powershell
cd ml-eng
$env:PYTHONPATH = "."
pytest opentrace_ontology/tests `
  ml/rag/tests/chatbot/test_ontology_routing.py `
  ml/rag/tests/retrievers/test_qdrant_filter_ontology.py `
  ml/rag/tests/retrievers/test_bq_value_literals.py `
  ml/rag/tests/chatbot/test_country_precedence.py `
  ml/rag/tests/chatbot/test_api_profile_country.py `
  ml/rag/tests/chatbot/test_acf_source_basis_cap.py `
  ml/rag/tests/chatbot/test_ontology_v3_acceptance.py `
  ml/rag/tests/text_processors/test_ingestion_ontology_tags.py `
  ml/rag/tests/chatbot/test_vector_cascade.py `
  -q --tb=line -rs
```

```
...........................................s............................ [ 53%]
...............................................................          [100%]
=========================== short test summary info ===========================
SKIPPED [1] opentrace_ontology\tests\test_v2_sqlite.py:66: fork start method unavailable (e.g. Windows)
134 passed, 1 skipped in 2.93s
```

### Value-index coverage gate

```powershell
cd ml-eng
$env:PYTHONPATH = "."
python opentrace_ontology/ontology_build/check_value_index_coverage.py --mvi ml/rag/value_index/mart_value_index.json
```

```
values=5358 ontology=2272 minted=3061 allowlisted=25 unmapped=0
```

### `plan_query` warm bench (n=200)

```
n=200 mean_ms=0.210 p95_ms=0.133 max_ms=21.487
```

(p95 ≪ 1 ms; mean skewed by a single cold/outlier sample.)

## Test plan (reviewer)

- [ ] Re-run the §10 pytest command above on a clean checkout after `make ontology` (or rely on `opentrace_ontology/tests/conftest.py` session build)
- [ ] Confirm all flags remain `0` in production env templates
- [ ] Dry-run backfill before enabling `ADZA_QDRANT_ONTOLOGY_FILTER`
- [ ] Do not merge/import the repo-root `opentrace_ontology/` scratch bundle into the runtime path
