# OpenTrace ontology (Ask ADZA package)

Offline agricultural ontology for Ask ADZA. Built at image/Makefile time into
`data/ontology.sqlite` + `matcher_keys.txt.gz`. No network calls at import or runtime.

## Routing (v3)

Call `plan_query(text)` once per turn via `ml.rag.chatbot.ontology_routing` (the only
RAG module that imports this package). Per-entity modes:

| mode | meaning |
|------|---------|
| `bq+qdrant` | Exact stored literals in `bq_targets` + Qdrant |
| `qdrant_only` | No BQ filter; multilingual `qdrant_terms` / `payload_ids` |
| `web_fallback` | Web only after BQ and Qdrant are both empty |

`source_basis` ∈ `structured` / `documents_only` / `web_only` / `none` drives optional ACF ceilings.

## Env flags (all default OFF)

| Flag | Effect |
|------|--------|
| `ADZA_ONTOLOGY_ROUTING` | Enable `plan_query` once per turn |
| `ADZA_ONTOLOGY_QUERY_VIEW` | Extra Qdrant view for `qdrant_only` subjects |
| `ADZA_QDRANT_ONTOLOGY_FILTER` | `MatchAny` on `ontology_ids` |
| `ADZA_ONTOLOGY_TAGGING` | Tag new chunks at ingest |
| `ADZA_ONTOLOGY_BQ_VALUES` | Pass value-map literals into NL2SQL |
| `ADZA_ONTOLOGY_BQ_STRICT` | Reject SQL literals not in the value map |
| `ADZA_ONTOLOGY_SKIP_BQ` | Skip BQ when `run_bigquery` is false |
| `ADZA_ACF_SOURCE_BASIS_CAP` | Cap ACF band by cited source kinds |
| `ADZA_SINGLE_COUNTRY_POLICY` | Hard geo override (already default off) |

Profile countries are cleaned with `clean_profile_country` (ISO2/name → African ISO3).

## Build

```bash
cd ml-eng && make ontology
# or
python opentrace_ontology/ontology_build/build_sqlite.py
python opentrace_ontology/ontology_build/build_value_map.py --mvi ml/rag/value_index/mart_value_index.json
```

`make ontology` also runs `report_alias_quality.py --fail` (Latin-as-preferred &lt; 10%, priority vernacular gaps = 0).

## Vernacular enrichment

Multilingual common names live in `seeds/ontology_aliases.csv.gz` (not `canonical_name_en`).
The dbt seed copy under `data-eng/dbt/seeds/ontology/` stays plain CSV.

| Script | Role |
|--------|------|
| `ontology_build/curated/africa_core.txt` | Curated Africa staples SoT (`lang[-ISO3]:alias`) |
| `merge_africa_core.py` | Upsert curated entities/aliases (`opentrace_curated` wins) |
| `clean_latin_preferred.py` | Demote Latin-looking non-eng `preferred` → `scientific` (keep language; drop exact dupes only) |
| `fetch_wikidata_labels.py` | Batched `wbgetentities` pull (optional network) |
| `merge_wikidata_labels.py` | QID-only join via `ontology_codes`; `review_status=unreviewed` for African langs |
| `report_alias_quality.py` | Counts + sqlite size / cold-start / p95 gates vs baseline |

Ranking (`SOURCE_RANK`): `opentrace_curated` &gt; `opentrace_repo` &gt; `faostat` &gt; `wikidata` &gt; `agrovoc` &gt; `generated` &gt; `hs`.

Typical refresh:

```bash
python opentrace_ontology/ontology_build/merge_africa_core.py
python opentrace_ontology/ontology_build/clean_latin_preferred.py
python opentrace_ontology/ontology_build/merge_wikidata_labels.py
python opentrace_ontology/ontology_build/sync_dbt_seeds.py --write
make ontology   # from repo root with paths as in Makefile
```

See `ml-eng/deploy/PRODUCTION_ENV.md` for production rollout notes.
