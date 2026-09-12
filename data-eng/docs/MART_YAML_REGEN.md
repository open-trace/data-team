# Mart table YAML regeneration

RAG bind contracts read from [`ml-eng/ml/rag/bq_mart_tables_yaml_files/`](../../ml-eng/ml/rag/bq_mart_tables_yaml_files/). Keep this directory in sync with dbt `mart_dev` models.

## When to run

- New or renamed table in [`data-eng/dbt/models/mart_dev/`](../../dbt/models/mart_dev/)
- Column schema change affecting filters or joins
- Stale `profiled_at` (CI freshness gate, default 120 days)

## Full workflow (BigQuery credentials required)

From repo root:

```bash
python data-eng/data/local/scripts/sync_mart_dev_tables_list.py
python data-eng/data/local/scripts/regenerate_mart_table_yamls.py
python ml-eng/ml/rag/helpers/patch_mart_yaml_semantics.py
python ml-eng/ml/rag/helpers/patch_mart_yaml_filter_contract.py
PYTHONPATH=ml-eng pytest ml-eng/ml/rag/tests/chatbot/test_mart_yaml_contract.py ml-eng/ml/rag/tests/chatbot/test_mart_knowledge_linkage.py ml-eng/ml/rag/tests/chatbot/test_mart_dev_tables_list_sync.py -v
```

Requires `GOOGLE_APPLICATION_CREDENTIALS` or gcloud ADC, and `BQ_PROJECT` (default `opentrace-prod-5ga4`).

## Offline bootstrap (no BigQuery)

When BQ is unavailable, create skeleton YAML from entity seed + dbt `schema.yml`:

```bash
python data-eng/data/local/scripts/sync_mart_dev_tables_list.py
python data-eng/data/local/scripts/bootstrap_mart_yaml_skeletons.py
python ml-eng/ml/rag/helpers/patch_mart_yaml_semantics.py
python ml-eng/ml/rag/helpers/patch_mart_yaml_filter_contract.py
```

Skeletons lack live `value_samples`; run full regen before production bind quality.

## CI checks

- [`test_mart_dev_tables_list_sync.py`](../../ml-eng/ml/rag/tests/chatbot/test_mart_dev_tables_list_sync.py) — allowlist matches dbt
- [`test_mart_yaml_contract.py`](../../ml-eng/ml/rag/tests/chatbot/test_mart_yaml_contract.py) — YAML shape + freshness
- [`test_mart_knowledge_linkage.py`](../../ml-eng/ml/rag/tests/chatbot/test_mart_knowledge_linkage.py) — routing ↔ YAML linkage

## Metadata sources

| Source | Role |
|--------|------|
| [`mart_entity_dictionary_seed.yaml`](mart_entity_dictionary_seed.yaml) | description, grain, domain |
| dbt `schema.yml` | column names (bootstrap) |
| BQ profiling | value_samples, value_stats, profiled_at |
| [`mart_table_semantics.yaml`](../ml/rag/helpers/mart_table_semantics.yaml) | filtering_guidance, semantic_role |
