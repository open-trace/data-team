# Langfuse planned-path scan recipe

Use this after seeding or production traffic to filter warehouse / multi-bind failures.
Does **not** replace human taxonomy scores (`nl2sql_bad`, `retrieval_miss`, …).

## Prerequisites

- `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`, `LANGFUSE_BASE_URL` set
- Smoke: `PYTHONPATH=. python scripts/verify_langfuse_tracing.py` (from `ml-eng/`)
- Seed strata: `PYTHONPATH=. python scripts/seed_langfuse_error_analysis.py --limit 40`

Seed tags of interest: `error_analysis_seed`, `seed:planned_multi_class`, `seed:region_blend`, `seed:compare_geo`, `seed:nl2sql_stress`.

## Emitted scores (on-true booleans)

| Score | Meaning |
|-------|---------|
| `planned_path` | Bind / class_engine / planned retrieve |
| `bind_present` | Non-empty `bind_contracts` |
| `multi_bind` | `bind_table_count >= 2` |
| `multi_class` | Supervisor primary + secondary ≥ 2 classes |
| `region_blend` | Expanded regions or ≥3 ISO countries |
| `nl2sql_empty` | Structured BQ empty after warehouse attempt |
| `nl2sql_validation_failed` | BQ validation failed text in results |
| `nl2sql_fallback` | Plan allowed NL2SQL escape |
| `compile_error` | Compile-error plan block |
| `sql_source_nl2sql` / `sql_source_bind_contract` | SQL provenance |

## Emitted tags (UI filters)

- `planned:1`, `retrieve_mode:planned|legacy`
- `plan_source:class_engine|analytical|compile_error|…`
- `sql_source:nl2sql|bind_contract|…`
- `multi_bind:1`, `multi_class:1`, `region_blend:1`
- Preserved: `plan_type:*`, `category:*`, caller `seed:*` / `error_analysis_seed`

## Filter recipes

1. **Planned multi-bind failures**  
   Score `multi_bind=true` AND (`nl2sql_empty` OR `nl2sql_validation_failed` OR `bq_failure`).

2. **Multi-class region panels**  
   Tags `multi_class:1` AND `region_blend:1` (or seed `seed:planned_multi_class`).

3. **NL2SQL vs bind miss**  
   Split human `nl2sql_bad` into:
   - empty rows → `nl2sql_empty`
   - invalid SQL → `nl2sql_validation_failed`
   - no bind on class turn → `planned_path` without `bind_present`

4. **Legacy vs planned**  
   Tag `retrieve_mode:legacy` vs `retrieve_mode:planned`.

## CLI sketch

```bash
# List recent traces (adjust project host/keys via env)
npx langfuse-cli api traces list --help

# Prefer filtering in Langfuse UI by tag=multi_bind:1 or score=multi_bind
```

Open coding / taxonomy: continue Round-1 queue labels; see `scripts/.langfuse_ea/ROUND1_SUMMARY.md`.

## Related

- Instrumentation: `ml/rag/observability.py` (`_planned_path_trace_fields`, `_record_planned_path_scores`)
- Architecture: `ml/rag/ARCHITECTURE.md` (bind-first + mode matrix)
