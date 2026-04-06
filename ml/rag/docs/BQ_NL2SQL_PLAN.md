# BQ Retriever: Dynamic NL-to-SQL (Bronze only)

## Goal

Make the BQ retriever **competent at turning chatbot questions into BigQuery SQL** over the **bronze dataset**, so the RAG returns real rows as context for the generator. Silver and gold are not queried by this retriever.

## Scope

| Item | Description |
|------|-------------|
| **Datasets** | Bronze only (no silver, gold, or landing). Dataset ID from env: `BQ_DATASET_BRONZE`. |
| **NL → SQL** | Use Llama 3.1 via Hugging Face Inference API (`HF_API_TOKEN`, `RAG_LLM_MODEL_ID`). |
| **Schema** | Load from BigQuery at runtime (list_tables + get_table per table), cached per process. |
| **Safety** | Only allow `SELECT`. Reject DML/DDL. Restrict to the configured bronze dataset. Enforce `LIMIT`. |

## Implementation Plan

### 1. Schema awareness

- **Source**: For `project` = `BQ_PROJECT` and `dataset` = `BQ_DATASET_BRONZE`, list tables then `get_table` for each to obtain columns.
- **Format**: Compact text summary for the LLM, e.g.  
  `[bronze] dataset_id = bronze. Tables: table_a (col1 STRING, col2 INT64), table_b (...).`
- **Caching**: Build once per `BQRetriever` instance (on first `retrieve()`), reuse for subsequent NL-to-SQL calls.

### 2. NL-to-SQL (text → SQL)

- **Input**: User question + schema summary + filter/table hints (bronze-aligned).
- **Model**: `RAG_LLM_MODEL_ID` (default Llama 3.1-8B-Instruct), `HF_API_TOKEN`.
- **Prompt**: System instructs bronze-only tables; user block includes schema + question. Output: single SQL string (strip code fences if present).

### 3. Safety and validation

- **Block**: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `MERGE`, `TRUNCATE`, `ALTER`, `GRANT`, etc.
- **Allow**: Only `SELECT` (and subqueries).
- **Datasets**: Every `project.dataset.table` reference must use the allowed bronze dataset id (from env).
- **LIMIT**: If no `LIMIT` in the query, append `LIMIT N` (retriever `max_rows`).

### 4. Execution and fallback

- **Execute**: Run validated SQL with the BigQuery client; map rows to context items (`content`, `source`, `metadata`).
- **Fallback**: If NL-to-SQL fails, keyword-based SQL (e.g. aggregates on `yield_raw_data` in bronze, or `SELECT *` from the first listed bronze table) still must pass the same validation.
- **Override**: If caller passes `kwargs["sql"]`, skip NL-to-SQL and validate + run that SQL (same safety rules).

### 5. Config / env

- **Required for BQ**: `BQ_PROJECT`, `BQ_DATASET_BRONZE`, BigQuery auth (`gcloud` ADC or `GOOGLE_APPLICATION_CREDENTIALS`).
- **NL-to-SQL**: `HF_API_TOKEN`, `RAG_LLM_MODEL_ID`; `RAG_BQ_NL2SQL_ENABLED=1` (default on).

### 6. Testing

- Unit: Mock BQ client and HF API; test schema build, validation (reject DROP, reject wrong dataset, allow SELECT with LIMIT).
- Integration: With real BQ and HF token, ask for bronze analytics (e.g. yields by country) and confirm context rows.

## Files to touch

| File | Role |
|------|------|
| `ml/rag/retrievers/bq_retriever.py` | Schema loader, `_nl_to_sql`, `_validate_sql`, `_fallback_sql`, `retrieve()`. |
| `ml/rag/README.md` | Documents bronze-only BQ retrieval. |

## Summary

- **Schema**: Load bronze from BQ, cache, format as text for the LLM.
- **NL-to-SQL**: Llama 3.1 via HF with strict bronze-only, SELECT-only instructions.
- **Safety**: Validate SQL (no DML/DDL), restrict to bronze dataset, add LIMIT.
- **Integration**: Same `retrieve()` interface; generator gets BQ rows as context.
