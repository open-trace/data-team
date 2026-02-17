# ML

**Feature store** logic, training, evaluation, RAG, and serving. Prototyped here; production runs on **Vertex AI** (training, registry, endpoints) with the **feature store** fed from the **gold database** (and silver) in BigQuery.

## Layout

| Area | Purpose |
|------|---------|
| `features/` | **Feature store**: definitions, BQ queries from gold/silver databases, and pipelines that populate the feature store (e.g. Vertex AI Feature Store or a BQ dataset). |
| `training/` | Training pipelines, model code, hyperparameter configs. Consume from the feature store. |
| `evaluation/` | Metrics, validation datasets, model comparison. |
| `rag/` | RAG pipelines: chunking, embeddings, vector store (e.g. Vertex AI Vector Search). |
| `serving/` | Inference code, API contracts, and configs for Vertex AI endpoints or Prediction + QA API. |

## Flow

1. **Feature store** is built from the **gold database** (and silver) in BigQuery; logic is prototyped in `features/` and deployed to the feature store.
2. **Training** consumes the feature store and outputs models to the Vertex AI model registry.
3. **Evaluation** runs against held-out data and tracks metrics.
4. **RAG** uses curated docs + vector DB for QA.
5. **Serving** exposes Prediction + QA API backed by Vertex AI.

## Conventions

- Use `requirements.txt` at repo root (or `ml/requirements.txt`) for dependencies.
- Prefer tests under each subfolder (e.g. `ml/features/tests/`) so `ml-tests.yml` can run them.
