# RAG pipeline (graph / agentic)

Modular RAG that queries **BigQuery** and a **vector DB**, then merges → reranks → generates an answer. The flow is implemented as a **LangGraph** so you can extend or swap nodes.

For a file-by-file map and end-to-end flow, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Graph shape

```
                    ┌─────────────────┐
                    │  bq_retrieve    │
                    └────────┬────────┘
  START ──┬──────────────────┼──────────────────┐
          │                  │                  │
          │                  ▼                  ▼
          │           ┌─────────────┐   ┌─────────────────┐
          └──────────►│    merge    │◄──│ vector_retrieve  │
                      └──────┬──────┘   └─────────────────┘
                             │
                             ▼
                      ┌─────────────┐
                      │   rerank    │
                      └──────┬──────┘
                             │
                             ▼
                      ┌─────────────┐
                      │  generate   │
                      └──────┬──────┘
                             │
                             ▼
                            END
```

- **bq_retrieve**: runs BigQuery over the **bronze** dataset only (`BQ_DATASET_BRONZE`). Uses **NL-to-SQL** (Llama 3.1 via HF) when no `sql` is passed; validates SELECT-only and allowed datasets. Tuned for agricultural/food-security questions (regions, yields, crops, rainfall, etc.). See [docs/BQ_NL2SQL_PLAN.md](docs/BQ_NL2SQL_PLAN.md) and [docs/EXPECTED_QUESTIONS.md](docs/EXPECTED_QUESTIONS.md).
- **vector_retrieve**: queries an **in-repo ChromaDB** at `data/local/vector_db/` (local embeddings, no API key).
- **merge**: concatenates BQ + vector results into one list.
- **rerank**: trims and orders context (placeholder; plug in Cohere/Jina/cross-encoder).
- **generate**: produces the final answer (placeholder; plug in Vertex AI / OpenAI / local LLM).

## Chat sessions and context memory (summary + verbatim window)

- **Retrieval** (decompose, BigQuery, vectors) always uses **only the latest user message**. Prior turns do not change retrieval.
- **Generation** sees a **rolling summary** of older dialogue plus the **last N user+assistant pairs** verbatim (default **N = 5**). When a new reply would exceed N pairs, the **oldest** pair is folded into the summary via an LLM call (`HF_API_TOKEN`; optional `RAG_SUMMARY_MODEL_ID`, else `RAG_LLM_MODEL_ID`). If the token is missing, folding uses a short **text stub** instead (see [`ml/rag/chat_memory.py`](ml/rag/chat_memory.py)).
- **Streamlit UI** keeps a **full message list** for scrolling; the compact **summary + recent_turns** is what gets sent to `run_rag` on the next turn.

**Env (optional)**

| Variable | Meaning |
|----------|---------|
| `RAG_CHAT_VERBATIM_TURNS` | Max verbatim **pairs** (overrides `RAG_CHAT_HISTORY_MAX_TURNS` if set) |
| `RAG_CHAT_HISTORY_MAX_TURNS` | Fallback max pairs (default **5**) |
| `RAG_CHAT_HISTORY_MAX_CHARS` | Soft cap on verbatim block size in the prompt (default **4000**) |
| `RAG_SUMMARY_MAX_CHARS` | Max length of the running summary string (default **2000**) |
| `RAG_SUMMARY_MODEL_ID` | Optional HF model for summarization |

See also [`ml/rag/chat_history.py`](ml/rag/chat_history.py) for **legacy** `chat_history`-only truncation (no summary).

**Streamlit** ([`streamlit_app.py`](ml/rag/streamlit_app.py)): multiple **chat sessions** in the sidebar; **pipeline debug** shows the last run’s decomposition and retrieval stats.

**API** (`POST /query`): responses include **`session_id`**. Reuse it for **server-side** `{conversation_summary, recent_turns}` storage (in-process + lock; **single worker**, lost on restart). Send **`conversation_history`** to supply prior turns from the client; history is compacted for that request only and the server store is **not** updated.

## Env and config

- **BigQuery**: `BQ_PROJECT` and `BQ_DATASET_BRONZE` (see `data/local/.env`). The BQ retriever loads schema and runs NL-to-SQL **only** against the bronze dataset. Silver/gold env vars remain for dbt and other tooling.
- **Bronze table hints (YAML + vectors)**: `match_bq_tables_from_descriptions` groups vector hits by `table_name` and fuses each with a compact column catalog from [`bronze_dataset_catalog.py`](bronze_dataset_catalog.py). Set **`RAG_BRONZE_MODEL_YAML`** to override the default path (`ml/rag/bronze_dataset_model.yml`). **`RAG_BRONZE_MODEL_SOURCE`** selects the dbt `sources` entry by name (default **`bronze`**); set it empty to merge every source in that file. If the primary YAML is missing or parses to no tables, the loader falls back to **`dbt/models/sources.yml`** (still honoring `RAG_BRONZE_MODEL_SOURCE`). Live BigQuery introspection remains the source of truth for runnable SQL.
- **Vector DB**: **ChromaDB** in-repo at `data/local/vector_db/` (set `RAG_VECTOR_DB_PATH` to override). Populate from BQ or sample docs with `python -m ml.rag.populate_vector_db`.
- **Chat memory**: variables in the table above; summarization needs **`HF_API_TOKEN`** (same as the answer generator).

## Run

From repo root:

```bash
# Install deps (repo root)
pip install -r requirements.txt
pip install -r ml/rag/requirements.txt

# Optional: populate in-repo vector DB (Chroma) from BQ or sample docs
PYTHONPATH=. python -m ml.rag.populate_vector_db
# Or from BQ: --sql "SELECT * FROM bronze.your_table LIMIT 100"

# Build chunks from ml/rag/BQ data description/*.docx
PYTHONPATH=. python -m ml.rag.bq_description_preprocessor
# Loads into data/local/bq_description_chunks.jsonl with metadata:
#   type: "BQ <table_name> description"

# Load those chunks into vector DB
PYTHONPATH=. python -m ml.rag.load_pdf_chunks_to_vector_db \
  --input data/local/bq_description_chunks.jsonl

# Build chunks from scraped news articles under data/local/web_news_rss/
PYTHONPATH=. python -m ml.rag.news_preprocessor \
  --input-dir data/local/web_news_rss \
  --output data/local/news_chunks.jsonl

# Load news chunks into vector DB
PYTHONPATH=. python -m ml.rag.load_pdf_chunks_to_vector_db \
  --input data/local/news_chunks.jsonl

# Run with a question
PYTHONPATH=. python -m ml.rag.run "What tables exist in bronze for yields?"
PYTHONPATH=. python -m ml.rag.run
```

### Test with Streamlit

```bash
streamlit run ml/rag/streamlit_app.py
```

Open the URL (e.g. http://localhost:8501), use the **chat** input, and switch sessions from the sidebar. Enable **pipeline debug** to see decomposition and retrieval stats.

Programmatic:

```python
from ml.rag.graph import run_rag

result = run_rag("Your question")
print(result["answer"])
# Multi-turn (generator): prefer summary + recent verbatim pairs
result2 = run_rag(
    "Follow-up using that context",
    conversation_summary="",
    recent_turns=[
        {"role": "user", "content": "Your question"},
        {"role": "assistant", "content": result["answer"]},
    ],
)
# Legacy: flat chat_history (verbatim only, truncated; no LLM summary fold)
# result3 = run_rag("Follow-up", chat_history=[{"role": "user", "content": "..."}, ...])
# result also has: bq_results, vector_results, merged_context, reranked_context, error
```

## Modules

| Path | Role |
|------|------|
| `state.py` | `RAGState` (legacy); graph uses `RAGGraphState` in `graph.py` |
| `retrievers/base.py` | `BaseRetriever` interface |
| `retrievers/bq_retriever.py` | BigQuery retrieval |
| `retrievers/vector_retriever.py` | Vector DB retrieval (wire your store here) |
| `reranker.py` | `rerank(query, context_items, top_k)` |
| `generator.py` | `generate(query, context_items)` |
| `graph.py` | LangGraph build + `run_rag(query)` |
| `chat_history.py` | Normalize/truncate prior messages (legacy flat history) |
| `chat_memory.py` | Summary + verbatim window, LLM fold-on-overflow |
| `run.py` | CLI entrypoint |

## Extending

1. **Vector DB**: The default is in-repo ChromaDB; you can point `RAG_VECTOR_DB_PATH` elsewhere or swap in another client in `vector_retriever.py`.
2. **Reranker**: In `reranker.py`, call your API or model and return ordered `list[dict]` with `content`/`text`.
3. **Generator**: In `generator.py`, call Vertex AI / OpenAI / local LLM with `query` and `context_items` and return the answer string.
4. **BQ NL-to-SQL**: In `BQRetriever.retrieve()`, add a step that turns `query` into SQL (e.g. LLM or templates) and pass it as `kwargs["sql"]` or set `sql` internally.

---

## Deploy on Hugging Face and expose API to the frontend

The RAG can be deployed as a **Hugging Face Space (Docker)** and its API called from your frontend chatbot.

### API

| Method | Path     | Description                    |
|--------|----------|--------------------------------|
| GET    | `/health`| Readiness check                |
| POST   | `/query` | Run RAG; body `{"query": "..."}`; optional `session_id` for chat memory |
| GET    | `/docs`  | Swagger UI                     |

**Example (frontend):**

```bash
curl -X POST "https://YOUR-SPACE-URL.hf.space/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "What bronze data can we use for crop yields?"}'
# Next turn: reuse session_id from the response body
# -d '{"query": "Focus on Kenya", "session_id": "<id from previous response>"}'
```

Response:

```json
{
  "answer": "...",
  "session_id": "abc123...",
  "error": null,
  "has_bq_results": true,
  "has_vector_results": false
}
```

### Deploy to Hugging Face Spaces

1. **Create a new Space** at [huggingface.co/spaces](https://huggingface.co/spaces): choose **Docker**, and either push this repo or a copy that includes `ml/rag` and the Dockerfile.

2. **Dockerfile**  
   Hugging Face expects a **Dockerfile at the repo root**. This repo has **`Dockerfile.rag`** at the root: in your Space, either **rename it to `Dockerfile`** (or copy its contents into `Dockerfile`) so the Space builds the RAG API. Build context is the repo root.

3. **Secrets (Space → Settings → Variables and Secrets)**  
   - `BQ_PROJECT` – GCP project ID  
   - `BQ_DATASET_BRONZE` – BigQuery bronze dataset (RAG queries this dataset only)  
   - For BigQuery auth: either attach a **GCP service account key** (e.g. paste JSON as a secret and set `GOOGLE_APPLICATION_CREDENTIALS` to a path you write it to at startup) or use Workload Identity if running on GCP.  
   - Any keys for vector DB or LLM (e.g. `COHERE_API_KEY`, `OPENAI_API_KEY`) if you use them.

4. **CORS**  
   For production, set **`RAG_CORS_ORIGINS`** to your frontend origin(s), comma-separated (e.g. `https://yourapp.com`). Default is `*`.

5. **Port**  
   The app listens on **7860** (required by Hugging Face Spaces).

### Local API (same interface as HF)

```bash
pip install fastapi "uvicorn[standard]"
PYTHONPATH=. uvicorn ml.rag.api:app --host 0.0.0.0 --port 7860
# Frontend: http://localhost:7860/docs and POST http://localhost:7860/query
```

### Docker Compose (local)

Prerequisites:

- **`data/local/.env` must exist** (Compose `env_file`); create it with your secrets — e.g. `BQ_PROJECT`, `BQ_DATASET_BRONZE`, `HF_API_TOKEN`, and any other `RAG_*` vars. If the file is missing, `docker compose` fails when starting RAG services.
- **GCP key** mounted read-only into the container (default host path **`data/local/keys/opentrace-bq-key.json`**). Override with env **`GCP_SA_KEY_HOST_PATH`** before `docker compose` if your key lives elsewhere.
- **Chroma on the host:** populate **`data/local/vector_db`** first (see [Run](#run) and loaders above). Override the host side with **`RAG_VECTOR_DB_HOST_PATH`** if needed. The container sets **`RAG_VECTOR_DB_PATH=/app/data/local/vector_db`** and bind-mounts that path; an empty folder means no news/academic vector hits.
- **Optional port / path overrides (shell env, not inside `.env` required):** `RAG_API_PORT` (default 7860), `RAG_STREAMLIT_PORT` (default 8501), `GCP_SA_KEY_HOST_PATH`, `RAG_VECTOR_DB_HOST_PATH`.

From repo root:

```bash
# API only (port 7860)
docker compose --profile rag up --build rag-api

# API + Streamlit (7860 and 8501)
docker compose --profile rag up --build rag-api rag-streamlit
```

- **API docs:** http://localhost:7860/docs  
- **Streamlit:** http://localhost:8501  

Stop: `docker compose --profile rag down` (or `down` without profile if no other services use those containers).

### Docker (local or CI)

From repo root:

```bash
docker build -f ml/rag/Dockerfile -t rag-api .

docker run --rm -p 7860:7860 \
  -e BQ_PROJECT=your-project \
  -e BQ_DATASET_BRONZE=bronze \
  -e HF_API_TOKEN=your-hf-token \
  -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/bq.json \
  -e RAG_VECTOR_DB_PATH=/app/data/local/vector_db \
  -v "$(pwd)/data/local/vector_db:/app/data/local/vector_db" \
  -v "$(pwd)/path/to/your-sa.json:/secrets/bq.json:ro" \
  rag-api
```

Adjust the service-account path to match your machine. Without the vector mount, Chroma starts empty inside the container.
