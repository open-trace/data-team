# ML / AI engineering (`ml-eng/`)

This subtree is the **ML/AI engineering sub-repo** for OpenTrace. It is intended to be **self-contained**:

- Dependencies live under `ml-eng/requirements*.txt`
- Configuration templates live under `ml-eng/config/`
- Docker artifacts live under `ml-eng/docker/`
- ML commands live in `ml-eng/Makefile`

The actual Python package is **`ml/`** (imports are `ml.*`) and is located at:

- `ml-eng/ml/` (source of truth)

For compatibility, the repo root may expose a `ml` symlink pointing to `ml-eng/ml` so existing imports and tooling still work.

## RAG vector DB

RAG uses **Qdrant Cloud** (not Chroma). Configure:

- `QDRANT_URL`
- `QDRANT_API_KEY`
- `QDRANT_COLLECTION` (default: `opentrace_rag`)

## Quickstart

From repo root:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r ml-eng/requirements.txt -r ml-eng/requirements-dev.txt
PYTHONPATH=ml-eng python -m ml.rag.run "What tables exist in bronze for yields?"
```

