"""
Populate the Qdrant collection from BigQuery (or from a few sample docs).
Run from repo root: PYTHONPATH=ml-eng python -m ml.rag.populate_vector_db [--sql "SELECT ..."] [--limit N]
This lets the vector retriever return results alongside BQ retrieval.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

# Load .env
_repo = Path(__file__).resolve().parents[2]
_env = _repo / "data" / "local" / ".env"
if _env.exists():
    with open(_env) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v


def _bq_fetch(sql: str, project: str, max_rows: int) -> list[dict]:
    from google.cloud import bigquery
    client = bigquery.Client(project=project)
    job = client.query(sql)
    rows = list(job.result())
    out = []
    for row in rows[:max_rows]:
        out.append(dict(row))
    return out


def _row_to_text(row: dict) -> str:
    parts = [f"{k}: {v}" for k, v in row.items() if v is not None]
    return " | ".join(parts)


def main() -> int:
    ap = argparse.ArgumentParser(description="Populate RAG vector DB from BQ or sample docs")
    ap.add_argument("--sql", type=str, help="BigQuery SQL returning rows to index (must include columns for text)")
    ap.add_argument("--limit", type=int, default=500, help="Max rows to index (default 500)")
    ap.add_argument("--collection", type=str, default=os.environ.get("QDRANT_COLLECTION", "opentrace_rag"), help="Qdrant collection name")
    ap.add_argument("--text-column", type=str, help="If set, use this column as document text; else concat all columns")
    args = ap.parse_args()

    project = os.environ.get("BQ_PROJECT", "").strip()
    dataset = os.environ.get("BQ_DATASET_GOLD", "gold").strip()

    def _safe_payload(row: dict[str, Any]) -> dict[str, str | int | float | bool]:
        out: dict[str, str | int | float | bool] = {}
        for k, v in row.items():
            if v is None:
                continue
            if isinstance(v, (str, int, float, bool)):
                out[str(k)] = v
            else:
                out[str(k)] = str(v)[:1000]
        return out

    metadatas: list[dict[str, str | int | float | bool]]
    if args.sql:
        if not project:
            print("Set BQ_PROJECT (e.g. in data/local/.env)", file=sys.stderr)
            return 1
        rows = _bq_fetch(args.sql, project, args.limit)
        if args.text_column:
            documents = [str(r.get(args.text_column, "")) for r in rows]
        else:
            documents = [_row_to_text(r) for r in rows]
        metadatas = [_safe_payload(r) for r in rows]
    else:
        # Default: add a few sample docs so retrieval works without BQ
        documents = [
            "OpenTrace gold dataset contains aggregated yield and food security indicators.",
            "BigQuery tables in gold schema are built from silver and bronze pipelines.",
            "Use the RAG to ask questions about data availability and schema.",
        ]
        metadatas = [_safe_payload({"source": "sample"}) for _ in documents]

    if not documents:
        print("No documents to add.", file=sys.stderr)
        return 0

    from ml.rag.retrievers.vector_retriever import _embed_texts, _get_qdrant_config, _safe_payload

    from qdrant_client import QdrantClient
    from qdrant_client.http.models import Distance, PointStruct, VectorParams

    url, api_key, _, timeout_s = _get_qdrant_config()
    client = QdrantClient(url=url, api_key=api_key, timeout=timeout_s)

    embed_mode = os.environ.get("RAG_EMBEDDINGS_MODE", "local")
    model_id = os.environ.get("RAG_EMBEDDING_MODEL_ID", "sentence-transformers/all-MiniLM-L6-v2")
    vectors = _embed_texts(documents, model_id=model_id, mode=embed_mode)
    dim = len(vectors[0]) if vectors else 0

    try:
        client.get_collection(collection_name=args.collection)
    except Exception:
        client.create_collection(
            collection_name=args.collection,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        )

    points = []
    for i, (doc, vec, meta) in enumerate(zip(documents, vectors, metadatas)):
        payload = dict(_safe_payload(meta))
        payload["content"] = doc
        points.append(PointStruct(id=f"doc_{i}", vector=vec, payload=payload))

    client.upsert(collection_name=args.collection, points=points)
    print(f"Indexed {len(documents)} documents into Qdrant collection '{args.collection}'")
    return 0


if __name__ == "__main__":
    sys.exit(main())
