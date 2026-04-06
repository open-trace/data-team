"""
Populate the in-repo ChromaDB from BigQuery (or from a CSV/file).
Run from repo root: PYTHONPATH=. python -m ml.rag.populate_vector_db [--sql "SELECT ..."] [--limit N]
This lets the vector retriever return results alongside BQ retrieval.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from pypdf import PdfReader
from typing import Any
from chromadb.api.types import Metadata

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
    ap.add_argument("--collection", type=str, default="opentrace_rag", help="Chroma collection name")
    ap.add_argument("--text-column", type=str, help="If set, use this column as document text; else concat all columns")
    args = ap.parse_args()

    project = os.environ.get("BQ_PROJECT", "").strip()
    dataset = os.environ.get("BQ_DATASET_GOLD", "gold").strip()

    def _safe_meta(row: dict[str, Any]) -> Metadata:
        out: Metadata = {}
        for k, v in row.items():
            if v is None or isinstance(v, (str, int, float, bool)):
                out[str(k)] = v
            else:
                out[str(k)] = str(v)[:1000]
        return out

    metadatas: list[Metadata]
    if args.sql:
        if not project:
            print("Set BQ_PROJECT (e.g. in data/local/.env)", file=sys.stderr)
            return 1
        rows = _bq_fetch(args.sql, project, args.limit)
        if args.text_column:
            documents = [str(r.get(args.text_column, "")) for r in rows]
        else:
            documents = [_row_to_text(r) for r in rows]
        metadatas = [_safe_meta(r) for r in rows]
    else:
        # Default: add a few sample docs so retrieval works without BQ
        documents = [
            "OpenTrace gold dataset contains aggregated yield and food security indicators.",
            "BigQuery tables in gold schema are built from silver and bronze pipelines.",
            "Use the RAG to ask questions about data availability and schema.",
        ]
        metadatas = [_safe_meta({"source": "sample"}) for _ in documents]

    if not documents:
        print("No documents to add.", file=sys.stderr)
        return 0

    from ml.rag.retrievers.vector_retriever import VectorRetriever
    retriever = VectorRetriever(collection_name=args.collection)
    coll = retriever._get_collection()
    # Add with IDs to avoid duplicates; for re-runs we could clear first or use hashes
    ids = [f"doc_{i}" for i in range(len(documents))]
    coll.upsert(ids=ids, documents=documents, metadatas=metadatas)
    print(f"Indexed {len(documents)} documents into collection '{args.collection}' at {retriever.persist_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
