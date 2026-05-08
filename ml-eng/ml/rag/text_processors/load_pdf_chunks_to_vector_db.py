"""
Load preprocessed chunk JSONL into the Qdrant collection.

Expected input format:
    One JSON object per line with keys:
      - id (str)
      - text (str)
      - metadata (dict)

Defaults:
    --input  data/local/pdf_chunks.jsonl
    --collection opentrace_rag

Usage:
    PYTHONPATH=ml-eng python -m ml.rag.load_pdf_chunks_to_vector_db
    PYTHONPATH=ml-eng python -m ml.rag.load_pdf_chunks_to_vector_db --reset
    PYTHONPATH=ml-eng python -m ml.rag.load_pdf_chunks_to_vector_db --batch-size 200
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = REPO_ROOT / "data" / "local" / "pdf_chunks.jsonl"

def _clean_semicolon_list(value: str) -> str:
    parts = [p.strip() for p in (value or "").split(";")]
    parts = [p for p in parts if p]
    # de-dupe while preserving order
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        if p.lower() in seen:
            continue
        seen.add(p.lower())
        out.append(p)
    return "; ".join(out)


def _normalize_metadata(meta: dict[str, Any]) -> dict[str, Any]:
    """
    Add a small consistent metadata spine across sources without changing preprocessors.

    Adds:
      - doc_kind: news_article | academic_article | bq_table_description | other
      - label: optional human label (e.g. original BQ 'type')
      - geo_scope: country | multi_country | regional | global | unknown
      - geo_countries: '; '-separated country list (string)
      - geo_country_primary: first country in geo_countries (string)
    """
    m = dict(meta or {})

    # --- doc_kind / label ---
    if isinstance(m.get("doc_kind"), str) and m.get("doc_kind"):
        doc_kind = m["doc_kind"].strip()
    else:
        info_type = m.get("info_type")
        bq_type = m.get("type")
        if isinstance(info_type, str) and info_type.strip():
            doc_kind = info_type.strip()
        elif isinstance(bq_type, str) and bq_type.strip().lower().startswith("bq "):
            doc_kind = "bq_table_description"
            # Keep the original BQ description label in a non-ambiguous key.
            m.setdefault("label", bq_type.strip())
        else:
            doc_kind = "other"
    m["doc_kind"] = doc_kind

    # For BQ descriptions, also add a stable source_kind if not present.
    if doc_kind == "bq_table_description":
        m.setdefault("source_kind", "bq_table_description_docx")
    elif doc_kind == "news_article":
        m.setdefault("source_kind", "web_news_rss_txt")
    elif doc_kind == "academic_article":
        m.setdefault("source_kind", "pdf_text_document")

    # --- geo normalization ---
    geo_countries = ""
    if isinstance(m.get("geo_countries"), str) and m.get("geo_countries"):
        geo_countries = _clean_semicolon_list(m["geo_countries"])
    else:
        # News: single country
        country = m.get("country")
        if isinstance(country, str) and country.strip():
            geo_countries = _clean_semicolon_list(country)
        # PDFs: place_of_focus is typically a semicolon-separated list
        else:
            pof = m.get("place_of_focus")
            if isinstance(pof, str) and pof.strip():
                geo_countries = _clean_semicolon_list(pof)

    if geo_countries:
        m["geo_countries"] = geo_countries
        primary = geo_countries.split(";")[0].strip()
        if primary:
            m.setdefault("geo_country_primary", primary)
        if ";" in geo_countries:
            m.setdefault("geo_scope", "multi_country")
        else:
            m.setdefault("geo_scope", "country")
    else:
        m.setdefault("geo_scope", "unknown")

    return m


def _safe_metadata(meta: dict[str, Any]) -> dict[str, str | int | float | bool]:
    """
    Qdrant payload supports scalar types. Coerce unsupported values to compact strings.
    """
    out: dict[str, str | int | float | bool] = {}
    for k, v in meta.items():
        if v is None:
            # Skip null to maximize compatibility across Chroma versions.
            continue
        if isinstance(v, (str, int, float, bool)):
            out[k] = v
        else:
            out[k] = str(v)[:1000]
    return out


def load_jsonl_chunks(path: Path) -> tuple[list[str], list[str], list[dict[str, str | int | float | bool]]]:
    """
    Parse chunk JSONL and return ids, documents, metadatas.
    Skips malformed lines and empty texts.
    """
    ids: list[str] = []
    docs: list[str] = []
    metadatas: list[dict[str, str | int | float | bool]] = []

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except json.JSONDecodeError:
                continue

            chunk_id = str(row.get("id", "")).strip()
            text = str(row.get("text", "")).strip()
            metadata = row.get("metadata", {})

            if not chunk_id or not text:
                continue
            if not isinstance(metadata, dict):
                metadata = {}

            ids.append(chunk_id)
            docs.append(text)
            metadatas.append(_safe_metadata(_normalize_metadata(metadata)))

    return ids, docs, metadatas


def _upsert_in_batches(
    client: Any,
    collection: str,
    ids: list[str],
    docs: list[str],
    metadatas: list[dict[str, str | int | float | bool]],
    batch_size: int,
) -> int:
    total = len(ids)
    inserted = 0
    for i in range(0, total, batch_size):
        b_ids = ids[i:i + batch_size]
        b_docs = docs[i:i + batch_size]
        b_meta = metadatas[i:i + batch_size]
        from ml.rag.retrievers.vector_retriever import _embed_texts
        from qdrant_client.http.models import PointStruct

        embed_mode = os.environ.get("RAG_EMBEDDINGS_MODE", "local")
        model_id = os.environ.get("RAG_EMBEDDING_MODEL_ID", "sentence-transformers/all-MiniLM-L6-v2")
        vectors = _embed_texts(b_docs, model_id=model_id, mode=embed_mode)
        points = []
        for pid, doc, meta, vec in zip(b_ids, b_docs, b_meta, vectors):
            payload = dict(meta)
            payload["content"] = doc
            points.append(PointStruct(id=pid, vector=vec, payload=payload))
        client.upsert(collection_name=collection, points=points)
        inserted += len(b_ids)
    return inserted


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Load chunk JSONL into Qdrant collection."
    )
    p.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Path to chunk JSONL (default: {DEFAULT_INPUT})",
    )
    p.add_argument(
        "--collection",
        type=str,
        default=os.environ.get("QDRANT_COLLECTION", "opentrace_rag"),
        help="Qdrant collection name (default: opentrace_rag)",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Upsert batch size (default: 100)",
    )
    p.add_argument(
        "--reset",
        action="store_true",
        help="Delete and recreate the collection before loading.",
    )
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    input_path: Path = args.input

    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    ids, docs, metadatas = load_jsonl_chunks(input_path)
    if not docs:
        raise SystemExit(f"No valid chunk rows found in: {input_path}")

    import os
    from ml.rag.retrievers.vector_retriever import _embed_texts, _get_qdrant_config
    from qdrant_client import QdrantClient
    from qdrant_client.http.models import Distance, VectorParams

    url, api_key, _, timeout_s = _get_qdrant_config()
    client = QdrantClient(url=url, api_key=api_key, timeout=timeout_s)

    embed_mode = os.environ.get("RAG_EMBEDDINGS_MODE", "local")
    model_id = os.environ.get("RAG_EMBEDDING_MODEL_ID", "sentence-transformers/all-MiniLM-L6-v2")
    dim = len(_embed_texts(["ping"], model_id=model_id, mode=embed_mode)[0])

    if args.reset:
        try:
            client.delete_collection(collection_name=args.collection)
        except Exception:
            pass

    try:
        client.get_collection(collection_name=args.collection)
    except Exception:
        client.create_collection(
            collection_name=args.collection,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        )

    inserted = _upsert_in_batches(
        client=client,
        collection=args.collection,
        ids=ids,
        docs=docs,
        metadatas=metadatas,
        batch_size=max(1, int(args.batch_size)),
    )

    print(
        f"Upserted {inserted} chunks into collection '{args.collection}' "
        f"in Qdrant"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

