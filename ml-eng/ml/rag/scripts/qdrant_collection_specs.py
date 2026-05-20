"""
Qdrant collection vector layouts (dense HNSW + INT8 quant + optional sparse).

Used by create_qdrant_collections and loaders on --reset.
"""
from __future__ import annotations

import os
from typing import Any

from qdrant_client.http import models

# 768-dim dense (e5-base class models); override for experiments
DENSE_DIM = max(32, int(os.getenv("RAG_QDRANT_VECTOR_SIZE", "768").strip() or "768"))
HNSW_M = 8


def _int8_quant(*, quantile: float | None = None, always_ram: bool = False) -> models.ScalarQuantization:
    cfg: dict[str, Any] = {"type": models.ScalarType.INT8}
    if quantile is not None:
        cfg["quantile"] = quantile
    if always_ram:
        cfg["always_ram"] = True
    return models.ScalarQuantization(scalar=models.ScalarQuantizationConfig(**cfg))


def dense_vector_params(
    *,
    ef_construct: int = 100,
    quantile: float | None = None,
    always_ram: bool = False,
) -> models.VectorParams:
    return models.VectorParams(
        size=DENSE_DIM,
        distance=models.Distance.COSINE,
        hnsw_config=models.HnswConfigDiff(m=HNSW_M, ef_construct=ef_construct),
        quantization_config=_int8_quant(quantile=quantile, always_ram=always_ram),
    )


def news_collection_kwargs() -> dict[str, Any]:
    return {
        "vectors_config": {"dense": dense_vector_params(ef_construct=100, quantile=0.99, always_ram=True)},
        "sparse_vectors_config": {"sparse": models.SparseVectorParams(modifier=models.Modifier.IDF)},
    }


def research_collection_kwargs() -> dict[str, Any]:
    vp = dense_vector_params(ef_construct=100)
    return {
        "vectors_config": {
            "abstract_vector": vp,
            "content_vector": vp,
        },
        "sparse_vectors_config": {"sparse": models.SparseVectorParams(modifier=models.Modifier.IDF)},
    }


def ota_collection_kwargs() -> dict[str, Any]:
    vp = dense_vector_params(ef_construct=100)
    return {
        "vectors_config": {
            "insight_vector": vp,
            "metric_vector": vp,
            "recommendation_vector": vp,
        },
        "sparse_vectors_config": {
            "sparse_insight": models.SparseVectorParams(modifier=models.Modifier.IDF),
            "sparse_recommendation": models.SparseVectorParams(modifier=models.Modifier.IDF),
        },
    }


def bq_collection_kwargs() -> dict[str, Any]:
    vp = dense_vector_params(ef_construct=80)
    return {
        "vectors_config": {
            "table_vector": vp,
            "schema_vector": vp,
            "business_vector": vp,
        },
    }


COLLECTION_BUILDERS: dict[str, Any] = {
    "news": news_collection_kwargs,
    "research": research_collection_kwargs,
    "ota": ota_collection_kwargs,
    "data_description": bq_collection_kwargs,
}

# Payload fields indexed for server-side Filter / Range (must match loader payloads).
PAYLOAD_INDEXES: dict[str, list[tuple[str, models.PayloadSchemaType]]] = {
    "news": [
        ("doc_kind", models.PayloadSchemaType.KEYWORD),
        ("published_at", models.PayloadSchemaType.KEYWORD),
        ("geo_country_primary", models.PayloadSchemaType.KEYWORD),
        ("country", models.PayloadSchemaType.KEYWORD),
        ("geo_scope", models.PayloadSchemaType.KEYWORD),
        ("domains", models.PayloadSchemaType.TEXT),
    ],
    "research": [
        ("doc_kind", models.PayloadSchemaType.KEYWORD),
        ("geo_country_primary", models.PayloadSchemaType.KEYWORD),
        ("geo_countries", models.PayloadSchemaType.TEXT),
    ],
    "ota": [
        ("doc_kind", models.PayloadSchemaType.KEYWORD),
        ("geo_country_primary", models.PayloadSchemaType.KEYWORD),
        ("geo_scope", models.PayloadSchemaType.KEYWORD),
    ],
    "data_description": [
        ("doc_kind", models.PayloadSchemaType.KEYWORD),
        ("table_name", models.PayloadSchemaType.KEYWORD),
    ],
}


def ensure_payload_indexes(client: Any, collection_name: str, corpus: str) -> list[str]:
    """Create payload indexes for a collection (idempotent). Returns field names created."""
    created: list[str] = []
    for field_name, schema in PAYLOAD_INDEXES.get(corpus, []):
        try:
            client.create_payload_index(
                collection_name=collection_name,
                field_name=field_name,
                field_schema=schema,
            )
            created.append(field_name)
        except Exception as exc:
            msg = str(exc).lower()
            if "already exists" in msg or "already indexed" in msg:
                continue
            raise
    return created


def estimate_points_per_gib(*, dim: int = DENSE_DIM, include_sparse: bool = True) -> int:
    """
    Rough capacity for a 1 GiB RAM single-node cluster.

    Assumes INT8 quantized dense vectors, HNSW m=8, optimized payload (~500 B),
    and optional sparse BM25 overhead.
    """
    dense_bytes = dim  # INT8 per dimension
    hnsw_overhead = HNSW_M * dim * 4  # graph links (approx)
    payload_bytes = 500
    sparse_bytes = 200 if include_sparse else 0
    per_point = dense_bytes + hnsw_overhead + payload_bytes + sparse_bytes
    gib = 1024**3
    usable = int(gib * 0.65)  # headroom for OS / Qdrant process
    return max(1, usable // max(per_point, 1))


def print_capacity_estimates() -> None:
    dim = DENSE_DIM
    pts_dense_only = estimate_points_per_gib(dim=dim, include_sparse=False)
    pts_hybrid = estimate_points_per_gib(dim=dim, include_sparse=True)
    print("\n==============================")
    print("CAPACITY ESTIMATES (1 GiB RAM node)")
    print("==============================")
    print(f"  Dense dim: {dim} (INT8), HNSW m={HNSW_M}")
    print(f"  ~{pts_dense_only:,} points (dense only)")
    print(f"  ~{pts_hybrid:,} points (dense + sparse BM25)")
    print("  News @ 500 tokens/chunk, max 10 chunks/doc → ~50k articles at 500k points")
    print("  Reindex after model/dim/HNSW changes (--reset on loaders).")
