"""
Vector DB retriever: Qdrant Cloud.

This module keeps the retriever interface stable (`BaseRetriever`) while allowing
the vector store implementation to be swapped. In this repo, RAG uses Qdrant Cloud.
"""

from __future__ import annotations

import os
import re
from typing import Any

from ml.rag.retrievers.base import BaseRetriever


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def sentence_embedding_model_id() -> str:
    """Model id for ``sentence`` vectors and legacy retrieval defaults when aligned with loaders."""
    return _env("RAG_EMBEDDING_MODEL_SENTENCE") or _env(
        "RAG_EMBEDDING_MODEL_ID", "sentence-transformers/all-MiniLM-L6-v2"
    )


def semantic_embedding_model_id() -> str:
    """Model id for ``semantic`` named vectors (dual-vector collections)."""
    return _env("RAG_EMBEDDING_MODEL_SEMANTIC") or sentence_embedding_model_id()


def _get_qdrant_config() -> tuple[str, str, str, float]:
    url = _env("QDRANT_URL")
    api_key = _env("QDRANT_API_KEY")
    collection = _env("QDRANT_COLLECTION", "opentrace_rag") or "opentrace_rag"
    try:
        timeout_s = float(_env("QDRANT_TIMEOUT_S", "30") or 30)
    except Exception:
        timeout_s = 30.0
    if not url or not api_key:
        raise RuntimeError(
            "Qdrant is not configured. Set QDRANT_URL and QDRANT_API_KEY "
            "(and optionally QDRANT_COLLECTION, QDRANT_TIMEOUT_S)."
        )
    return url, api_key, collection, timeout_s


def _embed_texts(texts: list[str], *, model_id: str, mode: str) -> list[list[float]]:
    mode = (mode or "local").strip().lower()
    if mode == "hf_api":
        token = _env("HF_API_TOKEN")
        if not token:
            raise RuntimeError("RAG_EMBEDDINGS_MODE=hf_api requires HF_API_TOKEN")
        import requests

        url = f"https://api-inference.huggingface.co/pipeline/feature-extraction/{model_id}"
        headers = {"Authorization": f"Bearer {token}"}
        out: list[list[float]] = []
        for t in texts:
            r = requests.post(url, headers=headers, json={"inputs": t}, timeout=120)
            r.raise_for_status()
            data = r.json()

            # HF feature-extraction commonly returns [seq][dim]; mean-pool if needed.
            if isinstance(data, list) and data and isinstance(data[0], list):
                first = data[0]
                if first and isinstance(first[0], (int, float)):
                    # [seq][dim]
                    dim = len(first)
                    sums = [0.0] * dim
                    n = 0
                    for row in data:
                        if not isinstance(row, list) or len(row) != dim:
                            continue
                        for i, x in enumerate(row):
                            sums[i] += float(x)
                        n += 1
                    vec = [s / max(1, n) for s in sums]
                else:
                    raise RuntimeError("Unexpected HF embedding response")
            elif isinstance(data, list) and data and isinstance(data[0], (int, float)):
                # [dim]
                vec = [float(x) for x in data]
            else:
                raise RuntimeError("Unexpected HF embedding response")

            out.append(vec)
        return out

    from sentence_transformers import SentenceTransformer

    m = SentenceTransformer(model_id)
    vecs = m.encode(texts, normalize_embeddings=True)
    return [[float(x) for x in row] for row in vecs]


def _safe_payload(meta: dict[str, Any]) -> dict[str, str | int | float | bool]:
    out: dict[str, str | int | float | bool] = {}
    for k, v in (meta or {}).items():
        if v is None:
            continue
        if isinstance(v, (str, int, float, bool)):
            out[str(k)] = v
        else:
            out[str(k)] = str(v)[:1000]
    return out


def _merge_scored_hits(hits_lists: list[list[Any]], limit: int) -> list[Any]:
    """Merge multiple search results by point id, keeping the higher score."""
    best: dict[Any, tuple[float, Any]] = {}
    for hits in hits_lists:
        for h in hits or []:
            pid = getattr(h, "id", None)
            if pid is None:
                continue
            sc = float(getattr(h, "score", 0.0) or 0.0)
            if pid not in best or sc > best[pid][0]:
                best[pid] = (sc, h)
    merged = sorted(best.values(), key=lambda x: x[0], reverse=True)
    return [t[1] for t in merged[:limit]]


class VectorRetriever(BaseRetriever):
    """
    Qdrant Cloud retriever.

    Env:
      - QDRANT_URL / QDRANT_API_KEY / QDRANT_COLLECTION
      - RAG_EMBEDDINGS_MODE=local|hf_api (optional)
      - RAG_EMBEDDING_MODEL_ID (legacy single-vector collections)
      - RAG_EMBEDDING_MODEL_SENTENCE / RAG_EMBEDDING_MODEL_SEMANTIC (named vectors)
      - RAG_QDRANT_QUERY_USING=sentence|semantic|both (dual-vector query)
      - RAG_QDRANT_VECTOR_SEARCH_MODE=legacy|dual|sentence_named (default for retrieve)
    """

    def __init__(
        self,
        collection_name: str | None = None,
        embed_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    ):
        self.collection_name = (
            (collection_name or _env("QDRANT_COLLECTION", "opentrace_rag")) or "opentrace_rag"
        )
        self.embed_model = embed_model
        self._client = None
        self._embed_mode = _env("RAG_EMBEDDINGS_MODE", "local") or "local"
        self._embed_model_id = _env("RAG_EMBEDDING_MODEL_ID", self.embed_model) or self.embed_model

    def _get_client(self):
        if self._client is not None:
            return self._client
        url, api_key, _, timeout_s = _get_qdrant_config()
        try:
            from qdrant_client import QdrantClient
        except ImportError as e:
            raise ImportError("Install qdrant-client: pip install qdrant-client") from e
        self._client = QdrantClient(url=url, api_key=api_key, timeout=timeout_s)
        return self._client

    def _ensure_collection(self) -> None:
        """Auto-create only legacy single-vector collections; named-vector collections are created by loaders."""
        client = self._get_client()
        collection = self.collection_name
        try:
            client.get_collection(collection_name=collection)
            return
        except Exception:
            pass

        dim = len(_embed_texts(["ping"], model_id=self._embed_model_id, mode=self._embed_mode)[0])
        from qdrant_client.http.models import Distance, VectorParams

        client.create_collection(
            collection_name=collection,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        )

    def _metadata_passes_filters(
        self,
        meta: dict[str, Any],
        *,
        doc_kind: str | None,
        geo_country: str | None,
        published_at_from: str | None,
        published_at_to: str | None,
        domains_substring: str | None,
    ) -> bool:
        if doc_kind:
            dk = str(meta.get("doc_kind") or "").strip()
            it = str(meta.get("info_type") or "").strip()
            if doc_kind == "news_article":
                if dk != "news_article" and it != "news_article":
                    return False
            elif doc_kind == "academic_article":
                if dk != "academic_article" and it != "academic_article":
                    return False
            elif doc_kind == "bq_table_description":
                if dk != "bq_table_description":
                    st = str(meta.get("type") or "")
                    if not st.strip().lower().startswith("bq "):
                        return False
            elif dk != doc_kind and it != doc_kind:
                return False

        if geo_country:
            gc = geo_country.strip().lower()
            primary = str(meta.get("geo_country_primary") or meta.get("country") or "").lower()
            blob = str(meta.get("geo_countries") or "").lower()
            if gc not in primary and gc not in blob:
                return False

        pub = str(meta.get("published_at") or "").strip()[:10]
        if pub and re.match(r"^\d{4}-\d{2}-\d{2}$", pub):
            if published_at_from and pub < published_at_from:
                return False
            if published_at_to and pub > published_at_to:
                return False

        if domains_substring:
            ds = (meta.get("domains") or meta.get("domain") or "")
            if domains_substring.lower() not in str(ds).lower():
                return False

        return True

    def retrieve(self, query: str, top_k: int = 10, **kwargs: Any) -> list[dict[str, Any]]:
        """
        Return top_k similar chunks from Qdrant. Each item: { "content", "score", "metadata", "source": "vector" }.

        Optional filters (post-filter after over-fetching):
          doc_kind: e.g. "news_article", "academic_article", "bq_table_description"
          geo_country: substring match against geo_country_primary / geo_countries / country
          published_at_from, published_at_to: YYYY-MM-DD (compared to metadata published_at)
          domains_substring: substring match in metadata domains or domain
          vector_search_mode: "legacy" (default unnamed vector) | "dual" (named sentence+semantic) |
            "sentence_named" (single named sentence). Overrides RAG_QDRANT_VECTOR_SEARCH_MODE.
        """
        doc_kind = kwargs.get("doc_kind")
        if isinstance(doc_kind, str):
            doc_kind = doc_kind.strip() or None
        else:
            doc_kind = None

        geo_country = kwargs.get("geo_country")
        if isinstance(geo_country, str):
            geo_country = geo_country.strip() or None
        else:
            geo_country = None

        published_at_from = kwargs.get("published_at_from")
        if not isinstance(published_at_from, str) or not published_at_from.strip():
            published_at_from = None
        else:
            published_at_from = published_at_from.strip()[:10]

        published_at_to = kwargs.get("published_at_to")
        if not isinstance(published_at_to, str) or not published_at_to.strip():
            published_at_to = None
        else:
            published_at_to = published_at_to.strip()[:10]

        domains_substring = kwargs.get("domains_substring")
        if isinstance(domains_substring, str):
            domains_substring = domains_substring.strip() or None
        else:
            domains_substring = None

        has_filters = any([doc_kind, geo_country, published_at_from, published_at_to, domains_substring])
        overfetch = int(kwargs.get("overfetch_multiplier", 8 if has_filters else 1))
        overfetch = max(1, min(overfetch, 50))

        vector_search_mode = kwargs.pop("vector_search_mode", None)
        if vector_search_mode is None:
            vector_search_mode = _env("RAG_QDRANT_VECTOR_SEARCH_MODE", "legacy") or "legacy"
        vector_search_mode = str(vector_search_mode).strip().lower()

        if vector_search_mode == "legacy":
            self._ensure_collection()
        client = self._get_client()
        collection = self.collection_name

        fetch_n = max(top_k, top_k * overfetch)

        q_filter = None
        if doc_kind and doc_kind in ("news_article", "academic_article", "bq_table_description"):
            try:
                from qdrant_client.http.models import FieldCondition, Filter, MatchValue

                q_filter = Filter(must=[FieldCondition(key="doc_kind", match=MatchValue(value=doc_kind))])
            except Exception:
                q_filter = None

        if vector_search_mode == "legacy":
            query_vec = _embed_texts([query], model_id=self._embed_model_id, mode=self._embed_mode)[0]
            hits = client.search(
                collection_name=collection,
                query_vector=query_vec,
                limit=fetch_n,
                with_payload=True,
                query_filter=q_filter,
            )
        elif vector_search_mode == "sentence_named":
            qv = _embed_texts(
                [query], model_id=sentence_embedding_model_id(), mode=self._embed_mode
            )[0]
            hits = client.search(
                collection_name=collection,
                query_vector=("sentence", qv),
                limit=fetch_n,
                with_payload=True,
                query_filter=q_filter,
            )
        elif vector_search_mode == "dual":
            using = (_env("RAG_QDRANT_QUERY_USING", "sentence") or "sentence").strip().lower()
            mid_s = sentence_embedding_model_id()
            mid_m = semantic_embedding_model_id()
            if using == "both":
                q_s = _embed_texts([query], model_id=mid_s, mode=self._embed_mode)[0]
                q_m = _embed_texts([query], model_id=mid_m, mode=self._embed_mode)[0]
                h1 = client.search(
                    collection_name=collection,
                    query_vector=("sentence", q_s),
                    limit=fetch_n,
                    with_payload=True,
                    query_filter=q_filter,
                )
                h2 = client.search(
                    collection_name=collection,
                    query_vector=("semantic", q_m),
                    limit=fetch_n,
                    with_payload=True,
                    query_filter=q_filter,
                )
                hits = _merge_scored_hits([h1 or [], h2 or []], fetch_n)
            elif using == "semantic":
                q_m = _embed_texts([query], model_id=mid_m, mode=self._embed_mode)[0]
                hits = client.search(
                    collection_name=collection,
                    query_vector=("semantic", q_m),
                    limit=fetch_n,
                    with_payload=True,
                    query_filter=q_filter,
                )
            else:
                q_s = _embed_texts([query], model_id=mid_s, mode=self._embed_mode)[0]
                hits = client.search(
                    collection_name=collection,
                    query_vector=("sentence", q_s),
                    limit=fetch_n,
                    with_payload=True,
                    query_filter=q_filter,
                )
        else:
            raise ValueError(
                f"Unknown vector_search_mode: {vector_search_mode!r} "
                "(use legacy, dual, or sentence_named)"
            )

        items: list[dict[str, Any]] = []
        for h in hits or []:
            payload = h.payload or {}
            if not isinstance(payload, dict):
                payload = {}
            content = str(payload.get("content") or "").strip()
            meta = _safe_payload({k: v for k, v in payload.items() if k != "content"})
            if not self._metadata_passes_filters(
                meta,
                doc_kind=doc_kind,
                geo_country=geo_country,
                published_at_from=published_at_from,
                published_at_to=published_at_to,
                domains_substring=domains_substring,
            ):
                continue
            items.append(
                {
                    "content": content,
                    "score": float(getattr(h, "score", 0.0) or 0.0),
                    "metadata": meta,
                    "source": "vector",
                }
            )
            if len(items) >= top_k:
                break

        items.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
        return items[:top_k]
