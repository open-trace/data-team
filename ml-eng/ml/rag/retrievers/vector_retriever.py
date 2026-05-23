"""
Vector DB retriever: Qdrant Cloud.

This module keeps the retriever interface stable (`BaseRetriever`) while allowing
the vector store implementation to be swapped. In this repo, RAG uses Qdrant Cloud.

Typical collection layouts (embedding model must match how points were indexed):

  news_data / research_other_papers        – single named ``dense`` vector (mode: **dense_named**; e5-small)
  legacy dual-vector collections           – named ``sentence`` + ``semantic`` (mode: **dual**)
  research_other_papers (legacy schema)    – ``abstract_vector`` + ``content_vector`` (mode: **research_dual**)
  data descriptions (DOCX loader)          – named ``sentence`` only (mode: **sentence_named**)
  OTA_insights                             – insight / metric / recommendation (mode: **ota_triple**)
  BQ_table_descriptions (triple schema)    – table / schema / business (mode: **bq_triple**)
  single-vector collections                – unnamed vector (mode: **legacy**)
"""

from __future__ import annotations

import os
import re
from typing import Any

from ml.rag.retrievers.base import BaseRetriever
from ml.rag.text_processors.chunking_config import profile_for_collection

DEFAULT_MODEL = "intfloat/multilingual-e5-small"


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _research_exclude_roles() -> frozenset[str]:
    from ml.rag.text_processors.preprocess.section_roles import (
        exclude_boilerplate_enabled,
        research_excluded_roles,
    )

    if not exclude_boilerplate_enabled():
        return frozenset()
    return research_excluded_roles()


def _is_research_collection(collection_name: str) -> bool:
    try:
        return profile_for_collection(collection_name).corpus == "research"
    except Exception:
        return False


def embedding_model_id(collection_name: str | None = None) -> str:
    """Embedding model for a collection (per-corpus profile) or global fallback."""
    if collection_name:
        return profile_for_collection(collection_name).embedding_model
    return _env("RAG_EMBEDDING_MODEL_ID", DEFAULT_MODEL) or DEFAULT_MODEL


def _is_e5_model(model_id: str) -> bool:
    return "e5" in (model_id or "").lower()


def _prefix_texts(texts: list[str], *, model_id: str, is_query: bool) -> list[str]:
    if not _is_e5_model(model_id):
        return texts
    prefix = "query: " if is_query else "passage: "
    return [prefix + (t or "") for t in texts]


def _embed_texts_for_indexing(
    texts: list[str],
    *,
    model_id: str,
    mode: str,
    is_query: bool = False,
) -> list[list[float]]:
    return _embed_texts(_prefix_texts(texts, model_id=model_id, is_query=is_query), model_id=model_id, mode=mode)


def _get_qdrant_config() -> tuple[str, str, str, float]:
    url = _env("QDRANT_URL").strip('"').strip("'")
    api_key = _env("QDRANT_API_KEY").strip('"').strip("'")
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


def make_qdrant_client(*, timeout_s: float | None = None):
    """Qdrant client using env config; skips client/server version compatibility check."""
    from qdrant_client import QdrantClient

    url, api_key, _, default_timeout = _get_qdrant_config()
    return QdrantClient(
        url=url,
        api_key=api_key,
        timeout=int(timeout_s if timeout_s is not None else default_timeout or 30),
        check_compatibility=False,
    )


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

            if isinstance(data, list) and data and isinstance(data[0], list):
                first = data[0]
                if first and isinstance(first[0], (int, float)):
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
                vec = [float(x) for x in data]
            else:
                raise RuntimeError("Unexpected HF embedding response")

            out.append(vec)
        return out

    from sentence_transformers import SentenceTransformer

    m = SentenceTransformer(model_id)
    vecs = m.encode(texts, normalize_embeddings=True)
    return [[float(x) for x in row] for row in vecs]


def build_qdrant_filter(
    *,
    doc_kind: str | None = None,
    doc_kinds: list[str] | None = None,
    geo_country: str | None = None,
    published_at_from: str | None = None,
    published_at_to: str | None = None,
    domains_substring: str | None = None,
    exclude_section_roles: frozenset[str] | None = None,
) -> Any | None:
    """Build a Qdrant Filter for indexed payload fields (requires payload indexes)."""
    try:
        from qdrant_client.http.models import (
            FieldCondition,
            Filter,
            MatchAny,
            MatchText,
            MatchValue,
            Range,
        )
    except ImportError:
        return None

    must: list[Any] = []
    must_not: list[Any] = []

    kinds: list[str] = []
    if doc_kinds:
        kinds = [str(k).strip() for k in doc_kinds if str(k).strip()]
    elif doc_kind:
        kinds = [doc_kind.strip()]
    if kinds:
        if len(kinds) == 1:
            must.append(FieldCondition(key="doc_kind", match=MatchValue(value=kinds[0])))
        else:
            must.append(FieldCondition(key="doc_kind", match=MatchAny(any=kinds)))

    if geo_country:
        gc = geo_country.strip()
        if gc:
            must.append(
                Filter(
                    should=[
                        FieldCondition(key="geo_country_primary", match=MatchValue(value=gc)),
                        FieldCondition(key="country", match=MatchValue(value=gc)),
                        FieldCondition(key="geo_countries", match=MatchText(text=gc)),
                    ]
                )
            )

    if published_at_from or published_at_to:
        range_args: dict[str, str] = {}
        if published_at_from:
            range_args["gte"] = published_at_from
        if published_at_to:
            range_args["lte"] = published_at_to
        # KEYWORD index on ISO date strings (YYYY-MM-DD); lexicographic range is valid.
        must.append(
            FieldCondition(key="published_at", range=Range(**range_args))  # type: ignore[arg-type]
        )

    if domains_substring:
        ds = domains_substring.strip()
        if ds:
            must.append(FieldCondition(key="domains", match=MatchText(text=ds)))

    if exclude_section_roles:
        for role in sorted(exclude_section_roles):
            must_not.append(FieldCondition(key="section_role", match=MatchValue(value=role)))

    if not must and not must_not:
        return None
    return Filter(must=must or None, must_not=must_not or None)


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


# -- Named vector definitions per collection schema --------------------------

RESEARCH_VECTORS = ("abstract_vector", "content_vector")
DUAL_SENTENCE_SEMANTIC = ("sentence", "semantic")
OTA_VECTORS = ("insight_vector", "metric_vector", "recommendation_vector")
BQ_VECTORS = ("table_vector", "schema_vector", "business_vector")

# Maps a query_using shorthand → actual named vector(s) to search.
_RESEARCH_USING: dict[str, tuple[str, ...]] = {
    "abstract": ("abstract_vector",),
    "content": ("content_vector",),
    "both": RESEARCH_VECTORS,
}
_OTA_USING: dict[str, tuple[str, ...]] = {
    "insight": ("insight_vector",),
    "metric": ("metric_vector",),
    "recommendation": ("recommendation_vector",),
    "merge": OTA_VECTORS,
}
_BQ_USING: dict[str, tuple[str, ...]] = {
    "table": ("table_vector",),
    "schema": ("schema_vector",),
    "business": ("business_vector",),
    "merge": BQ_VECTORS,
}


def _sparse_names_for_collection(collection_name: str) -> tuple[str, ...]:
    """Sparse vector names for hybrid RRF when enabled for this collection."""
    try:
        from ml.rag.sparse_embeddings import hybrid_search_enabled, sparse_embeddings_enabled
        from ml.rag.scripts.qdrant_collection_specs import sparse_vector_names
    except ImportError:
        return ()
    if not hybrid_search_enabled() or not sparse_embeddings_enabled():
        return ()
    prof = profile_for_collection(collection_name)
    return sparse_vector_names(prof.corpus)


def _collection_has_hybrid_sparse(collection_name: str) -> bool:
    return bool(_sparse_names_for_collection(collection_name))


class VectorRetriever(BaseRetriever):
    """
    Qdrant Cloud retriever.

    Env:
      - QDRANT_URL / QDRANT_API_KEY / QDRANT_COLLECTION
      - RAG_EMBEDDINGS_MODE=local|hf_api (optional, default local)
      - RAG_EMBEDDING_MODEL_ID (default BAAI/bge-m3)
      - RAG_QDRANT_VECTOR_SEARCH_MODE=legacy|dual|sentence_named|research_dual|ota_triple|bq_triple
      - RAG_QDRANT_DUAL_QUERY_USING=sentence|semantic|both (only for mode dual; default both)
      - RAG_QDRANT_RESEARCH_QUERY_USING=abstract|content|both
      - RAG_QDRANT_OTA_QUERY_USING=insight|metric|recommendation|merge
      - RAG_QDRANT_BQ_QUERY_USING=table|schema|business|merge
      - RAG_SPARSE_EMBEDDINGS=on|off (BM25 sparse vectors on upsert; default on)
      - RAG_QDRANT_HYBRID_SEARCH=on|off (dense+sparse RRF at query; default on)
      - RAG_HYBRID_DENSE_PREFETCH / RAG_HYBRID_SPARSE_PREFETCH / RAG_HYBRID_FUSION_LIMIT (default 20 each)
    """

    def __init__(
        self,
        collection_name: str | None = None,
        embed_model: str = DEFAULT_MODEL,
    ):
        self.collection_name = (
            (collection_name or _env("QDRANT_COLLECTION", "opentrace_rag")) or "opentrace_rag"
        )
        self.embed_model = embed_model
        self._client = None
        self._embed_mode = _env("RAG_EMBEDDINGS_MODE", "local") or "local"
        self._embed_model_id = embedding_model_id(self.collection_name)

    def _get_client(self):
        if self._client is not None:
            return self._client
        url, api_key, _, timeout_s = _get_qdrant_config()
        try:
            from qdrant_client import QdrantClient
        except ImportError as e:
            raise ImportError("Install qdrant-client: pip install qdrant-client") from e
        self._client = make_qdrant_client(timeout_s=timeout_s)
        return self._client

    def _ensure_collection(self) -> None:
        """Auto-create only legacy single-vector collections; named-vector collections are created by the create script."""
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
        doc_kinds: list[str] | None = None,
        geo_country: str | None,
        published_at_from: str | None,
        published_at_to: str | None,
        domains_substring: str | None,
        exclude_section_roles: frozenset[str] | None = None,
    ) -> bool:
        allowed_kinds: list[str] = []
        if doc_kinds:
            allowed_kinds = [str(k).strip() for k in doc_kinds if str(k).strip()]
        elif doc_kind:
            allowed_kinds = [doc_kind.strip()]

        if allowed_kinds:
            dk = str(meta.get("doc_kind") or "").strip()
            it = str(meta.get("info_type") or "").strip()
            matched = dk in allowed_kinds or it in allowed_kinds
            if not matched and "bq_table_description" in allowed_kinds:
                matched = dk == "bq_table_description" or str(meta.get("type") or "").strip().lower().startswith(
                    "bq "
                )
            if not matched:
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

        if exclude_section_roles:
            role = str(meta.get("section_role") or "").strip().lower()
            if role in exclude_section_roles:
                return False

        return True

    # -- internal: query by named vectors ------------------------------------

    def _query_hybrid(
        self,
        query: str,
        dense_names: tuple[str, ...],
        sparse_names: tuple[str, ...],
        *,
        dense_prefetch: int,
        sparse_prefetch: int,
        fusion_limit: int,
        q_filter: Any,
    ) -> list[Any]:
        """Dense + sparse prefetch fused with reciprocal rank fusion (RRF)."""
        from ml.rag.sparse_embeddings import embed_sparse_query
        from qdrant_client.http.models import Fusion, FusionQuery, Prefetch

        client = self._get_client()
        collection = self.collection_name
        qv = _embed_texts_for_indexing(
            [query], model_id=self._embed_model_id, mode=self._embed_mode, is_query=True
        )[0]
        sqv = embed_sparse_query(query)

        query_kwargs: dict[str, Any] = {"limit": fusion_limit, "with_payload": True}
        if q_filter is not None:
            query_kwargs["query_filter"] = q_filter

        prefetch: list[Any] = []
        for vname in dense_names:
            prefetch.append(Prefetch(query=qv, using=vname, limit=dense_prefetch))
        for sname in sparse_names:
            prefetch.append(Prefetch(query=sqv, using=sname, limit=sparse_prefetch))

        resp = client.query_points(
            collection_name=collection,
            prefetch=prefetch,
            query=FusionQuery(fusion=Fusion.RRF),
            **query_kwargs,
        )
        return resp.points or []

    def _query_named_vectors(
        self,
        query: str,
        vector_names: tuple[str, ...],
        fetch_n: int,
        q_filter: Any,
        *,
        top_k: int,
        sparse_names: tuple[str, ...] | None = None,
    ) -> list[Any]:
        """Embed query once, search dense named vectors; optionally hybrid with sparse RRF."""
        if sparse_names is None:
            sparse_names = _sparse_names_for_collection(self.collection_name)
        if sparse_names:
            try:
                from ml.rag.sparse_embeddings import (
                    hybrid_dense_prefetch_limit,
                    hybrid_fusion_limit,
                    hybrid_sparse_prefetch_limit,
                )

                return self._query_hybrid(
                    query,
                    vector_names,
                    sparse_names,
                    dense_prefetch=hybrid_dense_prefetch_limit(),
                    sparse_prefetch=hybrid_sparse_prefetch_limit(),
                    fusion_limit=hybrid_fusion_limit(top_k=top_k),
                    q_filter=q_filter,
                )
            except ImportError:
                pass
            except Exception:
                # Fall back to dense-only if hybrid fails (e.g. empty sparse index).
                pass

        client = self._get_client()
        collection = self.collection_name
        qv = _embed_texts_for_indexing(
            [query], model_id=self._embed_model_id, mode=self._embed_mode, is_query=True
        )[0]

        query_kwargs: dict[str, Any] = {"limit": fetch_n, "with_payload": True}
        if q_filter is not None:
            query_kwargs["query_filter"] = q_filter

        if len(vector_names) == 1:
            resp = client.query_points(
                collection_name=collection,
                query=qv,
                using=vector_names[0],
                **query_kwargs,
            )
            return resp.points or []

        all_hits: list[list[Any]] = []
        for vname in vector_names:
            resp = client.query_points(
                collection_name=collection,
                query=qv,
                using=vname,
                **query_kwargs,
            )
            all_hits.append(resp.points or [])
        return _merge_scored_hits(all_hits, fetch_n)

    # -- public retrieve -----------------------------------------------------

    def retrieve(self, query: str, top_k: int = 10, **kwargs: Any) -> list[dict[str, Any]]:
        """
        Return top_k similar chunks from Qdrant.

        Each item: { "content", "score", "metadata", "source": "vector" }.

        Kwargs:
          vector_search_mode: legacy | dual | sentence_named | research_dual | ota_triple | bq_triple
          doc_kind / doc_kinds / geo_country / published_at_from / published_at_to / domains_substring
        """
        doc_kind = kwargs.get("doc_kind")
        if isinstance(doc_kind, str):
            doc_kind = doc_kind.strip() or None
        else:
            doc_kind = None

        raw_doc_kinds = kwargs.get("doc_kinds")
        doc_kinds: list[str] | None = None
        if isinstance(raw_doc_kinds, (list, tuple)):
            doc_kinds = [str(k).strip() for k in raw_doc_kinds if str(k).strip()] or None

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

        has_filters = any([doc_kind, doc_kinds, geo_country, published_at_from, published_at_to, domains_substring])

        vector_search_mode = kwargs.pop("vector_search_mode", None)
        if vector_search_mode is None:
            vector_search_mode = _env("RAG_QDRANT_VECTOR_SEARCH_MODE", "legacy") or "legacy"
        vector_search_mode = str(vector_search_mode).strip().lower()

        client = self._get_client()
        collection = self.collection_name
        use_hybrid = _collection_has_hybrid_sparse(collection)
        if use_hybrid:
            overfetch = 1
        else:
            overfetch = int(kwargs.get("overfetch_multiplier", 8 if has_filters else 1))
            overfetch = max(1, min(overfetch, 50))
        fetch_n = max(top_k, top_k * overfetch)
        exclude_section_roles = _research_exclude_roles() if _is_research_collection(collection) else frozenset()

        try:
            q_filter = build_qdrant_filter(
                doc_kind=doc_kind,
                doc_kinds=doc_kinds,
                geo_country=geo_country,
                published_at_from=published_at_from,
                published_at_to=published_at_to,
                domains_substring=domains_substring,
                exclude_section_roles=exclude_section_roles,
            )
        except Exception:
            q_filter = None

        query_kwargs: dict[str, Any] = {"limit": fetch_n, "with_payload": True}
        if q_filter is not None:
            query_kwargs["query_filter"] = q_filter

        # ----- legacy: single unnamed vector --------------------------------
        if vector_search_mode == "legacy":
            self._ensure_collection()
            query_vec = _embed_texts_for_indexing(
                [query], model_id=self._embed_model_id, mode=self._embed_mode, is_query=True
            )[0]
            resp = client.query_points(
                collection_name=collection,
                query=query_vec,
                **query_kwargs,
            )
            hits = resp.points or []

        # ----- dual: sentence + semantic (news/research JSONL loaders) -----
        elif vector_search_mode == "dual":
            using = _env("RAG_QDRANT_DUAL_QUERY_USING", "both").lower()
            if using == "sentence":
                vector_names: tuple[str, ...] = ("sentence",)
            elif using == "semantic":
                vector_names = ("semantic",)
            else:
                vector_names = DUAL_SENTENCE_SEMANTIC
            hits = self._query_named_vectors(query, vector_names, fetch_n, q_filter, top_k=top_k)

        # ----- dense_named: single named ``dense`` vector (news_data) -----
        elif vector_search_mode == "dense_named":
            hits = self._query_named_vectors(query, ("dense",), fetch_n, q_filter, top_k=top_k)

        # ----- sentence_named: single named ``sentence`` vector ------------
        elif vector_search_mode == "sentence_named":
            hits = self._query_named_vectors(query, ("sentence",), fetch_n, q_filter, top_k=top_k)

        # ----- research_dual: abstract_vector + content_vector --------------
        elif vector_search_mode == "research_dual":
            using = _env("RAG_QDRANT_RESEARCH_QUERY_USING", "content").lower()
            vector_names = _RESEARCH_USING.get(using, ("content_vector",))
            hits = self._query_named_vectors(query, vector_names, fetch_n, q_filter, top_k=top_k)

        # ----- ota_triple: insight / metric / recommendation ----------------
        elif vector_search_mode == "ota_triple":
            using = _env("RAG_QDRANT_OTA_QUERY_USING", "merge").lower()
            vector_names = _OTA_USING.get(using, OTA_VECTORS)
            hits = self._query_named_vectors(query, vector_names, fetch_n, q_filter, top_k=top_k)

        # ----- bq_triple: table / schema / business -------------------------
        elif vector_search_mode == "bq_triple":
            using = _env("RAG_QDRANT_BQ_QUERY_USING", "merge").lower()
            vector_names = _BQ_USING.get(using, BQ_VECTORS)
            hits = self._query_named_vectors(query, vector_names, fetch_n, q_filter, top_k=top_k)

        else:
            raise ValueError(
                f"Unknown vector_search_mode: {vector_search_mode!r} "
                "(use legacy, dense_named, dual, sentence_named, research_dual, ota_triple, or bq_triple)"
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
                doc_kinds=doc_kinds,
                geo_country=geo_country,
                published_at_from=published_at_from,
                published_at_to=published_at_to,
                domains_substring=domains_substring,
                exclude_section_roles=exclude_section_roles,
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
