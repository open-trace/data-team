"""
Vector DB retriever: ChromaDB in-repo (or FAISS). Uses BQ for live queries; vector DB
for semantic search over ingested docs. Data dir: data/local/vector_db by default.
"""
from __future__ import annotations

from chromadb.api.types import Document
import os
from pathlib import Path
import re
from typing import Any

from ml.rag.retrievers.base import BaseRetriever

# Default: persist under repo so vector DB lives in repo (path can be gitignored)
_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DB_PATH = _REPO_ROOT / "data" / "local" / "vector_db"


def _get_db_path() -> Path:
    raw = os.environ.get("RAG_VECTOR_DB_PATH", "").strip()
    if raw:
        return Path(raw).resolve()
    return _DEFAULT_DB_PATH.resolve()


class VectorRetriever(BaseRetriever):
    """
    ChromaDB retriever with persistent storage under the repo (e.g. data/local/vector_db).
    Uses a local embedding model (sentence-transformers) so no API key is required.
    """

    def __init__(
        self,
        collection_name: str = "opentrace_rag",
        persist_path: str | Path | None = None,
        embed_model: str = "all-MiniLM-L6-v2",
    ):
        self.collection_name = collection_name
        self.persist_path = Path(persist_path) if persist_path else _get_db_path()
        self.embed_model = embed_model
        self._client = None
        self._collection = None

    def _get_collection(self):
        if self._collection is not None:
            return self._collection
        try:
            import chromadb  # type: ignore[import-untyped]
            from chromadb.config import Settings  # type: ignore[import-untyped]
            from chromadb.utils.embedding_functions import (
                DefaultEmbeddingFunction,
                SentenceTransformerEmbeddingFunction,
            )  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError(
                "Install chromadb and sentence-transformers: pip install chromadb sentence-transformers"
            ) from e
        self.persist_path.mkdir(parents=True, exist_ok=True)
        self._client = chromadb.PersistentClient(
            path=str(self.persist_path),
            settings=Settings(anonymized_telemetry=False),
        )
        try:
            ef = SentenceTransformerEmbeddingFunction(
                model_name=self.embed_model,
                device="cpu",
            )
        except Exception:
            # Fallback for environments with incompatible torch/transformers stacks.
            # Chroma's default embedding function does not require sentence-transformers.
            ef = DefaultEmbeddingFunction()
        self._collection = self._client.get_or_create_collection(
            name=self.collection_name,
            embedding_function=ef,  # type: ignore[arg-type]
            metadata={"hnsw:space": "cosine"},
        )
        return self._collection

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
        Return top_k similar chunks from ChromaDB. Each item: { "content", "score", "metadata", "source": "vector" }.

        Optional filters (post-filter after over-fetching):
          doc_kind: e.g. "news_article", "academic_article", "bq_table_description"
          geo_country: substring match against geo_country_primary / geo_countries / country
          published_at_from, published_at_to: YYYY-MM-DD (compared to metadata published_at)
          domains_substring: substring match in metadata domains or domain
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

        has_filters = any(
            [doc_kind, geo_country, published_at_from, published_at_to, domains_substring]
        )
        overfetch = int(kwargs.get("overfetch_multiplier", 8 if has_filters else 1))
        overfetch = max(1, min(overfetch, 50))

        coll = self._get_collection()
        n = coll.count()
        if n == 0:
            return []

        fetch_n = min(top_k * overfetch, n)
        fetch_n = max(fetch_n, min(top_k, n))

        where: dict[str, Any] | None = None
        if doc_kind and doc_kind in ("news_article", "academic_article", "bq_table_description"):
            where = {"doc_kind": doc_kind}

        try:
            results = coll.query(
                query_texts=[query],
                n_results=fetch_n,
                include=["documents", "metadatas", "distances"],
                where=where,
            )
        except Exception:
            # Older Chroma or missing index on metadata: fall back without where
            results = coll.query(
                query_texts=[query],
                n_results=fetch_n,
                include=["documents", "metadatas", "distances"],
            )

        items: list[dict[str, Any]] = []
        docs = results.get("documents") or [[]]
        metadatas = results.get("metadatas") or [[]]
        distances = results.get("distances") or [[]]
        for i, doc in enumerate[Document](docs[0] or []):  # type: ignore[reportUnknownReturnType, arg-type]
            meta = (metadatas[0] or [{}])[i] if i < len(metadatas[0] or []) else {}
            if not isinstance(meta, dict):
                meta = {}
            dist = (distances[0] or [0])[i] if i < len(distances[0] or []) else 0
            score = 1.0 / (1.0 + float(dist)) if dist is not None else 1.0
            if not self._metadata_passes_filters(
                meta,
                doc_kind=doc_kind,
                geo_country=geo_country,
                published_at_from=published_at_from,
                published_at_to=published_at_to,
                domains_substring=domains_substring,
            ):
                continue
            items.append({
                "content": doc,
                "score": score,
                "metadata": meta,
                "source": "vector",
            })
            if len(items) >= top_k * 3 and has_filters:
                # enough candidates; trim later
                pass

        if has_filters and len(items) < top_k:
            # Second pass: broader fetch without where if still short
            if where is not None:
                try:
                    results2 = coll.query(
                        query_texts=[query],
                        n_results=min(top_k * overfetch * 2, n),
                        include=["documents", "metadatas", "distances"],
                    )
                    docs2 = results2.get("documents") or [[]]
                    metadatas2 = results2.get("metadatas") or [[]]
                    distances2 = results2.get("distances") or [[]]
                    for i, doc in enumerate[Document](docs2[0] or []):  # type: ignore[arg-type]
                        meta = (metadatas2[0] or [{}])[i] if i < len(metadatas2[0] or []) else {}
                        if not isinstance(meta, dict):
                            meta = {}
                        dist = (distances2[0] or [0])[i] if i < len(distances2[0] or []) else 0
                        score = 1.0 / (1.0 + float(dist)) if dist is not None else 1.0
                        if not self._metadata_passes_filters(
                            meta,
                            doc_kind=doc_kind,
                            geo_country=geo_country,
                            published_at_from=published_at_from,
                            published_at_to=published_at_to,
                            domains_substring=domains_substring,
                        ):
                            continue
                        items.append({
                            "content": doc,
                            "score": score,
                            "metadata": meta,
                            "source": "vector",
                        })
                except Exception:
                    pass

        items.sort(key=lambda x: x.get("score", 0.0), reverse=True)
        return items[:top_k]
