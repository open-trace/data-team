from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CollectionSpec:
    """
    High-level ingestion target.

    `kind` is used by the CLI; `allowed_suffixes` is used during Drive sync; `default_doc_kind`
    is used by loaders when normalizing metadata.
    """

    kind: str
    collection_env: str
    default_collection_name: str
    allowed_suffixes: tuple[str, ...]


RESEARCH_PAPERS = CollectionSpec(
    kind="research",
    collection_env="QDRANT_COLLECTION_RESEARCH_PAPERS",
    default_collection_name="opentrace_research_papers",
    allowed_suffixes=(".pdf",),
)

NEWS = CollectionSpec(
    kind="news",
    collection_env="QDRANT_COLLECTION_NEWS",
    default_collection_name="opentrace_news",
    allowed_suffixes=(".txt",),
)

DATA_DESCRIPTIONS = CollectionSpec(
    kind="data_descriptions",
    collection_env="QDRANT_COLLECTION_DATA_DESCRIPTIONS",
    default_collection_name="opentrace_data_descriptions",
    allowed_suffixes=(".docx",),
)


ALL_SPECS: dict[str, CollectionSpec] = {
    RESEARCH_PAPERS.kind: RESEARCH_PAPERS,
    NEWS.kind: NEWS,
    DATA_DESCRIPTIONS.kind: DATA_DESCRIPTIONS,
}

