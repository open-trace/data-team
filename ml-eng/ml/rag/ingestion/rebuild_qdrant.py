from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from ml.rag.ingestion.collections import ALL_SPECS, CollectionSpec
from ml.rag.ingestion.gdrive.auth import build_drive_service, load_auth_config_from_env
from ml.rag.ingestion.gdrive.sync import SyncStats, sync_drive_folder_to_cache
from ml.rag.ingestion.settings import IngestionSettings, load_ingestion_settings_from_env


@dataclass(frozen=True)
class RebuildResult:
    kind: str
    collection_name: str
    staging_dir: Path
    chunk_jsonl_path: Path
    sync: SyncStats
    upserted: int


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _chunks_dir() -> Path:
    return (_repo_root() / "data" / "local" / "ingestion_chunks").resolve()


def _run_preprocessor(kind: str, staging_dir: Path, out_jsonl: Path) -> None:
    if kind == "research":
        from ml.rag.text_processors.research_papers_preprocessor import preprocess_research_papers

        preprocess_research_papers(input_dir=staging_dir, output_path=out_jsonl)
        return

    if kind == "news":
        from ml.rag.text_processors.news_collection_preprocessor import preprocess_news_collection

        preprocess_news_collection(
            input_dir=staging_dir,
            output_path=out_jsonl,
            chunk_chars=1200,
            overlap_chars=200,
            max_files=None,
        )
        return

    if kind == "data_descriptions":
        from ml.rag.text_processors.data_descriptions_preprocessor import preprocess_data_descriptions

        preprocess_data_descriptions(
            input_dir=staging_dir,
            output_path=out_jsonl,
            chunk_chars=1000,
            overlap_chars=120,
        )
        return

    raise ValueError(f"Unknown kind: {kind}")


def _collection_name(settings: IngestionSettings, kind: str) -> str:
    if kind == "research":
        return settings.qdrant_collection_research_papers
    if kind == "news":
        return settings.qdrant_collection_news
    if kind == "data_descriptions":
        return settings.qdrant_collection_data_descriptions
    raise ValueError(f"Unknown kind: {kind}")


def _folder_id(settings: IngestionSettings, kind: str) -> str:
    if kind == "research":
        return settings.gdrive_folder_research_papers_id
    if kind == "news":
        return settings.gdrive_folder_news_id
    if kind == "data_descriptions":
        return settings.gdrive_folder_data_descriptions_id
    raise ValueError(f"Unknown kind: {kind}")


def rebuild_one(
    *,
    kind: str,
    reset: bool,
    settings: IngestionSettings | None = None,
) -> RebuildResult:
    settings = settings or load_ingestion_settings_from_env()
    spec: CollectionSpec = ALL_SPECS[kind]

    auth_cfg = load_auth_config_from_env()
    service = build_drive_service(auth_cfg)

    folder_id = _folder_id(settings, kind)
    staging_root = settings.staging_root
    staging_root.mkdir(parents=True, exist_ok=True)
    staging_dir, sync_stats = sync_drive_folder_to_cache(
        service=service,
        folder_id=folder_id,
        cache_root=staging_root,
        allowed_suffixes=spec.allowed_suffixes,
    )

    chunks_dir = _chunks_dir()
    chunks_dir.mkdir(parents=True, exist_ok=True)
    chunk_jsonl = (chunks_dir / f"{kind}_chunks.jsonl").resolve()
    _run_preprocessor(kind, staging_dir, chunk_jsonl)

    collection = _collection_name(settings, kind)
    batch_size = int(os.environ.get("RAG_INGESTION_UPSERT_BATCH_SIZE", "200") or 200)

    # Upsert using per-collection loader modules.
    if kind == "research":
        from ml.rag.text_processors.research_papers_load_to_vector_db import load_research_papers_to_qdrant

        upserted = load_research_papers_to_qdrant(
            input_path=chunk_jsonl,
            collection=collection,
            reset=reset,
            batch_size=batch_size,
        )
    elif kind == "news":
        from ml.rag.text_processors.news_load_to_vector_db import load_news_to_qdrant

        upserted = load_news_to_qdrant(
            input_path=chunk_jsonl,
            collection=collection,
            reset=reset,
            batch_size=batch_size,
        )
    elif kind == "data_descriptions":
        from ml.rag.text_processors.data_descriptions_load_to_vector_db import load_data_descriptions_to_qdrant

        upserted = load_data_descriptions_to_qdrant(
            input_path=chunk_jsonl,
            collection=collection,
            reset=reset,
            batch_size=batch_size,
        )
    else:
        raise ValueError(f"Unknown kind: {kind}")

    return RebuildResult(
        kind=kind,
        collection_name=collection,
        staging_dir=staging_dir,
        chunk_jsonl_path=chunk_jsonl,
        sync=sync_stats,
        upserted=upserted,
    )


def rebuild_many(*, kind: str, reset: bool) -> list[RebuildResult]:
    settings = load_ingestion_settings_from_env()
    if kind == "all":
        kinds = ["research", "news", "data_descriptions"]
    else:
        kinds = [kind]
    out: list[RebuildResult] = []
    for k in kinds:
        out.append(rebuild_one(kind=k, reset=reset, settings=settings))
    return out

