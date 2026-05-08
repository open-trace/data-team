from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from ml.rag.ingestion.collections import ALL_SPECS


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


@dataclass(frozen=True)
class IngestionSettings:
    # Google Drive
    gdrive_folder_research_papers_id: str
    gdrive_folder_news_id: str
    gdrive_folder_data_descriptions_id: str
    # Local cache root for synced Drive files
    staging_root: Path

    # Qdrant collection names
    qdrant_collection_research_papers: str
    qdrant_collection_news: str
    qdrant_collection_data_descriptions: str


def _default_staging_root() -> Path:
    # repo root: .../ml/rag/ingestion/settings.py -> parents[3] == ml-eng
    ml_eng_root = Path(__file__).resolve().parents[3]
    return (ml_eng_root / "data" / "local" / "gdrive_cache").resolve()


def load_ingestion_settings_from_env() -> IngestionSettings:
    """
    Required env:
      - GDRIVE_FOLDER_RESEARCH_PAPERS_ID
      - GDRIVE_FOLDER_NEWS_ID
      - GDRIVE_FOLDER_DATA_DESCRIPTIONS_ID

    Optional env:
      - RAG_INGESTION_STAGING_ROOT (default: ml-eng/data/local/gdrive_cache)
      - QDRANT_COLLECTION_RESEARCH_PAPERS (default: opentrace_research_papers)
      - QDRANT_COLLECTION_NEWS (default: opentrace_news)
      - QDRANT_COLLECTION_DATA_DESCRIPTIONS (default: opentrace_data_descriptions)
    """
    rp = _env("GDRIVE_FOLDER_RESEARCH_PAPERS_ID")
    nw = _env("GDRIVE_FOLDER_NEWS_ID")
    dd = _env("GDRIVE_FOLDER_DATA_DESCRIPTIONS_ID")
    missing = [k for k, v in (
        ("GDRIVE_FOLDER_RESEARCH_PAPERS_ID", rp),
        ("GDRIVE_FOLDER_NEWS_ID", nw),
        ("GDRIVE_FOLDER_DATA_DESCRIPTIONS_ID", dd),
    ) if not v]
    if missing:
        raise RuntimeError(f"Missing env vars for ingestion: {', '.join(missing)}")

    staging = _env("RAG_INGESTION_STAGING_ROOT")
    staging_root = Path(staging).expanduser().resolve() if staging else _default_staging_root()

    return IngestionSettings(
        gdrive_folder_research_papers_id=rp,
        gdrive_folder_news_id=nw,
        gdrive_folder_data_descriptions_id=dd,
        staging_root=staging_root,
        qdrant_collection_research_papers=_env(
            "QDRANT_COLLECTION_RESEARCH_PAPERS",
            ALL_SPECS["research"].default_collection_name,
        ) or ALL_SPECS["research"].default_collection_name,
        qdrant_collection_news=_env(
            "QDRANT_COLLECTION_NEWS",
            ALL_SPECS["news"].default_collection_name,
        ) or ALL_SPECS["news"].default_collection_name,
        qdrant_collection_data_descriptions=_env(
            "QDRANT_COLLECTION_DATA_DESCRIPTIONS",
            ALL_SPECS["data_descriptions"].default_collection_name,
        ) or ALL_SPECS["data_descriptions"].default_collection_name,
    )

