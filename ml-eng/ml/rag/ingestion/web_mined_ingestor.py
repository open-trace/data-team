"""
Automated pipeline: web mining → preprocess → upsert to Qdrant news collection.

This module chains three steps:
  1. Run the RSS-first web mining pipeline (ml.web_data_mining) → .txt articles on disk
  2. Preprocess the .txt articles into chunk JSONL (ml.rag.text_processors)
  3. Upsert the chunks into the Qdrant news collection

Usage (CLI):
  python -m ml.rag.ingestion.cli mine-and-ingest \\
      --countries Nigeria,Ghana,Kenya \\
      --start-year 2025

Programmatic:
  from ml.rag.ingestion.web_mined_ingestor import mine_and_ingest
  mine_and_ingest(countries=["Nigeria", "Ghana"], start_year=2025, end_year=2025)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


@dataclass(frozen=True)
class MineAndIngestResult:
    articles_saved: int
    chunks_written: int
    upserted: int
    output_dir: Path
    chunk_jsonl: Path
    collection: str


def mine_and_ingest(
    *,
    countries: list[str],
    start_year: int | None = None,
    end_year: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_dir: Path | None = None,
    max_urls_per_country: int = 500,
    batch_size: int = 100,
    collection: str | None = None,
    reset: bool = False,
    upsert_batch_size: int = 200,
    feeds_path: Path | None = None,
    google_news_daily_slice: bool = False,
) -> MineAndIngestResult:
    """
    Run web mining then immediately preprocess and upsert into the Qdrant news collection.

    At least one of (start_year/end_year) or (start_date/end_date) is required.
    """
    from ml.web_data_mining.pipeline import run_pipeline
    from ml.web_data_mining.schemas import RunParams, DEFAULT_DOMAINS
    from ml.rag.ingestion.settings import load_ingestion_settings_from_env
    from ml.rag.text_processors.news_collection_preprocessor import preprocess_news_collection
    from ml.rag.text_processors.news_load_to_vector_db import load_news_to_qdrant
    from ml.rag.paths import preprocessed_data_root

    # Resolve Qdrant collection
    settings = load_ingestion_settings_from_env()
    effective_collection = collection or settings.qdrant_collection_news

    # Resolve output directory for mined articles
    if output_dir is None:
        raw = _env("MINING_OUTPUT_DIR")
        output_dir = Path(raw) if raw else Path("/data/web_news_rss")
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Step 1: Web mining ---
    this_year = date.today().year
    if start_date and end_date:
        params = RunParams(
            countries=countries,
            domains=list(DEFAULT_DOMAINS),
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
            max_urls_per_country=max_urls_per_country,
            output_dir=str(output_dir),
            feeds_path=str(feeds_path) if feeds_path else None,
            google_news_daily_slice=google_news_daily_slice,
        )
    else:
        params = RunParams(
            countries=countries,
            domains=list(DEFAULT_DOMAINS),
            start_year=start_year or this_year,
            end_year=end_year or this_year,
            batch_size=batch_size,
            max_urls_per_country=max_urls_per_country,
            output_dir=str(output_dir),
            feeds_path=str(feeds_path) if feeds_path else None,
            google_news_daily_slice=google_news_daily_slice,
        )
    articles_saved = run_pipeline(params)

    # --- Step 2: Preprocess ---
    chunks_dir = preprocessed_data_root()
    chunks_dir.mkdir(parents=True, exist_ok=True)
    chunk_jsonl = chunks_dir / "news_web_mined_chunks.jsonl"

    chunks_written = preprocess_news_collection(
        input_dir=output_dir,
        output_path=chunk_jsonl,
    )

    # --- Step 3: Upsert to Qdrant ---
    upserted = load_news_to_qdrant(
        input_path=chunk_jsonl,
        collection=effective_collection,
        reset=reset,
        batch_size=upsert_batch_size,
    )

    return MineAndIngestResult(
        articles_saved=articles_saved,
        chunks_written=chunks_written,
        upserted=upserted,
        output_dir=output_dir,
        chunk_jsonl=chunk_jsonl,
        collection=effective_collection,
    )
