from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from ml.rag.ingestion.collections import DRIVE_REBUILD_KINDS, LIBRARY_REBUILD_KINDS
from ml.rag.ingestion.rebuild_qdrant import rebuild_many


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Rebuild Qdrant collections from Google Drive folders.")
    sub = p.add_subparsers(dest="cmd", required=True)

    rebuild = sub.add_parser("rebuild", help="Sync from Drive, preprocess, and upsert into Qdrant.")
    rebuild.add_argument(
        "--kind",
        type=str,
        default="all",
        choices=sorted([*DRIVE_REBUILD_KINDS, *LIBRARY_REBUILD_KINDS, "all"]),
        help="Which collection pipeline to rebuild.",
    )
    rebuild.add_argument("--reset", action="store_true", help="Delete+recreate collection before upserting.")
    rebuild.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable output (one JSON object).",
    )

    mine = sub.add_parser(
        "mine-and-ingest",
        help="Mine articles from RSS feeds, preprocess, and upsert into the Qdrant news collection.",
    )
    mine.add_argument(
        "--countries",
        type=str,
        required=True,
        help="Comma-separated country names, e.g. Nigeria,Ghana,Kenya.",
    )
    mine.add_argument("--start-year", type=int, default=None, help="Inclusive start year (default: current year).")
    mine.add_argument("--end-year", type=int, default=None, help="Inclusive end year (default: current year).")
    mine.add_argument("--start-date", type=str, default=None, help="Inclusive start date YYYY-MM-DD.")
    mine.add_argument("--end-date", type=str, default=None, help="Inclusive end date YYYY-MM-DD.")
    mine.add_argument("--output-dir", type=Path, default=None, help="Directory for mined .txt articles.")
    mine.add_argument("--max-urls-per-country", type=int, default=500)
    mine.add_argument("--batch-size", type=int, default=100)
    mine.add_argument("--feeds", type=Path, default=None, help="Path to feeds.json.")
    mine.add_argument("--google-news-daily-slice", action="store_true")
    mine.add_argument("--collection", type=str, default=None, help="Qdrant collection name (default from env).")
    mine.add_argument("--reset", action="store_true", help="Delete+recreate the news collection before upserting.")
    mine.add_argument("--upsert-batch-size", type=int, default=200)
    mine.add_argument("--json", action="store_true", help="Print machine-readable output.")

    return p


def main() -> int:
    from ml.rag.local_env import load_rag_dotenv
    from ml.rag.paths import ML_ENG_ROOT

    load_rag_dotenv(ML_ENG_ROOT)
    args = build_arg_parser().parse_args()

    if args.cmd == "rebuild":
        results = rebuild_many(kind=args.kind, reset=bool(args.reset))
        if args.json:
            print(json.dumps([asdict(r) for r in results], ensure_ascii=False))
        else:
            for r in results:
                academic = ""
                if r.academic_collection_name:
                    academic = (
                        f" academic_chunks={r.academic_chunk_jsonl_path}"
                        f" -> collection={r.academic_collection_name} upserted={r.academic_upserted}"
                    )
                print(
                    f"[{r.kind}] synced scanned={r.sync.scanned} downloaded={r.sync.downloaded} skipped={r.sync.skipped} "
                    f"chunks={r.chunk_jsonl_path} -> collection={r.collection_name} upserted={r.upserted}"
                    f"{academic}"
                )
        return 0

    if args.cmd == "mine-and-ingest":
        from datetime import datetime
        from ml.web_data_mining.schemas import parse_countries
        from ml.rag.ingestion.web_mined_ingestor import mine_and_ingest

        countries = parse_countries(args.countries)
        if not countries:
            raise SystemExit("--countries must be a non-empty comma-separated list")

        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date() if args.start_date else None
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else None

        result = mine_and_ingest(
            countries=countries,
            start_year=args.start_year,
            end_year=args.end_year,
            start_date=start_date,
            end_date=end_date,
            output_dir=args.output_dir,
            max_urls_per_country=args.max_urls_per_country,
            batch_size=args.batch_size,
            collection=args.collection,
            reset=args.reset,
            upsert_batch_size=args.upsert_batch_size,
            feeds_path=args.feeds,
            google_news_daily_slice=args.google_news_daily_slice,
        )

        if args.json:
            print(json.dumps({
                "articles_saved": result.articles_saved,
                "chunks_written": result.chunks_written,
                "upserted": result.upserted,
                "collection": result.collection,
                "output_dir": str(result.output_dir),
                "chunk_jsonl": str(result.chunk_jsonl),
            }, ensure_ascii=False))
        else:
            print(
                f"[mine-and-ingest] articles={result.articles_saved} "
                f"chunks={result.chunks_written} "
                f"upserted={result.upserted} "
                f"-> collection={result.collection}"
            )
        return 0

    raise SystemExit("Unknown command")


if __name__ == "__main__":
    raise SystemExit(main())

