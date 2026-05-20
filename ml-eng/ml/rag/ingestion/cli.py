from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from ml.rag.ingestion.collections import DRIVE_REBUILD_KINDS
from ml.rag.ingestion.rebuild_qdrant import rebuild_many


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Rebuild Qdrant collections from Google Drive folders.")
    sub = p.add_subparsers(dest="cmd", required=True)

    rebuild = sub.add_parser("rebuild", help="Sync from Drive, preprocess, and upsert into Qdrant.")
    rebuild.add_argument(
        "--kind",
        type=str,
        default="all",
        choices=sorted(list(DRIVE_REBUILD_KINDS) + ["all"]),
        help="Which collection pipeline to rebuild.",
    )
    rebuild.add_argument("--reset", action="store_true", help="Delete+recreate collection before upserting.")
    rebuild.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable output (one JSON object).",
    )
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    if args.cmd == "rebuild":
        results = rebuild_many(kind=args.kind, reset=bool(args.reset))
        if args.json:
            print(json.dumps([asdict(r) for r in results], ensure_ascii=False))
        else:
            for r in results:
                print(
                    f"[{r.kind}] synced scanned={r.sync.scanned} downloaded={r.sync.downloaded} skipped={r.sync.skipped} "
                    f"chunks={r.chunk_jsonl_path} -> collection={r.collection_name} upserted={r.upserted}"
                )
        return 0
    raise SystemExit("Unknown command")


if __name__ == "__main__":
    raise SystemExit(main())

