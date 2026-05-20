from __future__ import annotations

import json
import statistics
from pathlib import Path

from ml.rag.text_processors.preprocess.tokens import count_tokens

REQUIRED_META = (
    "document_id",
    "chunk_index",
    "total_chunks",
    "content_hash",
    "ingest_version",
    "doc_kind",
)


def validate_jsonl(path: Path) -> dict[str, float | int | str]:
    token_counts: list[int] = []
    errors: list[str] = []
    rows = 0
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            rows += 1
            try:
                row = json.loads(raw)
            except json.JSONDecodeError as e:
                errors.append(f"line {line_no}: {e}")
                continue
            if not row.get("id") or not row.get("text"):
                errors.append(f"line {line_no}: missing id or text")
            meta = row.get("metadata") or {}
            if not isinstance(meta, dict):
                errors.append(f"line {line_no}: metadata not object")
                continue
            for k in REQUIRED_META:
                if k not in meta:
                    errors.append(f"line {line_no}: missing metadata.{k}")
            token_counts.append(count_tokens(str(row.get("text", ""))))

    if errors:
        raise ValueError("JSONL validation failed:\n" + "\n".join(errors[:20]))

    return {
        "rows": rows,
        "token_p50": statistics.median(token_counts) if token_counts else 0,
        "token_mean": statistics.mean(token_counts) if token_counts else 0,
        "token_max": max(token_counts) if token_counts else 0,
    }
