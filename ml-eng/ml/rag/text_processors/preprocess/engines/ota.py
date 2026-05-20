from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Iterable

from ml.rag.text_processors.chunk_contract import document_id_from_path, enrich_metadata
from ml.rag.text_processors.chunking_config import profile_for_corpus
from ml.rag.text_processors.domain_taxonomy import infer_domains
from ml.rag.text_processors.preprocess.llama_split import split_text_to_slices
from ml.rag.text_processors.preprocess.models import ChunkOutput

LANES = ("insight_text", "metric_text", "recommendation_text")
_ALIASES: dict[str, tuple[str, ...]] = {
    "insight_text": ("insight_text", "text_insight"),
    "metric_text": ("metric_text", "text_metric"),
    "recommendation_text": ("recommendation_text", "text_recommendation"),
}


def _pick(row: dict[str, Any], lane: str, fallback: str) -> str:
    for key in _ALIASES[lane]:
        v = row.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
        meta = row.get("metadata")
        if isinstance(meta, dict):
            v2 = meta.get(key)
            if isinstance(v2, str) and v2.strip():
                return v2.strip()
    return fallback


def _iter_records(path: Path) -> Iterable[dict[str, Any]]:
    if path.suffix.lower() == ".jsonl":
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw:
                    continue
                row = json.loads(raw)
                if isinstance(row, dict):
                    yield row
    elif path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    yield item
        elif isinstance(data, dict):
            yield data


def _record_id(row: dict[str, Any], source: str) -> str:
    rid = str(row.get("id") or row.get("ota_record_id") or "").strip()
    if rid:
        return rid
    return str(uuid.uuid5(uuid.NAMESPACE_URL, source + json.dumps(row, sort_keys=True, default=str)[:500]))


def preprocess_record(row: dict[str, Any], *, source_file: str) -> list[ChunkOutput]:
    profile = profile_for_corpus("ota")
    record_id = _record_id(row, source_file)
    doc_id = document_id_from_path(source_file, dedupe_id=record_id)
    fallback = str(row.get("text", "")).strip()

    base_meta: dict[str, Any] = dict(row.get("metadata") or {}) if isinstance(row.get("metadata"), dict) else {}
    base_meta.setdefault("info_type", "ota_insight")
    base_meta["ota_record_id"] = record_id
    base_meta["source_file"] = source_file

    combined = " ".join(filter(None, [_pick(row, lane, fallback) for lane in LANES]))
    if "domains" not in base_meta and combined:
        base_meta["domains"] = "; ".join(infer_domains(combined))

    chunks: list[ChunkOutput] = []
    chunk_index = 0
    for lane in LANES:
        text = _pick(row, lane, "")
        if not text:
            continue
        pieces = split_text_to_slices(text, profile)
        for piece in pieces:
            meta = enrich_metadata(
                dict(base_meta),
                corpus="ota",
                document_id=doc_id,
                chunk_index=chunk_index,
                total_chunks=0,
                text=piece,
                hierarchy_path=lane,
                section_title=lane,
                semantic_lane=lane,
            )
            meta[lane] = piece
            for lk in LANES:
                if lk != lane:
                    meta[lk] = _pick(row, lk, "")
            chunks.append(
                ChunkOutput(id=str(meta["id"]), text=piece, metadata={k: v for k, v in meta.items() if k != "id"})
            )
            chunk_index += 1

    total = len(chunks)
    fixed: list[ChunkOutput] = []
    for i, ch in enumerate(chunks):
        m = dict(ch.metadata)
        m["chunk_index"] = i
        m["total_chunks"] = total
        fixed.append(ChunkOutput(id=ch.id, text=ch.text, metadata=m))
    return fixed


def preprocess_folder(input_dir: Path) -> list[ChunkOutput]:
    out: list[ChunkOutput] = []
    for path in sorted(input_dir.rglob("*"), key=lambda p: str(p).lower()):
        if not path.is_file() or path.suffix.lower() not in (".json", ".jsonl"):
            continue
        for row in _iter_records(path):
            out.extend(preprocess_record(row, source_file=str(path)))
    return out
