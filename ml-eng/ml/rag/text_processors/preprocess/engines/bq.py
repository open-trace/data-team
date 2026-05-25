from __future__ import annotations

import html
import re
from dataclasses import dataclass

from pathlib import Path

from ml.rag.text_processors.chunk_contract import document_id_from_path, enrich_metadata
from ml.rag.text_processors.chunking_config import ChunkingProfile, profile_for_corpus
from ml.rag.text_processors.preprocess.llama_split import TextSlice
from ml.rag.text_processors.preprocess.models import ChunkOutput
from ml.rag.text_processors.preprocess.structure_blocks import ContentType, StructureBlock
from ml.rag.text_processors.preprocess.tokens import count_tokens
from ml.rag.text_processors.preprocess.unstructured_fast import partition_docx

_HEADING_ONLY_RE = re.compile(r"^(\d+\.\s*)?Table Overview\s*:?\s*$", re.IGNORECASE)
_TABLE_NAME_LINE_RE = re.compile(
    r"^(?:\d+\.\s*)?(?:Table\s+Name:|Name:)\s*(?P<full>[\w\-\.]+(?:\.[\w\-\.]+)*)\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_BQ_TABLE_ID_RE = re.compile(
    r"(opentrace[\w\-]*\.(?:bronze|silver|gold)\.[\w\d_]+)",
    re.IGNORECASE,
)
_ARCGIS_SUMMARY_HEADER = re.compile(r"Table Name \(in BigQuery\)", re.IGNORECASE)
_ARCGIS_DETAIL_SECTION_RE = re.compile(
    r"^\d+\.\s+.+\((?P<table>arcgis_[\w\d_]+)\)\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_MAJOR_SECTION_RE = re.compile(
    r"^(\d+\.\s*)?"
    r"(What the Table Entails|Source Details|Relationships|Column-Level Documentation|Data Insights|Notes)\b",
    re.IGNORECASE | re.MULTILINE,
)
_COLUMN_DOC_MARKER_RE = re.compile(
    r"Column Name\s*\||^Column Name:|^Column\s*$",
    re.IGNORECASE | re.MULTILINE,
)


@dataclass(frozen=True)
class _LogicalTable:
    table_name: str
    bq_table_id: str
    text: str


def _table_name_from_path(docx_path: Path) -> str:
    stem = re.sub(r"\s+", " ", docx_path.stem.strip())
    stem = re.sub(r"[^A-Za-z0-9_ ]+", "", stem).strip()
    return stem.replace(" ", "_") or "unknown_table"


def _short_table_name(full_id: str) -> str:
    cleaned = (full_id or "").strip().rstrip(".")
    if not cleaned:
        return "unknown_table"
    parts = cleaned.split(".")
    return parts[-1] if parts else cleaned


def _normalize_bq_text(raw: str) -> str:
    text = html.unescape(raw or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_primary_table_from_text(text: str, *, fallback: str) -> tuple[str, str]:
    match = _TABLE_NAME_LINE_RE.search(text)
    if match:
        full_id = match.group("full").strip().rstrip(".")
        return _short_table_name(full_id), full_id
    match = _BQ_TABLE_ID_RE.search(text)
    if match:
        full_id = match.group(1).strip().rstrip(".")
        return _short_table_name(full_id), full_id
    return fallback, ""


def _is_heading_only(text: str) -> bool:
    stripped = (text or "").strip()
    if not stripped:
        return True
    if _HEADING_ONLY_RE.match(stripped):
        return True
    if len(stripped) < 40 and stripped.lower().startswith("table overview"):
        return True
    return False


def _is_schema_text(text: str) -> bool:
    if _COLUMN_DOC_MARKER_RE.search(text):
        return True
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    pipe_rows = sum(1 for ln in lines if ln.startswith("|") and ln.count("|") >= 2)
    return pipe_rows >= 2


def _is_insights_text(text: str) -> bool:
    lowered = (text or "").lower()
    return any(
        marker in lowered
        for marker in ("data insights note", "data insights", "notes for data analysts", "note for data analysts")
    )


def _lane_from_slice(sl: TextSlice) -> str:
    title = (sl.section_title or "").lower()
    path = (sl.hierarchy_path or "").lower()
    if title.startswith("schema:") or "_schema" in path:
        return "schema"
    if title.startswith("insights:") or "_insights" in path:
        return "insights"
    if title.startswith("overview:") or "_overview" in path:
        return "overview"
    return _semantic_lane_for_text(sl.text)


def _semantic_lane_for_text(text: str) -> str:
    if _is_schema_text(text):
        return "schema"
    if _is_insights_text(text):
        return "insights"
    return "overview"


def _prepend_table_context(text: str, *, table_name: str, bq_table_id: str) -> str:
    body = (text or "").strip()
    if body.startswith(f"Table: {table_name}"):
        return body
    header = f"Table: {table_name}"
    if bq_table_id and bq_table_id != table_name:
        header = f"{header}\nBQ table: {bq_table_id}"
    return f"{header}\n\n{body}" if body else header


def _schema_blocks_from_text(text: str, *, table_name: str) -> list[StructureBlock]:
    """Split a single-table description into overview / schema / insights blocks."""
    lines = text.splitlines()
    blocks: list[StructureBlock] = []
    cur_title = f"table:{table_name}"
    cur_lines: list[str] = []
    cur_type: str = "prose"

    def flush() -> None:
        nonlocal cur_lines, cur_type
        body = "\n".join(cur_lines).strip()
        cur_lines = []
        if not body or _is_heading_only(body):
            return
        slug = re.sub(r"[^a-z0-9]+", "_", cur_title.lower())[:80] or "block"
        lane = _semantic_lane_for_text(body)
        content_type: ContentType = "table" if lane == "schema" and _is_schema_text(body) else "prose"
        blocks.append(
            StructureBlock(
                hierarchy_path=slug,
                section_title=cur_title[:200],
                text=body,
                content_type=content_type,
            )
        )

    for line in lines:
        stripped = line.strip()
        is_pipe_row = stripped.startswith("|") and "|" in stripped[1:]
        is_major = bool(_MAJOR_SECTION_RE.match(stripped))
        is_table_name_heading = bool(_TABLE_NAME_LINE_RE.match(stripped))

        if is_table_name_heading:
            if cur_lines:
                flush()
            cur_title = stripped[:120]
            continue

        if is_major and cur_lines:
            flush()
            cur_title = stripped[:120]
            cur_type = "schema" if "column" in stripped.lower() else "prose"
            continue

        if is_pipe_row and not cur_lines and blocks:
            flush()

        if _HEADING_ONLY_RE.match(stripped):
            if cur_lines:
                flush()
            cur_title = stripped[:120]
            continue

        cur_lines.append(line)

    flush()

    if not blocks and text.strip():
        lane = _semantic_lane_for_text(text)
        content_type = "table" if lane == "schema" else "prose"
        blocks.append(
            StructureBlock(
                hierarchy_path="description",
                section_title="description",
                text=text.strip(),
                content_type="table" if lane == "schema" else "prose",
            )
        )
    return blocks


def _parse_arcgis_detail_sections(text: str) -> dict[str, str]:
    """Map arcgis_* table short name -> detail section body."""
    sections: dict[str, list[str]] = {}
    current_key: str | None = None
    for line in text.splitlines():
        match = _ARCGIS_DETAIL_SECTION_RE.match(line.strip())
        if match:
            key = match.group("table").strip().lower()
            current_key = key
            sections[key] = [line.strip()]
            continue
        if current_key is not None:
            sections[current_key].append(line)
    return {k: "\n".join(v).strip() for k, v in sections.items() if v}


def _extract_arcgis_summary_rows(text: str) -> list[tuple[str, str, str, str]]:
    """Parse summary-table rows: bq_id, title, category, why-it-matters."""
    rows: list[tuple[str, str, str, str]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.lower().startswith("table name"):
            continue
        parts = [part.strip() for part in stripped.split("|")]
        if len(parts) >= 4:
            bq_id = parts[0].rstrip(".").strip()
            if bq_id.lower().startswith("opentrace"):
                rows.append((bq_id, parts[1], parts[2], "|".join(parts[3:]).strip()))
            continue

    if rows:
        return rows

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    start = 0
    for i, line in enumerate(lines):
        if _ARCGIS_SUMMARY_HEADER.search(line):
            start = i + 1
            break
    while start < len(lines) and not _BQ_TABLE_ID_RE.match(lines[start]):
        start += 1

    i = start
    while i + 3 < len(lines):
        bq_id = lines[i].rstrip(".").strip()
        if _ARCGIS_DETAIL_SECTION_RE.match(bq_id) or not bq_id.lower().startswith("opentrace"):
            break
        rows.append((bq_id, lines[i + 1], lines[i + 2], lines[i + 3]))
        i += 4
    return rows


def _extract_logical_tables(raw: str, *, fallback_table: str) -> list[_LogicalTable]:
    """One entry per BQ table; handles multi-table catalog DOCX (e.g. ArcGIS guide)."""
    text = _normalize_bq_text(raw)
    if not _ARCGIS_SUMMARY_HEADER.search(text):
        short, full = _extract_primary_table_from_text(text, fallback=fallback_table)
        bq_id = full or short
        return [_LogicalTable(table_name=short, bq_table_id=bq_id, text=text)]

    detail_by_table = _parse_arcgis_detail_sections(text)
    tables: list[_LogicalTable] = []
    seen: set[str] = set()

    for bq_id, title, category, why in _extract_arcgis_summary_rows(text):
        short = _short_table_name(bq_id)
        if short in seen:
            continue
        seen.add(short)

        summary = (
            f"Table: {short}\nBQ table: {bq_id}\n\n"
            f"Source title: {title}\n"
            f"Category: {category}\n"
            f"Why it matters: {why}"
        )
        detail_key = short.lower()
        combined = f"{summary}\n\n{detail_by_table[detail_key]}" if detail_key in detail_by_table else summary
        tables.append(_LogicalTable(table_name=short, bq_table_id=bq_id, text=combined))

    if tables:
        return tables

    short, full = _extract_primary_table_from_text(text, fallback=fallback_table)
    return [_LogicalTable(table_name=short, bq_table_id=full or short, text=text)]


def _merge_small_slices(slices: list[TextSlice], profile: ChunkingProfile) -> list[TextSlice]:
    if not slices:
        return []
    min_tok = max(1, profile.min_tokens)
    model_id = profile.embedding_model
    merged: list[TextSlice] = []
    pending: TextSlice | None = None

    for sl in slices:
        tok = count_tokens(sl.text, model_id=model_id)
        if pending is None:
            if tok < min_tok:
                pending = sl
            else:
                merged.append(sl)
            continue

        combined_text = f"{pending.text}\n\n{sl.text}".strip()
        pending = TextSlice(
            text=combined_text,
            section_title=sl.section_title or pending.section_title,
            hierarchy_path=sl.hierarchy_path or pending.hierarchy_path,
            content_type=sl.content_type,
        )
        if count_tokens(pending.text, model_id=model_id) >= min_tok:
            merged.append(pending)
            pending = None

    if pending is not None:
        if merged:
            prev = merged[-1]
            merged[-1] = TextSlice(
                text=f"{prev.text}\n\n{pending.text}".strip(),
                section_title=prev.section_title,
                hierarchy_path=prev.hierarchy_path,
                content_type=prev.content_type,
            )
        elif not _is_heading_only(pending.text):
            merged.append(pending)

    return merged


def _lane_for_block(block: StructureBlock) -> str:
    title = (block.section_title or "").lower()
    if block.content_type == "table" or "column" in title or _is_schema_text(block.text):
        return "schema"
    if "insight" in title or "notes for data" in title or "note for data" in title:
        return "insights"
    if _is_insights_text(block.text):
        return "insights"
    return "overview"


def _consolidate_blocks(blocks: list[StructureBlock], *, table_name: str) -> list[StructureBlock]:
    """Merge section blocks into overview / schema / insights wholes."""
    overview_parts: list[str] = []
    schema_parts: list[str] = []
    insight_parts: list[str] = []

    for block in blocks:
        lane = _lane_for_block(block)
        if lane == "schema":
            schema_parts.append(block.text.strip())
        elif lane == "insights":
            insight_parts.append(block.text.strip())
        else:
            overview_parts.append(block.text.strip())

    consolidated: list[StructureBlock] = []
    if overview_parts:
        consolidated.append(
            StructureBlock(
                hierarchy_path=f"table_{table_name}_overview",
                section_title=f"overview:{table_name}",
                text="\n\n".join(overview_parts),
                content_type="prose",
            )
        )
    if schema_parts:
        consolidated.append(
            StructureBlock(
                hierarchy_path=f"table_{table_name}_schema",
                section_title=f"schema:{table_name}",
                text="\n\n".join(schema_parts),
                content_type="table",
            )
        )
    if insight_parts:
        consolidated.append(
            StructureBlock(
                hierarchy_path=f"table_{table_name}_insights",
                section_title=f"insights:{table_name}",
                text="\n\n".join(insight_parts),
                content_type="prose",
            )
        )
    return consolidated


def _slices_from_blocks(blocks: list[StructureBlock], profile: ChunkingProfile) -> list[TextSlice]:
    from ml.rag.text_processors.preprocess.split_strategy import (
        _bq_structured_split,
        _split_bq_schema_block,
    )

    out: list[TextSlice] = []
    for block in blocks:
        tok = count_tokens(block.text, model_id=profile.embedding_model)
        if tok <= profile.target_tokens:
            pieces = [block.text]
        elif block.content_type == "table":
            pieces = _split_bq_schema_block(block.text, profile)
        else:
            pieces = _bq_structured_split(block.text, profile)
        for piece in pieces:
            if _is_heading_only(piece):
                continue
            out.append(
                TextSlice(
                    text=piece,
                    section_title=block.section_title,
                    hierarchy_path=block.hierarchy_path,
                    content_type=block.content_type,
                )
            )
    return out


def _cap_slices(slices: list[TextSlice], profile: ChunkingProfile) -> list[TextSlice]:
    cap = profile.max_chunks_per_doc
    if cap is not None and len(slices) > cap:
        return slices[:cap]
    return slices


def _chunks_for_logical_table(
    *,
    logical: _LogicalTable,
    source_file: str,
    profile: ChunkingProfile,
) -> list[ChunkOutput]:
    doc_id = document_id_from_path(source_file, dedupe_id=f"{source_file}|{logical.table_name}")
    blocks = _consolidate_blocks(
        _schema_blocks_from_text(logical.text, table_name=logical.table_name),
        table_name=logical.table_name,
    )
    slices = _cap_slices(_merge_small_slices(_slices_from_blocks(blocks, profile), profile), profile)

    chunks: list[ChunkOutput] = []
    total = len(slices)
    for i, sl in enumerate(slices):
        lane = _lane_from_slice(sl)
        body = _prepend_table_context(
            sl.text,
            table_name=logical.table_name,
            bq_table_id=logical.bq_table_id,
        )
        meta = enrich_metadata(
            {
                "doc_kind": "bq_table_description",
                "type": f"BQ {logical.table_name} description",
                "table_name": logical.table_name,
                "bq_table_id": logical.bq_table_id or logical.table_name,
                "source_kind": "bq_table_description_docx",
                "source_file": source_file,
                "label": logical.table_name,
            },
            corpus="data_description",
            document_id=doc_id,
            chunk_index=i,
            total_chunks=total,
            text=body,
            section_path=sl.hierarchy_path,
            section_title=sl.section_title,
            hierarchy_path=sl.hierarchy_path,
            semantic_lane=lane,
            content_type=sl.content_type,
        )
        chunks.append(
            ChunkOutput(id=str(meta["id"]), text=body, metadata={k: v for k, v in meta.items() if k != "id"})
        )
    return chunks


def preprocess_docx(docx_path: Path) -> list[ChunkOutput]:
    profile = profile_for_corpus("data_description")
    fallback_table = _table_name_from_path(docx_path)
    source_file = str(docx_path)

    elements = partition_docx(docx_path)
    raw = _normalize_bq_text("\n".join(e.text for e in elements if e.text.strip()))
    logical_tables = _extract_logical_tables(raw, fallback_table=fallback_table)

    out: list[ChunkOutput] = []
    for logical in logical_tables:
        out.extend(
            _chunks_for_logical_table(
                logical=logical,
                source_file=source_file,
                profile=profile,
            )
        )
    return out


def preprocess_folder(input_dir: Path) -> list[ChunkOutput]:
    out: list[ChunkOutput] = []
    for docx in sorted(input_dir.rglob("*.docx"), key=lambda p: str(p).lower()):
        if docx.is_file():
            out.extend(preprocess_docx(docx))
    return out
