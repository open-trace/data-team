from __future__ import annotations

import re
from pathlib import Path

from ml.rag.text_processors.chunk_contract import document_id_from_path, enrich_metadata
from ml.rag.text_processors.chunking_config import profile_for_corpus
from ml.rag.text_processors.preprocess.llama_split import TextSlice, cap_slices, split_blocks
from ml.rag.text_processors.preprocess.models import ChunkOutput
from ml.rag.text_processors.preprocess.structure_blocks import StructureBlock
from ml.rag.text_processors.preprocess.unstructured_fast import partition_docx


def _table_name_from_path(docx_path: Path) -> str:
    stem = re.sub(r"\s+", " ", docx_path.stem.strip())
    stem = re.sub(r"[^A-Za-z0-9_ ]+", "", stem).strip()
    return stem.replace(" ", "_") or "unknown_table"


def _schema_blocks_from_text(text: str, table_name: str) -> list[StructureBlock]:
    lines = text.splitlines()
    blocks: list[StructureBlock] = []
    cur_title = f"table:{table_name}" if table_name else "description"
    cur_lines: list[str] = []
    in_table = False

    def flush() -> None:
        nonlocal cur_lines
        body = "\n".join(cur_lines).strip()
        cur_lines = []
        if body:
            slug = re.sub(r"[^a-z0-9]+", "_", cur_title.lower())[:80] or "block"
            blocks.append(
                StructureBlock(hierarchy_path=slug, section_title=cur_title[:200], text=body)
            )

    for line in lines:
        stripped = line.strip()
        is_table_row = stripped.startswith("|") and "|" in stripped[1:]
        is_heading = bool(
            re.match(r"^(Table\s+Name:|#+\s|\d+(\.\d+)*\s+[A-Z])", stripped, re.IGNORECASE)
        )
        if is_heading and cur_lines:
            flush()
            cur_title = stripped[:120]
            continue
        if is_table_row and not in_table and cur_lines:
            flush()
        in_table = is_table_row
        cur_lines.append(line)
    flush()

    if not blocks and text.strip():
        blocks.append(StructureBlock(hierarchy_path="description", section_title="description", text=text.strip()))
    return blocks


def preprocess_docx(docx_path: Path) -> list[ChunkOutput]:
    profile = profile_for_corpus("data_description")
    table_name = _table_name_from_path(docx_path)
    source_file = str(docx_path)
    doc_id = document_id_from_path(source_file)

    elements = partition_docx(docx_path)
    raw = "\n\n".join(e.text for e in elements if e.text.strip())
    blocks = _schema_blocks_from_text(raw, table_name)

    tuples = [(b.hierarchy_path, b.section_title, b.text) for b in blocks]
    slices: list[TextSlice] = cap_slices(split_blocks(tuples, profile), profile)

    chunks: list[ChunkOutput] = []
    total = len(slices)
    for i, sl in enumerate(slices):
        meta = enrich_metadata(
            {
                "doc_kind": "bq_table_description",
                "type": f"BQ {table_name} description",
                "table_name": table_name,
                "source_kind": "bq_table_description_docx",
                "source_file": source_file,
            },
            corpus="data_description",
            document_id=doc_id,
            chunk_index=i,
            total_chunks=total,
            text=sl.text,
            section_path=sl.hierarchy_path,
            section_title=sl.section_title,
            hierarchy_path=sl.hierarchy_path,
            semantic_lane="schema",
        )
        chunks.append(ChunkOutput(id=str(meta["id"]), text=sl.text, metadata={k: v for k, v in meta.items() if k != "id"}))
    return chunks


def preprocess_folder(input_dir: Path) -> list[ChunkOutput]:
    out: list[ChunkOutput] = []
    for docx in sorted(input_dir.rglob("*.docx"), key=lambda p: str(p).lower()):
        if docx.is_file():
            out.extend(preprocess_docx(docx))
    return out
