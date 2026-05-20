from __future__ import annotations

from pathlib import Path

from ml.rag.text_processors.chunk_contract import document_id_from_path, enrich_metadata
from ml.rag.text_processors.chunking_config import profile_for_corpus
from ml.rag.text_processors.domain_taxonomy import (
    DEFAULT_MAX_DOMAINS_LONG_DOC,
    infer_domains,
    infer_info_type,
    infer_places_of_focus,
)
from ml.rag.text_processors.preprocess.lineage import section_parent_id, semantic_lane_for_path
from ml.rag.text_processors.preprocess.llama_split import TextSlice, cap_slices, split_blocks
from ml.rag.text_processors.preprocess.models import ChunkOutput
from ml.rag.text_processors.preprocess.structure_blocks import elements_to_blocks
from ml.rag.text_processors.preprocess.unstructured_fast import partition_pdf


def list_pdf_files(input_dir: Path) -> list[Path]:
    return sorted([p for p in input_dir.rglob("*.pdf") if p.is_file()], key=lambda p: str(p).lower())


def preprocess_pdf(pdf_path: Path, *, doc_kind: str = "academic_article") -> list[ChunkOutput]:
    profile = profile_for_corpus("research")
    source_file = str(pdf_path)
    doc_id = document_id_from_path(source_file)

    elements = partition_pdf(pdf_path)
    blocks = elements_to_blocks(elements)
    if not blocks:
        return []

    full_text = "\n\n".join(b.text for b in blocks)
    inferred_domains = infer_domains(full_text, max_domains=DEFAULT_MAX_DOMAINS_LONG_DOC)
    info_type = infer_info_type(full_text, source_file)
    places = infer_places_of_focus(full_text)

    tuples = [(b.hierarchy_path, b.section_title, b.text) for b in blocks]
    slices: list[TextSlice] = cap_slices(split_blocks(tuples, profile), profile)

    section_parents: dict[str, str] = {}
    for sl in slices:
        if sl.hierarchy_path not in section_parents:
            section_parents[sl.hierarchy_path] = section_parent_id(
                corpus="research",
                document_id=doc_id,
                hierarchy_path=sl.hierarchy_path,
            )

    chunks: list[ChunkOutput] = []
    total = len(slices)
    for i, sl in enumerate(slices):
        lane = semantic_lane_for_path(sl.hierarchy_path)
        parent_id = section_parents.get(sl.hierarchy_path, "")
        meta = enrich_metadata(
            {
                "doc_kind": doc_kind,
                "info_type": info_type,
                "domains": "; ".join(inferred_domains),
                "place_of_focus": "; ".join(places),
                "source_file": source_file,
                "strategy": "structure_token",
            },
            corpus="research",
            document_id=doc_id,
            chunk_index=i,
            total_chunks=total,
            text=sl.text,
            section_path=sl.hierarchy_path,
            section_title=sl.section_title,
            hierarchy_path=sl.hierarchy_path,
            parent_chunk_id=parent_id,
            semantic_lane=lane,
        )
        chunks.append(ChunkOutput(id=str(meta["id"]), text=sl.text, metadata={k: v for k, v in meta.items() if k != "id"}))
    return chunks


def preprocess_folder(
    input_dir: Path,
    *,
    doc_kind: str = "academic_article",
) -> list[ChunkOutput]:
    out: list[ChunkOutput] = []
    for pdf in list_pdf_files(input_dir):
        out.extend(preprocess_pdf(pdf, doc_kind=doc_kind))
    return out
