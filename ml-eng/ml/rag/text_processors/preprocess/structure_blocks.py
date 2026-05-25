from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from ml.rag.text_processors.preprocess.unstructured_fast import ParsedElement

ContentType = Literal["prose", "table"]


@dataclass(frozen=True)
class StructureBlock:
    hierarchy_path: str
    section_title: str
    text: str
    content_type: ContentType = "prose"


def _slug_heading(title: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", (title or "").lower()).strip("_")
    return s[:120] or "section"


def _is_table_element(kind: str) -> bool:
    return kind in ("table",)


def elements_to_blocks(elements: list[ParsedElement]) -> list[StructureBlock]:
    """Turn Unstructured elements into section blocks with hierarchy paths."""
    blocks: list[StructureBlock] = []
    path_stack: list[str] = []
    title_stack: list[str] = []
    buf: list[str] = []

    def flush(*, content_type: ContentType = "prose") -> None:
        nonlocal buf
        body = "\n\n".join(buf).strip()
        buf = []
        if not body:
            return
        hp = "/".join(path_stack) if path_stack else "body"
        st = title_stack[-1] if title_stack else "body"
        blocks.append(
            StructureBlock(hierarchy_path=hp, section_title=st, text=body, content_type=content_type)
        )

    def append_table_block(text: str) -> None:
        body = text.strip()
        if not body:
            return
        hp = "/".join(path_stack) if path_stack else "body"
        st = title_stack[-1] if title_stack else "body"
        table_hp = f"{hp}/table" if hp else "table"
        blocks.append(
            StructureBlock(
                hierarchy_path=table_hp,
                section_title=st,
                text=body,
                content_type="table",
            )
        )

    for el in elements:
        text = (el.text or "").strip()
        if not text:
            continue
        kind = (el.element_type or "").lower()
        if kind in ("title", "header"):
            flush()
            slug = _slug_heading(text)
            depth = int(el.metadata.get("category_depth", 0) or 0)
            if depth <= 0:
                depth = min(len(path_stack) + 1, 4)
            while len(path_stack) >= depth:
                path_stack.pop()
                if title_stack:
                    title_stack.pop()
            path_stack.append(slug)
            title_stack.append(text[:200])
            continue
        if _is_table_element(kind):
            flush()
            append_table_block(text)
            continue
        buf.append(text)

    flush()
    if not blocks and elements:
        joined = "\n\n".join(e.text.strip() for e in elements if e.text.strip())
        if joined:
            blocks.append(StructureBlock(hierarchy_path="body", section_title="body", text=joined))
    return blocks


def paragraphs_to_blocks(text: str, *, default_section: str = "body") -> list[StructureBlock]:
    parts = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    if not parts:
        return []
    return [
        StructureBlock(
            hierarchy_path=f"{default_section}/p{i+1}" if i else default_section,
            section_title=default_section if i == 0 else f"{default_section} {i+1}",
            text=para,
        )
        for i, para in enumerate(parts)
    ]
