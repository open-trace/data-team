from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_HEADING_RE = re.compile(r"^\s*(\d+(\.\d+)*\s+)?[A-Z][A-Z0-9 ,:;()/-]{8,}\s*$")


@dataclass
class ParsedElement:
    element_type: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)


def _use_unstructured() -> bool:
    return os.environ.get("RAG_USE_UNSTRUCTURED", "").strip().lower() in ("1", "true", "yes")


def _normalize_text(raw: str) -> str:
    text = raw.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _normalize_table_text(raw: str) -> str:
    text = raw.replace("\r\n", "\n").replace("\r", "\n")
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.split("\n")]
    lines = [ln for ln in lines if ln]
    return "\n".join(lines)


def _html_table_to_text(html: str) -> str:
    text = re.sub(r"</tr>", "\n", html, flags=re.I)
    text = re.sub(r"</t[dh]>", " | ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return _normalize_table_text(text)


def _partition_strategy() -> str:
    raw = os.environ.get("RAG_UNSTRUCTURED_STRATEGY", "fast").strip().lower()
    return raw or "fast"


def _partition_languages() -> list[str] | None:
    raw = os.environ.get("RAG_UNSTRUCTURED_LANGUAGES", "eng,fra").strip()
    if not raw:
        return None
    langs = [part.strip() for part in raw.split(",") if part.strip()]
    return langs or None


def _element_category(el: Any) -> str:
    cat = getattr(el, "category", None)
    if isinstance(cat, str):
        return cat
    return str(getattr(cat, "name", "NarrativeText") or "NarrativeText")


def _element_text(el: Any, *, is_table: bool) -> str:
    if is_table and hasattr(el, "metadata") and el.metadata:
        html = getattr(el.metadata, "text_as_html", None)
        if isinstance(html, str) and html.strip():
            converted = _html_table_to_text(html)
            if converted:
                return converted
    raw = str(el) or ""
    return _normalize_table_text(raw) if is_table else _normalize_text(raw)


def _pypdf_text(pdf_path: Path) -> str:
    from pypdf import PdfReader  # type: ignore[import-not-found]

    reader = PdfReader(str(pdf_path))
    pages = [(p.extract_text() or "") for p in reader.pages]
    return _normalize_text("\n".join(pages))


def _docx_zip_text(docx_path: Path) -> str:
    import zipfile
    from xml.etree import ElementTree as ET

    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paragraphs: list[str] = []
    with zipfile.ZipFile(docx_path) as zf:
        root = ET.fromstring(zf.read("word/document.xml"))
    for para in root.findall(".//w:p", ns):
        runs = para.findall(".//w:t", ns)
        text = "".join((r.text or "") for r in runs).strip()
        if text:
            paragraphs.append(text)
    return _normalize_text("\n\n".join(paragraphs))


def _is_false_heading(line: str) -> bool:
    s = line.strip()
    if len(s) < 10:
        return True
    if re.match(r"^(ISBN|ISSN|DOI)\b", s, re.IGNORECASE):
        return True
    if re.match(r"^https?://", s, re.IGNORECASE):
        return True
    return False


def _text_to_elements_with_headings(text: str) -> list[ParsedElement]:
    """Heading-aware element stream when Unstructured is unavailable."""
    out: list[ParsedElement] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if _HEADING_RE.match(stripped) and not _is_false_heading(stripped):
            out.append(ParsedElement(element_type="Title", text=stripped))
        else:
            out.append(ParsedElement(element_type="NarrativeText", text=stripped))
    if not out and text.strip():
        return _fallback_paragraph_elements(text)
    return out


def _unstructured_partition(path: Path) -> list[ParsedElement]:
    from unstructured.partition.auto import partition  # type: ignore[import-not-found]

    kwargs: dict[str, Any] = {"filename": str(path), "strategy": _partition_strategy()}
    langs = _partition_languages()
    if langs:
        kwargs["languages"] = langs
    elements = partition(**kwargs)
    out: list[ParsedElement] = []
    for el in elements:
        name = _element_category(el)
        is_table = name.lower() == "table"
        text = _element_text(el, is_table=is_table)
        if not text:
            continue
        meta: dict[str, Any] = {}
        if hasattr(el, "metadata") and el.metadata:
            md = el.metadata.to_dict() if hasattr(el.metadata, "to_dict") else {}
            if isinstance(md, dict):
                meta = md
        out.append(ParsedElement(element_type=str(name), text=text, metadata=meta))
    return out


def _fallback_paragraph_elements(text: str) -> list[ParsedElement]:
    return [
        ParsedElement(element_type="NarrativeText", text=p)
        for p in re.split(r"\n\s*\n", text)
        if p.strip()
    ]


def _partition(path: Path, *, raw_text_fn) -> list[ParsedElement]:
    if _use_unstructured():
        try:
            return _unstructured_partition(path)
        except Exception:
            pass
    raw = raw_text_fn(path)
    return _text_to_elements_with_headings(raw)


def partition_pdf(pdf_path: Path) -> list[ParsedElement]:
    return _partition(pdf_path, raw_text_fn=_pypdf_text)


def partition_docx(docx_path: Path) -> list[ParsedElement]:
    return _partition(docx_path, raw_text_fn=_docx_zip_text)
