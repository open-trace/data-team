"""Read DOCX text without extra dependencies."""
from __future__ import annotations

import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET


def read_docx_text(docx_path: Path) -> str:
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paragraphs: list[str] = []
    with zipfile.ZipFile(docx_path) as zf:
        root = ET.fromstring(zf.read("word/document.xml"))
    for para in root.findall(".//w:p", ns):
        runs = para.findall(".//w:t", ns)
        text = "".join((r.text or "") for r in runs).strip()
        if text:
            paragraphs.append(text)
    return "\n\n".join(paragraphs).strip()
