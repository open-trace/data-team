"""
Preprocess PDFs into chunk records for vector embedding.

Default input folder:
    ml/rag/Text Documents

Outputs JSONL rows with:
    - id
    - source_file
    - chunk_type: sequential | contextual
    - text
    - metadata

Usage:
    PYTHONPATH=. python -m ml.rag.pdf_preprocessor
    PYTHONPATH=. python -m ml.rag.pdf_preprocessor --input-dir "ml/rag/Text Documents" --output "data/local/pdf_chunks.jsonl"
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable


@dataclass
class ChunkRecord:
    id: str
    source_file: str
    chunk_type: str
    text: str
    metadata: dict[str, str | int | float | bool]


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_DIR = REPO_ROOT / "ml" / "rag" / "Text Documents"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "data" / "local" / "pdf_chunks.jsonl"

# Domains are agrifood / agricultural development context (OpenTrace).
DOMAIN_KEYWORDS: dict[str, tuple[str, ...]] = {
    "agriculture": (
        "agriculture",
        "agricultural",
        "farming",
        "farm",
        "crop",
        "livestock",
        "smallholder",
        "agronomy",
    ),
    "economics": (
        "farm income",
        "agricultural income",
        "food price",
        "commodity price",
        "farm gate",
        "subsidy",
        "agricultural subsidy",
        "gdp",
        "inflation",
        "rural economy",
    ),
    "International Trade (Exports & Imports)": (
        "export",
        "exports",
        "import",
        "imports",
        "re-export",
        "cross-border",
        "customs",
        "tariff",
        "trade barrier",
        "phytosanitary",
        "trade agreement",
        "agricultural export",
        "agricultural import",
        "food export",
        "food import",
        "cash crop export",
        "commodity export",
    ),
    "Environmental & Climate": (
        "climate",
        "rainfall",
        "drought",
        "temperature",
        "extreme weather",
        "emissions",
        "agricultural climate",
    ),
    "Land Use & Soil Health": (
        "land use",
        "soil",
        "erosion",
        "fertility",
        "degradation",
    ),
    "Investment Readiness & Enterprise": (
        "investment",
        "enterprise",
        "entrepreneur",
        "finance",
        "credit",
        "bank",
    ),
    "Technology & Innovation": (
        "technology",
        "innovation",
        "digital",
        "ai",
        "satellite",
        "remote sensing",
    ),
    "Market Access & Infrastructure": (
        "market access",
        "rural market",
        "road",
        "logistics",
        "infrastructure",
        "transport",
        "storage",
        "post-harvest",
    ),
    "Production & Yield": (
        "yield",
        "production",
        "productivity",
        "harvest",
    ),
    "Policy & Institutional": (
        "policy",
        "institution",
        "governance",
        "regulation",
        "ministry",
    ),
    "Gender, Youth & Inclusion": (
        "gender",
        "women",
        "youth",
        "inclusion",
        "smallholder",
    ),
    "Nutrition & Food Security": (
        "nutrition",
        "food security",
        "malnutrition",
        "stunting",
        "hunger",
    ),
    "Food Systems & Value Chain": (
        "value chain",
        "food system",
        "processing",
        "distribution",
        "retail",
    ),
    "Humanitarian & Agricultural Emergency": (
        "humanitarian",
        "emergency",
        "crisis",
        "famine",
        "displacement",
        "conflict",
    ),
}

COUNTRIES = (
    "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi", "Cameroon",
    "Cape Verde", "Central African Republic", "Chad", "Comoros", "Congo", "Djibouti",
    "Egypt", "Equatorial Guinea", "Eritrea", "Eswatini", "Ethiopia", "Gabon", "Gambia",
    "Ghana", "Guinea", "Guinea-Bissau", "Ivory Coast", "Cote d'Ivoire", "Kenya", "Lesotho",
    "Liberia", "Libya", "Madagascar", "Malawi", "Mali", "Mauritania", "Mauritius",
    "Morocco", "Mozambique", "Namibia", "Niger", "Nigeria", "Rwanda", "Sao Tome and Principe",
    "Senegal", "Seychelles", "Sierra Leone", "Somalia", "South Africa", "South Sudan",
    "Sudan", "Tanzania", "Togo", "Tunisia", "Uganda", "Zambia", "Zimbabwe", "DRC",
    "Democratic Republic of the Congo", "Congo, Democratic Republic of the",
)

# Longer names first so "South Sudan" wins over "Sudan", "Nigeria" over "Niger", etc.
COUNTRIES_BY_LENGTH: tuple[str, ...] = tuple(sorted(COUNTRIES, key=len, reverse=True))

# Max domains to keep (keyword scoring avoids tagging every domain on generic ag text).
MAX_DOMAINS = 6
# Max countries to output; require min mentions in body text to reduce bibliography noise.
MAX_PLACES = 12
MIN_COUNTRY_MENTIONS = 2


def _main_text_for_inference(full_text: str) -> str:
    """Use body only: bibliography/reference sections list many countries spuriously."""
    for pat in (
        r"\n\s*References\s*\n",
        r"\n\s*REFERENCES\s*\n",
        r"\n\s*Bibliography\s*\n",
        r"\n\s*BIBLIOGRAPHY\s*\n",
        r"\n\s*Works Cited\s*\n",
    ):
        m = re.search(pat, full_text, flags=re.IGNORECASE)
        if m:
            return full_text[: m.start()]
    return full_text


def _keyword_hits(text_lower: str, keyword: str) -> int:
    """Count non-overlapping whole-word-ish matches for a keyword phrase."""
    if " " in keyword:
        return len(re.findall(re.escape(keyword), text_lower))
    return len(re.findall(rf"\b{re.escape(keyword)}\b", text_lower))


def infer_domains(full_text: str) -> list[str]:
    """
    Score domains by keyword frequency; return top domains (not all weak matches).
    Avoids tagging every domain when words like 'production' or 'crop' appear everywhere.
    """
    lowered = full_text.lower()
    scores: list[tuple[str, int]] = []
    for domain, keywords in DOMAIN_KEYWORDS.items():
        total = sum(_keyword_hits(lowered, k) for k in keywords)
        scores.append((domain, total))
    scores.sort(key=lambda x: x[1], reverse=True)
    # Keep domains with positive score, cap at MAX_DOMAINS; drop zeros.
    picked = [d for d, s in scores if s > 0][:MAX_DOMAINS]
    if not picked:
        return ["agriculture"]
    return picked


def infer_info_type(full_text: str, source_file: str) -> str:
    """Infer high-level document type from full text and filename."""
    body = _main_text_for_inference(full_text)
    lowered = body.lower()
    file_lower = source_file.lower()

    # Strong academic signals (prefer over weak 'republic of' in citations).
    if (
        re.search(r"\bdoi:\s*10\.", body, re.IGNORECASE)
        or re.search(r"\bjournal of\b", lowered)
        or re.search(r"\bpeer[- ]reviewed\b", lowered)
        or "1-s2.0" in file_lower
        or re.search(r"\babstract\b", lowered[:8000])
    ):
        return "academic_article"

    gov_markers = (
        "national bureau of statistics",
        "official gazette",
        "government of",
        "ministry of",
        "department of agriculture",
        "parliament",
        "policy brief",
    )
    gov_hits = sum(1 for m in gov_markers if m in lowered)

    academic_markers = ("methodology", "literature review", "vol.", "issue no.")
    if any(m in lowered for m in academic_markers):
        return "academic_article"

    news_markers = (
        "breaking", "reported today", "news", "editorial", "press release",
    )
    if any(m in lowered for m in news_markers):
        return "news_article"

    if gov_hits >= 2:
        return "government_report"

    return "academic_article"


def infer_places_of_focus(full_text: str) -> list[str]:
    """
    Countries emphasized in the document body (not reference lists).
    Counts mentions; keeps top countries by count with a minimum threshold.
    """
    body = _main_text_for_inference(full_text)
    counts: dict[str, int] = {}

    for country in COUNTRIES_BY_LENGTH:
        n = len(
            re.findall(
                rf"\b{re.escape(country)}\b",
                body,
                flags=re.IGNORECASE,
            )
        )
        if n <= 0:
            continue
        canon = country
        if country in {"DRC", "Congo, Democratic Republic of the"}:
            canon = "Democratic Republic of the Congo"
        counts[canon] = counts.get(canon, 0) + n

    # Sort by count; require min mentions unless we have very few candidates.
    ranked = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    min_m = MIN_COUNTRY_MENTIONS
    if len(ranked) <= 3:
        min_m = 1

    out: list[str] = []
    for name, c in ranked:
        if c >= min_m:
            out.append(name)
        if len(out) >= MAX_PLACES:
            break

    # If nothing passed threshold, take top few by raw count (still from body only).
    if not out and ranked:
        out = [name for name, _ in ranked[: min(5, len(ranked))]]

    return out


def list_pdf_files(input_dir: Path) -> list[Path]:
    """Return all PDF files under input_dir (recursive), sorted by path."""
    return sorted([p for p in input_dir.rglob("*.pdf") if p.is_file()], key=lambda p: str(p).lower())


def read_pdf_text(pdf_path: Path) -> str:
    """
    Extract text from a PDF using pypdf.
    Raises ImportError with a clear message if pypdf is missing.
    """
    try:
        from pypdf import PdfReader  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ImportError(
            "Missing dependency 'pypdf'. Install it with: pip install pypdf"
        ) from exc

    reader = PdfReader(str(pdf_path))
    pages: list[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        pages.append(text)
    return "\n".join(pages)


def normalize_text(raw_text: str) -> str:
    """Basic whitespace cleanup for chunking stability."""
    text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _chunk_id(source_file: str, chunk_type: str, index: int, text: str) -> str:
    digest = hashlib.sha1(f"{source_file}|{chunk_type}|{index}|{text[:160]}".encode("utf-8")).hexdigest()
    return f"{chunk_type}_{digest}"


def build_sequential_chunks(text: str, chunk_chars: int = 900, overlap_chars: int = 150) -> list[str]:
    """
    Build fixed-size sequential chunks with overlap.
    Attempts to end on sentence/pargraph boundaries when possible.
    """
    chunks: list[str] = []
    n = len(text)
    start = 0
    while start < n:
        end = min(start + chunk_chars, n)
        window = text[start:end]

        # Try to snap to a natural boundary near the end.
        boundary = max(window.rfind("\n\n"), window.rfind(". "), window.rfind("! "), window.rfind("? "))
        if boundary > int(chunk_chars * 0.6):
            end = start + boundary + 1

        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)

        if end >= n:
            break
        start = max(0, end - overlap_chars)

    return chunks


def _is_false_section_heading(line: str) -> bool:
    """Lines that look like ALL-CAPS titles but are metadata (ISBN, DOI, etc.)."""
    s = line.strip()
    if len(s) < 10:
        return True
    if re.match(r"^ISBN\b", s, re.IGNORECASE):
        return True
    if re.match(r"^ISSN\b", s, re.IGNORECASE):
        return True
    if re.match(r"^DOI\b", s, re.IGNORECASE):
        return True
    if re.match(r"^https?://", s, re.IGNORECASE):
        return True
    if "@" in s and ".com" in s.lower():
        return True
    return False


def _split_sections(text: str) -> list[tuple[str, str]]:
    """
    Split text into coarse sections using heading-like lines.
    If no heading-like lines are found, returns one section.
    """
    lines = text.splitlines()
    heading_re = re.compile(r"^\s*(\d+(\.\d+)*\s+)?[A-Z][A-Z0-9 ,:;()/-]{8,}\s*$")

    sections: list[tuple[str, list[str]]] = []
    current_title = "Document"
    current_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        is_heading = bool(heading_re.match(stripped)) and not _is_false_section_heading(stripped)
        if is_heading and current_lines:
            sections.append((current_title, current_lines))
            current_title = line.strip()[:160]
            current_lines = []
        else:
            current_lines.append(line)

    if current_lines:
        sections.append((current_title, current_lines))

    if not sections:
        return [("Document", text)]

    out: list[tuple[str, str]] = []
    for title, body_lines in sections:
        body = "\n".join(body_lines).strip()
        if body:
            out.append((title, body))
    return out if out else [("Document", text)]


def _split_sentences(text: str) -> list[str]:
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]


def build_contextual_chunks(
    text: str,
    section_max_chars: int = 1500,
    neighbor_window: int = 1,
) -> list[tuple[str, str]]:
    """
    Build contextual chunks that preserve section semantics.
    Each chunk includes a compact context header:
        Section: <title>
        Nearby sections: <neighbor titles>
    """
    sections = _split_sections(text)
    contextual_chunks: list[tuple[str, str]] = []

    for idx, (title, body) in enumerate(sections):
        lo = max(0, idx - neighbor_window)
        hi = min(len(sections), idx + neighbor_window + 1)
        neighbors = [
            sections[i][0]
            for i in range(lo, hi)
            if i != idx and not _is_false_section_heading(sections[i][0])
        ]
        header = f"Section: {title}"
        if neighbors:
            header += f"\nNearby sections: {', '.join(neighbors[:4])}"

        if len(body) <= section_max_chars:
            contextual_chunks.append((title, f"{header}\n\n{body}"))
            continue

        # Large section: split by sentence groups.
        sentences = _split_sentences(body)
        if not sentences:
            contextual_chunks.append((title, f"{header}\n\n{body[:section_max_chars]}"))
            continue

        bucket: list[str] = []
        size = 0
        for sentence in sentences:
            if bucket and size + len(sentence) + 1 > section_max_chars:
                contextual_chunks.append((title, f"{header}\n\n{' '.join(bucket)}"))
                bucket = [sentence]
                size = len(sentence)
            else:
                bucket.append(sentence)
                size += len(sentence) + 1
        if bucket:
            contextual_chunks.append((title, f"{header}\n\n{' '.join(bucket)}"))

    return contextual_chunks


def preprocess_pdf(
    pdf_path: Path,
    sequential_chunk_chars: int = 900,
    sequential_overlap_chars: int = 150,
    contextual_section_max_chars: int = 1500,
) -> list[ChunkRecord]:
    """Read one PDF and return chunk records for both sequential and contextual strategies."""
    source_file = str(pdf_path)
    raw_text = read_pdf_text(pdf_path)
    text = normalize_text(raw_text)
    if not text:
        return []

    records: list[ChunkRecord] = []
    inferred_domains = infer_domains(text)
    inferred_info_type = infer_info_type(text, source_file)
    inferred_places = infer_places_of_focus(text)
    common_metadata: dict[str, str | int | float | bool] = {
        "domain": "; ".join(inferred_domains),
        "info_type": inferred_info_type,
        "place_of_focus": "; ".join(inferred_places),
    }

    seq_chunks = build_sequential_chunks(
        text=text,
        chunk_chars=sequential_chunk_chars,
        overlap_chars=sequential_overlap_chars,
    )
    for i, chunk in enumerate(seq_chunks):
        records.append(
            ChunkRecord(
                id=_chunk_id(source_file, "sequential", i, chunk),
                source_file=source_file,
                chunk_type="sequential",
                text=chunk,
                metadata={
                    "strategy": "sequential",
                    "chunk_index": i,
                    "total_chunks": len(seq_chunks),
                    **common_metadata,
                },
            )
        )

    ctx_chunks = build_contextual_chunks(
        text=text,
        section_max_chars=contextual_section_max_chars,
        neighbor_window=1,
    )
    for i, (section_title, chunk) in enumerate(ctx_chunks):
        records.append(
            ChunkRecord(
                id=_chunk_id(source_file, "contextual", i, chunk),
                source_file=source_file,
                chunk_type="contextual",
                text=chunk,
                metadata={
                    "strategy": "contextual",
                    "section_title": section_title[:200],
                    "chunk_index": i,
                    "total_chunks": len(ctx_chunks),
                    **common_metadata,
                },
            )
        )

    return records


def preprocess_pdf_folder(
    input_dir: Path,
    sequential_chunk_chars: int = 900,
    sequential_overlap_chars: int = 150,
    contextual_section_max_chars: int = 1500,
) -> list[ChunkRecord]:
    """Process all PDFs in a folder recursively."""
    records: list[ChunkRecord] = []
    pdf_files = list_pdf_files(input_dir)
    for pdf_file in pdf_files:
        records.extend(
            preprocess_pdf(
                pdf_path=pdf_file,
                sequential_chunk_chars=sequential_chunk_chars,
                sequential_overlap_chars=sequential_overlap_chars,
                contextual_section_max_chars=contextual_section_max_chars,
            )
        )
    return records


def write_chunks_jsonl(records: Iterable[ChunkRecord], output_path: Path) -> int:
    """Write chunk records to JSONL and return count."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(asdict(rec), ensure_ascii=False) + "\n")
            count += 1
    return count


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Preprocess PDFs into sequential/contextual chunks for vector embedding."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Folder containing PDFs (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output JSONL path (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument("--seq-chars", type=int, default=900, help="Sequential chunk size in chars.")
    parser.add_argument("--seq-overlap", type=int, default=150, help="Sequential chunk overlap in chars.")
    parser.add_argument(
        "--ctx-max-chars",
        type=int,
        default=1500,
        help="Maximum chars per contextual chunk.",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    input_dir: Path = args.input_dir
    output_path: Path = args.output

    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist or is not a directory: {input_dir}")

    records = preprocess_pdf_folder(
        input_dir=input_dir,
        sequential_chunk_chars=args.seq_chars,
        sequential_overlap_chars=args.seq_overlap,
        contextual_section_max_chars=args.ctx_max_chars,
    )
    count = write_chunks_jsonl(records, output_path)
    print(f"Wrote {count} chunks to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
