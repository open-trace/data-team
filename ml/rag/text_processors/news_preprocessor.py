"""
Preprocess scraped news article .txt files (YAML front matter + body) into chunk JSONL for RAG.

Input folder (default):
    data/local/web_news_rss

Output JSONL rows:
    - id (str)
    - text (str)
    - metadata (dict)

Metadata includes:
    - info_type: "news_article"
    - published_at: (string, from front matter when available)
    - title: (string)
    - country: (string, from front matter or inferred from folder)
    - domains: (string, inferred per chunk; '; '-separated)
    - url, rss_url, feed_name, source, body_source, etc. when present

Usage:
    PYTHONPATH=. python -m ml.rag.news_preprocessor
    PYTHONPATH=. python -m ml.rag.news_preprocessor --input-dir data/local/web_news_rss --output data/local/news_chunks.jsonl
"""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import yaml


@dataclass
class ChunkRow:
    id: str
    text: str
    metadata: dict[str, str | int | float | bool]


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_DIR = REPO_ROOT / "data" / "local" / "web_news_rss"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "data" / "local" / "news_chunks.jsonl"


# Keep in sync (conceptually) with the web mining domain taxonomy.
DOMAIN_KEYWORDS: dict[str, tuple[str, ...]] = {
    "agriculture": (
        "agriculture",
        "agricultural",
        "agribusiness",
        "farming",
        "farm",
        "farmer",
        "crop",
        "livestock",
        "smallholder",
        "agronomy",
        "food security",
    ),
    "Agricultural Economics": (
        "farm income",
        "agricultural income",
        "food price",
        "commodity price",
        "farm gate",
        "subsidy",
        "inflation",
        "gdp",
        "rural economy",
    ),
    "Agricultural International Trade (Exports & Imports)": (
        "export",
        "exports",
        "import",
        "imports",
        "cross-border",
        "customs",
        "tariff",
        "trade agreement",
        "phytosanitary",
    ),
    "Agricultural Environmental & Climate": (
        "climate",
        "rainfall",
        "drought",
        "temperature",
        "extreme weather",
        "flood",
        "heatwave",
        "emissions",
    ),
    "Land Use & Soil Health": (
        "land use",
        "soil",
        "erosion",
        "fertility",
        "degradation",
    ),
    "Agricultural Investment Readiness & Enterprise": (
        "investment",
        "enterprise",
        "entrepreneur",
        "finance",
        "credit",
        "loan",
        "bank",
        "funding",
    ),
    "Agricultural Technology & Innovation": (
        "technology",
        "innovation",
        "digital",
        "ai",
        "satellite",
        "remote sensing",
        "irrigation technology",
    ),
    "Agricultural Market Access & Infrastructure": (
        "market access",
        "logistics",
        "infrastructure",
        "transport",
        "storage",
        "post-harvest",
        "warehouse",
        "cold chain",
    ),
    "Agricultural Production & Yield": (
        "yield",
        "production",
        "productivity",
        "harvest",
        "acreage",
    ),
    "Agricultural Policy & Institutional": (
        "policy",
        "regulation",
        "ministry",
        "institution",
        "governance",
        "subsidy",
    ),
    "Agricultural Gender, Youth & Inclusion": (
        "gender",
        "women",
        "youth",
        "inclusion",
        "smallholder",
    ),
    "Agricultural Nutrition & Food Security": (
        "nutrition",
        "food security",
        "malnutrition",
        "hunger",
        "stunting",
        "food insecure",
    ),
    "Agricultural Food Systems & Value Chain": (
        "value chain",
        "food system",
        "processing",
        "distribution",
        "retail",
        "supply chain",
    ),
    "Agricultural Humanitarian & Agricultural Emergency": (
        "humanitarian",
        "emergency",
        "crisis",
        "famine",
        "displacement",
        "conflict",
    ),
}


MAX_DOMAINS_PER_CHUNK = 4


def _keyword_hits(text_lower: str, keyword: str) -> int:
    if " " in keyword:
        return len(re.findall(re.escape(keyword), text_lower))
    return len(re.findall(rf"\b{re.escape(keyword)}\b", text_lower))


def infer_domains(text: str) -> list[str]:
    lowered = text.lower()
    scores: list[tuple[str, int]] = []
    for domain, keywords in DOMAIN_KEYWORDS.items():
        total = sum(_keyword_hits(lowered, k) for k in keywords)
        scores.append((domain, total))
    scores.sort(key=lambda x: x[1], reverse=True)
    picked = [d for d, s in scores if s > 0][:MAX_DOMAINS_PER_CHUNK]
    return picked or ["agriculture"]


def list_news_txt_files(input_dir: Path) -> list[Path]:
    return sorted([p for p in input_dir.rglob("*.txt") if p.is_file()], key=lambda p: str(p).lower())


def _parse_front_matter_and_body(raw: str) -> tuple[dict[str, Any], str]:
    """
    Expect:
      ---
      <yaml>
      ---

      <body>
    """
    s = raw.lstrip("\ufeff")  # strip UTF-8 BOM if present

    # Some historical files may start with "--" instead of "---" due to earlier writer bugs.
    if s.startswith("--\n") and not s.startswith("---\n"):
        s = "---\n" + s[3:]

    if not s.startswith("---\n"):
        return {}, s.strip()

    # Find the closing delimiter on its own line.
    end_marker = "\n---\n"
    end_idx = s.find(end_marker, len("---\n"))
    if end_idx == -1:
        return {}, s.strip()

    yaml_block = s[len("---\n") : end_idx]
    body = s[end_idx + len(end_marker) :]
    try:
        meta = yaml.safe_load(yaml_block) or {}
        if not isinstance(meta, dict):
            meta = {}
    except Exception:
        meta = {}
    return meta, body.strip()


def _infer_country_from_path(path: Path, input_dir: Path) -> str:
    """
    For default layout: <root>/<country_slug>/<year>/<id>.txt
    """
    try:
        rel = path.relative_to(input_dir)
        if rel.parts:
            return rel.parts[0].replace("_", " ").title()
    except Exception:
        pass
    return ""


def normalize_text(raw_text: str) -> str:
    text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _looks_like_binary_text(text: str, scan_chars: int = 800) -> bool:
    """
    Detect cases where the stored "text" is actually binary (images/archives/etc.) decoded with replacement.
    Examples we have seen start with "\ufffdPNG" and contain many control characters.
    """
    if not text:
        return False
    head = text.lstrip()[:16]
    if head.startswith("\ufffdPNG") or head.startswith("�PNG"):
        return True
    scan = text[:scan_chars]
    ctrl = sum(1 for ch in scan if (ord(ch) < 32 and ch not in "\n\r\t"))
    rep = scan.count("\ufffd")
    # Very conservative thresholds: normal articles rarely contain control chars or many replacements.
    return ctrl >= 10 or rep >= 40


def build_chunks(text: str, chunk_chars: int = 1200, overlap_chars: int = 200) -> list[str]:
    chunks: list[str] = []
    n = len(text)
    start = 0
    while start < n:
        end = min(start + chunk_chars, n)
        window = text[start:end]
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


def _chunk_id(source_path: str, index: int, chunk_text: str) -> str:
    digest = hashlib.sha1(f"{source_path}|{index}|{chunk_text[:180]}".encode("utf-8")).hexdigest()
    return f"news_{digest}"


def _safe_meta(meta: dict[str, Any]) -> dict[str, str | int | float | bool]:
    out: dict[str, str | int | float | bool] = {}
    for k, v in meta.items():
        if v is None:
            continue
        if isinstance(v, (str, int, float, bool)):
            out[str(k)] = v
        else:
            out[str(k)] = str(v)[:1000]
    return out


def preprocess_news_file(
    path: Path,
    input_dir: Path,
    chunk_chars: int,
    overlap_chars: int,
) -> list[ChunkRow]:
    raw = path.read_text(encoding="utf-8", errors="replace")
    fm, body = _parse_front_matter_and_body(raw)
    body = normalize_text(body)
    if not body:
        return []

    # Guardrail: some historical saves accidentally wrote raw PDF bytes decoded as text.
    # Never chunk that into the vector DB.
    if body.lstrip().startswith("%PDF-"):
        return []
    if _looks_like_binary_text(body):
        return []

    title = str(fm.get("title", "")).strip()
    url = str(fm.get("url", "")).strip()
    rss_url = str(fm.get("rss_url", "")).strip()
    published_at = fm.get("published_at")
    published_at_s = str(published_at).strip() if published_at is not None else ""
    published_at_norm = normalize_published_at(published_at_s)

    country = str(fm.get("country", "")).strip()
    if not country:
        country = _infer_country_from_path(path, input_dir)

    chunks = build_chunks(body, chunk_chars=chunk_chars, overlap_chars=overlap_chars)
    total = len(chunks)

    base_meta: dict[str, Any] = {
        "info_type": "news_article",
        "published_at": published_at_norm,
        "title": title,
        "country": country,
        "url": url,
        "rss_url": rss_url,
        "source": str(fm.get("source", "")).strip(),
        "feed_name": str(fm.get("feed_name", "")).strip(),
        "body_source": str(fm.get("body_source", "")).strip(),
        "source_file": str(path),
        "dedupe_id": fm.get("dedupe_id"),
        "cluster_id": fm.get("cluster_id"),
    }

    # Include extra metadata that can help ranking without bloating too much.
    for k in (
        "domain",
        "best_domain",
        "domain_score",
        "published_at_source",
        "published_at_confidence",
        "article_updated_at",
        "ingested_at",
        "extracted_at",
        "phase1_fetch_url",
        "article_fetch_url",
        "enriched",
        "enrichment_used",
    ):
        if k in fm:
            base_meta[k] = fm.get(k)

    rows: list[ChunkRow] = []
    for i, chunk in enumerate(chunks):
        domains = infer_domains(chunk)
        meta = dict(base_meta)
        meta["domains"] = "; ".join(domains)
        meta["chunk_index"] = i
        meta["total_chunks"] = total
        rows.append(
            ChunkRow(
                id=_chunk_id(str(path), i, chunk),
                text=chunk,
                metadata=_safe_meta(meta),
            )
        )
    return rows


def normalize_published_at(value: str) -> str:
    """
    Normalize any supported published_at string to YYYY-MM-DD.
    Accepts:
      - YYYY-MM-DD
      - YYYY-MM-DDTHH:MM:SSZ / +00:00
      - quoted YAML strings
    Returns "" if it can't parse.
    """
    s = (value or "").strip().strip("'").strip('"')
    if not s:
        return ""

    # Fast path: already a date prefix.
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    if re.match(r"^\d{4}-\d{2}-\d{2}[ T]", s):
        return s[:10]

    # Handle trailing Z.
    if s.endswith("Z") and "T" in s:
        s = s[:-1] + "+00:00"

    # Attempt ISO parse.
    try:
        dt = datetime.fromisoformat(s)
        return dt.date().isoformat()
    except Exception:
        pass

    # Last resort: find first date-like substring.
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    return ""


def preprocess_folder(
    input_dir: Path,
    chunk_chars: int,
    overlap_chars: int,
    max_files: int | None,
) -> list[ChunkRow]:
    files = list_news_txt_files(input_dir)
    if max_files is not None:
        files = files[: max(0, int(max_files))]

    out: list[ChunkRow] = []
    for p in files:
        out.extend(
            preprocess_news_file(
                path=p,
                input_dir=input_dir,
                chunk_chars=chunk_chars,
                overlap_chars=overlap_chars,
            )
        )
    return out


def write_jsonl(rows: Iterable[ChunkRow], output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with output_path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(asdict(row), ensure_ascii=False) + "\n")
            n += 1
    return n


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Chunk news .txt articles into JSONL for vector DB.")
    p.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Root directory for saved news .txt articles (default: {DEFAULT_INPUT_DIR})",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output JSONL path (default: {DEFAULT_OUTPUT_PATH})",
    )
    p.add_argument("--chunk-chars", type=int, default=1200, help="Chunk size in chars.")
    p.add_argument("--overlap", type=int, default=200, help="Chunk overlap in chars.")
    p.add_argument("--max-files", type=int, default=None, help="Optional cap on number of files processed.")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    input_dir: Path = args.input_dir
    output_path: Path = args.output
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist or is not a directory: {input_dir}")

    rows = preprocess_folder(
        input_dir=input_dir,
        chunk_chars=max(200, int(args.chunk_chars)),
        overlap_chars=max(0, int(args.overlap)),
        max_files=args.max_files,
    )
    count = write_jsonl(rows, output_path)
    print(f"Wrote {count} chunks to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

