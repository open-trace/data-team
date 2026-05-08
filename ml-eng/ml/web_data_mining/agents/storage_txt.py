from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml


def _slug(s: str) -> str:
    s = s.strip().lower().replace(" ", "_")
    return re.sub(r"[^a-z0-9_]+", "", s)[:80] or "unknown"


def article_id_from_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def write_news_txt(
    output_root: Path,
    country: str,
    meta: dict[str, Any],
    body: str,
) -> Path:
    """
    Write one article as YAML front matter + blank line + body (UTF-8).
    """
    published = meta.get("published_at")
    year = datetime.utcnow().year
    if isinstance(published, str) and len(published) >= 4 and published[:4].isdigit():
        year = int(published[:4])
    elif isinstance(published, date):
        year = published.year

    aid = str(meta.get("id", article_id_from_url(str(meta.get("url", "")))))
    country_dir = output_root / _slug(country) / str(year)
    country_dir.mkdir(parents=True, exist_ok=True)
    path = country_dir / f"{aid}.txt"

    # YAML-friendly meta: only scalars / small structures
    dump_meta = dict(meta)
    for k, v in list(dump_meta.items()):
        if isinstance(v, date) and not isinstance(v, datetime):
            dump_meta[k] = v.isoformat()

    front = yaml.safe_dump(dump_meta, sort_keys=False, allow_unicode=True, default_flow_style=False)
    content = f"---\n{front}---\n\n{body.strip()}\n"
    path.write_text(content, encoding="utf-8")
    return path
