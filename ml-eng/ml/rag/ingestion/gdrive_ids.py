"""Parse Google Drive folder IDs from env values (raw ID or share URL)."""
from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

_FOLDER_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{10,}$")


def normalize_drive_folder_id(raw: str) -> str:
    """
    Accept a bare folder ID or a Drive URL such as:
      https://drive.google.com/drive/folders/<id>?usp=...
    """
    value = (raw or "").strip()
    if not value:
        return ""

    if _FOLDER_ID_RE.match(value):
        return value

    parsed = urlparse(value)
    if parsed.scheme in ("http", "https") and "drive.google.com" in (parsed.netloc or ""):
        parts = [p for p in parsed.path.split("/") if p]
        if "folders" in parts:
            idx = parts.index("folders")
            if idx + 1 < len(parts):
                return parts[idx + 1]
        open_id = parse_qs(parsed.query).get("id", [None])[0]
        if open_id:
            return str(open_id).strip()

    raise ValueError(f"Not a valid Drive folder ID or folder URL: {raw!r}")
