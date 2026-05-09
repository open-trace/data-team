from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ml.rag.ingestion.gdrive.drive_client import DriveFile, download_file, walk_folder


@dataclass(frozen=True)
class SyncStats:
    scanned: int
    downloaded: int
    skipped: int


def _sha1_bytes(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()


def _manifest_path(cache_root: Path, folder_id: str) -> Path:
    return cache_root / folder_id / ".gdrive_manifest.json"


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"files": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data.setdefault("files", {})
            if isinstance(data["files"], dict):
                return data
    except Exception:
        pass
    return {"files": {}}


def _save_manifest(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _local_relpath(parts: tuple[str, ...]) -> str:
    # Ensure we never traverse outside cache root via weird names.
    safe = []
    for p in parts:
        p = p.replace("\\", "_").replace("/", "_").strip()
        if not p or p in (".", ".."):
            p = "_"
        safe.append(p)
    return str(Path(*safe))


def _should_download(local_path: Path, df: DriveFile, manifest: dict[str, Any], rel: str) -> bool:
    rec = (manifest.get("files") or {}).get(rel) or {}
    if not local_path.exists():
        return True
    if df.md5_checksum and rec.get("md5") != df.md5_checksum:
        return True
    if df.size is not None and rec.get("size") != df.size:
        return True
    # If we don't have stable server-side hashes (e.g. for Google Docs), rely on size/exists.
    return False


def sync_drive_folder_to_cache(
    *,
    service: Any,
    folder_id: str,
    cache_root: Path,
    allowed_suffixes: tuple[str, ...] | None = None,
) -> tuple[Path, SyncStats]:
    """
    Download all files under Drive folder_id into:
      <cache_root>/<folder_id>/<relative path>

    Returns (local_root, stats).
    """
    local_root = (cache_root / folder_id).resolve()
    manifest_path = _manifest_path(cache_root, folder_id)
    manifest = _load_manifest(manifest_path)
    files_map: dict[str, Any] = manifest.get("files") if isinstance(manifest.get("files"), dict) else {}

    scanned = downloaded = skipped = 0
    for parts, df in walk_folder(service, folder_id):
        scanned += 1
        rel = _local_relpath(parts)
        if allowed_suffixes:
            if not any(rel.lower().endswith(s.lower()) for s in allowed_suffixes):
                skipped += 1
                continue
        local_path = local_root / rel
        if _should_download(local_path, df, manifest, rel):
            download_file(service, df.id, local_path)
            files_map[rel] = {"id": df.id, "md5": df.md5_checksum, "size": df.size}
            downloaded += 1
        else:
            skipped += 1

    manifest["files"] = files_map
    _save_manifest(manifest_path, manifest)
    return local_root, SyncStats(scanned=scanned, downloaded=downloaded, skipped=skipped)

