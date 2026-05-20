from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from googleapiclient.http import MediaIoBaseDownload


FOLDER_MIME = "application/vnd.google-apps.folder"


@dataclass(frozen=True)
class DriveFile:
    id: str
    name: str
    mime_type: str
    md5_checksum: str | None
    size: int | None


def _is_folder(f: DriveFile) -> bool:
    return f.mime_type == FOLDER_MIME


def _as_drive_file(item: dict[str, Any]) -> DriveFile:
    return DriveFile(
        id=str(item.get("id") or ""),
        name=str(item.get("name") or ""),
        mime_type=str(item.get("mimeType") or ""),
        md5_checksum=(str(item["md5Checksum"]) if item.get("md5Checksum") else None),
        size=(int(item["size"]) if item.get("size") not in (None, "") else None),
    )


def list_children(service: Any, folder_id: str) -> list[DriveFile]:
    """
    List direct children of a folder.
    """
    out: list[DriveFile] = []
    page_token: str | None = None
    while True:
        resp = (
            service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="nextPageToken, files(id,name,mimeType,md5Checksum,size)",
                pageSize=1000,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=page_token,
            )
            .execute()
        )
        files = resp.get("files") or []
        for item in files:
            if isinstance(item, dict):
                df = _as_drive_file(item)
                if df.id and df.name and df.mime_type:
                    out.append(df)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return out


def walk_folder(service: Any, folder_id: str, *, prefix_parts: tuple[str, ...] = ()) -> Iterable[tuple[tuple[str, ...], DriveFile]]:
    """
    Depth-first walk producing (relative_path_parts, file) for all non-folder files under folder_id.
    """
    children = list_children(service, folder_id)
    # stable order for reproducible sync
    children.sort(key=lambda x: (0 if _is_folder(x) else 1, x.name.lower()))
    for child in children:
        parts = (*prefix_parts, child.name)
        if _is_folder(child):
            yield from walk_folder(service, child.id, prefix_parts=parts)
        else:
            yield parts, child


def download_file(service: Any, file_id: str, dest_path: Path) -> None:
    """
    Download a Drive file (binary) to dest_path.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    fh = io.BytesIO()
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
    done = False
    while not done:
        _status, done = downloader.next_chunk()
    dest_path.write_bytes(fh.getvalue())

