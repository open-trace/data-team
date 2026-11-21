"""
Migrate Qdrant collections from the external Qdrant Cloud instance to the
self-managed Qdrant instance on Railway (ML-073 / ML-074), via snapshot
copy. Points are moved byte-for-byte — no re-embedding.

For each collection:
  1. Create a snapshot on the OLD (source) instance.
  2. Stream-download the snapshot file locally.
  3. Upload it to the NEW (target) instance via the snapshot-recover endpoint
     (creates the collection if it does not already exist).
  4. Compare point counts on both sides and report.

Env:
  QDRANT_OLD_URL, QDRANT_OLD_API_KEY   — source (Qdrant Cloud)
  QDRANT_URL, QDRANT_API_KEY           — target (self-managed Railway Qdrant)

Run (as a one-off Railway service — see Dockerfile.migrate-qdrant, so the
target's private `*.railway.internal` URL is reachable; the source is public
so no networking changes are needed there):

  PYTHONPATH=. python -m ml.rag.scripts.migrate_qdrant_cloud
  PYTHONPATH=. python -m ml.rag.scripts.migrate_qdrant_cloud --collections news_data,policies
  PYTHONPATH=. python -m ml.rag.scripts.migrate_qdrant_cloud --verify-only
"""

from __future__ import annotations

import argparse
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

import requests

from ml.rag.local_env import load_data_local_dotenv
from ml.rag.paths import ML_ENG_ROOT

DEFAULT_COLLECTIONS = [
    "news_data",
    "academic_papers",
    "policies",
    "public_reports",
    "formation",
    "OTA_insights",
]


@dataclass(frozen=True)
class QdrantTarget:
    url: str
    api_key: str

    def headers(self) -> dict[str, str]:
        return {"api-key": self.api_key}


@dataclass
class MigrationResult:
    collection: str
    ok: bool
    old_points: int | None = None
    new_points: int | None = None
    error: str | None = None


def _env(name: str) -> str:
    return os.environ.get(name, "").strip()


def load_targets() -> tuple[QdrantTarget, QdrantTarget]:
    old = QdrantTarget(url=_env("QDRANT_OLD_URL").rstrip("/"), api_key=_env("QDRANT_OLD_API_KEY"))
    new = QdrantTarget(url=_env("QDRANT_URL").rstrip("/"), api_key=_env("QDRANT_API_KEY"))
    missing = [
        name
        for name, val in [
            ("QDRANT_OLD_URL", old.url),
            ("QDRANT_OLD_API_KEY", old.api_key),
            ("QDRANT_URL", new.url),
            ("QDRANT_API_KEY", new.api_key),
        ]
        if not val
    ]
    if missing:
        raise RuntimeError(f"Missing env var(s): {', '.join(missing)}")
    return old, new


def point_count(target: QdrantTarget, collection: str) -> int | None:
    resp = requests.get(
        f"{target.url}/collections/{collection}",
        headers=target.headers(),
        timeout=30,
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()["result"]["points_count"]


def create_snapshot(target: QdrantTarget, collection: str) -> str:
    resp = requests.post(
        f"{target.url}/collections/{collection}/snapshots",
        headers=target.headers(),
        params={"wait": "true"},
        timeout=600,
    )
    resp.raise_for_status()
    return resp.json()["result"]["name"]


def download_snapshot(target: QdrantTarget, collection: str, snapshot_name: str, dest: Path) -> None:
    with requests.get(
        f"{target.url}/collections/{collection}/snapshots/{snapshot_name}",
        headers=target.headers(),
        stream=True,
        timeout=600,
    ) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)


def upload_snapshot(target: QdrantTarget, collection: str, snapshot_path: Path) -> None:
    with open(snapshot_path, "rb") as f:
        resp = requests.post(
            f"{target.url}/collections/{collection}/snapshots/upload",
            headers=target.headers(),
            params={"wait": "true", "priority": "snapshot"},
            files={"snapshot": (snapshot_path.name, f, "application/octet-stream")},
            timeout=1800,
        )
    resp.raise_for_status()


def migrate_collection(old: QdrantTarget, new: QdrantTarget, collection: str, tmp_dir: Path) -> MigrationResult:
    try:
        old_points = point_count(old, collection)
        if old_points is None:
            return MigrationResult(collection, ok=False, error="not found on source")

        print(f"[{collection}] creating snapshot on source ({old_points} points)...")
        snapshot_name = create_snapshot(old, collection)

        snapshot_path = tmp_dir / f"{collection}.snapshot"
        print(f"[{collection}] downloading snapshot '{snapshot_name}'...")
        download_snapshot(old, collection, snapshot_name, snapshot_path)
        size_mb = snapshot_path.stat().st_size / (1024 * 1024)
        print(f"[{collection}] downloaded {size_mb:.1f} MB, uploading to target...")

        upload_snapshot(new, collection, snapshot_path)
        snapshot_path.unlink(missing_ok=True)

        new_points = point_count(new, collection)
        ok = new_points == old_points
        return MigrationResult(collection, ok=ok, old_points=old_points, new_points=new_points)
    except Exception as exc:  # noqa: BLE001 - report per collection, keep going
        return MigrationResult(collection, ok=False, error=str(exc))


def verify_only(old: QdrantTarget, new: QdrantTarget, collections: list[str]) -> list[MigrationResult]:
    results = []
    for c in collections:
        try:
            old_points = point_count(old, c)
            new_points = point_count(new, c)
            results.append(
                MigrationResult(
                    c,
                    ok=(old_points == new_points and old_points is not None),
                    old_points=old_points,
                    new_points=new_points,
                )
            )
        except Exception as exc:  # noqa: BLE001 - report per collection, keep going
            results.append(MigrationResult(c, ok=False, error=str(exc)))
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate Qdrant collections from Qdrant Cloud to self-managed Qdrant.")
    parser.add_argument(
        "--collections",
        type=str,
        default=",".join(DEFAULT_COLLECTIONS),
        help="Comma-separated collection names to migrate.",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Skip migration; only compare point counts between source and target.",
    )
    args = parser.parse_args()

    load_data_local_dotenv(ML_ENG_ROOT)

    collections = [c.strip() for c in args.collections.split(",") if c.strip()]
    old, new = load_targets()

    if args.verify_only:
        results = verify_only(old, new, collections)
    else:
        with tempfile.TemporaryDirectory() as tmp:
            results = [migrate_collection(old, new, c, Path(tmp)) for c in collections]

    print("\n==============================")
    print("MIGRATION SUMMARY")
    print("==============================")
    failed = False
    for r in results:
        if r.error:
            print(f"[FAIL] {r.collection}: {r.error}")
            failed = True
        elif r.ok:
            print(f"[OK]   {r.collection}: {r.old_points} -> {r.new_points} points")
        else:
            print(f"[MISMATCH] {r.collection}: source={r.old_points} target={r.new_points}")
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
