"""Transparent CSV / CSV.GZ seed I/O for opentrace_ontology builds."""
from __future__ import annotations

import csv
import gzip
import hashlib
import os
from typing import Any, Iterable, TextIO


def resolve_seed_path(seeds_dir: str, name: str) -> str:
    """Resolve a seed file under seeds_dir.

    ``name`` may be a stem (``ontology_aliases``), ``*.csv``, or ``*.csv.gz``.
    Prefer an existing plain ``.csv``, else ``.csv.gz``.
    """
    if name.endswith(".csv.gz"):
        stem = name[: -len(".csv.gz")]
    elif name.endswith(".csv"):
        stem = name[: -len(".csv")]
    else:
        stem = name
    plain = os.path.join(seeds_dir, f"{stem}.csv")
    gz = os.path.join(seeds_dir, f"{stem}.csv.gz")
    if os.path.isfile(plain):
        return plain
    if os.path.isfile(gz):
        return gz
    # Default target path for writers (aliases prefer gz in ml-eng)
    if stem == "ontology_aliases":
        return gz
    return plain


def _is_gzip_path(path: str) -> bool:
    return path.endswith(".gz")


def open_seed(path: str, mode: str = "rt") -> TextIO:
    """Open a seed CSV; use gzip when ``path`` ends with ``.gz``."""
    if "b" in mode:
        raise ValueError("open_seed is text-mode only; use open() for binary")
    # Ensure newline='' for csv module
    if "t" not in mode and "b" not in mode:
        mode = mode + "t" if mode.endswith("+") or mode in ("r", "w", "a", "x") else mode
    kwargs: dict[str, Any] = {"encoding": "utf-8", "newline": ""}
    if _is_gzip_path(path):
        return gzip.open(path, mode, **kwargs)  # type: ignore[return-value]
    return open(path, mode, **kwargs)


def write_seed_rows(path: str, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    """Write CSV rows; gzip when path ends with .gz or basename is ontology_aliases.csv."""
    base = os.path.basename(path)
    use_gz = _is_gzip_path(path) or base in ("ontology_aliases.csv", "ontology_aliases.csv.gz")
    out_path = path
    if base == "ontology_aliases.csv" and not _is_gzip_path(path):
        out_path = path + ".gz"
        use_gz = True
    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    if use_gz:
        with gzip.open(out_path, "wt", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
            w.writeheader()
            for row in rows:
                w.writerow({k: row.get(k, "") for k in fields})
        # ml-eng package form is .gz only for aliases
        if "ontology_aliases" in os.path.basename(out_path):
            plain = out_path[: -len(".gz")] if out_path.endswith(".gz") else out_path
            if plain.endswith(".csv") and os.path.isfile(plain):
                os.remove(plain)
    else:
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
            w.writeheader()
            for row in rows:
                w.writerow({k: row.get(k, "") for k in fields})


def sha256_seed_content(path: str) -> str:
    """SHA256 of decompressed UTF-8 seed bytes (fair compare across .csv / .csv.gz)."""
    h = hashlib.sha256()
    if _is_gzip_path(path):
        with gzip.open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
    else:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
    return h.hexdigest()


def seed_file_bytes_for_hash(path: str) -> bytes:
    """Full decompressed content (for small scripts that hash once)."""
    if _is_gzip_path(path):
        with gzip.open(path, "rb") as f:
            return f.read()
    with open(path, "rb") as f:
        return f.read()


def gunzip_to_path(src_gz: str, dst_csv: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(dst_csv)) or ".", exist_ok=True)
    with gzip.open(src_gz, "rb") as fin, open(dst_csv, "wb") as fout:
        while True:
            chunk = fin.read(1 << 20)
            if not chunk:
                break
            fout.write(chunk)


def gzip_file(src_csv: str, dst_gz: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(dst_gz)) or ".", exist_ok=True)
    with open(src_csv, "rb") as fin, gzip.open(dst_gz, "wb", compresslevel=9) as fout:
        while True:
            chunk = fin.read(1 << 20)
            if not chunk:
                break
            fout.write(chunk)
