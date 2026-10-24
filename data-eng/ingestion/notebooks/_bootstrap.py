"""Make `ingestion` importable from notebooks without fighting kernel pickers.

Put this next to ingest notebooks. The first code cell should call
`ensure_ingestion()` (see usage in world_bank_data360_harmonized_country_poverty_landing_dev.ipynb).
"""

from __future__ import annotations

import sys
from pathlib import Path


def _candidate_package_roots(start: Path) -> list[Path]:
    roots: list[Path] = []
    for base in [start, *start.parents]:
        roots.extend(
            [
                base / "package",
                base / "ingestion" / "package",
                base / "data-eng" / "ingestion" / "package",
            ]
        )
    return roots


def ensure_ingestion() -> Path:
    """Ensure `import ingestion` works; return the package/ directory on sys.path."""
    try:
        import ingestion

        return Path(ingestion.__file__).resolve().parent.parent
    except ModuleNotFoundError:
        pass

    starts = [Path.cwd().resolve(), Path(__file__).resolve().parent]
    seen: set[Path] = set()
    for start in starts:
        for pkg_root in _candidate_package_roots(start):
            pkg_root = pkg_root.resolve()
            if pkg_root in seen:
                continue
            seen.add(pkg_root)
            if (pkg_root / "ingestion" / "__init__.py").is_file():
                path = str(pkg_root)
                if path not in sys.path:
                    sys.path.insert(0, path)
                import ingestion  # noqa: F401

                return pkg_root

    raise ModuleNotFoundError(
        "Could not locate ingestion/package. "
        "Expected data-eng/ingestion/package next to this repo. "
        "From data-eng you can also: pip install -e \"./ingestion[dev]\" "
        "using data-eng/venv."
    )
