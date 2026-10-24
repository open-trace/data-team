"""Discover sources/*/source.yaml (skip underscore-prefixed folders)."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

SOURCES_DIR = Path(__file__).resolve().parent / "sources"


@dataclass
class SourceSpec:
    name: str
    path: Path
    config: dict[str, Any]

    @property
    def table(self) -> str:
        return str(self.config["table"])

    @property
    def url(self) -> str | None:
        return self.config.get("url")

    @property
    def load_mode(self) -> str:
        load = self.config.get("load") or {}
        return str(load.get("mode", "replace_partition"))

    @property
    def key_cols(self) -> list[str]:
        load = self.config.get("load") or {}
        return list(load.get("key") or [])

    @property
    def since_column(self) -> str | None:
        return self.config.get("since_column")

    @property
    def expect(self) -> dict[str, Any]:
        return dict(self.config.get("expect") or {})


def list_sources(sources_dir: Path | None = None) -> list[SourceSpec]:
    root = sources_dir or SOURCES_DIR
    specs: list[SourceSpec] = []
    if not root.is_dir():
        return specs
    for child in sorted(root.iterdir()):
        if not child.is_dir() or child.name.startswith("_"):
            continue
        yaml_path = child / "source.yaml"
        if not yaml_path.is_file():
            continue
        config = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
        name = str(config.get("name") or child.name)
        specs.append(SourceSpec(name=name, path=child, config=config))
    return specs


def get_source(name: str, sources_dir: Path | None = None) -> SourceSpec:
    for spec in list_sources(sources_dir):
        if spec.name == name:
            return spec
    known = ", ".join(s.name for s in list_sources(sources_dir)) or "(none)"
    raise KeyError(f"Unknown source {name!r}. Known: {known}")


def load_source_modules(spec: SourceSpec) -> tuple[Any, Any]:
    """Import extract/transform modules for a source package."""
    pkg = f"ingestion.sources.{spec.path.name}"
    extract_mod = importlib.import_module(f"{pkg}.extract")
    transform_mod = importlib.import_module(f"{pkg}.transform")
    return extract_mod, transform_mod
