from __future__ import annotations

from typing import Any

from typing_extensions import TypedDict

from ml.web_data_mining.schemas import RunParams


class _RequiredPipelineState(TypedDict):
    params: RunParams


class MiningPipelineState(_RequiredPipelineState, total=False):
    node_trace: list[str]
    exit_code: int
    stop_run: bool
    error: str
    include_rss: bool
    feeds_by_country: dict[str, list[dict[str, str]]]
    candidates: list[dict[str, Any]]
    ranked_candidates: list[dict[str, Any]]
    candidate_idx: int
    done: bool
    skip_current: bool
    current: dict[str, Any]
    page_title: str
    body: str
    fetch_url: str
    phase1_fetch_url: str
    extract_meta: dict[str, Any]
    body_source: str
    needs_enrichment: bool
    saved_count: int
    seen_norm_urls: dict[str, str]
    canonical_title: dict[str, tuple[str, str]]
    canonical_body_hash: dict[str, str]
