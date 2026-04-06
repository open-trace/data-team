from __future__ import annotations

from datetime import date

from ml.web_data_mining.agents.orchestrator import RssMiningOrchestrator
from ml.web_data_mining.agentic.pipeline_graph import run_pipeline_with_langgraph
from ml.web_data_mining.schemas import RunParams


def _year_windows(start_year: int, end_year: int) -> list[tuple[int, int]]:
    return [(year, year) for year in range(start_year, end_year + 1)]


def _date_windows(start_date: date, end_date: date) -> list[tuple[str, str]]:
    # Keep simple day-level window for now; caller can further batch if needed.
    return [(start_date.isoformat(), end_date.isoformat())]


def build_planned_windows(params: RunParams) -> list[str]:
    if params.start_date and params.end_date:
        ranges = _date_windows(params.start_date, params.end_date)
        return [f"{s}..{e}" for s, e in ranges]
    assert params.start_year is not None and params.end_year is not None
    ranges = _year_windows(params.start_year, params.end_year)
    return [f"{s}..{e}" for s, e in ranges]


def run_pipeline(params: RunParams) -> int:
    """
    RSS-first pipeline; optional Tavily (linear or LangGraph) enriches thin fetches.
    """
    windows = build_planned_windows(params)
    print("Pipeline parameters:")
    for k, v in params.to_dict().items():
        print(f"  {k}: {v}")
    print(f"Planned time windows ({len(windows)}): {windows[:10]}{' ...' if len(windows) > 10 else ''}")

    if getattr(params, "use_pipeline_langgraph", False):
        return run_pipeline_with_langgraph(params)

    orchestrator = RssMiningOrchestrator(
        min_article_chars=params.min_article_chars,
        min_rss_summary_chars=params.min_rss_summary_chars,
    )
    return orchestrator.run(params)

