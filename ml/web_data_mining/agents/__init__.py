"""RSS-only agentic web mining (domain specialists + orchestration)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ml.web_data_mining.agents.orchestrator import RssMiningOrchestrator

__all__ = ["RssMiningOrchestrator"]


def __getattr__(name: str) -> Any:
    if name == "RssMiningOrchestrator":
        from ml.web_data_mining.agents.orchestrator import RssMiningOrchestrator

        return RssMiningOrchestrator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
