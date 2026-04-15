"""
LangGraph state for the mining research agent (pattern from deep_research_from_scratch).

See: https://github.com/langchain-ai/deep_research_from_scratch
"""
from __future__ import annotations

import operator
from typing import Annotated, List, Sequence

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict


class MiningResearcherState(TypedDict):
    """Message history + slots filled by the compress node."""

    researcher_messages: Annotated[Sequence[BaseMessage], add_messages]
    tool_call_iterations: int
    research_topic: str
    compressed_research: str
    raw_notes: Annotated[List[str], operator.add]
