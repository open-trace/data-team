"""
LangGraph research loop: LLM ↔ Tavily tools → compress (deep_research_from_scratch pattern).

Reference: https://github.com/langchain-ai/deep_research_from_scratch
Requires: pip install -r ml/web_data_mining/requirements-agent-graph.txt
Env: TAVILY_API_KEY, OPENAI_API_KEY (optional: MINING_RESEARCH_MODEL, default gpt-4o-mini).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Sequence

from langchain_core.messages import (
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
    filter_messages,
)
from langchain_core.tools import tool
from langgraph.graph import END, START, StateGraph

from ml.web_data_mining.agentic.mining_prompts import (
    COMPRESS_HUMAN,
    COMPRESS_SYSTEM,
    MINING_RESEARCH_SYSTEM,
)
from ml.web_data_mining.agentic.state_mining_research import MiningResearcherState
from ml.web_data_mining.agentic.tavily_tools import tavily_api_key, tavily_extract_urls, tavily_search_news


def _today() -> str:
    return datetime.now().strftime("%B %d, %Y")


@tool
def tavily_news_search(query: str) -> str:
    """Search the web for recent news snippets related to the query. Use focused queries."""
    text, _results, err = tavily_search_news(query.strip(), max_results=5, search_depth="basic")
    if err:
        return f"Tavily search error: {err}"
    return text or "(no results)"


@tool
def tavily_extract_tool(urls: str) -> str:
    """Extract full article text from publisher URLs. Pass comma-separated https URLs (up to ~3)."""
    parts = [u.strip() for u in urls.split(",") if u.strip().startswith("http")]
    if not parts:
        return "No valid URLs; pass comma-separated https URLs."
    extracted, err = tavily_extract_urls(parts[:3])
    if err:
        return f"Tavily extract error: {err}"
    return (extracted or "")[:80_000]


@tool
def research_reflection(reflection: str) -> str:
    """Briefly note what you found, what's missing, and whether to search again. Keeps the loop deliberate."""
    return f"Reflection recorded ({len(reflection)} chars)."


MINING_TOOLS = [tavily_news_search, tavily_extract_tool, research_reflection]
_TOOLS_BY_NAME = {t.name: t for t in MINING_TOOLS}


def mining_graph_llm_missing_reason() -> str | None:
    """After loading data/local/.env (via Tavily key load), return a message if OpenAI is not configured."""
    tavily_api_key()
    if (os.environ.get("OPENAI_API_KEY") or "").strip():
        return None
    return "OPENAI_API_KEY missing for LangGraph (add to env or data/local/.env)"


def _require_keys() -> str | None:
    if not tavily_api_key():
        return "TAVILY_API_KEY is not set"
    if not (os.environ.get("OPENAI_API_KEY") or "").strip():
        return "OPENAI_API_KEY is not set (required for the LangGraph research agent)"
    return None


def _chat_model():
    """OpenAI chat model via langchain-openai (install: pip install -r requirements-agent-graph.txt)."""
    import importlib

    try:
        co = importlib.import_module("langchain_openai")
    except ImportError as exc:
        raise ImportError(
            "langchain-openai is required for the mining research graph. "
            "Install: pip install -r ml/web_data_mining/requirements-agent-graph.txt"
        ) from exc
    ChatOpenAI = co.ChatOpenAI
    model = (os.environ.get("MINING_RESEARCH_MODEL") or "gpt-4o-mini").strip()
    return ChatOpenAI(model=model, temperature=0.2, timeout=120.0)


def llm_call(state: MiningResearcherState) -> dict[str, Any]:
    model = _chat_model().bind_tools(MINING_TOOLS)
    sys = MINING_RESEARCH_SYSTEM.format(today=_today())
    msg = model.invoke([SystemMessage(content=sys), *state["researcher_messages"]])
    return {"researcher_messages": [msg]}


def tool_node(state: MiningResearcherState) -> dict[str, Any]:
    last = state["researcher_messages"][-1]
    tool_calls = getattr(last, "tool_calls", None) or []
    if not tool_calls:
        return {"researcher_messages": []}
    observations: list[str] = []
    for tc in tool_calls:
        name = tc["name"]
        fn = _TOOLS_BY_NAME.get(name)
        if fn is None:
            observations.append(f"Unknown tool: {name}")
            continue
        try:
            observations.append(fn.invoke(tc["args"]))
        except Exception as exc:
            observations.append(f"Tool error ({name}): {exc}")
    outs = [
        ToolMessage(content=obs, name=tc["name"], tool_call_id=tc["id"])
        for obs, tc in zip(observations, tool_calls)
    ]
    n = state.get("tool_call_iterations", 0) + len(tool_calls)
    return {"researcher_messages": outs, "tool_call_iterations": n}


def compress_research(state: MiningResearcherState) -> dict[str, Any]:
    model = _chat_model()
    sys = COMPRESS_SYSTEM.format(today=_today())
    msgs = list(state.get("researcher_messages", []))
    messages: list[BaseMessage] = [SystemMessage(content=sys), *msgs, HumanMessage(content=COMPRESS_HUMAN)]
    response = model.invoke(messages)
    raw_notes = [
        str(m.content)
        for m in filter_messages(state["researcher_messages"], include_types=["tool", "ai"])
    ]
    return {
        "compressed_research": str(response.content),
        "raw_notes": ["\n".join(raw_notes)],
    }


def should_continue(state: MiningResearcherState) -> Literal["tool_node", "compress_research"]:
    messages: Sequence[BaseMessage] = state["researcher_messages"]
    last = messages[-1]
    if getattr(last, "tool_calls", None):
        return "tool_node"
    return "compress_research"


def build_mining_research_graph() -> Any:
    g = StateGraph(MiningResearcherState)
    g.add_node("llm_call", llm_call)
    g.add_node("tool_node", tool_node)
    g.add_node("compress_research", compress_research)
    g.add_edge(START, "llm_call")
    g.add_conditional_edges(
        "llm_call",
        should_continue,
        {"tool_node": "tool_node", "compress_research": "compress_research"},
    )
    g.add_edge("tool_node", "llm_call")
    g.add_edge("compress_research", END)
    return g.compile()


@dataclass
class MiningResearchResult:
    ok: bool
    body: str
    error: str | None = None


def _user_message_for_article(
    *,
    country: str,
    domain: str,
    rss_title: str,
    rss_summary: str,
    rss_url: str,
    candidate_urls: list[str],
    existing_body_preview: str,
    min_chars: int,
) -> str:
    urls = ", ".join(candidate_urls[:5]) if candidate_urls else "(none)"
    prev = (existing_body_preview or "").strip()[:1200]
    return f"""Recover corpus-quality article text for this RSS item.

Country: {country}
Assigned domain tag: {domain}
RSS link: {rss_url}
Headline: {rss_title}
Summary: {rss_summary or "(empty)"}
Candidate publisher URLs from our fetch pipeline: {urls}

Local fetch/snippet (may be incomplete): 
---
{prev}
---

Target: at least ~{min_chars} characters of substantive, source-grounded content via your tools.
When done, respond without tool calls so the run can compress your findings."""


def run_mining_research_for_article(
    *,
    country: str,
    domain: str,
    rss_title: str,
    rss_summary: str,
    rss_url: str,
    candidate_urls: list[str],
    existing_body_preview: str,
    min_chars: int,
    recursion_limit: int = 28,
) -> MiningResearchResult:
    err = _require_keys()
    if err:
        return MiningResearchResult(ok=False, body="", error=err)

    graph = build_mining_research_graph()
    topic = rss_title[:200]
    user = _user_message_for_article(
        country=country,
        domain=domain,
        rss_title=rss_title,
        rss_summary=rss_summary,
        rss_url=rss_url,
        candidate_urls=candidate_urls,
        existing_body_preview=existing_body_preview,
        min_chars=min_chars,
    )
    initial: MiningResearcherState = {
        "researcher_messages": [HumanMessage(content=user)],
        "tool_call_iterations": 0,
        "research_topic": topic,
        "compressed_research": "",
        "raw_notes": [],
    }
    try:
        out = graph.invoke(initial, config={"recursion_limit": recursion_limit})
    except Exception as exc:
        return MiningResearchResult(ok=False, body="", error=str(exc))

    body = (out.get("compressed_research") or "").strip()
    if len(body) < min_chars:
        return MiningResearchResult(
            ok=False,
            body=body,
            error=f"compressed body too short ({len(body)} < {min_chars})",
        )
    return MiningResearchResult(ok=True, body=body, error=None)


def run_mining_research_freeform(question: str, *, recursion_limit: int = 28) -> MiningResearchResult:
    """Standalone research (CLI / notebooks) — same graph, minimal user message."""
    err = _require_keys()
    if err:
        return MiningResearchResult(ok=False, body="", error=err)
    graph = build_mining_research_graph()
    initial: MiningResearcherState = {
        "researcher_messages": [HumanMessage(content=question)],
        "tool_call_iterations": 0,
        "research_topic": question[:200],
        "compressed_research": "",
        "raw_notes": [],
    }
    try:
        out = graph.invoke(initial, config={"recursion_limit": recursion_limit})
    except Exception as exc:
        return MiningResearchResult(ok=False, body="", error=str(exc))
    body = (out.get("compressed_research") or "").strip()
    return MiningResearchResult(ok=True if body else False, body=body, error=None if body else "empty output")
