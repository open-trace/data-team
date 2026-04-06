# Optional Tavily + LangChain layer

The RSS → two-phase fetch pipeline stays the **primary** path. When `--tavily-enrich` is enabled (or `tavily_enrich: true` in config), the orchestrator may call **Tavily** only if the article body is still **short** or **headline-only** after local fetch and RSS summary fallbacks.

Reference: [Tavily × LangChain](https://docs.tavily.com/documentation/integrations/langchain).

## Install

```bash
pip install -r ml/web_data_mining/requirements-agent.txt
```

## Credentials

Set **`TAVILY_API_KEY`** in the environment. The mining tools also load **`data/local/.env`** from the repo root (without overriding variables already set in your shell). A common typo **`TRAVILY_API_KEY`** is accepted as a fallback. Get a key at [Tavily sign-in](https://app.tavily.com/sign-in).

If the key is missing or `langchain-tavily` is not installed, the pipeline continues **RSS-only** (no crash).

## Pipeline flags

- **`--tavily-enrich`** — enable enrichment for thin items.
- **`--discovery-mode rss|tavily|hybrid`** — where candidate URLs come from before extraction.
- **`--tavily-discovery-max-results N`** — per-domain discovery cap for `tavily`/`hybrid` (default `5`).
- **`--tavily-discovery-min-domain-score N`** — discovery-stage domain gate for Tavily candidates (default `1`; set `0` to inspect broader candidates).
- **`--tavily-max-search-results N`** — default `3`, max `20`.
- **`--tavily-no-extract-first`** — skip **Tavily Extract** on publisher URLs; use **Tavily Search** only.
- **`--min-article-chars`** / **`--min-rss-summary-chars`** — tune save threshold and when RSS summary alone is enough (defaults `200` / `80`). With **`--tavily-enrich`**, **Google consent/gate** pages with a **short** RSS summary still get a **Tavily recovery** pass instead of being dropped immediately.
- **Tavily Search bodies** (`body_source: tavily_search`) are **multi-snippet**, not one full article. **By default they are not saved.** Use **`--allow-tavily-search-snippets`** to opt in; optional **`--tavily-search-max-distinct-domains`** and relevance (country/title) checks reduce junk. Prefer **`tavily_extract`** or **`--tavily-langgraph`** for higher-quality text.

Config JSON (same keys as `RunParams.to_dict()`):

```json
{
  "tavily_enrich": true,
  "tavily_max_search_results": 5,
  "tavily_extract_first": true
}
```

## Saved metadata

When Tavily supplies text, front matter **`body_source`** is set to `tavily_extract` or `tavily_search`. Normal saves remain `fetched_html`, `rss_summary_*`, etc.

## Discovery diagnostics

Each run now prints a candidate funnel per country:

- RSS: `seen`, `accepted`
- Tavily: `discovered`, `dropped_score`, `dropped_domain`, `accepted`
- Combined funnel: `pre_dedupe`, `unique_urls`, `slice_urls`

Use this to see exactly where `Saved 0` happened.

## LangGraph “deep research” mode (optional)

Aligned with [deep_research_from_scratch](https://github.com/langchain-ai/deep_research_from_scratch): **LLM ↔ tools loop** (Tavily search + extract + reflection) then a **compress** node that writes one corpus-ready note.

```bash
pip install -r ml/web_data_mining/requirements-agent.txt
pip install -r ml/web_data_mining/requirements-agent-graph.txt
export TAVILY_API_KEY=...
export OPENAI_API_KEY=...    # required for LangGraph (optional: MINING_RESEARCH_MODEL=gpt-4o-mini)

PYTHONPATH=. python -m ml.web_data_mining.run_pipeline \
  --countries Nigeria --start-date 2024-01-01 --end-date 2024-01-31 \
  --tavily-enrich --tavily-langgraph
```

- **`--tavily-langgraph`** requires **`--tavily-enrich`**. If the graph fails or returns short text, the pipeline **falls back** to linear extract→search enrichment.
- Saved articles show `body_source: tavily_langgraph` when the graph produced the body.
- **`--tavily-graph-recursion-limit`** caps LangGraph steps (default `28`).

Freeform research (no RSS):

```bash
PYTHONPATH=. python -m ml.web_data_mining.run_mining_research -q "Nigeria fertilizer subsidy policy 2024"
```

## Module layout

| Module | Role |
|--------|------|
| `agentic/tavily_tools.py` | `TavilyExtract` / `TavilySearch` wrappers (`topic="news"` for search). |
| `agentic/enrichment.py` | `try_enrich_with_tavily(...)` — linear + optional LangGraph path. |
| `agentic/mining_research_graph.py` | LangGraph: `llm_call` → `tool_node` loop → `compress_research`. |
| `agentic/state_mining_research.py` | `MiningResearcherState` TypedDict. |
| `agentic/mining_prompts.py` | System prompts for research + compress nodes. |

## Ad-hoc search (no LLM)

```bash
PYTHONPATH=. python -m ml.web_data_mining.run_tavily_cli --query "Nigeria cocoa export news 2024"
```

## Full LangChain agent (optional)

If you add `langchain`, `langchain-openai`, and set **`OPENAI_API_KEY`**, you can build an agent with `TavilySearch` as in the [official example](https://docs.tavily.com/documentation/integrations/langchain). This repo does not require an LLM for the mining pipeline—Tavily is used as **tools** only there.
