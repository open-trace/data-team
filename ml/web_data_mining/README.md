# Web data mining pipeline (RSS-first + optional Tavily)

## Architecture

- **Orchestrator** (`agents/orchestrator.py`): per-country loop, caps URLs, coordinates agents.
- **Domain specialists** (`agents/domain_agent.py`): keyword scoring per domain on RSS title+summary (and full article after fetch). A story must show at least one **agrifood context** signal (e.g. farmer, crop, food security) before any domain is assigned—reduces mis-tags on generic politics/macro news.
- **RSS discovery** (`agents/rss_discovery.py`): loads `feeds.json`, fetches and parses RSS/Atom.
- **Fetch / extract** (`agents/fetch_extract.py`): **two-phase** GET — (1) `item.url` with the original RSS **`USER_AGENT`** (`OpenTraceWebMiner/…`), (2) if a direct article URL is inferred (RSS hints + mining phase-1 HTML), a **browser `User-Agent`** GET + **trafilatura** (then `<article>` / full-page fallback). Front matter includes **`phase1_fetch_url`** and **`article_fetch_url`** (same as body source when phase-2 succeeds).
- **Storage** (`agents/storage_txt.py`): one `.txt` per article = **YAML front matter + body**.

## Dependencies

```bash
pip install -r ml/web_data_mining/requirements.txt
```

(`feedparser`, `requests`, `PyYAML`, `trafilatura`, `lxml_html_clean` — article text uses **trafilatura** so nav/ads don’t drown the story body.)

### Optional: Tavily + LangChain tools

To **strengthen** thin fetches (short body or headline-only) **without replacing** RSS or the two-phase fetch:

```bash
pip install -r ml/web_data_mining/requirements-agent.txt
export TAVILY_API_KEY=...   # https://app.tavily.com/sign-in
```

Then pass **`--tavily-enrich`** to `run_pipeline`, or set `tavily_enrich` / `tavily_max_search_results` / `tavily_extract_first` in your config JSON. Saved articles will show `body_source: tavily_extract` or `tavily_search` when Tavily supplied the text.

Details: [`AGENTIC_TAVILY.md`](AGENTIC_TAVILY.md) · [Tavily × LangChain docs](https://docs.tavily.com/documentation/integrations/langchain).

Standalone news search (no LLM):

```bash
PYTHONPATH=. python -m ml.web_data_mining.run_tavily_cli -q "Ghana cocoa export news"
```

### LangGraph deep-research mode (LLM + Tavily)

Matches the loop from [langchain-ai/deep_research_from_scratch](https://github.com/langchain-ai/deep_research_from_scratch) (scope/research/write style: **agent ↔ tools** then **compress**). Install graph extras and use **`--tavily-enrich --tavily-langgraph`** on `run_pipeline`, or run ad-hoc:

```bash
pip install -r ml/web_data_mining/requirements-agent-graph.txt
export OPENAI_API_KEY=...   # LangGraph LLM (optional: MINING_RESEARCH_MODEL)
PYTHONPATH=. python -m ml.web_data_mining.run_mining_research -q "Your research question"
```

See [`AGENTIC_TAVILY.md`](AGENTIC_TAVILY.md) for env vars and flags.

## Run the pipeline

From **repo root** (`data-team/`):

```bash
PYTHONPATH=. python -m ml.web_data_mining.run_pipeline --help
```

### Example (five countries, year range, dry-run)

```bash
PYTHONPATH=. python -m ml.web_data_mining.run_pipeline \
  --countries Nigeria,Rwanda,"Sierra Leone",Ghana,Senegal \
  --start-year 2005 \
  --end-year 2026 \
  --batch-size 100 \
  --max-urls-per-country 500 \
  --dry-run
```

### Live run (network)

Removes `--dry-run`. Writes under `--output-dir` (default `data/local/web_news_rss`):

```bash
PYTHONPATH=. python -m ml.web_data_mining.run_pipeline \
  --countries Ghana \
  --start-year 2024 \
  --end-year 2025 \
  --feeds ml/web_data_mining/config/feeds.json \
  --output-dir data/local/web_news_rss
```

### Feeds config

- Default feeds file: **`ml/web_data_mining/config/feeds.json`** when present, else `feeds.example.json` (country → `[{ "name", "url" }]`).
- Replace URLs with **trusted outlet RSS** where possible. Google News RSS entries are a **starting point only** — check **terms of use**, robots, and rate limits. The pipeline rewrites `news.google.com/rss/articles/...` using **`source` / `links` / `<a href>` in RSS HTML`**, then (before fetch) tries a **browser-style GET** to follow redirects and read **canonical / `og:url`** from Google’s HTML. If the body would only repeat the headline (`rss_summary_*`), the item is **skipped** instead of saved. When HTML was fetched from a different URL than the RSS `link`, **`fetch_url`** is stored in the front matter.
- A longer **candidate URL list** is in [`config/FEEDS_SUGGESTIONS.md`](config/FEEDS_SUGGESTIONS.md). Re-check endpoints after URL rot:

```bash
PYTHONPATH=. python -m ml.web_data_mining.scripts.validate_feeds
# Regenerate feeds.json from candidates that return HTTP 200 (excludes known HTML hubs):
PYTHONPATH=. python -m ml.web_data_mining.scripts.validate_feeds --write
```

### Historical coverage (2000 → today?)

- **Direct outlet RSS** (newspapers, etc.) almost never contains decades of items—only a **recent rolling window**. Widening `--start-year` does not pull articles from 2000 if they are no longer in the feed.
- **Google News rows** in `feeds.json` can be queried **day-by-day** (experimental): the pipeline adds `after:YYYY-MM-DD before:YYYY-MM-DD` to the search `q` for each day in your run window.

```bash
# One calendar month (under the default ~93-day guard)
PYTHONPATH=. python -m ml.web_data_mining.run_pipeline \
  --countries Nigeria \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --google-news-daily-slice \
  --max-urls-per-country 300 \
  --dry-run

# Multi-year / 2000–today: many thousands of RSS requests — opt in + chunk by year/month
PYTHONPATH=. python -m ml.web_data_mining.run_pipeline \
  --countries Nigeria \
  --start-year 2000 --end-year 2000 \
  --google-news-daily-slice --allow-large-google-slice \
  --google-slice-delay 2.0
```

Use **`--allow-large-google-slice`** when the day span is **> ~93 days**. Prefer **smaller windows** (e.g. one year per run) to reduce blocks and runtime.

### Other flags

- `--output-dir` — root for saved `.txt` files.
- `--feeds` — path to your feeds JSON.
- `--discovery-mode rss|tavily|hybrid` — choose candidate URL source before fetch/extract.
- `--tavily-discovery-max-results` — per-domain Tavily discovery cap in `tavily`/`hybrid`.
- **Omit `--domains`** to use the full default taxonomy (avoids comma-split issues on the CLI).

### Config file

```bash
PYTHONPATH=. python -m ml.web_data_mining.run_pipeline \
  --config ml/web_data_mining/config/run.example.json \
  --dry-run
```

CLI overrides config when both set (`output_dir`, `feeds_path`, countries, years, etc.).

## Output format

Each article is UTF-8 text:

```text
---
id: ...
url: ...
title: ...
country: ...
domain: ...
...
---

<body text>
```

## Compliance

Respect site **ToS**, **robots.txt**, and use reasonable **rate limits** (small delay between article fetches). This tool is intended for **research / internal corpora** with clear attribution of sources.

### Google consent / `400` on `consent.google.com`

If you run from the **EU/UK** (or Google maps your IP there), `news.google.com` often redirects to **`consent.google.com`**, which may return **400/403** for non-browser clients. The pipeline **does not treat that as a hard HTTP error** anymore: it tries to **mine publisher URLs** from any HTML returned, then falls back to **RSS summary** or **skips**. For reliable full text, prefer **direct outlet RSS** in `feeds.json` and use Google News only as a supplement.

### `published_at` vs text you see in the file

`published_at` comes from the **RSS/Atom entry** (`published` / `updated`). The **body** is whatever HTML we got from **`url`**. If Google News gives a **publisher homepage** (e.g. `https://newsinfo.inquirer.net`) instead of the story link, we fetch today’s homepage—so you can see **2026** headlines while **`published_at` stays 2024**. The pipeline now **skips** obvious site-root / single-segment hub URLs and prefers article-looking links when resolving Google News entries; re-run to avoid those saves.
