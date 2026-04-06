# Suggested RSS / Atom feeds (Africa + agriculture)

**Use at your own risk:** URLs change, feeds break, and **terms of use** differ by publisher.  
**Before production:** open each URL in a browser or run `curl -I <url>` and parse with a reader.  
**AllAfrica** and some aggregators require **attribution** and may restrict commercial reuse—read their terms.

**Curated file:** working URLs from periodic checks live in [`feeds.json`](feeds.json) (default when present).  
**Re-validate all candidates** (HTTP probe, same spirit as `curl -sS -A "OpenTraceFeedCheck/1.0" -I URL`):

```bash
PYTHONPATH=. python -m ml.web_data_mining.scripts.validate_feeds
PYTHONPATH=. python -m ml.web_data_mining.scripts.validate_feeds --write   # refresh feeds.json (skips known HTML hubs)
```

Copy working entries into `feeds.example.json` or extend `feeds.json` under the right country key.

---

## Nigeria

| Name | URL (verify) |
|------|----------------|
| Punch — latest (API-style RSS) | `https://rss.punchng.com/v1/category/latest_news` |
| Punch — business | `https://rss.punchng.com/v1/category/business` |
| Vanguard — main (WordPress-style) | `https://www.vanguardngr.com/feed/` |
| Guardian Nigeria | `https://guardian.ng/feed/` |
| The Nation Nigeria | `https://thenationonlineng.net/feed/` |
| Business Day Nigeria | `https://businessday.ng/feed/` |
| Daily Trust | `https://dailytrust.com/feed/` |
| Premium Times | `https://www.premiumtimesng.com/feed/` |
| Nairametrics (business/econ) | `https://nairametrics.com/feed/` |

**Government / technical (often stable):**

| Name | URL (verify) |
|------|----------------|
| FMITI / agric-adjacent (search site for “RSS” or “subscribe”) | varies |
| NBS / policy docs | often no RSS—use manual lists instead |

---

## Ghana

| Name | URL (verify) |
|------|----------------|
| Graphic Online — features | `https://www.graphic.com.gh/features/features.feed?type=rss` |
| Graphic — general (try) | `https://www.graphic.com.gh/feed/` |
| Citinewsroom | `https://citinewsroom.com/feed/` |
| MyJoyOnline | `https://www.myjoyonline.com/feed/` |
| GhanaWeb | `https://www.ghanaweb.com/GhanaHomePage/rss/news.xml` (verify path) |
| Business & Financial Times Ghana | search site for RSS |

---

## Rwanda

| Name | URL (verify) |
|------|----------------|
| The New Times Rwanda | `https://www.newtimes.co.rw/feed` or `https://www.newtimes.co.rw/feed/` |
| KT Press | `https://www.ktpress.rw/feed/` |
| Rwanda Broadcasting Agency / outlets | check official site for feed links |

---

## Senegal

| Name | URL (verify) |
|------|----------------|
| Le Soleil | `https://lesoleil.sn/feed/` (verify) |
| Seneweb | search for official RSS (site structure changes) |
| APS (Senegal press agency) | check `aps.sn` for syndication / RSS |

---

## Sierra Leone

| Name | URL (verify) |
|------|----------------|
| Awoko Newspaper | `https://awokonews.com/feed/` (verify) |
| Sierra Leone Telegraph / outlets | search “site name RSS feed” |
| Concord Times | check for `/feed/` |

---

## Pan-Africa / topic feeds (attribute + ToS)

Build **country-specific** URLs from AllAfrica’s RSS tool (do not guess opaque URLs):

- **AllAfrica RSS hub:** `https://allafrica.com/tools/rss/` or `https://www.allafrica.com/misc/tools/rss.html`  
  Use their UI to generate feeds for **Food and Agriculture** + **Nigeria / Ghana / …**.

| Name | Notes |
|------|--------|
| AllAfrica — Agriculture topic | Generate from their RSS page |
| AllAfrica — per country | Generate from their RSS page |

---

## UN / multilateral (global but Africa-relevant stories)

| Name | URL (verify) |
|------|----------------|
| FAO — RSS index | `https://www.fao.org/news/rss-feed/en/` (pick specific `.xml` links from that page) |
| FAO — trade & markets (if listed on index) | from same index |
| IFAD news | `https://www.ifad.org/en/rss` or `/rss/news` (verify on site) |
| WFP news | search `wfp.org` for “RSS” |
| World Bank — news | `https://www.worldbank.org/en/news/rss` (verify) |

---

## Agriculture / climate verticals (English)

| Name | URL (verify) |
|------|----------------|
| Climate Home News | `https://www.climatechangenews.com/feed/` |
| Mongabay / Africa environment | check Mongabay RSS sections |
| SciDev.Net | `https://www.scidev.net/global/rss.xml` (verify) |

---

## Google News (already in `feeds.example.json`)

Template (replace query + region):

`https://news.google.com/rss/search?q=KEYWORDS+COUNTRY&hl=en&gl=XX&ceid=XX:en`

- Good for **discovery**; check **Google’s terms** and **rate limits**.
- Pair with **direct outlet RSS** for stability.

---

## JSON snippet pattern

```json
"Nigeria": [
  { "name": "Punch latest", "url": "https://rss.punchng.com/v1/category/latest_news" },
  { "name": "Vanguard", "url": "https://www.vanguardngr.com/feed/" }
]
```

---

## Quick validation (terminal)

```bash
curl -sS -A "OpenTraceFeedCheck/1.0" -I "https://www.vanguardngr.com/feed/" | head -5
```

You should see `200` and often `content-type: application/rss+xml` or `text/xml`.
