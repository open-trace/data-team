"""
Probe RSS candidate URLs (HTTP GET, follow redirects) and optionally write feeds.json.

Mirrors the manual check:
  curl -sS -A "OpenTraceFeedCheck/1.0" -I URL | head -5

We use GET with -L because some hosts mishandle HEAD.

Usage:
  PYTHONPATH=. python -m ml.web_data_mining.scripts.validate_feeds
  PYTHONPATH=. python -m ml.web_data_mining.scripts.validate_feeds --write
  PYTHONPATH=. python -m ml.web_data_mining.scripts.validate_feeds --min-code 200 --timeout 30
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

USER_AGENT = "OpenTraceFeedCheck/1.0 (+OpenTrace data-team)"

# HTTP 200 but not machine RSS endpoints — do not add to feeds.json via --write
EXCLUDE_URLS_FROM_WRITE: frozenset[str] = frozenset(
    {
        "https://www.allafrica.com/misc/tools/rss.html",
        "https://www.fao.org/news/rss-feed/en/",
    }
)

# All concrete URLs from config/FEEDS_SUGGESTIONS.md plus alternates probed during setup.
FEED_CANDIDATES: list[dict[str, str]] = [
    # Nigeria
    {"country": "Nigeria", "name": "Punch — latest news", "url": "https://rss.punchng.com/v1/category/latest_news"},
    {"country": "Nigeria", "name": "Punch — business", "url": "https://rss.punchng.com/v1/category/business"},
    {"country": "Nigeria", "name": "Vanguard", "url": "https://www.vanguardngr.com/feed/"},
    {"country": "Nigeria", "name": "Guardian Nigeria", "url": "https://guardian.ng/feed/"},
    {"country": "Nigeria", "name": "The Nation Nigeria", "url": "https://thenationonlineng.net/feed/"},
    {"country": "Nigeria", "name": "Business Day Nigeria", "url": "https://businessday.ng/feed/"},
    {"country": "Nigeria", "name": "Daily Trust", "url": "https://dailytrust.com/feed/"},
    {"country": "Nigeria", "name": "Premium Times", "url": "https://www.premiumtimesng.com/feed/"},
    {"country": "Nigeria", "name": "Nairametrics", "url": "https://nairametrics.com/feed/"},
    {"country": "Nigeria", "name": "Google News — Nigeria agriculture", "url": "https://news.google.com/rss/search?q=Nigeria+agriculture&hl=en&gl=NG&ceid=NG:en"},
    # Ghana
    {"country": "Ghana", "name": "Graphic Online — features", "url": "https://www.graphic.com.gh/features/features.feed?type=rss"},
    {"country": "Ghana", "name": "Graphic — general", "url": "https://www.graphic.com.gh/feed/"},
    {"country": "Ghana", "name": "Citinewsroom", "url": "https://citinewsroom.com/feed/"},
    {"country": "Ghana", "name": "MyJoyOnline", "url": "https://www.myjoyonline.com/feed/"},
    {"country": "Ghana", "name": "GhanaWeb — news RSS", "url": "https://www.ghanaweb.com/GhanaHomePage/rss/news.xml"},
    {"country": "Ghana", "name": "Google News — Ghana agriculture", "url": "https://news.google.com/rss/search?q=Ghana+agriculture&hl=en&gl=GH&ceid=GH:en"},
    # Rwanda
    {"country": "Rwanda", "name": "The New Times — /feed/", "url": "https://www.newtimes.co.rw/feed/"},
    {"country": "Rwanda", "name": "The New Times — /rss", "url": "https://www.newtimes.co.rw/rss"},
    {"country": "Rwanda", "name": "KT Press", "url": "https://www.ktpress.rw/feed/"},
    {"country": "Rwanda", "name": "Google News — Rwanda agriculture", "url": "https://news.google.com/rss/search?q=Rwanda+agriculture&hl=en&gl=RW&ceid=RW:en"},
    # Senegal
    {"country": "Senegal", "name": "Le Soleil", "url": "https://lesoleil.sn/feed/"},
    {"country": "Senegal", "name": "Google News — Senegal agriculture", "url": "https://news.google.com/rss/search?q=Senegal+agriculture&hl=en&gl=SN&ceid=SN:en"},
    # Sierra Leone
    {"country": "Sierra Leone", "name": "Awoko (listed in doc)", "url": "https://awokonews.com/feed/"},
    {"country": "Sierra Leone", "name": "Sierra Leone Telegraph", "url": "https://www.thesierraleonetelegraph.com/feed/"},
    {"country": "Sierra Leone", "name": "Global Times SL", "url": "https://globaltimes-sl.com/feed/"},
    {"country": "Sierra Leone", "name": "Google News — Sierra Leone agriculture", "url": "https://news.google.com/rss/search?q=Sierra%20Leone+agriculture&hl=en&gl=SL&ceid=SL:en"},
    # Global / vertical (often attached to Nigeria in curated feeds.json)
    {"country": "Nigeria", "name": "Climate Home News (global)", "url": "https://www.climatechangenews.com/feed/"},
    {"country": "Nigeria", "name": "SciDev.Net (global)", "url": "https://www.scidev.net/global/rss.xml"},
    {"country": "Nigeria", "name": "FAO RSS index (HTML — expect fail as RSS)", "url": "https://www.fao.org/news/rss-feed/en/"},
    {"country": "Nigeria", "name": "IFAD /en/rss", "url": "https://www.ifad.org/en/rss"},
    {"country": "Nigeria", "name": "World Bank news rss", "url": "https://www.worldbank.org/en/news/rss"},
    {"country": "Nigeria", "name": "AllAfrica RSS hub (HTML)", "url": "https://www.allafrica.com/misc/tools/rss.html"},
]


def curl_status_code(url: str, timeout: int, user_agent: str) -> tuple[int, str]:
    """Return (http_code, error_message). code 0 means transport error."""
    cmd = [
        "curl",
        "-sS",
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
        "-A",
        user_agent,
        "-L",
        "--max-time",
        str(timeout),
        url,
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 5)
        if out.returncode != 0:
            return 0, (out.stderr or out.stdout or "curl failed").strip()
        text = (out.stdout or "").strip()
        if not text.isdigit():
            return 0, text or "non-numeric curl output"
        return int(text), ""
    except subprocess.TimeoutExpired:
        return 0, "timeout"
    except FileNotFoundError:
        return 0, "curl not installed"


def build_feeds_json(ok_rows: list[dict[str, str]]) -> dict[str, Any]:
    by_country: dict[str, list[dict[str, str]]] = defaultdict(list)
    seen: set[tuple[str, str]] = set()
    for row in ok_rows:
        c, name, url = row["country"], row["name"], row["url"]
        key = (c, url)
        if key in seen:
            continue
        seen.add(key)
        by_country[c].append({"name": name, "url": url})
    out: dict[str, Any] = {
        "_comment": "Generated by validate_feeds.py --write. Re-validate periodically; ToS/robots apply.",
    }
    for c in sorted(by_country.keys()):
        out[c] = by_country[c]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="HTTP-probe RSS feed URLs from FEEDS_SUGGESTIONS list.")
    ap.add_argument("--write", action="store_true", help="Write ml/web_data_mining/config/feeds.json from OK URLs.")
    ap.add_argument("--min-code", type=int, default=200, help="Treat status >= this as OK (default 200).")
    ap.add_argument("--timeout", type=int, default=25, help="Per-request timeout seconds.")
    ap.add_argument("--user-agent", type=str, default=USER_AGENT, help="User-Agent header.")
    args = ap.parse_args()

    results: list[tuple[dict[str, str], int, str]] = []
    for row in FEED_CANDIDATES:
        code, err = curl_status_code(row["url"], args.timeout, args.user_agent)
        results.append((row, code, err))

    ok: list[dict[str, str]] = []
    print(f"{'CODE':<6} {'COUNTRY':<14} {'NAME':<40} URL")
    print("-" * 120)
    for row, code, err in results:
        ok_flag = code >= args.min_code and code < 400
        line = f"{code:<6} {row['country']:<14} {row['name'][:40]:<40} {row['url']}"
        if not ok_flag and err:
            line += f"  # {err[:60]}"
        print(line)
        if ok_flag:
            ok.append(row)

    print("-" * 120)
    print(f"OK: {len(ok)} / {len(results)}")

    if args.write:
        repo_config = Path(__file__).resolve().parents[1] / "config" / "feeds.json"
        ok_write = [r for r in ok if r["url"] not in EXCLUDE_URLS_FROM_WRITE]
        skipped = len(ok) - len(ok_write)
        if skipped:
            print(f"Excluded {skipped} URL(s) from write (HTML hubs / non-RSS): {EXCLUDE_URLS_FROM_WRITE & {r['url'] for r in ok}}")
        data = build_feeds_json(ok_write)
        if repo_config.exists():
            try:
                prev = json.loads(repo_config.read_text(encoding="utf-8"))
                for k, v in prev.items():
                    if str(k).startswith("_") and k not in data:
                        data[k] = v
            except Exception:
                pass
        repo_config.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Wrote {repo_config}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
