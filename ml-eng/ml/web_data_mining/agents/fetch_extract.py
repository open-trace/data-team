from __future__ import annotations

import html as html_module
import io
import re
import time
from urllib.parse import urlparse

import requests

from ml.web_data_mining.agents.html_text import extract_og_title_regex, extract_title_regex, html_to_plain_text
from ml.web_data_mining.agents.temporal import extract_html_dates, utc_now_iso
from ml.web_data_mining.agents.rss_discovery import (
    USER_AGENT,
    RssItem,
    is_site_root_or_hub_url,
    pick_best_publisher_url,
    plain_http_urls_in_text,
    strip_url_trailing_junk,
)


BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"
)


def _looks_like_pdf_url(url: str) -> bool:
    u = (url or "").lower().strip()
    return u.endswith(".pdf") or ".pdf?" in u


def _is_pdf_content_type(content_type: str | None) -> bool:
    ct = (content_type or "").lower()
    return "application/pdf" in ct


def _looks_like_pdf_bytes(payload: bytes | None) -> bool:
    """
    Content sniffing fallback: some publishers mislabel PDFs as text/html or octet-stream,
    and some URLs don't end with .pdf due to redirects/CDNs.
    """
    if not payload:
        return False
    head = payload.lstrip()[:8]
    return head.startswith(b"%PDF-")


def _looks_like_binary_bytes(payload: bytes | None) -> bool:
    """
    Content sniffing for common non-HTML binary payloads that are sometimes mislabelled as text.
    If we accidentally decode these as text, downstream storage/chunking becomes illegible.
    """
    if not payload:
        return False
    head = payload.lstrip()[:32]
    if head.startswith(b"%PDF-"):
        return True
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return True
    if head.startswith(b"\xff\xd8\xff"):  # JPEG
        return True
    if head.startswith(b"GIF87a") or head.startswith(b"GIF89a"):
        return True
    if head.startswith(b"\x1f\x8b"):  # gzip
        return True
    if head.startswith(b"PK\x03\x04"):  # zip
        return True
    # WebP: RIFF....WEBP
    if len(head) >= 12 and head[0:4] == b"RIFF" and head[8:12] == b"WEBP":
        return True
    return False


def _extract_text_from_pdf_bytes(pdf_bytes: bytes) -> tuple[str, str]:
    """
    Return (title, text) extracted from a PDF payload.
    Raises ImportError if pypdf is unavailable.
    """
    from pypdf import PdfReader  # type: ignore[import-untyped]

    reader = PdfReader(io.BytesIO(pdf_bytes))
    title = ""
    try:
        meta = reader.metadata
        if meta and getattr(meta, "title", None):
            title = str(meta.title).strip()
    except Exception:
        pass

    pages: list[str] = []
    for page in reader.pages:
        try:
            text = (page.extract_text() or "").strip()
        except Exception:
            text = ""
        if text:
            pages.append(text)
    return title, "\n\n".join(pages).strip()


def is_google_news_rss_article_url(url: str) -> bool:
    u = (url or "").lower()
    return "news.google.com" in u and "rss/articles" in u


def _google_news_host(url: str) -> bool:
    try:
        return "news.google." in urlparse(url).netloc.lower()
    except Exception:
        return False


def _extract_publisher_url_from_google_html(page_html: str) -> str | None:
    """Try to find a non-Google article URL inside Google's wrapper / consent HTML."""
    patterns = (
        r'<link[^>]+rel=["\']canonical["\'][^>]*href=["\']([^"\']+)["\']',
        r'<link[^>]+href=["\']([^"\']+)["\'][^>]*rel=["\']canonical["\']',
        r'<meta[^>]+property=["\']og:url["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]*property=["\']og:url["\']',
    )
    for pat in patterns:
        m = re.search(pat, page_html, re.I | re.DOTALL)
        if m:
            u = html_module.unescape(m.group(1).strip())
            if u.startswith("http") and not _google_news_host(u) and not is_site_root_or_hub_url(u):
                return u
    for pat in (
        r'"canonicalUrl"\s*:\s*"(https:[^"]+)"',
        r'"articleUrl"\s*:\s*"(https:[^"]+)"',
        r'"url"\s*:\s*"(https://[^"]+\.(?:ng|com|net|org|co\.[a-z]{2})/[^"]+)"',
    ):
        for m in re.finditer(pat, page_html):
            u = html_module.unescape(m.group(1).strip())
            if "news.google." in u.lower():
                continue
            if not u.startswith("http"):
                continue
            if is_site_root_or_hub_url(u):
                continue
            if urlparse(u).path.count("/") < 2:
                continue
            return u
    return None


_GOOGLE_WRAPPER_NEWS_HOSTS: tuple[str, ...] = (
    "premiumtimesng.com",
    "guardian.ng",
    "dailytrust.com",
    "punchng.com",
    "vanguardngr.com",
    "businessday.ng",
    "nairametrics.com",
    "thisdaylive.com",
    "thecable.ng",
    "leadership.ng",
    "tribuneonlineng.com",
    "thenationonlineng.net",
    "dailypost.ng",
    "thewhistler.ng",
    "ripplesng.com",
    "graphic.com.gh",
    "citinewsroom.com",
    "myjoyonline.com",
    "ghanaweb.com",
    "farmersreviewafrica.com",
    "humanglemedia.com",
    "climatechangenews.com",
    "scidev.net",
    "reuters.com",
    "afp.com",
    "apnews.com",
    "bbc.com",
    "bbc.co.uk",
)


def _mine_known_hosts_in_google_html(page_html: str) -> list[str]:
    out: list[str] = []
    for host in _GOOGLE_WRAPPER_NEWS_HOSTS:
        pat = re.compile(rf"https?://(?:www\.|m\.|mobile\.)?{re.escape(host)}/[^\s\"'<>#\\]+", re.I)
        for m in pat.finditer(page_html):
            out.append(strip_url_trailing_junk(m.group(0)))
    return out


def _decode_google_amp_embeds(page_html: str) -> list[str]:
    out: list[str] = []
    for m in re.finditer(
        r"https://www\.google\.com/amp/s/(https?://[^\s\"'<>]+)",
        page_html,
        re.I,
    ):
        u = html_module.unescape(m.group(1).strip())
        u = strip_url_trailing_junk(u)
        if u.startswith("http") and not _google_news_host(u):
            out.append(u)
    return out


def extract_best_publisher_url_from_google_page_html(page_html: str) -> str | None:
    candidates: list[str] = []
    canon = _extract_publisher_url_from_google_html(page_html)
    if canon:
        candidates.append(canon)
    candidates.extend(_mine_known_hosts_in_google_html(page_html))
    candidates.extend(_decode_google_amp_embeds(page_html))
    return pick_best_publisher_url(candidates)


def resolve_google_news_fetch_url(url: str, timeout: float = 25.0) -> str:
    """
    Optional pre-resolve (browser GET) for Google News links. Not used by the default
    two-phase fetch; kept for tooling/tests.
    """
    if not is_google_news_rss_article_url(url):
        return url
    headers = {
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        final = (resp.url or "").strip()
        text = resp.text or ""
        if "consent.google.com" in final.lower():
            found = extract_best_publisher_url_from_google_page_html(text)
            if found:
                return found
            return url
        if not resp.ok:
            return url
        if not _google_news_host(final):
            if not is_site_root_or_hub_url(final):
                return final
        found = extract_best_publisher_url_from_google_page_html(text)
        if found:
            return found
    except Exception:
        pass
    return url


def is_google_consent_or_gate_page(title: str, body: str) -> bool:
    t = (title or "").strip().lower()
    b = (body or "").lower()
    if "before you continue" in t:
        return True
    if "error 400" in t or "bad request" in t:
        return True
    head = b[:1200]
    if "before you continue" in head and ("google" in head or "g.co/privacy" in head):
        return True
    if "we use cookies and data to" in head and "privacy policy" in head and "terms of service" in head:
        return True
    return False


def phase1_fetch_initial(url: str, timeout: float = 25.0) -> tuple[str, str]:
    """
    First fetch: original pipeline style — RSS / discovery User-Agent (not a full browser).
    Returns (body_text_or_html, final_url_after_redirects). Does not raise on consent 4xx.
    For PDF responses, body is extracted with pypdf when available.
    """
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    final = (resp.url or "").strip()
    content_type = (resp.headers.get("Content-Type") or "").lower()
    is_pdf = _is_pdf_content_type(content_type) or _looks_like_pdf_url(final) or _looks_like_pdf_bytes(resp.content)
    is_binary = _looks_like_binary_bytes(resp.content)

    text = ""
    if is_pdf:
        try:
            _, text = _extract_text_from_pdf_bytes(resp.content or b"")
        except ImportError:
            text = ""
        except Exception:
            text = ""
    elif is_binary:
        # Don't try to decode images/archives as text; treat as non-extractable content.
        text = ""
    else:
        text = resp.text or ""

    if "consent.google.com" in final.lower():
        return text, final
    if resp.ok:
        return text, final
    return text, final


def fetch_html_browser(url: str, timeout: float = 30.0) -> tuple[str, str]:
    """Second fetch: browser-like GET for direct article scraping or PDF text extraction."""
    headers = {
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    final = (resp.url or "").strip()
    content_type = (resp.headers.get("Content-Type") or "").lower()
    is_pdf = _is_pdf_content_type(content_type) or _looks_like_pdf_url(final) or _looks_like_pdf_bytes(resp.content)
    is_binary = _looks_like_binary_bytes(resp.content)
    if "consent.google.com" in final.lower():
        return (resp.text or ""), final
    if is_pdf:
        try:
            _, text = _extract_text_from_pdf_bytes(resp.content or b"")
        except ImportError:
            text = ""
        except Exception:
            text = ""
        return text, final
    if is_binary:
        return "", final
    resp.raise_for_status()
    return resp.text, final


def choose_phase2_article_url(phase1_html: str, phase1_final: str, item: RssItem) -> str | None:
    """
    Pick a direct publisher URL for the second fetch: RSS hints, plain URLs in text,
    phase-1 redirect target, and mining phase-1 HTML (Google wrapper / consent).
    """
    candidates: list[str] = []
    if phase1_final and "consent.google.com" not in phase1_final.lower():
        if not _google_news_host(phase1_final) and not is_site_root_or_hub_url(phase1_final):
            candidates.append(phase1_final)
    candidates.extend(list(item.extra_fetch_url_hints))
    candidates.extend(plain_http_urls_in_text(f"{item.title}\n{item.summary}"))
    mined = extract_best_publisher_url_from_google_page_html(phase1_html)
    if mined:
        candidates.append(mined)
    best = pick_best_publisher_url(candidates)
    if not best:
        return None
    if _google_news_host(best) or "consent.google.com" in best.lower():
        return None
    if is_site_root_or_hub_url(best):
        return None
    return best


def extract_article_text(html: str, document_url: str = "") -> tuple[str, str]:
    if html.strip():
        try:
            import trafilatura  # type: ignore[import-untyped]
            from trafilatura.metadata import extract_metadata  # type: ignore[import-untyped]

            extracted = trafilatura.extract(
                html,
                url=document_url or None,
                include_comments=False,
                include_tables=False,
                favor_precision=True,
            )
            if extracted and len(extracted.strip()) >= 240:
                title = ""
                try:
                    meta = extract_metadata(html, default_url=document_url or None)
                    if meta and meta.title:
                        title = meta.title.strip()
                except Exception:
                    pass
                if not title:
                    title = extract_og_title_regex(html) or extract_title_regex(html)
                return title, extracted.strip()
        except ImportError:
            pass
        except Exception:
            pass

    title = extract_og_title_regex(html) or extract_title_regex(html)
    lower = html.lower()
    start = lower.find("<article")
    if start != -1:
        end = lower.find("</article>", start)
        if end != -1:
            chunk = html[start : end + len("</article>")]
            text = html_to_plain_text(chunk)
            if len(text) > 200:
                return title, text
    main_start = lower.find("<main")
    if main_start != -1:
        end = lower.find("</main>", main_start)
        if end != -1:
            chunk = html[main_start : end + len("</main>")]
            text = html_to_plain_text(chunk)
            if len(text) > 200:
                return title, text
    text = html_to_plain_text(html)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return title, text


def fetch_and_extract(item: RssItem, polite_delay_s: float = 0.35) -> tuple[str, str, str, str, dict[str, str | None]]:
    """
    Two-phase article fetch:
      1) GET item.url with the original RSS/miner User-Agent (phase1_fetch_initial).
      2) If a direct article URL is known, GET it with a browser UA and scrape (trafilatura + fallbacks).

    Returns (page_title, plain_body, url_used_for_body, phase1_final_url, extraction_meta).
    """
    initial_url = (item.url or "").strip()
    time.sleep(polite_delay_s)

    html1, url1 = phase1_fetch_initial(initial_url)
    html_pub_1, html_upd_1, html_raw_1 = extract_html_dates(html1)
    article_url = choose_phase2_article_url(html1, url1, item)

    min_body = 200

    if article_url:
        time.sleep(polite_delay_s)
        try:
            html2, url2 = fetch_html_browser(article_url)
            if "consent.google.com" not in url2.lower():
                t2, body2 = extract_article_text(html2, document_url=url2)
                if len(body2.strip()) >= min_body:
                    html_pub_2, html_upd_2, html_raw_2 = extract_html_dates(html2)
                    return t2, body2, url2, url1, {
                        "html_published_at": html_pub_2 or html_pub_1,
                        "article_updated_at": html_upd_2 or html_upd_1,
                        "html_published_raw": html_raw_2.get("html_published_raw")
                        or html_raw_1.get("html_published_raw"),
                        "html_updated_raw": html_raw_2.get("html_updated_raw")
                        or html_raw_1.get("html_updated_raw"),
                        "extracted_at": utc_now_iso(),
                    }
        except Exception:
            pass

    t1, body1 = extract_article_text(html1, document_url=url1)
    return t1, body1, url1, url1, {
        "html_published_at": html_pub_1,
        "article_updated_at": html_upd_1,
        "html_published_raw": html_raw_1.get("html_published_raw"),
        "html_updated_raw": html_raw_1.get("html_updated_raw"),
        "extracted_at": utc_now_iso(),
    }
