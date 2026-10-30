# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Conservative public URL normalization shared by discovery and testing."""

import html
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def reference_url(value):
    """Validate an extracted public link without normalizing its URL identity.

    Remove document punctuation only. Preserve query order, tracking parameters,
    case, encoding, fragments and trailing slashes for exact-string comparison.
    """
    text = str(value).strip().rstrip(".,;:'")
    while text.endswith(")") and text.count(")") > text.count("("):
        text = text[:-1]
    return text if clean_url(text) else None


def clean_url(value):
    text = html.unescape(str(value)).strip().rstrip(".,;:'")
    while text.endswith(")") and text.count(")") > text.count("("):
        text = text[:-1]
    try:
        parts = urlsplit(text)
        if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
            return None
        if parts.username or parts.password:
            return None
        query = parse_qsl(parts.query, keep_blank_values=True)
        sensitive = {"token", "access_token", "api_key", "apikey", "signature", "x-amz-signature"}
        if any(key.lower() in sensitive for key, _ in query):
            return None
        query = [(key, val) for key, val in query if not key.lower().startswith("utm_")]
        return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path or "/", urlencode(query), parts.fragment))
    except ValueError:
        return None
