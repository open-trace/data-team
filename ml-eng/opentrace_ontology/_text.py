"""Text normalisation shared by the build and the runtime (must stay identical)."""
from __future__ import annotations
import re, unicodedata

def norm(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "").lower().strip()
    s = re.sub(r"[\u2019`´]", "'", s)
    return re.sub(r"\s+", " ", s)

def fold(s: str) -> str:
    """Lowercase + strip accents (latin). Non-latin scripts pass through unchanged."""
    return "".join(c for c in unicodedata.normalize("NFKD", norm(s)) if not unicodedata.combining(c))

TOKEN_RE = re.compile(r"[\w'\-]+|[^\w\s]", re.UNICODE)

STOPWORDS = frozenset("""a an the and or of in on at to for from by with without about as is are was were be been being
do does did done have has had what which who whom whose when where why how much many more most less least
this that these those there here it its it's they them their we our you your i me my he she his her
not no yes can could should would will shall may might must current currently now today state status
trend trends data show give tell me please latest recent price prices cost production produce produced
yield yields area total per year years month months week day country countries region africa african
compare between versus vs than top best rate level levels value values share number amount growth
also all any some each other new old high low large small major main use used using get like
""".split())

def key_of(s: str) -> str:
    """Canonical matcher key: folded tokens joined by single spaces (same tokenizer as runtime)."""
    return " ".join(fold(t) for t in TOKEN_RE.findall(s or ""))
