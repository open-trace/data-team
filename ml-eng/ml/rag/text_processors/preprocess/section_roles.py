"""
Classify research section headings/paths and decide indexability for RAG.

Env:
  RAG_EXCLUDE_BOILERPLATE_CHUNKS=on|off  (default on) — drop excluded roles at preprocess
  RAG_RESEARCH_EXCLUDE_ROLES=acknowledgements,references,boilerplate,appendix
"""
from __future__ import annotations

import os
import re
from typing import Literal

SectionRole = Literal[
    "abstract",
    "introduction",
    "methods",
    "results",
    "discussion",
    "conclusion",
    "acknowledgements",
    "references",
    "appendix",
    "boilerplate",
    "table",
    "content",
]

ContentType = Literal["prose", "table"]

_DEFAULT_EXCLUDED_ROLES = "acknowledgements,references,boilerplate,appendix"

# EN + FR heading/path slugs (after _slug_heading normalization).
_ROLE_PATTERNS: list[tuple[SectionRole, re.Pattern[str]]] = [
    ("abstract", re.compile(r"(^|/)(abstract|resume|resumé|summary)(/|$)", re.I)),
    (
        "introduction",
        re.compile(r"(^|/)(introduction|intro|background|contexte|context)(/|$)", re.I),
    ),
    (
        "methods",
        re.compile(
            r"(^|/)(methods|methodology|materials_and_methods|materials|methods_and_materials|"
            r"methodes|materiel_et_methodes|méthodes)(/|$)",
            re.I,
        ),
    ),
    (
        "results",
        re.compile(r"(^|/)(results|findings|resultats|résultats)(/|$)", re.I),
    ),
    (
        "discussion",
        re.compile(r"(^|/)(discussion|interpretation)(/|$)", re.I),
    ),
    (
        "conclusion",
        re.compile(r"(^|/)(conclusion|conclusions|concluding_remarks)(/|$)", re.I),
    ),
    (
        "acknowledgements",
        re.compile(
            r"(^|/)(acknowledgements|acknowledgments|acknowledgement|remerciements|"
            r"funding|financial_support|grant_support)(/|$)",
            re.I,
        ),
    ),
    (
        "references",
        re.compile(
            r"(^|/)(references|bibliography|bibliographie|literature_cited|"
            r"works_cited|cited_references)(/|$)",
            re.I,
        ),
    ),
    (
        "appendix",
        re.compile(r"(^|/)(appendix|appendices|supplementary|supplemental|annexe|annexes)(/|$)", re.I),
    ),
]

_BOILERPLATE_RE = re.compile(
    r"(journal_homepage|elsevier|springer|wiley|doi_org|https?_|www_|"
    r"available_online|copyright|all_rights_reserved|open_access|"
    r"creative_commons|issn|isbn|article_history|received_date|"
    r"land_use_policy_\d|sciencedirect|sci_hub)",
    re.I,
)

_TITLE_ROLE_HINTS: list[tuple[SectionRole, re.Pattern[str]]] = [
    ("acknowledgements", re.compile(r"^acknowledg", re.I)),
    ("acknowledgements", re.compile(r"^remerciements", re.I)),
    ("references", re.compile(r"^references?$", re.I)),
    ("references", re.compile(r"^bibliograph", re.I)),
    ("abstract", re.compile(r"^abstract$", re.I)),
    ("appendix", re.compile(r"^append", re.I)),
]


def exclude_boilerplate_enabled() -> bool:
    raw = os.environ.get("RAG_EXCLUDE_BOILERPLATE_CHUNKS", "on").strip().lower()
    return raw not in ("0", "false", "off", "no")


def research_excluded_roles() -> frozenset[str]:
    raw = os.environ.get("RAG_RESEARCH_EXCLUDE_ROLES", _DEFAULT_EXCLUDED_ROLES).strip()
    if not raw or raw.lower() in ("0", "false", "off", "no"):
        return frozenset()
    return frozenset(part.strip().lower() for part in raw.split(",") if part.strip())


def should_exclude_section_role(role: str) -> bool:
    if not exclude_boilerplate_enabled():
        return False
    return (role or "").strip().lower() in research_excluded_roles()


def classify_section(
    section_title: str,
    hierarchy_path: str,
    *,
    content_type: ContentType = "prose",
) -> SectionRole:
    if content_type == "table":
        return "table"

    hp = (hierarchy_path or "").lower()
    title = (section_title or "").strip()

    if _BOILERPLATE_RE.search(hp):
        return "boilerplate"

    for role, pat in _TITLE_ROLE_HINTS:
        if title and pat.search(title):
            return role

    for role, pat in _ROLE_PATTERNS:
        if pat.search(hp):
            return role

    if hp == "body" or not hp:
        return "content"
    return "content"


def semantic_lane_for_section(
    section_role: SectionRole,
    *,
    content_type: ContentType = "prose",
) -> str:
    if content_type == "table" or section_role == "table":
        return "table"
    if section_role == "abstract":
        return "abstract"
    return "content"
