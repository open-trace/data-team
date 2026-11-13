"""v3 runtime: resolve text -> ontology entities with BigQuery targets + Qdrant terms, and route each entity.

    >>> from opentrace_ontology import resolve, route
    >>> ents = resolve("What is the current state of casssava in Nigeria")
    >>> [e.entity_id for e in ents]                    # ['OT:CROP:cassava', 'OT:GEO:NGA']
    >>> route(ents)["source_basis"]                    # 'structured'

Pure offline: reads the bundled ontology.sqlite (tables value_map, geo, aux_aliases, mart_values). No network.
Modes:
  bq+qdrant    entity has value_map rows -> filter BigQuery on the exact stored strings AND search Qdrant
  qdrant_only  ontology knows it but no mart stores it -> skip BQ filter, search Qdrant with multilingual terms/ids
  web_fallback not in the ontology at all -> Tavily/Wikipedia (only after Qdrant also comes back empty)
"""
from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from functools import lru_cache
from ._text import fold, TOKEN_RE, STOPWORDS
from . import _data
from .core import lookup, expand, get, ancestors, descendants, CORE_TYPES

MAX_N = 6
MODE_BQ, MODE_QD, MODE_WEB = "bq+qdrant", "qdrant_only", "web_fallback"
# Entity types that are routing-relevant subjects. Measures/units/indicator classes describe *what* is asked,
# they still carry bq_targets (e.g. fct_production.element='Yield') but do not decide source_basis on their own.
SUBJECT_TYPES = CORE_TYPES - {"measure", "unit", "indicator_class", "value_chain_stage", "season"} | {"country"}

@dataclass
class ResolvedEntity:
    entity_id: str
    name: str
    entity_type: str
    span: tuple[int, int]
    text: str
    via: str                                   # 'ontology' | 'country' | 'mart_value'
    fuzzy: bool = False
    bq_targets: dict[str, list[str]] = field(default_factory=dict)   # "table.column" -> exact stored strings
    bq_targets_narrower: dict[str, list[str]] = field(default_factory=dict)
    qdrant_terms: list[str] = field(default_factory=list)
    payload_ids: list[str] = field(default_factory=list)              # for MatchAny on payload 'ontology_ids'
    is_africa: bool | None = None
    def to_dict(self): return asdict(self)

# ---------- cached tables (loaded once; a few hundred KB)
@lru_cache(maxsize=1)
def _value_map() -> dict[str, dict[str, list[str]]]:
    out: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for oid, t, c, v in _data.q('SELECT entity_id,"table","column",stored_value FROM value_map ORDER BY rowid'):
        out[oid][f"{t}.{c}"].append(v)
    return {k: {c: list(dict.fromkeys(vs)) for c, vs in d.items()} for k, d in out.items()}

@lru_cache(maxsize=1)
def _aux() -> dict[str, list[tuple[str, str]]]:
    d: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for a, oid, kind in _data.q("SELECT alias_norm,entity_id,kind FROM aux_aliases WHERE kind!='mart_structural'"):
        d[a].append((oid, kind))
    return dict(d)

@lru_cache(maxsize=1)
def _geo() -> dict[str, tuple[str, str, str, int]]:
    return {r[0]: r[1:] for r in _data.q("SELECT entity_id,iso2,iso3,name,is_africa FROM geo")}

@lru_cache(maxsize=1)
def _minted() -> dict[str, tuple[str, str]]:
    return {r[0]: (r[1], r[2]) for r in _data.q("SELECT entity_id,kind,label FROM mart_values")}

def has_value_map() -> bool:
    try: return bool(_data.q("SELECT 1 FROM value_map LIMIT 1"))
    except Exception: return False

# ---------- helpers
def bq_targets(entity_id: str) -> dict[str, list[str]]:
    """Exact stored strings per 'table.column' for an entity (country forms differ per column: NG / NGA / Nigeria)."""
    return {k: list(v) for k, v in _value_map().get(entity_id, {}).items()}

@lru_cache(maxsize=8192)
def _narrower_targets(entity_id: str) -> tuple:
    vm = _value_map(); out: dict[str, list[str]] = defaultdict(list)
    for d in descendants(entity_id, max_depth=2)[:200]:
        for col, vals in vm.get(d, {}).items(): out[col].extend(vals)
    return tuple((k, tuple(dict.fromkeys(v))) for k, v in sorted(out.items()))

@lru_cache(maxsize=8192)
def _terms(entity_id: str, max_terms: int) -> tuple:
    if entity_id.startswith("OT:GEO:"):
        iso2, iso3, name, _ = _geo()[entity_id]
        forms = [name, iso3, iso2] + [a for a, l in _aux_by_entity().get(entity_id, []) if len(a) > 3]
        seen, out = set(), []
        for f in forms:
            if f and fold(f) not in seen: seen.add(fold(f)); out.append(f)
        return tuple(out)[:max_terms]
    if entity_id.startswith("OT:MV:"):
        return (_minted()[entity_id][1],)
    terms = expand(entity_id, limit=max_terms)                       # aliases (all languages) + children + by-products
    for a in ancestors(entity_id, max_depth=1)[:3]:                  # broader, e.g. cassava -> root crops
        e = get(a)
        if e and e["entity_type"] in CORE_TYPES: terms.append(e["canonical_name_en"])
    return tuple(dict.fromkeys(terms))[:max_terms]

@lru_cache(maxsize=1)
def _aux_by_entity() -> dict[str, list[tuple[str, str]]]:
    d: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for a, lst in _aux().items():
        for oid, kind in lst: d[oid].append((a, kind))
    return dict(d)

def _tokens(text):
    return [(m.group(0), m.start(), m.end()) for m in TOKEN_RE.finditer(text)]

def _aux_spans(text: str, used: list[tuple[int, int]]):
    """Leftmost-longest n-gram match against country forms + warehouse labels. Short codes (NG, NGA)
    only match when written in upper case, so 'in', 'per', 'can' never become India/Peru/Canada."""
    aux = _aux(); toks = _tokens(text); f = [fold(t) for t, _, _ in toks]; i = 0; out = []
    while i < len(toks):
        hit = None
        for n in range(min(MAX_N, len(toks) - i), 0, -1):
            k = " ".join(f[i:i + n])
            if k in aux:
                s, e = toks[i][1], toks[i + n - 1][2]; raw = text[s:e]
                if n == 1 and (k in STOPWORDS or len(k) <= 3) and not (raw.isupper() and len(raw) in (2, 3)): continue
                if any(not (e <= a or s >= b) for a, b in used): continue
                hit = (s, e, raw, aux[k]); break
        if hit: out.append(hit); i += max(1, len(TOKEN_RE.findall(hit[2])))
        else: i += 1
    return out

# ---------- public API
def resolve(text: str, lang: str | None = None, country: str | None = None, *, max_terms: int = 60,
            include_measures: bool = True) -> list[ResolvedEntity]:
    """Entities in text with per-entity bq_targets (exact stored strings) and qdrant_terms (multilingual)."""
    out: list[ResolvedEntity] = []; seen: set[str] = set(); used = []
    for m in lookup(text, lang, country):
        if not include_measures and m.entity_type not in SUBJECT_TYPES: continue
        if m.ontology_id in seen: continue
        seen.add(m.ontology_id); used.append(m.span)
        out.append(ResolvedEntity(m.ontology_id, m.canonical_name, m.entity_type, m.span, m.text, "ontology", m.fuzzy))
    for s, e, raw, cands in _aux_spans(text, used):
        geo = [c for c in cands if c[0].startswith("OT:GEO:")]
        oid, kind = (geo or cands)[0]
        if oid in seen: continue
        seen.add(oid)
        if oid.startswith("OT:GEO:"):
            g = _geo()[oid]; out.append(ResolvedEntity(oid, g[2], "country", (s, e), raw, "country", is_africa=bool(g[3])))
        elif oid.startswith("OT:MV:"):
            if not include_measures: continue
            k, label = _minted()[oid]; out.append(ResolvedEntity(oid, label, "mart_" + k, (s, e), raw, "mart_value"))
        else:
            ent = get(oid)
            if ent: out.append(ResolvedEntity(oid, ent["canonical_name_en"], ent["entity_type"], (s, e), raw, "mart_value"))
    for r in out:
        r.bq_targets = bq_targets(r.entity_id)
        if not r.entity_id.startswith(("OT:GEO:", "OT:MV:")):
            r.bq_targets_narrower = {k: list(v) for k, v in _narrower_targets(r.entity_id)}
            r.payload_ids = [r.entity_id] + descendants(r.entity_id, max_depth=2)[:49]
        else:
            r.payload_ids = [r.entity_id]
        r.qdrant_terms = list(_terms(r.entity_id, max_terms))
    return sorted(out, key=lambda r: r.span)

def unresolved_terms(text: str, entities: list[ResolvedEntity]) -> list[str]:
    """Content words not covered by any resolved entity (candidates for web_fallback)."""
    spans = [e.span for e in entities]; out = []
    for t, s, e in _tokens(text):
        f = fold(t)
        if len(f) < 4 or not f.isalpha() or f in STOPWORDS: continue
        if any(a <= s and e <= b for a, b in spans): continue
        out.append(t)
    return out

ACF_CEILING = {"structured": None, "documents_only": "moderate", "web_only": "limited", "none": "low"}

def route(entities: list[ResolvedEntity], unresolved: list[str] | None = None) -> dict:
    """Per-entity mode + overall source_basis for ACF.
    bq+qdrant : value_map rows (own or narrower) -> BQ filter on stored strings + Qdrant
    qdrant_only : ontology-known, no mart stores it -> no BQ filter, Qdrant with qdrant_terms/payload_ids
    web_fallback : unresolved terms (not in ontology)
    source_basis: 'structured' if any subject entity has BQ rows; 'documents_only' if subjects exist but none in BQ;
    'web_only' if only unknown terms; 'none' if nothing at all. Countries alone never make it 'structured'."""
    plan = []
    for e in entities:
        has_bq = bool(e.bq_targets or e.bq_targets_narrower)
        plan.append({"entity_id": e.entity_id, "name": e.name, "entity_type": e.entity_type,
                     "mode": MODE_BQ if has_bq else MODE_QD,
                     "bq_targets": e.bq_targets or e.bq_targets_narrower,
                     "bq_targets_from": "self" if e.bq_targets else ("narrower" if has_bq else None),
                     "qdrant_terms": e.qdrant_terms, "payload_ids": e.payload_ids})
    for t in unresolved or []:
        plan.append({"entity_id": None, "name": t, "entity_type": "unknown", "mode": MODE_WEB,
                     "bq_targets": {}, "bq_targets_from": None, "qdrant_terms": [t], "payload_ids": []})
    subj = [p for p in plan if p["entity_type"] in SUBJECT_TYPES and p["entity_type"] != "country"]
    if any(p["mode"] == MODE_BQ for p in subj): basis = "structured"
    elif subj: basis = "documents_only"
    elif any(p["mode"] == MODE_WEB for p in plan): basis = "web_only"
    elif any(p["mode"] == MODE_BQ for p in plan): basis = "structured"   # e.g. "yield in Kenya": measure+country in BQ
    else: basis = "none"
    run_bq = any(p["mode"] == MODE_BQ for p in subj) or (not subj and any(p["mode"] == MODE_BQ for p in plan))
    return {"entities": plan, "source_basis": basis, "acf_ceiling": ACF_CEILING[basis],
            "run_bigquery": run_bq, "run_qdrant": bool(plan), "web_fallback_terms": [p["name"] for p in plan if p["mode"] == MODE_WEB],
            "countries": [p["entity_id"][7:] for p in plan if p["entity_type"] == "country"],
            "african_countries": [p["entity_id"][7:] for p in plan if p["entity_type"] == "country" and _geo()[p["entity_id"]][3]]}

def plan_query(text: str, lang: str | None = None, country: str | None = None) -> dict:
    """resolve + unresolved_terms + route in one call (what the RAG graph should call once per question)."""
    ents = resolve(text, lang, country)
    return route(ents, unresolved_terms(text, ents))

def tag_chunk(text: str, lang: str | None = None, *, max_ids: int = 64) -> dict:
    """Ingestion helper: payload fields for a Qdrant chunk -> {'ontology_ids': [...], 'ontology_countries': [...]}.
    Includes broader ancestors (depth<=2) so a filter on 'root crops' also finds cassava chunks."""
    ids: list[str] = []; geos: list[str] = []
    for e in resolve(text, lang, max_terms=1):
        if e.entity_type == "country": geos.append(e.entity_id[7:]); continue
        if e.via == "mart_value" and e.entity_id.startswith("OT:MV:"): continue
        if e.entity_type not in SUBJECT_TYPES: continue
        ids.append(e.entity_id); ids += [a for a in ancestors(e.entity_id, max_depth=2)]
    return {"ontology_ids": list(dict.fromkeys(ids))[:max_ids], "ontology_countries": list(dict.fromkeys(geos))}

def clean_profile_country(value: str | None, *, africa_only: bool = True) -> str | None:
    """API-boundary cleaner for profile/SSO country: any form (NG, NGA, 'Nigeria', 'Nigéria') -> ISO3, or None
    if unknown or (africa_only) outside Africa."""
    if not value: return None
    v = value.strip()
    k = " ".join(fold(t) for t in TOKEN_RE.findall(v.replace("\u2019", "'")))
    for oid, kind in _aux().get(k, []):
        if oid.startswith("OT:GEO:"):
            g = _geo()[oid]
            return g[1] if (g[3] or not africa_only) else None
    for oid, (iso2, iso3, name, af) in _geo().items():
        if v.upper() in (iso2, iso3): return iso3 if (af or not africa_only) else None
    return None
