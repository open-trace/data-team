"""Public API: lookup, normalize_query, expand, codes, hierarchy. Pure offline."""
from __future__ import annotations
from dataclasses import dataclass, field
from functools import lru_cache
from importlib import import_module
import re
from ._text import fold, TOKEN_RE, STOPWORDS
from . import _data, _matcher

CORE_TYPES = {"crop", "animal", "product", "by_product", "input", "disease_pest", "commodity",
              "crop_group", "animal_group", "variety", "breed", "service", "season", "unit",
              "value_chain_stage", "indicator_class", "measure"}
SOURCE_RANK = {
    "opentrace_curated": 0,
    "opentrace_repo": 1,
    "faostat": 2,
    "wikidata": 3,
    "agrovoc": 4,
    "generated": 5,
    "hs": 6,
}
MAX_NGRAM = 6

def _dist_pure(a: str, b: str, cutoff: int) -> int:
    if abs(len(a) - len(b)) > cutoff:
        return cutoff + 1
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        if min(cur) > cutoff:
            return cutoff + 1
        prev = cur
    return prev[-1]


try:  # optional accelerator (not a required dep)
    _lev = import_module("rapidfuzz.distance").Levenshtein

    def _dist_rapid(a: str, b: str, cutoff: int) -> int:
        return _lev.distance(a, b, score_cutoff=cutoff)

    _dist = _dist_rapid
except Exception:
    _dist = _dist_pure

@dataclass
class Match:
    ontology_id: str
    canonical_name: str
    entity_type: str
    score: float
    span: tuple[int, int]
    text: str
    matched_alias: str
    alias_type: str
    language: str
    fuzzy: bool = False
    ambiguous: bool = False
    candidates: list[str] = field(default_factory=list)

def _rank(cand, lang, country):
    eid, l, c, typ, w = cand
    oid, et, src, taxon = _data.light_entities()[eid]
    s = w
    if lang and l == lang: s += 0.1
    if country and c == country: s += 0.1
    if et in CORE_TYPES and not taxon: s += 0.05
    s -= 0.01 * SOURCE_RANK.get(src, 4)
    return s

def _allowed(key, cand, lang):
    eid, l, c, typ, w = cand
    if key in STOPWORDS and not (lang and l == lang and l != "eng"): return False
    if len(key) <= 3 and l not in ("eng", "lat") and lang != l: return False   # e.g. Igbo 'ji', Yoruba 'ata'
    if len(key) <= 2 and typ != "abbrev" and not (lang and l == lang): return False
    return True

def _tokens(text):
    return [(m.group(0), m.start(), m.end()) for m in TOKEN_RE.finditer(text)]

def _mk(cands, lang, country, s, e, text, key, fuzzy, score=None, atype=None, max_candidates=5):
    LE = _data.light_entities()
    cands = sorted(cands, key=lambda c: -_rank(c, lang, country))
    top = cands[0]; r0 = _rank(top, lang, country)
    ids = list(dict.fromkeys(LE[c[0]][0] for c in cands))
    near = {LE[c[0]][0] for c in cands if r0 - _rank(c, lang, country) < 0.02}
    ent = _data.entity_by_eid(top[0])
    if ent is None:
        raise LookupError(f"missing ontology entity eid={top[0]}")
    return Match(ent["ontology_id"], ent["canonical_name_en"], ent["entity_type"],
                 round(min(1.0, top[4]) if score is None else score, 3), (s, e), text[s:e], key,
                 atype or top[3], top[1], fuzzy or top[3] == "misspelling", len(near) > 1, ids[:max_candidates])

def lookup(text: str, lang: str | None = None, country: str | None = None, *,
           fuzzy: bool = True, types: set[str] | None = None, max_candidates: int = 5) -> list[Match]:
    """Find ontology entities mentioned in text: one Aho-Corasick pass over normalised tokens, greedy
    leftmost-longest selection, then light fuzzy matching on unmatched tokens only.
    lang = ISO 639-3 ('hau','yor','fra'...), country = ISO3; both re-rank and unlock short local aliases."""
    LE = _data.light_entities()
    toks = _tokens(text); ftoks = [fold(t) for t, _, _ in toks]
    def ok(key):
        cs = [c for c in _data.candidates(key) if _allowed(key, c, lang)]
        if types: cs = [c for c in cs if LE[c[0]][1] in types]
        return cs
    found = sorted(_matcher.spans(ftoks), key=lambda x: (x[0], -x[1]))
    used = [False] * len(toks); out: list[Match] = []; nxt = 0
    for st, n in found:
        if st < nxt or n > MAX_NGRAM: continue
        key = " ".join(ftoks[st:st + n]); cs = ok(key)
        if not cs: continue
        s, e = toks[st][1], toks[st + n - 1][2]
        out.append(_mk(cs, lang, country, s, e, text, key, False, max_candidates=max_candidates))
        for k in range(st, st + n): used[k] = True
        nxt = st + n
    if fuzzy and not (lang and lang not in ("eng", "fra", "por")):
        vocab, known = _data.fuzzy_vocab()
        for k, (tok, s, e) in enumerate(toks):
            f = ftoks[k]
            if used[k] or len(f) < 5 or not f.isalpha() or f in STOPWORDS or f in known: continue
            m = _fuzzy_one(f)
            if not m: continue
            key, d = m; cs = list(_data.candidates(key))
            if types: cs = [c for c in cs if LE[c[0]][1] in types]
            if cs: out.append(_mk(cs, lang, country, s, e, text, key, True, 0.9 - 0.1 * d, "fuzzy", max_candidates))
    return sorted(out, key=lambda m: m.span)

@lru_cache(maxsize=20000)
def _fuzzy_one(w: str):
    vocab, _ = _data.fuzzy_vocab()
    cutoff = 1 if len(w) < 8 else 2
    best = None
    for v in _bucket(len(w)):
        if v[0] != w[0] and v[-1] != w[-1]: continue
        d = _dist(w, v, cutoff)
        if d <= cutoff and (best is None or d < best[1]): best = (v, d)
        if best and best[1] == 1: break
    return best

@lru_cache(maxsize=64)
def _bucket(n):
    vocab, _ = _data.fuzzy_vocab()
    return tuple(v for v in vocab if abs(len(v) - n) <= 2)

def normalize_query(text: str, lang: str | None = None, country: str | None = None, *,
                    canonicalize: str = "typos") -> str:
    """Rewrite a query for downstream parsers.
    canonicalize='typos' : fix misspellings/fuzzy hits only (casssava -> cassava)
    canonicalize='all'   : also replace local/foreign names of core entities with English canonical (rogo -> cassava)."""
    ms = lookup(text, lang, country)
    out, last = [], 0
    for m in ms:
        ent = _data.entity(m.ontology_id)
        if ent is None or ent["entity_type"] not in CORE_TYPES:
            continue
        repl = None
        if m.fuzzy or m.alias_type in ("misspelling", "fuzzy"):
            repl = ent["canonical_name_en"]
        elif canonicalize == "all" and m.alias_type in ("local", "synonym", "plural") and m.language not in ("lat",):
            repl = ent["canonical_name_en"]
        if repl and fold(repl) != fold(m.text):
            out.append(text[last:m.span[0]]); out.append(repl); last = m.span[1]
    out.append(text[last:])
    return "".join(out)

def get(ontology_id: str) -> dict | None:
    e = _data.entity(ontology_id)
    if e is None: return None
    e = dict(e); e.pop("eid", None); return e

def _eid(oid):
    e = _data.entity(oid); return e["eid"] if e else None

def _ids(rows):
    LE = _data.light_entities(); return [LE[r[0]][0] for r in rows]

@lru_cache(maxsize=8192)
def _aliases(ontology_id):
    return tuple(_data.q("SELECT COALESCE(alias,alias_norm),lang,atype FROM aliases WHERE eid=? ORDER BY rid", (_eid(ontology_id),)))

def aliases(ontology_id: str, languages: set[str] | None = None, include_misspellings: bool = False) -> list[str]:
    return list(dict.fromkeys(a for a, l, t in _aliases(ontology_id)
                              if (include_misspellings or t != "misspelling") and (not languages or l in languages)))

def children(ontology_id: str) -> list[str]:
    return sorted(_ids(_data.q("SELECT src FROM relations WHERE dst=? AND rel='broader'", (_eid(ontology_id),))))

def ancestors(ontology_id: str, rel: str = "broader", max_depth: int | None = None) -> list[str]:
    """Transitive ancestors from the precomputed closure, nearest first."""
    rows = _data.q("SELECT ancestor FROM closure WHERE descendant=? AND rel=? AND depth<=? ORDER BY depth, ancestor",
                   (_eid(ontology_id), rel, max_depth or 999))
    return _ids(rows)

def descendants(ontology_id: str, rel: str = "broader", max_depth: int | None = None) -> list[str]:
    rows = _data.q("SELECT descendant FROM closure WHERE ancestor=? AND rel=? AND depth<=? ORDER BY depth, descendant",
                   (_eid(ontology_id), rel, max_depth or 999))
    return _ids(rows)

def derived_products(ontology_id: str, recursive: bool = True) -> list[str]:
    e = _eid(ontology_id)
    if e is None: return []
    if recursive:
        out = _ids(_data.q("SELECT descendant FROM closure WHERE ancestor=? AND rel='derived_from' ORDER BY depth, descendant", (e,)))
    else:
        out = _ids(_data.q("SELECT src FROM relations WHERE dst=? AND rel='derived_from'", (e,)))
    out += _ids(_data.q("SELECT src FROM relations WHERE dst=? AND rel='product_of' UNION SELECT dst FROM relations WHERE src=? AND rel='has_product'", (e, e)))
    return [x for x in dict.fromkeys(out) if x != ontology_id]

def expand(ontology_id: str, languages: set[str] | None = None, *, include_children: bool = True,
           include_derived: bool = True, limit: int = 120) -> list[str]:
    """Terms for Qdrant/BM25 query expansion: aliases of the entity, its children and derived products."""
    ids = [ontology_id] + (children(ontology_id) if include_children else []) + \
          (derived_products(ontology_id) if include_derived else [])
    terms: list[str] = [
        ent["canonical_name_en"] for i in ids if (ent := get(i)) is not None
    ]
    terms += aliases(ontology_id, languages)
    for i in ids[1:]:
        terms += aliases(i, languages)
    seen, out = set(), []
    for t in terms:
        k = fold(t)
        if k not in seen: seen.add(k); out.append(t)
    return out[:limit]

@lru_cache(maxsize=8192)
def _codes(eid):
    d: dict = {}
    for s, c in _data.q("SELECT scheme, code FROM codes WHERE eid=?", (eid,)): d.setdefault(s, []).append(c)
    return d

def codes(ontology_id: str) -> dict[str, list[str]]:
    e = _eid(ontology_id)
    if e is None: return {}
    c = {k: list(v) for k, v in _codes(e).items()}
    for (o,) in _data.q("SELECT dst FROM relations WHERE src=? AND rel='same_commodity_as' UNION SELECT src FROM relations WHERE dst=? AND rel='same_commodity_as'", (e, e)):
        for k, v in _codes(o).items():
            c.setdefault(k, []); c[k] += [x for x in v if x not in c[k]]
    return c

def to_faostat(ontology_id: str) -> list[str]:
    return codes(ontology_id).get("faostat_item", [])

def to_hs(ontology_id: str) -> list[str]:
    return codes(ontology_id).get("hs", [])

def warehouse(ontology_id: str) -> list[dict]:
    rows = _data.q("SELECT bq_table,col,value,match_type FROM warehouse_map WHERE eid=?", (_eid(ontology_id),))
    return [dict(zip(("bq_table", "column", "value", "match_type"), r)) for r in rows]

def search(text: str, limit: int = 20) -> list[tuple[str, str]]:
    """FTS5 prefix/term search over real aliases (for autocomplete / admin tools)."""
    toks = [t for t in TOKEN_RE.findall(fold(text)) if t.replace("'", "").replace("-", "").isalnum()]
    if not toks: return []
    qs = " ".join('"%s"*' % t.replace('"', "") for t in toks)
    rows = _data.q("SELECT a.eid, COALESCE(a.alias,a.alias_norm) FROM aliases_fts f JOIN aliases a ON a.rid=f.rowid "
                   "WHERE aliases_fts MATCH ? ORDER BY rank LIMIT ?", (qs, limit))
    LE = _data.light_entities()
    return [(LE[e][0], a) for e, a in rows]
