"""Offline, read-only data access. SQLite opened with mode=ro&immutable=1 (no locks, no writes, no network).
Thread-safe: one connection per thread; process-safe: connections are re-opened after fork (pid check)."""
from __future__ import annotations
import gzip, json, os, sqlite3, threading
from functools import lru_cache

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "ontology.sqlite")
_local = threading.local()

def conn() -> sqlite3.Connection:
    c = getattr(_local, "c", None)
    if c is None or getattr(_local, "pid", None) != os.getpid():
        uri = "file:" + DB_PATH.replace("?", "%3f").replace("#", "%23") + "?mode=ro&immutable=1"
        c = sqlite3.connect(uri, uri=True, check_same_thread=False)
        c.execute("PRAGMA query_only=1")
        _local.c, _local.pid = c, os.getpid()
    return c

def q(sql: str, args=()):
    return conn().execute(sql, args).fetchall()

ENT_COLS = ("eid", "ontology_id", "entity_type", "canonical_name_en", "scientific_name", "parent_id", "attributes", "primary_source")

def _ent(row):
    d = dict(zip(ENT_COLS, row)); d["attributes"] = json.loads(d["attributes"] or "{}"); return d

@lru_cache(maxsize=65536)
def entity(oid: str) -> dict | None:
    r = q("SELECT eid,id,type,name,sci,parent,attrs,src FROM entities WHERE id=?", (oid,))
    return _ent(r[0]) if r else None

@lru_cache(maxsize=65536)
def entity_by_eid(eid: int) -> dict | None:
    r = q("SELECT eid,id,type,name,sci,parent,attrs,src FROM entities WHERE eid=?", (eid,))
    return _ent(r[0]) if r else None

@lru_cache(maxsize=1)
def light_entities() -> dict[int, tuple]:
    """eid -> (ontology_id, type, src, is_taxon) for ranking; ~33k small tuples."""
    return {e: (i, t, s, '"taxon": true' in a) for e, i, t, s, a in q("SELECT eid,id,type,src,attrs FROM entities")}

@lru_cache(maxsize=200000)
def candidates(key: str) -> tuple:
    return tuple(q("SELECT eid,lang,country,atype,weight FROM aliases WHERE alias_norm=?", (key,)))

@lru_cache(maxsize=1)
def matcher_keys() -> list[str]:
    with gzip.open(os.path.join(DATA_DIR, "matcher_keys.txt.gz"), "rt", encoding="utf-8") as f:
        return f.read().split("\n")

@lru_cache(maxsize=1)
def fuzzy_vocab() -> tuple[tuple[str, ...], frozenset]:
    return tuple(r[0] for r in q("SELECT w FROM fuzzy_vocab")), frozenset(r[0] for r in q("SELECT w FROM known_words"))

def meta() -> dict:
    return dict(q("SELECT k,v FROM meta"))
