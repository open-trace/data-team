import os, socket, sqlite3, threading, multiprocessing as mp
import pytest
import opentrace_ontology as oo
from opentrace_ontology import _data, _matcher

@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def guard(*a, **k): raise AssertionError("network access attempted")
    monkeypatch.setattr(socket, "socket", guard)
    monkeypatch.setattr(socket, "create_connection", guard)

@pytest.mark.parametrize("q,exp", [("price of palm kernel cake in Ghana", "OT:PRODUCT:palm_kernel_cake"),
    ("black soldier fly larvae farming", "OT:ANIMAL:black_soldier_fly"), ("East Coast fever outbreak", "OT:DISEASE:ecf"),
    ("high quality cassava flour exports", "OT:PRODUCT:cassava_flour")])
def test_multiword_one_pass(q, exp):
    ms = oo.lookup(q); assert ms[0].ontology_id == exp

def test_multiword_beats_parts():
    ids = [m.ontology_id for m in oo.lookup("palm kernel cake and palm oil")]
    assert ids == ["OT:PRODUCT:palm_kernel_cake", "OT:PRODUCT:palm_oil"]

def test_closure_ancestors_descendants():
    assert "OT:GROUP:root_tuber_crops" in oo.ancestors("OT:CROP:cassava")
    assert "OT:CROP:cassava" in oo.descendants("OT:GROUP:root_tuber_crops")
    # derived_from is transitive: garri <- cassava roots <- cassava
    assert oo.ancestors("OT:PRODUCT:garri", rel="derived_from") == ["OT:PRODUCT:cassava_roots", "OT:CROP:cassava"]
    assert "OT:PRODUCT:garri" in oo.descendants("OT:CROP:cassava", rel="derived_from")
    assert oo.descendants("OT:CROP:cassava", rel="derived_from", max_depth=1)

def test_hs_and_fts():
    assert oo.to_hs("OT:HS:071410") == ["071410"]
    assert any(i == "OT:CROP:cassava" for i, _ in oo.search("manio"))
    assert oo.warehouse("OT:FAO:125")

def test_sqlite_is_read_only():
    with pytest.raises(sqlite3.OperationalError):
        _data.conn().execute("CREATE TABLE x(a)")
    with pytest.raises(sqlite3.OperationalError):
        _data.conn().execute("DELETE FROM entities")

def test_pure_python_automaton_matches_c_backend():
    keys = ["palm", "palm oil", "palm kernel cake", "kernel", "cake"]
    py = _matcher._PyAho(keys)
    toks = "the palm kernel cake and palm oil".split()
    got = sorted((i - n + 1, n) for i, n in py.iter_tokens(toks))
    assert (1, 3) in got and (5, 2) in got and (1, 1) in got
    try:
        c = _matcher._CAho(keys)
        assert sorted((i - n + 1, n) for i, n in c.iter_tokens(toks)) == got
    except ImportError:
        pass

def test_thread_safety():
    errs = []
    def run():
        try:
            for _ in range(50): assert oo.lookup("cassava and maize")[0].ontology_id == "OT:CROP:cassava"
        except Exception as e: errs.append(e)
    ts = [threading.Thread(target=run) for _ in range(8)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert not errs

def _child(q):
    q.put(oo.lookup("mandioca")[0].ontology_id)

@pytest.mark.skipif(
    "fork" not in mp.get_all_start_methods(),
    reason="fork start method unavailable (e.g. Windows)",
)
def test_fork_safety():
    oo.lookup("maize")
    ctx = mp.get_context("fork"); q = ctx.Queue(); p = ctx.Process(target=_child, args=(q,)); p.start(); p.join(10)
    assert q.get(timeout=5) == "OT:CROP:cassava"

def test_unmatched_token_fuzzy_only():
    ms = oo.lookup("maize and cassavaa")
    assert [m.fuzzy for m in ms] == [False, True] or ms[1].alias_type == "misspelling"
