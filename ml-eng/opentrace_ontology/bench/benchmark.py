#!/usr/bin/env python3
"""Benchmark: cold start, p50/p95/p99 for lookup & normalize_query on ~1000 queries, RSS.
Run: python bench/benchmark.py [--pure]   (from ml-eng/)"""
import os, random, resource, sys, time, json, statistics
if "--pure" in sys.argv: os.environ["OPENTRACE_ONTOLOGY_PURE_PYTHON"] = "1"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
def rss_mb():
    with open("/proc/self/status") as f:
        for l in f:
            if l.startswith("VmRSS"): return int(l.split()[1]) / 1024
rss0 = rss_mb(); t0 = time.perf_counter()
import opentrace_ontology as oo
from opentrace_ontology import _matcher
oo.lookup("cassava")                       # forces sqlite open + automaton build
cold = time.perf_counter() - t0; rss1 = rss_mb()
random.seed(7)
items = ["cassava","casssava","maize","manioc","mandioca","muhogo","rogo","agbado","garri","palm kernel cake","fall armyworm",
         "black soldier fly","tilapia","cocoa beans","shea butter","urea","NPK fertilizer","extension services","cowpea","beans",
         "sorghum","teff","yam","sweet potato","goat meat","eggs","honey","Newcastle disease","East Coast fever","palm oil",
         "groundnut cake","cassva","tomatoe","maiz","onions","cattle","poultry feed","crop insurance","warehouse receipt system"]
tmpl = ["What is the current state of {} in {}","{} price in {} last year","production of {} and {} in {}","how to control {} on farms in {}",
        "export volume of {} from {} since 2020","yield of {} per hectare in {}","prix du {} au {}","bei ya {} {}","farashin {} a {}"]
countries = ["Nigeria","Ghana","Kenya","Ethiopia","Tanzania","Senegal","Cameroun","Uganda","Malawi","Côte d'Ivoire"]
Q = []
for i in range(1000):
    t = random.choice(tmpl); n = t.count("{}")
    args = [random.choice(items) for _ in range(n - 1)] + [random.choice(countries)]
    Q.append(t.format(*args))
def bench(fn):
    lat = []
    for q in Q:
        s = time.perf_counter(); fn(q); lat.append((time.perf_counter() - s) * 1000)
    lat.sort(); pct = lambda p: lat[min(len(lat) - 1, int(p / 100 * len(lat)))]
    return {"p50_ms": round(pct(50), 3), "p95_ms": round(pct(95), 3), "p99_ms": round(pct(99), 3), "mean_ms": round(statistics.mean(lat), 3)}
r = {"backend": _matcher.backend(), "cold_start_s": round(cold, 3), "queries": len(Q),
     "lookup_first_pass": bench(oo.lookup), "lookup_warm": bench(oo.lookup), "normalize_query_warm": bench(oo.normalize_query),
     "plan_query_first_pass_v3": bench(oo.plan_query), "plan_query_warm_v3": bench(oo.plan_query),
     "rss_before_import_mb": round(rss0, 1), "rss_after_load_mb": round(rss1, 1), "rss_end_mb": round(rss_mb(), 1),
     "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)}
print(json.dumps(r, indent=1))
