"""One-pass multi-word mention detection over token-normalised text.
Backend: pyahocorasick if importable, else a pure-python Aho-Corasick over tokens. Built lazily from
data/matcher_keys.txt.gz (the serialized key set) and cached for the process."""
from __future__ import annotations
from functools import lru_cache
from importlib import import_module
from . import _data

class _PyAho:
    """Token-level Aho-Corasick automaton (keys are space-joined token sequences)."""
    def __init__(self, keys):
        self.goto = [{}]; self.out = [0]; self.fail = [0]
        for k in keys:
            s = 0
            for t in k.split(" "):
                nxt = self.goto[s].get(t)
                if nxt is None:
                    nxt = len(self.goto); self.goto[s][t] = nxt; self.goto.append({}); self.out.append(0); self.fail.append(0)
                s = nxt
            self.out[s] = len(k.split(" "))     # store token length of longest key ending here
        from collections import deque
        dq = deque()
        for t, s in self.goto[0].items(): dq.append(s)
        while dq:
            r = dq.popleft()
            for t, s in self.goto[r].items():
                dq.append(s); f = self.fail[r]
                while f and t not in self.goto[f]: f = self.fail[f]
                self.fail[s] = self.goto[f].get(t, 0) if self.goto[f].get(t, 0) != s else 0
        self.outs = {}
    def _outputs(self, s):
        o = self.outs.get(s)
        if o is None:
            o = []; x = s
            while x:
                if self.out[x]: o.append(self.out[x])
                x = self.fail[x]
            self.outs[s] = o
        return o
    def iter_tokens(self, toks):
        """yield (end_token_index_inclusive, n_tokens)"""
        s = 0
        for i, t in enumerate(toks):
            while s and t not in self.goto[s]: s = self.fail[s]
            s = self.goto[s].get(t, 0)
            for n in self._outputs(s): yield i, n

class _CAho:
    def __init__(self, keys):
        # Optional accelerator; not a required install (pure-Python _PyAho fallback).
        ahocorasick = import_module("ahocorasick")
        a = ahocorasick.Automaton()
        for k in keys: a.add_word(" " + k + " ", k.count(" ") + 1)
        a.make_automaton(); self.a = a
    def iter_tokens(self, toks):
        s = " " + " ".join(toks) + " "
        # char offset -> token index
        ends = {}; pos = 1
        for i, t in enumerate(toks):
            pos += len(t); ends[pos] = i; pos += 1
        for end, n in self.a.iter(s):
            i = ends.get(end)   # end points at the trailing space
            if i is not None: yield i, n

@lru_cache(maxsize=1)
def automaton():
    keys = [k for k in _data.matcher_keys() if k]
    import os
    try:
        if os.environ.get("OPENTRACE_ONTOLOGY_PURE_PYTHON") == "1": raise ImportError
        return _CAho(keys)
    except ImportError:
        return _PyAho(keys)

def backend() -> str:
    return type(automaton()).__name__

def spans(toks):
    """All (start, n) key occurrences in token list."""
    return [(i - n + 1, n) for i, n in automaton().iter_tokens(toks)]
