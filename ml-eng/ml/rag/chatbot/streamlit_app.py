"""
Streamlit test UI for the multi-source RAG pipeline:
decompose → parallel retrieval (BQ descriptions + news + academic) → BQ → merge → rerank → generate.

Multi-turn chat: sessions in st.session_state; prior turns passed to the generator (retrieval uses latest message only).

Run from repo root: streamlit run ml/rag/streamlit_app.py
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path

# Load .env from data/local
_env = Path(__file__).resolve().parents[2] / "data" / "local" / ".env"
if _env.exists():
    with open(_env) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v

import streamlit as st

# #region agent log
def _agent_debug_log(message: str, data: dict, hypothesis_id: str, run_id: str = "pre-fix") -> None:
    import json
    import time

    _p = Path(__file__).resolve().parents[2] / ".cursor" / "debug-4fd6d6.log"
    try:
        _p.parent.mkdir(parents=True, exist_ok=True)
        with _p.open("a", encoding="utf-8") as _f:
            _f.write(
                json.dumps(
                    {
                        "sessionId": "4fd6d6",
                        "timestamp": int(time.time() * 1000),
                        "location": "streamlit_app.py:startup",
                        "message": message,
                        "data": data,
                        "runId": run_id,
                        "hypothesisId": hypothesis_id,
                    }
                )
                + "\n"
            )
    except OSError:
        pass


try:
    import importlib.util as _ilu

    _tv = _ilu.find_spec("torchvision") is not None
except Exception:
    _tv = False
_agent_debug_log("import_probe", {"torchvision_spec_found": _tv}, "A")

try:
    import sys as _sys

    _agent_debug_log(
        "streamlit_runtime_probe",
        {
            "streamlit_version": getattr(st, "__version__", None),
            # Note: Streamlit executes the app in a separate runtime where argv may not include CLI flags.
            "argv": list(getattr(_sys, "argv", [])[:12]),
            "env_STREAMLIT_SERVER_FILE_WATCHER_TYPE": os.environ.get("STREAMLIT_SERVER_FILE_WATCHER_TYPE"),
        },
        "B",
    )
except Exception:
    pass

try:
    import streamlit.config as _st_config

    _agent_debug_log(
        "streamlit_config_probe",
        {
            "server.fileWatcherType": _st_config.get_option("server.fileWatcherType"),
        },
        "B",
    )
except Exception:
    pass

try:
    # Hypothesis E: container exits with 137 due to SIGTERM/SIGINT (user stop) vs SIGKILL (OOM).
    import signal as _signal

    def _agent_signal_handler(signum, _frame):  # type: ignore[no-untyped-def]
        _agent_debug_log("signal_received", {"signum": int(signum)}, "E")

    _signal.signal(_signal.SIGTERM, _agent_signal_handler)
    _signal.signal(_signal.SIGINT, _agent_signal_handler)
except Exception:
    pass

try:
    # Hypothesis F: memory pressure / cgroup limits (OOM kill → SIGKILL, no handler).
    def _read_text(p: str) -> str | None:
        try:
            with open(p, "r", encoding="utf-8") as _f:
                return _f.read().strip()
        except OSError:
            return None

    _status = _read_text("/proc/self/status") or ""
    _mem_lines = [ln for ln in _status.splitlines() if ln.startswith(("VmRSS:", "VmHWM:", "VmSize:"))]
    _agent_debug_log(
        "mem_probe",
        {
            "proc_status_mem": _mem_lines[:6],
            "cgroup_memory_max": _read_text("/sys/fs/cgroup/memory.max"),
            "cgroup_memory_current": _read_text("/sys/fs/cgroup/memory.current"),
        },
        "F",
    )
except Exception:
    pass
# #endregion

from ml.rag.chat_memory import append_turn_and_compact

st.set_page_config(page_title="OpenTrace RAG (test)", page_icon="🔍", layout="wide")
st.title("OpenTrace RAG — test interface")
st.caption(
    "Decomposition → BQ table match + filtered news + academic → BigQuery → merge → rerank → answer"
)


def _session_label(sid: str) -> str:
    sess = st.session_state.rag_sessions.get(sid) or {}
    msgs = sess.get("messages") or []
    if not msgs:
        return f"{sid[:8]}… (empty)"
    for m in msgs:
        if m.get("role") == "user":
            t = (m.get("content") or "").strip().replace("\n", " ")
            return (t[:40] + "…") if len(t) > 40 else t
    return f"{sid[:8]}…"


def _ensure_sessions() -> None:
    if "rag_sessions" not in st.session_state:
        sid = uuid.uuid4().hex
        st.session_state.rag_sessions = {sid: {"messages": []}}
        st.session_state.active_session_id = sid
    if "active_session_id" not in st.session_state:
        st.session_state.active_session_id = next(iter(st.session_state.rag_sessions))


def _new_chat() -> None:
    sid = uuid.uuid4().hex
    st.session_state.rag_sessions[sid] = {"messages": []}
    st.session_state.active_session_id = sid


def _delete_active_session() -> None:
    opts = list(st.session_state.rag_sessions.keys())
    cur = st.session_state.active_session_id
    if len(opts) <= 1:
        st.session_state.rag_sessions = {uuid.uuid4().hex: {"messages": []}}
        st.session_state.active_session_id = next(iter(st.session_state.rag_sessions))
        return
    del st.session_state.rag_sessions[cur]
    st.session_state.active_session_id = opts[0] if opts[0] != cur else opts[1]


_ensure_sessions()

with st.sidebar:
    st.subheader("Chat sessions")
    opts = list(st.session_state.rag_sessions.keys())
    if st.session_state.active_session_id not in opts:
        st.session_state.active_session_id = opts[0]
    ix = opts.index(st.session_state.active_session_id)
    chosen = st.selectbox(
        "Active session",
        range(len(opts)),
        index=ix,
        format_func=lambda i: _session_label(opts[i]),
    )
    st.session_state.active_session_id = opts[chosen]
    c1, c2 = st.columns(2)
    with c1:
        if st.button("New chat"):
            _new_chat()
            st.rerun()
    with c2:
        if st.button("Delete session"):
            _delete_active_session()
            st.rerun()

    st.divider()
    st.subheader("Retrieval controls")
    news_top_k = st.number_input("News chunks (top_k)", min_value=1, max_value=50, value=8)
    academic_top_k = st.number_input("Academic chunks (top_k)", min_value=1, max_value=30, value=5)
    bq_top_k = st.number_input("BQ rows (top_k)", min_value=1, max_value=100, value=15)
    rerank_top_k = st.number_input("Rerank context size", min_value=1, max_value=30, value=8)
    st.divider()
    geo_override = st.text_input("Geography override (optional)", placeholder="e.g. Nigeria")
    t_start = st.text_input("Time start YYYY-MM-DD (optional)", placeholder="2020-01-01")
    t_end = st.text_input("Time end YYYY-MM-DD (optional)", placeholder="2025-12-31")
    show_debug = st.checkbox("Show pipeline debug (last run)", value=False)

active = st.session_state.active_session_id
sess = st.session_state.rag_sessions[active]
if "conversation_summary" not in sess:
    sess["conversation_summary"] = ""
if "recent_turns" not in sess:
    sess["recent_turns"] = []
messages: list[dict[str, str]] = sess["messages"]
prior_summary = str(sess.get("conversation_summary") or "")
prior_recent = list(sess.get("recent_turns") or [])

for m in messages:
    with st.chat_message(m["role"]):
        st.markdown(m.get("content") or "")

prompt = st.chat_input("Ask a question…")

if prompt:
    kwargs: dict = {
        "news_top_k": int(news_top_k),
        "academic_top_k": int(academic_top_k),
        "bq_top_k": int(bq_top_k),
        "rerank_top_k": int(rerank_top_k),
    }
    if geo_override.strip():
        kwargs["geo_override"] = geo_override.strip()
    if t_start.strip():
        kwargs["time_start_override"] = t_start.strip()[:10]
    if t_end.strip():
        kwargs["time_end_override"] = t_end.strip()[:10]
    if prior_summary.strip() or prior_recent:
        kwargs["conversation_summary"] = prior_summary
        kwargs["recent_turns"] = prior_recent

    with st.spinner("Running pipeline…"):
        try:
            from ml.rag.graph import run_rag

            result = run_rag(prompt.strip(), **kwargs)
            answer = result.get("answer") or ""
            err = result.get("error")
            if err:
                answer = f"**Error:** {err}\n\n{answer}".strip()

            messages.append({"role": "user", "content": prompt.strip()})
            messages.append({"role": "assistant", "content": answer})
            new_summary, new_recent = append_turn_and_compact(
                prior_summary,
                prior_recent,
                prompt.strip(),
                answer,
            )
            sess["conversation_summary"] = new_summary
            sess["recent_turns"] = new_recent
            if show_debug:
                st.session_state.last_rag_debug = result
            st.rerun()
        except Exception as e:
            st.exception(e)

if show_debug and "last_rag_debug" in st.session_state:
    result = st.session_state.last_rag_debug
    with st.expander("Pipeline debug (last run)", expanded=True):
        dec = result.get("decomposition") or {}
        st.subheader("Query decomposition")
        st.json(dec)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("BQ table-description matches", len(result.get("bq_table_candidates") or []))
        with c2:
            st.metric("News chunks", len(result.get("vector_news_results") or []))
        with c3:
            st.metric("Academic chunks", len(result.get("vector_academic_results") or []))
        bq_rows = result.get("bq_results") or []
        if bq_rows:
            sql = (bq_rows[0].get("metadata") or {}).get("sql", "")
            if sql:
                st.code(sql, language="sql")

st.divider()
st.markdown("**CLI**")
st.code(
    "PYTHONPATH=. python -m ml/rag.run \"Your question\"\n"
    "streamlit run ml/rag/streamlit_app.py",
    language="bash",
)
