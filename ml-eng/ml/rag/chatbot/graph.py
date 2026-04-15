"""
RAG graph: query → decompose → parallel retrieval (BQ table match + news + academic)
→ BigQuery lookup → merge → rerank → generate.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, TypedDict

from ml.rag.chatbot.bq_table_matcher import match_bq_tables_from_descriptions
from ml.rag.chatbot.generator import generate
from ml.rag.chatbot.query_decomposer import decompose_query
from ml.rag.chatbot.reranker import rerank
from ml.rag.retrievers.bq_retriever import BQRetriever
from ml.rag.retrievers.vector_retriever import VectorRetriever


class RAGGraphState(TypedDict, total=False):
    query: str
    decomposition: dict[str, Any]
    bq_table_candidates: list[dict[str, Any]]
    vector_news_results: list[dict[str, Any]]
    vector_academic_results: list[dict[str, Any]]
    vector_results: list[dict[str, Any]]
    bq_results: list[dict[str, Any]]
    merged_context: list[dict[str, Any]]
    reranked_context: list[dict[str, Any]]
    answer: str
    error: str | None
    # Optional UI / API overrides (see run_rag)
    geo_override: str | None
    time_start_override: str | None
    time_end_override: str | None
    news_top_k: int | None
    academic_top_k: int | None
    bq_top_k: int | None
    rerank_top_k: int | None
    # Generator memory: rolling summary + last N verbatim pairs (see ml.rag.chat_memory)
    conversation_summary: str | None
    recent_turns: list[dict[str, Any]] | None
    chat_history: list[dict[str, Any]] | None  # legacy: verbatim-only, no summary


def node_decompose(state: RAGGraphState) -> dict[str, Any]:
    q = (state.get("query") or "").strip()
    return {"decomposition": decompose_query(q)}


def _tag_vector(item: dict[str, Any], kind: str) -> dict[str, Any]:
    return {
        **item,
        "source": kind,
        "_context_kind": kind,
    }


def _retrieve_news(state: RAGGraphState) -> list[dict[str, Any]]:
    q = (state.get("query") or "").strip()
    dec = state.get("decomposition") or {}
    vr = VectorRetriever()
    geo = (state.get("geo_override") or "").strip()
    if not geo:
        geo_list = dec.get("geography") or []
        geo = geo_list[0] if geo_list else ""

    ts = (state.get("time_start_override") or dec.get("time_start") or "").strip()[:10]
    te = (state.get("time_end_override") or dec.get("time_end") or "").strip()[:10]
    domains = dec.get("domains") or []
    domain_sub = domains[0] if domains else None

    top_k = int(state.get("news_top_k") or 15)
    kwargs: dict[str, Any] = {
        "doc_kind": "news_article",
        "top_k": top_k,
        "overfetch_multiplier": 20,
    }
    if geo:
        kwargs["geo_country"] = geo
    if ts:
        kwargs["published_at_from"] = ts
    if te:
        kwargs["published_at_to"] = te
    if domain_sub:
        kwargs["domains_substring"] = domain_sub

    raw = vr.retrieve(q, **kwargs)
    return [_tag_vector(x, "news") for x in raw]


def _retrieve_academic(state: RAGGraphState) -> list[dict[str, Any]]:
    q = (state.get("query") or "").strip()
    top_k = int(state.get("academic_top_k") or 20)
    vr = VectorRetriever()
    raw = vr.retrieve(
        q,
        top_k=top_k,
        doc_kind="academic_article",
        overfetch_multiplier=30,
    )
    return [_tag_vector(x, "academic") for x in raw]


def _retrieve_bq_tables(state: RAGGraphState) -> list[dict[str, Any]]:
    q = (state.get("query") or "").strip()
    return match_bq_tables_from_descriptions(q, top_k=10)


def node_parallel_retrieve(state: RAGGraphState) -> dict[str, Any]:
    """Run BQ table-description match, news retrieval, and academic retrieval in parallel."""
    bq_cands: list[dict[str, Any]] = []
    news_out: list[dict[str, Any]] = []
    academic_out: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {
            ex.submit(_retrieve_bq_tables, state): "bq_tables",
            ex.submit(_retrieve_news, state): "news",
            ex.submit(_retrieve_academic, state): "academic",
        }
        for fut in as_completed(futs):
            kind = futs[fut]
            try:
                res = fut.result()
            except Exception:
                res = []
            if kind == "bq_tables":
                bq_cands = res
            elif kind == "news":
                news_out = res
            else:
                academic_out = res

    combined = list(news_out) + list(academic_out)
    return {
        "bq_table_candidates": bq_cands,
        "vector_news_results": news_out,
        "vector_academic_results": academic_out,
        "vector_results": combined,
    }


def node_bq_retrieve(state: RAGGraphState) -> dict[str, Any]:
    q = (state.get("query") or "").strip()
    cands = state.get("bq_table_candidates") or []
    hints = [str(c.get("content") or "") for c in cands[:12] if c.get("content")]
    top_k = int(state.get("bq_top_k") or 15)
    retriever = BQRetriever()
    results = retriever.retrieve(q, top_k=top_k, table_hints=hints)
    return {"bq_results": results}


def node_merge(state: RAGGraphState) -> dict[str, Any]:
    merged: list[dict[str, Any]] = []
    for r in state.get("bq_results") or []:
        merged.append(
            {
                **r,
                "source": r.get("source", "bigquery"),
                "_context_kind": "bigquery",
            }
        )
    for item in state.get("vector_news_results") or []:
        text = item.get("content") or ""
        meta = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        merged.append(
            {
                "content": f"[News] {text}",
                "source": "news",
                "_context_kind": "news",
                "metadata": meta,
                "score": item.get("score"),
            }
        )
    for item in state.get("vector_academic_results") or []:
        text = item.get("content") or ""
        meta = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        merged.append(
            {
                "content": f"[Academic] {text}",
                "source": "academic",
                "_context_kind": "academic",
                "metadata": meta,
                "score": item.get("score"),
            }
        )
    return {"merged_context": merged}


def node_rerank(state: RAGGraphState) -> dict[str, Any]:
    query = state.get("query") or ""
    merged = state.get("merged_context") or []
    top_k = int(state.get("rerank_top_k") or 21)
    top = rerank(query, merged, top_k=top_k)
    return {"reranked_context": top}


def node_generate(state: RAGGraphState) -> dict[str, Any]:
    query = state.get("query") or ""
    context = state.get("reranked_context") or []
    dec = state.get("decomposition")
    gkw: dict[str, Any] = {"decomposition": dec if isinstance(dec, dict) else None}
    cs = state.get("conversation_summary")
    rt = state.get("recent_turns")
    has_mem = (isinstance(cs, str) and cs.strip()) or (
        isinstance(rt, list) and len(rt) > 0
    )
    if has_mem:
        gkw["conversation_summary"] = cs if isinstance(cs, str) else ""
        gkw["recent_turns"] = list(rt) if isinstance(rt, list) else []
    elif state.get("chat_history"):
        gkw["chat_history"] = state.get("chat_history")
    answer = generate(query, context, **gkw)
    return {"answer": answer}


def build_graph():
    """Build and compile the LangGraph RAG graph. Requires langgraph."""
    try:
        from langgraph.graph import END, START, StateGraph  # type: ignore[import-untyped]
    except ImportError:
        raise ImportError("Install langgraph: pip install langgraph") from None

    graph = StateGraph(RAGGraphState)

    graph.add_node("decompose", node_decompose)
    graph.add_node("parallel_retrieve", node_parallel_retrieve)
    graph.add_node("bq_retrieve", node_bq_retrieve)
    graph.add_node("merge", node_merge)
    graph.add_node("rerank", node_rerank)
    graph.add_node("generate", node_generate)

    graph.add_edge(START, "decompose")
    graph.add_edge("decompose", "parallel_retrieve")
    graph.add_edge("parallel_retrieve", "bq_retrieve")
    graph.add_edge("bq_retrieve", "merge")
    graph.add_edge("merge", "rerank")
    graph.add_edge("rerank", "generate")
    graph.add_edge("generate", END)

    return graph.compile()


def run_rag(query: str, **kwargs: Any) -> dict[str, Any]:
    """Run the RAG pipeline and return the state (including answer)."""
    graph = build_graph()
    initial: RAGGraphState = {"query": query}
    for key in (
        "geo_override",
        "time_start_override",
        "time_end_override",
        "news_top_k",
        "academic_top_k",
        "bq_top_k",
        "rerank_top_k",
        "chat_history",
    ):
        if key in kwargs and kwargs[key] is not None:
            initial[key] = kwargs[key]  # type: ignore[assignment]
    if "conversation_summary" in kwargs:
        initial["conversation_summary"] = kwargs["conversation_summary"]  # type: ignore[assignment]
    if "recent_turns" in kwargs:
        initial["recent_turns"] = kwargs["recent_turns"]  # type: ignore[assignment]
    cfg = kwargs.get("config")
    if cfg is not None:
        result = graph.invoke(initial, config=cfg)
    else:
        result = graph.invoke(initial)
    return dict(result)
