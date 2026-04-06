"""
FastAPI app for the RAG pipeline. Exposes POST /query for the frontend chatbot.
Run locally: uvicorn ml.rag.api:app --reload --host 0.0.0.0 --port 7860
Hugging Face Spaces: use port 7860 and set env/secrets for BQ and vector DB.
"""
from __future__ import annotations

import os
import threading
import uuid
from pathlib import Path
from typing import Any

# Load .env from data/local when present (e.g. local dev)
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

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from ml.rag.chat_history import normalize_messages
from ml.rag.chat_memory import append_turn_and_compact, flat_messages_to_memory

app = FastAPI(
    title="OpenTrace RAG API",
    description="Query BigQuery + vector DB via a graph RAG; use from the frontend chatbot.",
    version="0.1.0",
)

# Allow frontend to call from another origin (set RAG_CORS_ORIGINS for production)
_cors_origins = os.environ.get("RAG_CORS_ORIGINS", "*").strip().split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _cors_origins if o.strip()],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# session_id -> {conversation_summary, recent_turns} (in-memory; single-process only)
_SESSION_STORE: dict[str, dict[str, Any]] = {}
_SESSION_LOCK = threading.Lock()


def _empty_session_blob() -> dict[str, Any]:
    return {"conversation_summary": "", "recent_turns": []}


class ChatMessage(BaseModel):
    role: str = Field(..., description="user or assistant")
    content: str = Field(..., min_length=1)


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1, description="Natural language question for the RAG")
    include_trace: bool = Field(False, description="Include decomposition and retrieval counts in response")
    session_id: str | None = Field(
        None,
        description="Omit to start a new session; reuse for multi-turn chat (server-side memory)",
    )
    conversation_history: list[ChatMessage] | None = Field(
        None,
        description="If set, used as prior turns instead of server session store for this request",
    )
    geo_override: str | None = None
    time_start_override: str | None = None
    time_end_override: str | None = None
    news_top_k: int | None = None
    academic_top_k: int | None = None
    bq_top_k: int | None = None
    rerank_top_k: int | None = None


class QueryResponse(BaseModel):
    answer: str
    session_id: str = Field(..., description="Pass on the next request for chat continuity")
    error: str | None = None
    has_bq_results: bool = False
    has_vector_results: bool = False
    bq_sql: str | None = None
    trace: dict | None = None


@app.get("/health")
async def health():
    """Readiness for Hugging Face Spaces and load balancers."""
    return {"status": "ok", "service": "rag"}


def _resolve_prior_memory(request: QueryRequest) -> tuple[str, str, list[dict[str, str]]]:
    """Return (session_id, conversation_summary, recent_turns) before the current user message."""
    if request.conversation_history is not None:
        sid = (request.session_id or "").strip() or uuid.uuid4().hex
        raw_msgs = [
            m.model_dump() if hasattr(m, "model_dump") else m.dict()
            for m in request.conversation_history
        ]
        prior = normalize_messages(raw_msgs)
        summary, recent = flat_messages_to_memory(prior)
        return sid, summary, recent

    sid = (request.session_id or "").strip() or uuid.uuid4().hex
    with _SESSION_LOCK:
        blob = _SESSION_STORE.get(sid) or _empty_session_blob()
        summary = str(blob.get("conversation_summary") or "")
        recent = normalize_messages(blob.get("recent_turns"))
    return sid, summary, recent


def _persist_session_turn(session_id: str, user_msg: str, assistant_msg: str) -> None:
    with _SESSION_LOCK:
        blob = _SESSION_STORE.get(session_id) or _empty_session_blob()
        summary, recent = append_turn_and_compact(
            str(blob.get("conversation_summary") or ""),
            blob.get("recent_turns"),
            user_msg,
            assistant_msg,
        )
        _SESSION_STORE[session_id] = {
            "conversation_summary": summary,
            "recent_turns": recent,
        }


@app.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    """Run the RAG pipeline and return the answer for the frontend chatbot."""
    try:
        from ml.rag.graph import run_rag

        session_id, prior_summary, prior_recent = _resolve_prior_memory(request)
        kwargs: dict = {}
        if prior_summary.strip() or prior_recent:
            kwargs["conversation_summary"] = prior_summary
            kwargs["recent_turns"] = prior_recent
        for key in (
            "geo_override",
            "time_start_override",
            "time_end_override",
            "news_top_k",
            "academic_top_k",
            "bq_top_k",
            "rerank_top_k",
        ):
            val = getattr(request, key, None)
            if val is not None:
                kwargs[key] = val

        result = run_rag(request.query, **kwargs)
        # Try to extract the SQL used for BQ retrieval (if any)
        bq_sql: str | None = None
        for item in result.get("bq_results") or []:
            meta = item.get("metadata") or {}
            sql = meta.get("sql")
            if isinstance(sql, str) and sql.strip():
                bq_sql = sql.strip()
                break
        trace: dict | None = None
        if request.include_trace:
            trace = {
                "decomposition": result.get("decomposition"),
                "bq_table_candidates_count": len(result.get("bq_table_candidates") or []),
                "vector_news_count": len(result.get("vector_news_results") or []),
                "vector_academic_count": len(result.get("vector_academic_results") or []),
                "merged_context_count": len(result.get("merged_context") or []),
                "reranked_context_count": len(result.get("reranked_context") or []),
            }

        answer = result.get("answer", "") or ""
        if request.conversation_history is None:
            _persist_session_turn(session_id, request.query.strip(), answer)

        return QueryResponse(
            answer=answer,
            session_id=session_id,
            error=result.get("error"),
            has_bq_results=bool(result.get("bq_results")),
            has_vector_results=bool(result.get("vector_results")),
            bq_sql=bq_sql,
            trace=trace,
        )
    except Exception as e:
        import traceback
        detail = str(e)
        if os.environ.get("RAG_DEBUG", "").strip().lower() in ("1", "true", "on"):
            detail += "\n\n" + traceback.format_exc()
        elif "nn" in detail.lower() or "not defined" in detail.lower():
            detail += ". If using the vector retriever, install PyTorch: pip install torch"
        raise HTTPException(status_code=500, detail=detail)


@app.get("/")
async def root():
    return {"message": "OpenTrace RAG API", "docs": "/docs", "health": "/health", "query": "POST /query"}
