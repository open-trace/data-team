"""Non-answer routes must not report Strong confidence (ML-057, beta finding #9).

A greeting or an out-of-scope refusal carries no cited evidence, so stamping it
with the curated-KB signal ("Strong confidence", score 90) told the user the
opposite of what the evidence supported. Meta and product routes keep the
curated signal -- those answer from a vetted knowledge base.
"""

from __future__ import annotations

from ml.rag.chatbot.acf_scoring import curated_product_acf, no_evidence_acf
from ml.rag.chatbot.graph import node_generate_social


def test_greeting_is_not_strong_confidence() -> None:
    state = {"query": "hello", "answer_lang": "en"}
    out = node_generate_social(state)  # type: ignore[arg-type]
    assert out["acf_band"] != "strong"
    assert out["acf_band"] == "no_evidence"
    assert out["acf_score"] == 0


def test_out_of_scope_is_not_strong_confidence() -> None:
    state = {"query": "who won the football match last night", "answer_lang": "en"}
    out = node_generate_social(state)  # type: ignore[arg-type]
    assert out["acf_band"] != "strong"
    assert out["acf_score"] == 0


def test_greeting_and_out_of_scope_explanations_differ() -> None:
    """Distinct wording keeps Langfuse traces readable."""
    greeting = node_generate_social({"query": "hello", "answer_lang": "en"})  # type: ignore[arg-type]
    oos = node_generate_social(
        {"query": "who won the football match last night", "answer_lang": "en"}  # type: ignore[arg-type]
    )
    if greeting.get("is_greeting_query") and oos.get("is_out_of_scope_query"):
        assert greeting["acf_explanation"] != oos["acf_explanation"]


def test_social_route_returns_no_citations() -> None:
    out = node_generate_social({"query": "hello", "answer_lang": "en"})  # type: ignore[arg-type]
    assert out["citations"] == []


def test_curated_product_acf_still_strong_for_product_kb() -> None:
    """Regression guard: the curated signal itself is unchanged."""
    acf = curated_product_acf()
    assert acf.band == "strong"
    assert acf.score == 90


def test_no_evidence_acf_accepts_tailored_explanation() -> None:
    acf = no_evidence_acf(explanation="Greeting — no agricultural claim was made.")
    assert acf.band == "no_evidence"
    assert acf.score == 0
    assert "Greeting" in acf.explanation
