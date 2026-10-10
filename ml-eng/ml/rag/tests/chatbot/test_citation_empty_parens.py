"""Empty parenthetical wrappers must not survive invalid-citation stripping."""
from __future__ import annotations

from ml.rag.chatbot.generator import SourceRef, _strip_invalid_citation_markers


def _registry(*ids: int) -> list[SourceRef]:
    return [SourceRef(source_id=i, item={}, citation_line=f"[{i}] Example") for i in ids]


def test_invalid_citations_in_parens_leave_no_empty_parens() -> None:
    answer = (
        "Rwanda scores high on the Inability to adapt index ([3]), "
        "while mitigation gets 52% of funding ([7])."
    )
    out = _strip_invalid_citation_markers(answer, _registry(1))
    assert "()" not in out
    assert "[3]" not in out
    assert "[7]" not in out


def test_valid_citation_in_parens_is_preserved() -> None:
    answer = "Senegal launched the NAP program ([1]) with GCF funding."
    out = _strip_invalid_citation_markers(answer, _registry(1))
    assert "([1])" in out


def test_mixed_valid_and_invalid_citations_in_parens() -> None:
    answer = "Maize yield rose in Kenya ([1]) and fell in Uganda ([9])."
    out = _strip_invalid_citation_markers(answer, _registry(1))
    assert "([1])" in out
    assert "()" not in out
    assert "[9]" not in out


def test_empty_parens_with_stray_punctuation_removed() -> None:
    answer = "Funding rose sharply ( , ) according to the report ([5])."
    out = _strip_invalid_citation_markers(answer, _registry(1))
    assert "(" not in out and ")" not in out
    assert "[5]" not in out
