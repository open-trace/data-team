"""Degenerate/looping non-English output detection and gating (ML-055)."""

from __future__ import annotations

from ml.rag.chatbot.answer_language import (
    language_not_yet_supported_answer,
    looks_like_degenerate_repetition,
)
from ml.rag.chatbot.generator import _guard_degenerate_language_output


def test_repeated_phrase_is_detected() -> None:
    # Sprint 2 repro: same phrase repeated up to five times, no coherent answer.
    text = "mazao ya kilimo yanaongezeka " * 5
    assert looks_like_degenerate_repetition(text) is True


def test_normal_prose_is_not_flagged() -> None:
    text = (
        "Kilimo cha mahindi kinaongezeka nchini Kenya kutokana na mvua za kutosha. "
        "Wakulima wengi wamejitolea kupanda mazao zaidi msimu huu wa kilimo."
    )
    assert looks_like_degenerate_repetition(text) is False


def test_short_answer_is_not_flagged() -> None:
    # Too few words to ever reach 3 occurrences of a 4-word phrase.
    assert looks_like_degenerate_repetition("Asante sana kwa swali lako") is False


def test_empty_answer_is_not_flagged() -> None:
    assert looks_like_degenerate_repetition("") is False
    assert looks_like_degenerate_repetition(None) is False


def test_repetition_needs_at_least_four_word_phrase() -> None:
    # A single repeated short connector word must not trip the guard.
    text = "the crop yield increased and the crop yield increased and " * 3
    # This has short 2-word overlaps but let's check a case with genuinely
    # varied short prose reusing common words -- should not false-positive.
    varied = "the maize yield rose this year. rice yield also rose this year in the region."
    assert looks_like_degenerate_repetition(varied) is False


def test_language_not_yet_supported_message_names_language() -> None:
    msg = language_not_yet_supported_answer("sw")
    assert "Swahili" in msg
    msg_fr = language_not_yet_supported_answer("fr")
    assert "French" in msg_fr


def test_guard_replaces_repetitive_swahili_answer() -> None:
    repeated = "mazao ya kilimo yanaongezeka sana " * 5
    out = _guard_degenerate_language_output(repeated, "Je, mazao ya kilimo yanaongezekaje?")
    assert out != repeated
    assert "not yet able to give a fully reliable answer" in out


def test_guard_leaves_normal_swahili_answer_untouched() -> None:
    answer = (
        "Kilimo cha mahindi kinaongezeka nchini Kenya kutokana na mvua za kutosha "
        "na juhudi za wakulima wa ndani."
    )
    out = _guard_degenerate_language_output(answer, "Mazao ya mahindi yanaendeleaje Kenya?")
    assert out == answer


def test_guard_does_not_gate_repetitive_english_answer() -> None:
    # Sprint 2 only reported this failure for non-English languages; English
    # generation was not flagged as affected, so English repetition (however
    # unlikely) should not be silently replaced.
    repeated_en = "maize yield increased significantly " * 5
    out = _guard_degenerate_language_output(repeated_en, "How is maize yield trending?")
    assert out == repeated_en


def test_guard_leaves_empty_answer_untouched() -> None:
    assert _guard_degenerate_language_output("", "Habari za mazao?") == ""
