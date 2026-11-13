from __future__ import annotations

from ml.rag.chatbot.bq_sql_diagnostics import (
    MAX_DIAGNOSTIC_SQL_LENGTH,
    MAX_DIAGNOSTIC_SQL_STATEMENTS,
    redact_sql_literals,
    sql_attempts_for_diagnostics,
    sql_statements_for_diagnostics,
)


def test_redact_sql_literals_preserves_structure_and_escaped_quotes() -> None:
    sql = (
        "SELECT `country_name` FROM mart_dev.fct_production "
        "WHERE producer = 'O''Reilly' AND country_name = \"Kenya\" "
        "AND year = 2024 AND metric1 = 0xFF"
    )

    redacted = redact_sql_literals(sql)

    assert redacted == (
        "SELECT `country_name` FROM mart_dev.fct_production "
        "WHERE producer = '<REDACTED>' AND country_name = \"<REDACTED>\" "
        "AND year = <REDACTED> AND metric1 = <REDACTED>"
    )
    assert "O''Reilly" not in redacted
    assert "Kenya" not in redacted
    assert "2024" not in redacted
    assert "0xFF" not in redacted


def test_redact_sql_literals_removes_comment_contents() -> None:
    sql = "SELECT 'private' -- hidden note\nFROM mart_dev.fct_production /* hidden */"

    redacted = redact_sql_literals(sql)

    assert redacted == (
        "SELECT '<REDACTED>' -- <REDACTED>\nFROM mart_dev.fct_production /* <REDACTED> */"
    )
    assert "private" not in redacted
    assert "hidden" not in redacted


def test_sql_statements_for_diagnostics_are_bounded_and_redacted() -> None:
    sqls = [f"SELECT '{i}'" for i in range(MAX_DIAGNOSTIC_SQL_STATEMENTS + 2)]

    diagnostics = sql_statements_for_diagnostics(sqls)

    assert len(diagnostics) == MAX_DIAGNOSTIC_SQL_STATEMENTS
    assert diagnostics[0] == "SELECT '<REDACTED>'"
    assert all(str(i) not in statement for i, statement in enumerate(diagnostics))


def test_sql_attempt_diagnostics_keep_status_and_job_id() -> None:
    attempts = sql_attempts_for_diagnostics(
        [
            {
                "sql": "SELECT * FROM mart_dev.fct_production WHERE country = 'Kenya'",
                "status": "timeout",
                "job_id": "job-123",
                "sql_source": "pattern",
                "execution_error": "not exposed in this view",
            }
        ]
    )

    assert attempts == [
        {
            "sql": (
                "SELECT * FROM mart_dev.fct_production "
                "WHERE country = '<REDACTED>'"
            ),
            "status": "timeout",
            "job_id": "job-123",
            "sql_source": "pattern",
        }
    ]


def test_redact_sql_literals_marks_truncated_output() -> None:
    sql = "SELECT '" + ("x" * MAX_DIAGNOSTIC_SQL_LENGTH) + "'"

    redacted = redact_sql_literals(sql, max_length=MAX_DIAGNOSTIC_SQL_LENGTH)

    assert len(redacted) < MAX_DIAGNOSTIC_SQL_LENGTH + 50
    assert "truncated for diagnostics" in redacted
    assert "x" * 30 not in redacted
