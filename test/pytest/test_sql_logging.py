"""SQL display must preserve quoted values and stay separate from execution."""

from types import SimpleNamespace
from unittest.mock import Mock

from ingestion import meta
from ingestion.logging import SQL_RELAY_ENV, format_sql_for_log, log_sql, restore_sql_blocks


def test_nested_query_keeps_quoted_sql_intact():
    sql = ('(SELECT MIN([a, b]) AS lo, MAX([a, b]) AS hi FROM '
           '(SELECT "FROM" FROM [some WHERE table] WHERE "FROM" = \'x AND y\') q) bounds')
    assert format_sql_for_log(sql).splitlines() == [
        '(',
        '  SELECT MIN([a, b]) AS lo,',
        '    MAX([a, b]) AS hi',
        '  FROM (',
        '    SELECT "FROM"',
        '    FROM [some WHERE table]',
        '    WHERE "FROM" = \'x AND y\'',
        '  ) q',
        ') bounds',
    ]


def test_rendered_pg_meta_sql_is_not_executed(caplog):
    sql = "SELECT 1 FROM ingestion.incremental_batches WHERE entity_fqn = %s"
    params = ("it's WHERE, here",)
    cursor = SimpleNamespace(
        connection=SimpleNamespace(encoding="UTF8"),
        mogrify=Mock(return_value=b"SELECT 1 FROM ingestion.incremental_batches WHERE entity_fqn = 'it''s WHERE, here'"),
        execute=Mock(),
    )
    with caplog.at_level("INFO", logger=meta.__name__):
        meta._execute(cursor, sql, params)
    cursor.mogrify.assert_called_once_with(sql, params)
    cursor.execute.assert_called_once_with(sql, params)
    assert "\n  WHERE entity_fqn = 'it''s WHERE, here'" in caplog.text
    assert "%s" not in caplog.text


def test_rendering_failure_leaves_execution_unchanged(caplog):
    sql, params = "SELECT %s", (42,)
    cursor = SimpleNamespace(mogrify=Mock(side_effect=ValueError("render unavailable")), execute=Mock())
    with caplog.at_level("INFO", logger=meta.__name__):
        meta._execute(cursor, sql, params)
    cursor.execute.assert_called_once_with(sql, params)
    assert "pg-meta SQL (template):\n  SELECT %s" in caplog.text
    assert "pg-meta SQL params=(42,)" in caplog.text


def test_sql_block_survives_line_oriented_driver_output(monkeypatch, caplog):
    monkeypatch.setenv(SQL_RELAY_ENV, "1")
    with caplog.at_level("INFO", logger=meta.__name__):
        log_sql(meta.LOG, "JDBC SQL", "SELECT a, b FROM t WHERE id = 1")
    message = caplog.records[-1].getMessage()
    assert len(message.splitlines()) == 1
    assert list(restore_sql_blocks(["unrelated\n", f"INFO ingestion.meta: {message}\n"])) == [
        "unrelated\n", "JDBC SQL:\n  SELECT a,\n    b\n  FROM t\n  WHERE id = 1",
    ]
