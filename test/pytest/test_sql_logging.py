"""SQL display must preserve quoted values and stay separate from execution."""

from types import SimpleNamespace
from unittest.mock import Mock

from ingestion import meta
from ingestion.logging import format_sql_for_log


def test_nested_query_keeps_quoted_sql_intact():
    sql = ('(SELECT MIN([a, b]) AS lo, MAX([a, b]) AS hi FROM '
           '(SELECT "FROM" FROM [some WHERE table] WHERE "FROM" = \'x AND y\') q) bounds')
    assert format_sql_for_log(sql).splitlines() == [
        '  | (',
        '  |   SELECT MIN([a, b]) AS lo,',
        '  |     MAX([a, b]) AS hi',
        '  |   FROM (',
        '  |     SELECT "FROM"',
        '  |     FROM [some WHERE table]',
        '  |     WHERE "FROM" = \'x AND y\'',
        '  |   ) q',
        '  | ) bounds',
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
    assert "\n  | WHERE entity_fqn = 'it''s WHERE, here'" in caplog.text
    assert "%s" not in caplog.text


def test_rendering_failure_leaves_execution_unchanged(caplog):
    sql, params = "SELECT %s", (42,)
    cursor = SimpleNamespace(mogrify=Mock(side_effect=ValueError("render unavailable")), execute=Mock())
    with caplog.at_level("INFO", logger=meta.__name__):
        meta._execute(cursor, sql, params)
    cursor.execute.assert_called_once_with(sql, params)
    assert "pg-meta SQL (template):\n  | SELECT %s" in caplog.text
    assert "pg-meta SQL params=(42,)" in caplog.text
