"""SQL display must preserve quoted values and stay separate from execution."""

import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock

from ingestion import meta
from ingestion.logging import SQL_RELAY_ENV, format_sql_for_log, restore_sql_blocks
from ingestion.spark.raw.iceberg import build_create_table_sql


def test_create_table_displays_one_column_per_line_and_separate_clauses():
    sql = build_create_table_sql("raw.bank.retail_bank__deals", [
        ("deal_id", "bigint"), ("FROM, WHERE", "decimal(12,2)"),
    ])
    assert format_sql_for_log(sql).splitlines() == [
        "CREATE TABLE raw.bank.retail_bank__deals (",
        "  `deal_id` bigint,",
        "  `FROM, WHERE` decimal(12,2),",
        "  `__load_dttm` timestamp_ltz,",
        "  `__process_id` bigint",
        ")",
        "USING iceberg",
        "PARTITIONED BY (days(`__load_dttm`))",
        "LOCATION 's3://datalake/raw/bank/retail_bank__deals'",
        "TBLPROPERTIES ('write.spark.accept-any-schema'='true')",
    ]


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


def test_sql_block_survives_line_oriented_driver_output(monkeypatch):
    monkeypatch.setenv(SQL_RELAY_ENV, "1")
    driver = (
        "import logging\n"
        "from ingestion.logging import configure_driver_logging, log_sql\n"
        "configure_driver_logging()\n"
        "log_sql(logging.getLogger(__name__), 'JDBC SQL', 'SELECT a, b FROM t WHERE id = 1')\n"
    )
    result = subprocess.run([sys.executable, "-c", driver], capture_output=True, text=True, check=True)
    lines = result.stderr.splitlines(keepends=True)
    assert len(lines) == 1
    assert list(restore_sql_blocks(["unrelated\n", *lines])) == [
        "unrelated\n", "JDBC SQL:\n  SELECT a,\n    b\n  FROM t\n  WHERE id = 1",
    ]
