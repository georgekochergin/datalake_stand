"""SQL display must preserve quoted values and stay separate from execution."""

import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

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


def test_execute_logs_template_and_params(caplog):
    sql = "SELECT 1 FROM ingestion.incremental_batches WHERE entity_fqn = %s"
    params = ("it's WHERE, here",)
    cursor = SimpleNamespace(execute=Mock())
    with caplog.at_level("INFO", logger=meta.__name__):
        meta._execute(cursor, sql, params)
    cursor.execute.assert_called_once_with(sql, params)
    assert "\n  WHERE entity_fqn = %s" in caplog.text
    assert "pg-meta SQL params=(\"it's WHERE, here\",)" in caplog.text


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
        "unrelated\n", "INFO __main__: JDBC SQL:\n  SELECT a,\n    b\n  FROM t\n  WHERE id = 1",
    ]


def test_pg_meta_commit_summary_keeps_only_changed_fields_at_info(caplog):
    conn = MagicMock()
    before = {"status": "RUNNING", "row_count": None, "target_table": "unchanged"}
    after = dict(before, status="SUCCEEDED", row_count=42)
    with caplog.at_level("DEBUG", logger=meta.__name__):
        with meta._operation(lambda: conn, "finalize_batch", process_id=5) as (_, changes):
            changes.append(("incremental_batches", before, after))
    messages = [record.getMessage() for record in caplog.records if record.levelname == "INFO"]
    result = "pg-meta committed operation=finalize_batch table=ingestion.incremental_batches "
    assert result + "status='RUNNING'→'SUCCEEDED' row_count=None→42" in messages
    assert not any("unchanged" in message for message in messages)
    assert any(record.levelname == "DEBUG" and f"before={before!r} row={after!r}" in record.getMessage()
               for record in caplog.records)
