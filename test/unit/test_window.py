"""Unit tests for the incremental window preparation (incremental/window.py)."""

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.sources.rdbms import registry
from ingestion.spark.raw.rdbms.incremental import window

MSSQL_URL = "jdbc:sqlserver://host:1433;databaseName=retail_bank"
ORACLE_URL = "jdbc:oracle:thin:@//host:1521/sydafa"


def make_args(**overrides):
    """Build the incremental loader's args namespace."""
    values = {
        "jdbc_url": MSSQL_URL,
        "jdbc_user": "sydafa",
        "jdbc_password": "secret",
        "source_table": "retail_bank.transactions",
        "num_partitions": 4,
        "no_jdbc_partitioning": False,
        "incremental_column": "_batch_id",
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


def make_batch(**changes):
    """Build a frozen batch dict."""
    values = {"lower_bound": None, "lower_bound_op": ">", "upper_bound": None, "empty_at_plan": False}
    values.update(changes)
    return values


class TestWindowFilters(unittest.TestCase):
    """Pure assembly of the frozen-window predicates."""

    def setUp(self):
        self.dialect = registry.by_conn_type("mssql")

    def filters(self, batch, kind="bigint"):
        return window.window_filters(self.dialect, batch, kind, "_batch_id")

    def test_strict_window(self):
        batch = make_batch(lower_bound="5", upper_bound="10")
        self.assertEqual(self.filters(batch), ["[_batch_id] > 5", "[_batch_id] <= 10"])

    def test_inclusive_lower_and_missing_bounds(self):
        batch = make_batch(lower_bound_op=">=", upper_bound="10")
        self.assertEqual(self.filters(batch), ["[_batch_id] <= 10"])
        batch["upper_bound"] = None
        self.assertEqual(self.filters(batch), [])

    def test_empty_at_plan_proves_no_rows(self):
        batch = make_batch(lower_bound="10", upper_bound="10", empty_at_plan=True)
        self.assertEqual(self.filters(batch), ["[_batch_id] > 10", "[_batch_id] <= 10", "1=0"])

    def test_temporal_literal_rendered_by_kind(self):
        batch = make_batch(lower_bound="2026-10-23T00:00:00Z", lower_bound_op=">=",
                           upper_bound="2026-10-26T12:34:56Z")
        self.assertEqual(
            self.filters(batch, kind="timestamp_instant(0)"),
            ["[_batch_id] >= CAST('2026-10-23T00:00:00Z' AS datetimeoffset(0))",
             "[_batch_id] <= CAST('2026-10-26T12:34:56Z' AS datetimeoffset(0))"],
        )


class TestPositionSelect(unittest.TestCase):
    """The exact typed extraction position appended to the read projection."""

    def test_integral_position_is_plain_column(self):
        dialect = registry.by_conn_type("mssql")
        self.assertEqual(
            window.position_select(dialect, "bigint", "_batch_id"),
            ', [_batch_id] AS "__extract_position"',
        )

    def test_temporal_position_uses_dialect_expression(self):
        dialect = registry.by_conn_type("oracle")
        self.assertIn(
            "TO_CHAR",
            window.position_select(dialect, "timestamp_instant(6)", "updated_at"),
        )


class TestReadWindowFrame(unittest.TestCase):
    """Composition of predicates and the position into the shared JDBC reader."""

    def test_composes_filters_and_position_into_shared_reader(self):
        batch = make_batch(lower_bound="5", upper_bound="10")
        with patch.object(window.reader, "read_extract_frame", return_value=MagicMock()) as read_frame:
            result = window.read_window_frame(MagicMock(), make_args(), batch, "bigint")
        self.assertIs(result, read_frame.return_value)
        kwargs = read_frame.call_args.kwargs
        self.assertEqual(kwargs["filters"], ["[_batch_id] > 5", "[_batch_id] <= 10"])
        self.assertEqual(kwargs["extra_select"], ', [_batch_id] AS "__extract_position"')

    def test_oracle_window_uses_oracle_dialect(self):
        batch = make_batch(lower_bound="5", upper_bound="10")
        args = make_args(jdbc_url=ORACLE_URL)
        with patch.object(window.reader, "read_extract_frame", return_value=MagicMock()) as read_frame:
            window.read_window_frame(MagicMock(), args, batch, "bigint")
        self.assertEqual(
            read_frame.call_args.kwargs["filters"],
            ['"_batch_id" > 5', '"_batch_id" <= 10'],
        )


if __name__ == "__main__":
    unittest.main()
