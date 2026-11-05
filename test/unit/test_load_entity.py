"""Unit tests for spark/raw/load_entity.py (pure functions)."""

import importlib.util
import sys
import types
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.sources.rdbms import registry
from ingestion.sources.rdbms.contracts import RdbmsDialect

MODULE_PATH = Path(__file__).resolve().parents[2] / "ingestion" / "spark" / "raw" / "rdbms" / "load_entity.py"
_SPEC = importlib.util.spec_from_file_location("load_entity", MODULE_PATH)
load_entity = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(load_entity)

MSSQL_URL = "jdbc:sqlserver://host:1433;databaseName=retail_bank"
ORACLE_URL = "jdbc:oracle:thin:@//host:1521/sydafa"

BASE_ARGS = [
    "--jdbc-url",
    MSSQL_URL,
    "--jdbc-user",
    "sydafa",
    "--jdbc-password",
    "secret",
    "--source-table",
    "retail_bank.clients",
    "--target-table",
    "raw.bank.retail_bank__clients",
    "--process-id",
    "42",
]


def build_argv(*extra: str) -> list[str]:
    """Base CLI arguments plus extras."""
    return BASE_ARGS + list(extra)


class TestParseArgs(unittest.TestCase):
    """CLI argument parsing and validation."""

    def test_happy_path(self):
        args = load_entity.parse_args(build_argv())
        self.assertEqual(args.source_table, "retail_bank.clients")
        self.assertEqual(args.target_table, "raw.bank.retail_bank__clients")
        self.assertEqual(args.process_id, 42)
        self.assertIsNone(args.incremental_column)
        self.assertEqual(args.num_partitions, 4)

    def test_incremental_args(self):
        args = load_entity.parse_args(build_argv("--incremental-column", "_batch_id"))
        self.assertEqual(args.incremental_column, "_batch_id")

    def test_managed_incremental_args(self):
        args = load_entity.parse_args(build_argv(
            "--source-id", "bank", "--dag-id", "dag", "--run-id", "run",
            "--incremental-column", "updated_at", "--lookback-size", "3", "--lookback-unit", "h",
            "--lookback-given", "--force-full", "--reason", "repair",
        ))
        self.assertEqual((args.source_id, args.dag_id, args.run_id), ("bank", "dag", "run"))
        self.assertEqual((args.lookback_size, args.lookback_unit), (3, "h"))
        self.assertTrue(args.lookback_given)
        self.assertTrue(args.force_full)
        self.assertEqual(args.reason, "repair")

    def test_lookback_given_defaults_off(self):
        args = load_entity.parse_args(build_argv("--source-id", "bank"))
        self.assertFalse(args.lookback_given)
        self.assertEqual(args.lookback_size, 0)
        self.assertIsNone(args.lookback_unit)

    def test_unknown_lookback_unit_rejected(self):
        with self.assertRaises(SystemExit):
            load_entity.parse_args(build_argv("--lookback-unit", "minutes"))

    def test_missing_required_argument(self):
        with self.assertRaises(SystemExit):
            load_entity.parse_args(build_argv()[2:])

    def test_zero_process_id(self):
        with self.assertRaises(ValueError):
            load_entity.parse_args(build_argv("--process-id", "0"))

    def test_negative_process_id(self):
        with self.assertRaises(ValueError):
            load_entity.parse_args(build_argv("--process-id", "-1"))

    def test_zero_num_partitions(self):
        with self.assertRaises(ValueError):
            load_entity.parse_args(build_argv("--num-partitions", "0"))

    def test_parallelism_switch(self):
        self.assertFalse(load_entity.parse_args(build_argv()).no_parallelism)
        args = load_entity.parse_args(build_argv("--no-parallelism", "--num-partitions", "7"))
        self.assertTrue(args.no_parallelism)
        self.assertEqual(args.num_partitions, 7)

    def test_invalid_incremental_column(self):
        with self.assertRaises(ValueError):
            load_entity.parse_args(build_argv("--incremental-column", "Client-Id"))




class StubPartitionDialect(RdbmsDialect):
    """Minimal adapter accepting only its own vendor-specific type name."""

    partition_type_names = frozenset({"INT8"})


class TestPickPartitionColumn(unittest.TestCase):
    """Auto-evaluation of the parallel-read column stays dialect-classified."""

    def setUp(self):
        self.dialect = registry.by_conn_type("mssql")

    def pick(self, candidates):
        return load_entity.pick_partition_column(self.dialect, candidates)

    def test_numeric_pk_preferred_over_later_date(self):
        candidates = [("client_id", "int"), ("updated_at", "datetime2")]
        self.assertEqual(self.pick(candidates), "client_id")

    def test_string_column_skipped(self):
        candidates = [("code", "nvarchar"), ("updated_at", "datetime2")]
        self.assertEqual(self.pick(candidates), "updated_at")

    def test_order_respected(self):
        candidates = [("code", "varchar"), ("num", "bigint"), ("other", "int")]
        self.assertEqual(self.pick(candidates), "num")

    def test_identity_type_variant(self):
        self.assertEqual(self.pick([("id", "int identity")]), "id")

    def test_timestamp_with_zone_variant(self):
        self.assertEqual(self.pick([("ts", "TIMESTAMP WITH TIME ZONE")]), "ts")

    def test_oracle_number(self):
        self.assertEqual(
            load_entity.pick_partition_column(registry.by_conn_type("oracle"), [("client_id", "NUMBER")]),
            "client_id",
        )

    def test_stub_adapter_accepts_own_type_without_loader_changes(self):
        self.assertEqual(
            load_entity.pick_partition_column(StubPartitionDialect(), [("id", "int8")]),
            "id",
        )
        self.assertIsNone(self.pick([("id", "int8")]))

    def test_no_suitable_column(self):
        candidates = [("code", "varchar"), ("payload", "nvarchar")]
        self.assertIsNone(self.pick(candidates))


class TestBoundLiteral(unittest.TestCase):
    """Rendering of JDBC partition bounds."""

    def test_integral_decimal_collapses_to_integer(self):
        self.assertEqual(
            load_entity._bound_literal(Decimal("1.0000000000")),
            "1",
        )

    def test_fractional_decimal_kept_as_is(self):
        self.assertEqual(
            load_entity._bound_literal(Decimal("12.50")),
            "12.50",
        )

    def test_int_and_float_untouched(self):
        self.assertEqual(load_entity._bound_literal(7), "7")
        self.assertEqual(load_entity._bound_literal(1.5), "1.5")



class TestMain(unittest.TestCase):
    """The entry point requires a managed source and dispatches only to managed_load."""

    def test_missing_source_id_rejected(self):
        with self.assertRaisesRegex(ValueError, "source-id"):
            load_entity.main(build_argv("--incremental-column", "_batch_id"))

    def test_dispatches_to_managed_load(self):
        spark = MagicMock()
        session = MagicMock()
        session.builder.appName.return_value.getOrCreate.return_value = spark
        sql = types.ModuleType("pyspark.sql")
        sql.SparkSession = session
        pyspark = types.ModuleType("pyspark")
        pyspark.sql = sql
        managed = MagicMock()
        with patch.dict(sys.modules, {"pyspark": pyspark, "pyspark.sql": sql}), patch.object(
            load_entity, "managed_load", managed
        ):
            load_entity.main(build_argv("--source-id", "bank", "--dag-id", "dag", "--run-id", "run"))
        spark.stop.assert_called_once()
        managed.assert_called_once()
        self.assertIs(managed.call_args.args[0], spark)
        self.assertEqual(managed.call_args.args[1].source_id, "bank")



class TestEmptyIncrementalWindow(unittest.TestCase):
    """Only a zero-lookback window with equal typed bounds is provably empty."""

    def batch(self, mode="incremental", lower="10", upper="10"):
        return {"mode": mode, "lower_bound": lower, "upper_bound": upper}

    def test_equal_bounds_with_lookback_gap_is_empty(self):
        self.assertTrue(load_entity.empty_incremental_window(self.batch()))
        self.assertTrue(load_entity.empty_incremental_window(self.batch(lower="2026-10-26", upper="2026-10-26")))

    def test_strict_window_with_distinct_bounds_is_not_empty(self):
        for changes in ({"lower": "7"}, {"upper": "12"}, {"lower": None}, {"lower": None, "upper": None},
                        {"mode": "full_snapshot"}, {"mode": "full_snapshot", "lower": None, "upper": None}):
            with self.subTest(changes=changes):
                self.assertFalse(load_entity.empty_incremental_window(self.batch(**changes)))


if __name__ == "__main__":
    unittest.main()
