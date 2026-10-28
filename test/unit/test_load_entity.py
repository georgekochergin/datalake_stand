"""Unit tests for spark/raw/load_entity.py (pure functions)."""

import importlib.util
import sys
import types
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dlh_ingestion.sources.rdbms import jdbc

MODULE_PATH = Path(__file__).resolve().parents[2] / "spark" / "raw" / "rdbms" / "load_entity.py"
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
        args = load_entity.parse_args(
            build_argv("--incremental-column", "_batch_id", "--watermark-value", "100")
        )
        self.assertEqual(args.incremental_column, "_batch_id")
        self.assertEqual(args.watermark_value, "100")

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
        self.assertIsNone(args.watermark_value)

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




class TestBuildReadQuery(unittest.TestCase):
    """JDBC dbtable expression construction."""

    def test_full_snapshot_returns_quoted_table(self):
        self.assertEqual(
            load_entity.build_read_query("retail_bank.clients", None, None, MSSQL_URL),
            "[retail_bank].[clients]",
        )
        self.assertEqual(
            load_entity.build_read_query("trading_core.transactions", None, None, ORACLE_URL),
            '"trading_core"."transactions"',
        )

    def test_incremental_builds_subquery_mssql(self):
        self.assertEqual(
            load_entity.build_read_query("retail_bank.clients", "_batch_id", "100", MSSQL_URL),
            "(SELECT * FROM [retail_bank].[clients] WHERE [_batch_id] > 100) q",
        )

    def test_incremental_builds_subquery_oracle_quotes_table_and_column(self):
        self.assertEqual(
            load_entity.build_read_query("trading_core.transactions", "_batch_id", "100", ORACLE_URL),
            '(SELECT * FROM "trading_core"."transactions" WHERE "_batch_id" > 100) q',
        )

    def test_bootstrap_without_watermark_reads_everything(self):
        self.assertEqual(
            load_entity.build_read_query("retail_bank.clients", "_batch_id", None, MSSQL_URL),
            "[retail_bank].[clients]",
        )

    def test_non_numeric_watermark_rejected(self):
        with self.assertRaises(ValueError):
            load_entity.build_read_query("retail_bank.clients", "_batch_id", "1; DROP TABLE x", MSSQL_URL)

    def test_injection_in_source_table_rejected(self):
        with self.assertRaises(ValueError):
            load_entity.build_read_query("retail_bank.clients; DROP TABLE x", None, None, MSSQL_URL)

    def test_quoted_fragment_allowed(self):
        self.assertEqual(
            load_entity.build_read_query('retail_bank."sub.clients"', None, None, MSSQL_URL),
            "[retail_bank].[sub.clients]",
        )

    def test_unknown_dialect_rejected(self):
        with self.assertRaises(ValueError):
            load_entity.build_read_query("retail_bank.clients", None, None, "")
        with self.assertRaises(ValueError):
            load_entity.build_read_query("retail_bank.clients", None, None, "jdbc:postgresql://h/db")


class TestPickPartitionColumn(unittest.TestCase):
    """Auto-evaluation of the parallel-read column."""

    def test_numeric_pk_preferred_over_later_date(self):
        candidates = [("client_id", "int"), ("updated_at", "datetime2")]
        self.assertEqual(load_entity.pick_partition_column(candidates), "client_id")

    def test_string_column_skipped(self):
        candidates = [("code", "nvarchar"), ("updated_at", "datetime2")]
        self.assertEqual(load_entity.pick_partition_column(candidates), "updated_at")

    def test_order_respected(self):
        candidates = [("code", "varchar"), ("num", "bigint"), ("other", "int")]
        self.assertEqual(load_entity.pick_partition_column(candidates), "num")

    def test_identity_type_variant(self):
        self.assertEqual(
            load_entity.pick_partition_column([("id", "int identity")]),
            "id",
        )

    def test_timestamp_with_zone_variant(self):
        self.assertEqual(
            load_entity.pick_partition_column([("ts", "TIMESTAMP WITH TIME ZONE")]),
            "ts",
        )

    def test_oracle_number(self):
        self.assertEqual(
            load_entity.pick_partition_column([("client_id", "NUMBER")]),
            "client_id",
        )

    def test_no_suitable_column(self):
        candidates = [("code", "varchar"), ("payload", "nvarchar")]
        self.assertIsNone(load_entity.pick_partition_column(candidates))


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



class _FakeType:
    """Minimal stand-in for a pyspark DataType."""

    def __init__(self, name: str, scale: int | None = None):
        self._name = name
        self.scale = scale

    def typeName(self) -> str:
        return self._name


class _FakeField:
    """Minimal stand-in for a pyspark StructField."""

    def __init__(self, name: str, data_type: _FakeType):
        self.name = name
        self.dataType = data_type


class _FakeSchema:
    """Minimal stand-in for a pyspark StructType."""

    def __init__(self, *fields: _FakeField):
        self.fields = list(fields)



class TestMain(unittest.TestCase):
    """Exercise empty-source and partitioning behavior without installing Spark."""

    def run_loader(self, extra=(), schema=None):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        source = MagicMock()
        source.schema = schema or _FakeSchema(_FakeField("_batch_id", _FakeType("long")))
        frame = source.withColumn.return_value.withColumn.return_value.cache.return_value
        frame.count.return_value = 0
        frame.agg.return_value.collect.return_value = [(None,)]
        reader = MagicMock()
        reader.load.return_value = source
        reader.option.return_value = reader
        sql = types.ModuleType("pyspark.sql")
        sql.SparkSession = MagicMock()
        sql.SparkSession.builder.appName.return_value.getOrCreate.return_value = spark
        functions = types.ModuleType("pyspark.sql.functions")
        functions.current_timestamp = MagicMock()
        functions.lit = MagicMock()
        functions.max = MagicMock()
        pyspark = types.ModuleType("pyspark")
        pyspark.sql = sql
        sql.functions = functions
        with patch.dict(sys.modules, {"pyspark": pyspark, "pyspark.sql": sql, "pyspark.sql.functions": functions}), patch.object(
            load_entity, "_jdbc_reader", return_value=reader
        ), patch.object(load_entity, "schema_columns", return_value=[(field.name, "bigint") for field in source.schema.fields]), patch.object(
            jdbc, "key_columns", return_value=[]
        ) as detect, patch.object(load_entity, "_read_bounds", return_value=(None, None)) as bounds:
            try:
                load_entity.main(build_argv(*extra))
            finally:
                spark.stop.assert_called_once()
        return spark, reader, frame, detect, bounds

    def test_no_parallelism_skips_metadata_and_bounds(self):
        for extra in ((), ("--incremental-column", "_batch_id"), ("--partition-column", "_batch_id")):
            with self.subTest(extra=extra):
                spark, reader, frame, detect, bounds = self.run_loader(("--no-parallelism", *extra))
                detect.assert_not_called()
                bounds.assert_not_called()
                reader.option.assert_not_called()
                reader.load.assert_called_once()

    def test_empty_bounds_still_create_and_append(self):
        spark, reader, frame, detect, bounds = self.run_loader(("--incremental-column", "_batch_id"))
        bounds.assert_called_once()
        reader.option.assert_not_called()
        self.assertTrue(any(call.args[0].startswith("CREATE TABLE raw.bank.") for call in spark.sql.call_args_list))
        writer = frame.writeTo.return_value
        writer.option.assert_called_once_with("mergeSchema", "true")
        writer.option.return_value.option.assert_called_once_with("check-ordering", "false")
        writer.option.return_value.option.return_value.append.assert_called_once()
        frame.unpersist.assert_called_once()

    def test_empty_incremental_schema_is_validated(self):
        for schema in (
            _FakeSchema(_FakeField("other", _FakeType("long"))),
            _FakeSchema(_FakeField("_batch_id", _FakeType("string"))),
        ):
            with self.subTest(schema=schema), self.assertRaises(ValueError):
                self.run_loader(("--incremental-column", "_batch_id"), schema)

    def test_metadata_collision_still_rejected_for_empty_source(self):
        for name in ("__process_id", "__load_dttm", "__batch_id", "__extract_position"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "collides"):
                self.run_loader(("--no-parallelism",), _FakeSchema(_FakeField(name, _FakeType("long"))))



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


class TestValidateIncrementalColumn(unittest.TestCase):
    """Existence and integral or temporal type checks on the incremental column."""

    def schema_with(self, data_type):
        return _FakeSchema(
            _FakeField("account_id", _FakeType("bigint")),
            _FakeField("_batch_id", data_type),
        )

    def test_missing_column_rejected(self):
        with self.assertRaises(ValueError):
            load_entity.validate_incremental_column(
                _FakeSchema(_FakeField("account_id", _FakeType("bigint"))), "_batch_id"
            )

    def test_integral_types_accepted(self):
        for type_name in ("byte", "short", "integer", "long"):
            load_entity.validate_incremental_column(
                self.schema_with(_FakeType(type_name)), "_batch_id"
            )

    def test_integral_decimal_accepted(self):
        load_entity.validate_incremental_column(
            self.schema_with(_FakeType("decimal", scale=0)), "_batch_id"
        )

    def test_fractional_decimal_rejected(self):
        with self.assertRaises(ValueError):
            load_entity.validate_incremental_column(
                self.schema_with(_FakeType("decimal", scale=2)), "_batch_id"
            )

    def test_temporal_types_accepted(self):
        for type_name in ("date", "timestamp", "timestamp_ntz"):
            with self.subTest(type_name=type_name):
                load_entity.validate_incremental_column(self.schema_with(_FakeType(type_name)), "_batch_id")

    def test_unsupported_types_rejected(self):
        for type_name in ("string", "boolean", "double", "float", "binary"):
            with self.assertRaises(ValueError):
                load_entity.validate_incremental_column(
                    self.schema_with(_FakeType(type_name)), "_batch_id"
                )


if __name__ == "__main__":
    unittest.main()
