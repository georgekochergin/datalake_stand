"""Unit tests for spark/raw/load_entity.py (pure functions)."""

import importlib.util
import sys
import types
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.sources.rdbms import jdbc
from ingestion.sources.rdbms import registry

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
            "--lookback-given",
        ))
        self.assertEqual((args.source_id, args.dag_id, args.run_id), ("bank", "dag", "run"))
        self.assertEqual((args.lookback_size, args.lookback_unit), (3, "h"))
        self.assertTrue(args.lookback_given)
        self.assertFalse(hasattr(args, "force_full"))

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

    def test_jdbc_partitioning_switch(self):
        self.assertFalse(load_entity.parse_args(build_argv()).no_jdbc_partitioning)
        args = load_entity.parse_args(
            build_argv("--no-jdbc-partitioning", "--num-partitions", "7")
        )
        self.assertTrue(args.no_jdbc_partitioning)
        self.assertEqual(args.num_partitions, 7)

    def test_invalid_incremental_column(self):
        with self.assertRaises(ValueError):
            load_entity.parse_args(build_argv("--incremental-column", "Client-Id"))




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

    def batch(self, lower="10", upper="10", lookback=0):
        return {"lower_bound": lower, "upper_bound": upper, "lookback_size": lookback}

    def test_equal_bounds_with_zero_lookback_is_empty(self):
        self.assertTrue(load_entity.empty_incremental_window(self.batch()))
        self.assertTrue(load_entity.empty_incremental_window(self.batch(lower="2026-10-26", upper="2026-10-26")))

    def test_lookback_window_with_equal_bounds_is_not_empty(self):
        self.assertFalse(load_entity.empty_incremental_window(self.batch(lookback=3)))
        self.assertFalse(load_entity.empty_incremental_window(self.batch(lower=None, lookback=3)))

    def test_strict_window_with_distinct_bounds_is_not_empty(self):
        for changes in ({"lower": "7"}, {"upper": "12"}, {"lower": None}, {"lower": None, "upper": None}):
            with self.subTest(changes=changes):
                self.assertFalse(load_entity.empty_incremental_window(self.batch(**changes)))


def fake_pyspark_types():
    """A pyspark.sql.types double with sibling temporal classes, as upstream."""
    module = types.ModuleType("pyspark.sql.types")
    for name in (
        "BinaryType", "BooleanType", "ByteType", "DateType", "DecimalType",
        "DoubleType", "FloatType", "IntegerType", "LongType", "ShortType",
        "StringType", "TimestampNTZType", "TimestampType",
    ):
        setattr(module, name, type(name, (), {}))
    return module


class RecordingReader:
    """A JDBC reader double recording option calls and returning itself."""

    def __init__(self, collected=None):
        self.options = {}
        self.collected = collected

    def format(self, _):
        """Accept the format step."""
        return self

    def option(self, key, value):
        """Record one option and keep chaining."""
        self.options[key] = value
        return self

    def load(self):
        """Return the frame double."""
        return self

    def cache(self):
        """Return the cached frame double."""
        return self

    def collect(self):
        """Return the prepared rows."""
        return self.collected


class TestRenderSourceTable(unittest.TestCase):
    """Dialect-quoted rendering of the source table reference."""

    def test_mssql_brackets(self):
        dialect = registry.by_conn_type("mssql")
        self.assertEqual(
            load_entity.render_source_table(dialect, "retail_bank.clients"),
            "[retail_bank].[clients]",
        )

    def test_oracle_quotes_preserve_case(self):
        dialect = registry.by_conn_type("oracle")
        self.assertEqual(
            load_entity.render_source_table(dialect, 'retail_bank."Clients"'),
            '"retail_bank"."Clients"',
        )

    def test_expression_passthrough_only_when_allowed(self):
        dialect = registry.by_conn_type("mssql")
        expression = "(SELECT * FROM s.t WHERE 1=0)"
        self.assertEqual(
            load_entity.render_source_table(dialect, expression, allow_expression=True),
            expression,
        )
        with self.assertRaises(ValueError):
            load_entity.render_source_table(dialect, expression)

    def test_malformed_reference_rejected(self):
        with self.assertRaises(ValueError):
            load_entity.render_source_table(registry.by_conn_type("mssql"), "a..b")


class TestJdbcReader(unittest.TestCase):
    """Reader construction from CLI arguments and dialect options."""

    def test_mssql_reader_options(self):
        spark = MagicMock()
        reader = RecordingReader()
        spark.read.format.return_value = reader
        args = load_entity.parse_args(build_argv())
        built = load_entity._jdbc_reader(spark, args, "(SELECT 1) q")
        self.assertIs(built, reader)
        self.assertEqual(reader.options, {
            "url": MSSQL_URL, "dbtable": "(SELECT 1) q", "user": "sydafa",
            "password": "secret", "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "preferTimestampNTZ": "true",
        })

    def test_oracle_driver_selected_by_url(self):
        spark = MagicMock()
        reader = RecordingReader()
        spark.read.format.return_value = reader
        args = load_entity.parse_args(build_argv("--jdbc-url", ORACLE_URL))
        load_entity._jdbc_reader(spark, args, "q")
        self.assertEqual(reader.options["driver"], "oracle.jdbc.OracleDriver")


class TestReadBounds(unittest.TestCase):
    """MIN/MAX probing over the read query."""

    def test_query_shape_and_tuple(self):
        spark = MagicMock()
        args = load_entity.parse_args(build_argv())
        reader = RecordingReader(collected=[(1, 100)])
        with patch.object(load_entity, "_jdbc_reader", return_value=reader) as builder:
            self.assertEqual(load_entity._read_bounds(spark, args, "(SELECT 1) q", "client_id"), (1, 100))
        query = builder.call_args.args[2]
        self.assertEqual(
            query,
            "(SELECT MIN([client_id]) AS lower_bound, MAX([client_id]) AS upper_bound "
            "FROM (SELECT 1) q) bounds",
        )


class TestSourcePosition(unittest.TestCase):
    """Source-side incremental inspection over a stubbed JDBC layer."""

    def args(self, **extra):
        argv = build_argv("--incremental-column", "_batch_id", *sum(([k, v] for k, v in extra.items()), []))
        return load_entity.parse_args(argv)

    def inspect(self, metadata, row):
        spark = MagicMock()
        with patch.object(load_entity.jdbc, "connection") as managed, patch.object(
            load_entity.jdbc, "column_metadata", return_value=metadata
        ) as columns, patch.object(load_entity.jdbc, "first_row", return_value=row) as first:
            result = load_entity.source_position(spark, self.args())
        managed.assert_called_once()
        self.assertIn("FROM [retail_bank].[clients]", columns.call_args.args[1])
        return result, first.call_args.args[1]

    def test_regular_inspection(self):
        result, sql = self.inspect([("_batch_id", "bigint", 19, 0)], ["10", "5", "5"])
        self.assertEqual(result, ("bigint", "10", False, False))
        self.assertEqual(sql, "SELECT MAX([_batch_id]), COUNT(*), COUNT([_batch_id]) FROM [retail_bank].[clients]")

    def test_empty_and_null_diagnostics(self):
        self.assertEqual(self.inspect([("_batch_id", "bigint", 19, 0)], ["0", "0", "0"])[0][2:], (True, False))
        self.assertEqual(self.inspect([("_batch_id", "bigint", 19, 0)], ["5", "5", "4"])[0][2:], (False, True))

    def test_oracle_temporal_kind(self):
        spark = MagicMock()
        args = load_entity.parse_args(build_argv("--jdbc-url", ORACLE_URL, "--incremental-column", "_batch_id"))
        with patch.object(load_entity.jdbc, "connection"), patch.object(
            load_entity.jdbc, "column_metadata", return_value=[("_batch_id", "TIMESTAMP(6) WITH TIME ZONE", 0, 6)]
        ), patch.object(load_entity.jdbc, "first_row", return_value=["2026-01-01T00:00:00.000000Z", "3", "3"]) as first:
            result = load_entity.source_position(spark, args)
        self.assertEqual(result[0], "timestamp_instant(6)")
        self.assertIn("TO_CHAR", first.call_args.args[1])


class TestPlanBatch(unittest.TestCase):
    """Batch resolution and planning before any source window is read."""

    def setUp(self):
        from ingestion import meta

        self.meta = meta
        self.spark = MagicMock()

    def args(self, *extra):
        argv = build_argv("--source-id", "bank", "--dag-id", "dag", "--run-id", "run", *extra)
        return load_entity.parse_args(argv)

    def plan(self, batch, planned=None, position=None, args=None):
        with patch.object(self.meta, "get_batch", return_value=batch) as got, patch.object(
            self.meta, "plan_batch", return_value=planned
        ) as plan, patch.object(load_entity, "source_position", return_value=position) as probe:
            result = load_entity._plan_batch(self.spark, args or self.args(), lambda: None)
        return result, got, plan, probe

    def test_full_slice_entity_skips_registry(self):
        result, got, plan, probe = self.plan(None)
        self.assertIsNone(result)
        got.assert_not_called()
        plan.assert_not_called()
        probe.assert_not_called()

    def test_incremental_entity_captures_detected_position(self):
        planned = {"status": "PLANNED"}
        args = self.args("--incremental-column", "_batch_id")
        result, _, plan, probe = self.plan(None, planned=planned,
                                           position=("bigint", "10", False, False), args=args)
        self.assertIs(result, planned)
        self.assertEqual(
            (plan.call_args.kwargs["incremental_type"], plan.call_args.kwargs["upper_bound"],
             plan.call_args.kwargs["empty_at_plan"]),
            ("bigint", "10", False),
        )
        probe.assert_called_once()

    def test_default_lookback_resolved_by_runtime_kind(self):
        args = self.args("--incremental-column", "_batch_id",
                         "--lookback-size", "3", "--lookback-unit", "h")
        _, _, plan, _ = self.plan(None, planned={"status": "PLANNED"},
                                  position=("bigint", "10", False, False), args=args)
        self.assertEqual((plan.call_args.kwargs["lookback_size"],
                          plan.call_args.kwargs["lookback_unit"]), (0, None))
        timestamp_args = self.args("--incremental-column", "updated_at",
                                   "--lookback-size", "3", "--lookback-unit", "h")
        _, _, plan, _ = self.plan(None, planned={"status": "PLANNED"},
                                  position=("timestamp_instant(6)", "2026-01-01T00:00:00.000000Z", False, False),
                                  args=timestamp_args)
        self.assertEqual((plan.call_args.kwargs["lookback_size"],
                          plan.call_args.kwargs["lookback_unit"]), (3, "h"))

    def test_succeeded_batch_short_circuits_without_source(self):
        batch = {"target_table": "raw.bank.retail_bank__clients",
                 "incremental_column": "_batch_id", "status": "SUCCEEDED",
                 "row_count": 5, "finished_at": "2026-11-06"}
        args = self.args("--incremental-column", "_batch_id")
        with self.assertLogs("load_entity", level="INFO") as logs:
            result, _, plan, probe = self.plan(batch, args=args)
        self.assertIs(result, batch)
        plan.assert_not_called()
        probe.assert_not_called()
        self.assertTrue(any("repeat read skipped" in line for line in logs.output))

    def test_existing_batch_target_mismatch_rejected(self):
        batch = {"target_table": "raw.bank.other", "status": "PLANNED"}
        args = self.args("--incremental-column", "_batch_id")
        with self.assertRaisesRegex(ValueError, "target does not match"):
            self.plan(batch, args=args)

    def test_retry_after_manifest_column_change_rejected(self):
        for status in ("PLANNED", "RUNNING", "FAILED", "SUCCEEDED"):
            with self.subTest(status=status):
                batch = {"target_table": "raw.bank.retail_bank__clients",
                         "incremental_column": "_batch_id", "status": status,
                         "row_count": 5, "finished_at": "2026-11-07"}
                args = self.args("--incremental-column", "other_position")
                with self.assertRaisesRegex(ValueError, "incremental column does not match"):
                    self.plan(batch, args=args)

    def test_retry_replays_frozen_window_without_source_access(self):
        batch = {"target_table": "raw.bank.retail_bank__clients",
                 "incremental_column": "_batch_id", "incremental_type": "bigint",
                 "status": "RUNNING"}
        args = self.args("--incremental-column", "_batch_id")
        result, _, plan, probe = self.plan(batch, position=("bigint", "99", False, True), args=args)
        self.assertIs(result, batch)
        plan.assert_not_called()
        probe.assert_not_called()

    def test_null_incremental_value_rejected_for_new_batch(self):
        args = self.args("--incremental-column", "_batch_id")
        with self.assertRaisesRegex(ValueError, "NULL incremental"):
            self.plan(None, position=("bigint", "10", False, True), args=args)

    def test_calculate_lower_rejects_regression(self):
        _, _, plan, _ = self.plan(None, planned={"status": "PLANNED"},
                                  position=("bigint", "10", False, False),
                                  args=self.args("--incremental-column", "_batch_id"))
        calculate_lower = plan.call_args.kwargs["calculate_lower"]
        with self.assertRaisesRegex(ValueError, "SOURCE_POSITION_REGRESSION"):
            calculate_lower("12")
        self.assertEqual(calculate_lower("8"), ("8", ">"))

    def test_calculate_lower_bootstrap_has_no_lower(self):
        args = self.args("--incremental-column", "_batch_id")
        _, _, plan, _ = self.plan(None, planned={"status": "PLANNED"},
                                  position=("bigint", "10", False, False), args=args)
        self.assertEqual(plan.call_args.kwargs["calculate_lower"](None), (None, ">"))

    def test_calculate_lower_empty_source_with_watermark_rejected(self):
        args = self.args("--incremental-column", "_batch_id")
        _, _, plan, _ = self.plan(None, planned={"status": "PLANNED"},
                                  position=("bigint", None, True, False), args=args)
        with self.assertRaisesRegex(ValueError, "SOURCE_POSITION_REGRESSION"):
            plan.call_args.kwargs["calculate_lower"]("12")

    def test_invalid_lookback_rejected_before_planning(self):
        args = self.args("--incremental-column", "_batch_id", "--lookback-size", "1",
                         "--lookback-unit", "h", "--lookback-given")
        with self.assertRaisesRegex(ValueError, "forbids a unit"):
            self.plan(None, position=("bigint", "10", False, False), args=args)


class TestReadExtractFrame(unittest.TestCase):
    """Frozen-window query assembly and heuristic partition discovery."""

    def setUp(self):
        self.spark = MagicMock()

    def key_metadata(self, primary_key=(), unique_columns=(), columns=()):
        """Build KeyMetadata from (name, type) shorthand pairs."""
        return jdbc.KeyMetadata(
            primary_key=tuple(primary_key),
            unique_columns=tuple(unique_columns),
            columns={
                name.lower(): jdbc.ColumnMeta(name, type_name, None, None, True)
                for name, type_name in columns
            },
        )

    def frame(self, args, batch, kind, projection=None, keys=None, bounds=(1, 100)):
        reader = RecordingReader()
        with patch.object(load_entity.jdbc, "compatibility_projection", return_value=projection), patch.object(
            load_entity.jdbc, "key_metadata", return_value=keys or self.key_metadata()
        ) as key_call, patch.object(load_entity, "_read_bounds", return_value=bounds) as probe, patch.object(
            load_entity, "_jdbc_reader", return_value=reader
        ) as builder:
            result = load_entity._read_extract_frame(self.spark, args, batch, kind)
        return result, reader, builder.call_args.args[2], key_call

    def test_snapshot_without_keys_reads_single_partition(self):
        args = load_entity.parse_args(build_argv())
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        result, reader, query, key_call = self.frame(args, batch, None)
        self.assertIs(result, reader)
        self.assertEqual(
            query,
            "(SELECT extract_source.* FROM (SELECT * FROM [retail_bank].[clients]) extract_source) extract_batch",
        )
        key_call.assert_called_once()
        self.assertNotIn("partitionColumn", reader.options)

    def test_snapshot_with_integer_pk_partitions_read(self):
        args = load_entity.parse_args(build_argv())
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(primary_key=["client_id"], columns=[("client_id", "int")])
        _, reader, _, _ = self.frame(args, batch, None, keys=keys)
        self.assertEqual(
            {key: reader.options[key] for key in ("partitionColumn", "lowerBound", "upperBound", "numPartitions")},
            {"partitionColumn": "client_id", "lowerBound": "1", "upperBound": "100", "numPartitions": 4},
        )

    def test_incremental_column_has_no_partition_priority(self):
        args = load_entity.parse_args(build_argv("--incremental-column", "_batch_id"))
        batch = {"lower_bound": "5", "lower_bound_op": ">",
                 "upper_bound": "10", "empty_at_plan": False}
        keys = self.key_metadata(primary_key=["transaction_id"],
                                 columns=[("transaction_id", "bigint"), ("_batch_id", "int")])
        _, reader, query, _ = self.frame(args, batch, "bigint", keys=keys, bounds=(5, 10))
        self.assertIn("[_batch_id] > 5 AND [_batch_id] <= 10", query)
        self.assertIn(', [_batch_id] AS "__extract_position"', query)
        self.assertEqual(reader.options["partitionColumn"], "transaction_id")

    def test_timestamp_column_partitions_by_derived_day(self):
        args = load_entity.parse_args(build_argv("--jdbc-url", ORACLE_URL))
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(columns=[("updated_at", "TIMESTAMP(9) WITH TIME ZONE")])
        _, reader, query, _ = self.frame(args, batch, None, keys=keys)
        self.assertIn(
            'CAST(TO_NUMBER(TO_CHAR(SYS_EXTRACT_UTC(extract_source."updated_at"), \'J\'))'
            ' AS NUMBER(10,0)) AS "__jdbc_day"',
            query,
        )
        self.assertEqual(reader.options["partitionColumn"], "__jdbc_day")

    def test_date_column_partitions_directly(self):
        args = load_entity.parse_args(build_argv())
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(columns=[("posting_date", "date")])
        _, reader, query, _ = self.frame(args, batch, None, keys=keys)
        self.assertNotIn("__jdbc_day", query)
        self.assertEqual(reader.options["partitionColumn"], "posting_date")

    def test_composite_pk_and_no_candidates_single_partition(self):
        args = load_entity.parse_args(build_argv())
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(primary_key=["code", "num"],
                                 columns=[("code", "nvarchar"), ("num", "int")])
        _, reader, _, _ = self.frame(args, batch, None, keys=keys)
        self.assertNotIn("partitionColumn", reader.options)

    def test_equal_bounds_read_single_partition(self):
        args = load_entity.parse_args(build_argv())
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(primary_key=["client_id"], columns=[("client_id", "int")])
        _, reader, _, _ = self.frame(args, batch, None, keys=keys, bounds=(7, 7))
        self.assertNotIn("partitionColumn", reader.options)

    def test_aligned_lookback_lower_uses_inclusive_operator(self):
        args = load_entity.parse_args(build_argv("--incremental-column", "_batch_id"))
        batch = {"lower_bound": "2026-10-23T00:00:00Z",
                 "lower_bound_op": ">=", "upper_bound": "2026-10-26T12:34:56Z", "empty_at_plan": False}
        _, _, query, _ = self.frame(args, batch, "timestamp_instant(0)")
        self.assertIn("[_batch_id] >= ", query)

    def test_missing_lower_bound_fully_dropped(self):
        args = load_entity.parse_args(build_argv("--incremental-column", "_batch_id"))
        batch = {"lower_bound": None, "lower_bound_op": ">=",
                 "upper_bound": "10", "empty_at_plan": False}
        _, _, query, _ = self.frame(args, batch, "bigint")
        self.assertNotIn(">=", query)
        self.assertIn("[_batch_id] <= 10", query)

    def test_empty_window_proves_no_rows(self):
        args = load_entity.parse_args(build_argv("--incremental-column", "_batch_id"))
        batch = {"lower_bound": "10", "lower_bound_op": ">",
                 "upper_bound": "10", "empty_at_plan": True}
        _, _, query, _ = self.frame(args, batch, "bigint")
        self.assertIn("1=0", query)

    def test_disabled_partitioning_skips_probing(self):
        args = load_entity.parse_args(build_argv("--no-jdbc-partitioning"))
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        with patch.object(load_entity, "_jdbc_reader", return_value=RecordingReader()), patch.object(
            load_entity.jdbc, "key_metadata"
        ) as keys, patch.object(load_entity, "_read_bounds") as probe:
            load_entity._read_extract_frame(self.spark, args, batch, None)
        keys.assert_not_called()
        probe.assert_not_called()

    def test_compatibility_projection_rewrites_select(self):
        args = load_entity.parse_args(build_argv("--jdbc-url", ORACLE_URL))
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        projection = ['extract_source."a"', 'SYS_EXTRACT_UTC(extract_source."b") AS "b"']
        _, _, query, _ = self.frame(args, batch, None, projection=projection)
        self.assertIn('SELECT extract_source."a", SYS_EXTRACT_UTC(extract_source."b") AS "b"', query)
        self.assertIn('FROM (SELECT * FROM "retail_bank"."clients")', query)


class FakeSummary:
    """A row-like aggregation result."""

    def __init__(self, values):
        self.values = values

    def __getitem__(self, key):
        """Look up one aggregated value."""
        return self.values[key]


class TestAppendRaw(unittest.TestCase):
    """Raw append: aggregation, DDL bootstrap, and metadata column stamping."""

    def setUp(self):
        self.spark = MagicMock()

    def functions(self):
        module = types.ModuleType("pyspark.sql.functions")
        for name in ("count", "max", "lit", "current_timestamp"):
            setattr(module, name, MagicMock())
        return module

    def functions_modules(self):
        """Register the functions and types doubles with their parent packages."""
        root = types.ModuleType("pyspark")
        sql = types.ModuleType("pyspark.sql")
        functions = self.functions()
        sql.functions = functions
        root.sql = sql
        return {
            "pyspark": root,
            "pyspark.sql": sql,
            "pyspark.sql.functions": functions,
            "pyspark.sql.types": fake_pyspark_types(),
        }

    def append(self, args, summary_values, fields, exists=False, incremental=True):
        frame = MagicMock()
        frame.agg.return_value.first.return_value = FakeSummary(summary_values)
        clean = MagicMock()
        doubles = self.functions_modules()
        long_type = doubles["pyspark.sql.types"].LongType()
        clean.schema.fields = [
            types.SimpleNamespace(name=field, dataType=long_type)
            if isinstance(field, str) else field
            for field in fields
        ]
        frame.drop.return_value = clean
        self.spark.catalog.tableExists.return_value = exists
        argv = build_argv(*((["--incremental-column", "_batch_id"] if incremental else [])))
        args = args or load_entity.parse_args(argv)
        with patch.dict(sys.modules, doubles):
            return load_entity._append_raw(
                self.spark, args, frame, "bigint" if incremental else None
            ), clean

    def test_rows_appended_with_metadata_columns(self):
        result, clean = self.append(None, {"n": 5, "position": "10"}, ["client_id"])
        self.assertEqual(result, (5, "10"))
        first = clean.withColumn
        second = first.return_value.withColumn
        self.assertEqual(first.call_args.args[0], "__load_dttm")
        self.assertEqual(second.call_args.args[0], "__process_id")
        portion = second.return_value
        portion.writeTo.assert_called_once_with("raw.bank.retail_bank__clients")
        write = portion.writeTo.return_value
        self.assertEqual(write.option.call_args.args, ("mergeSchema", "true"))
        self.assertEqual(write.option.return_value.option.call_args.args, ("check-ordering", "false"))
        write.option.return_value.option.return_value.append.assert_called_once_with()
        self.spark.catalog.tableExists.assert_called_once_with("raw.bank.retail_bank__clients")

    def test_first_load_creates_namespace_and_table(self):
        self.append(None, {"n": 0}, ["client_id"], exists=False, incremental=False)
        executed = [call.args[0] for call in self.spark.sql.call_args_list]
        self.assertEqual(executed[0], "CREATE NAMESPACE IF NOT EXISTS raw.bank")
        self.assertTrue(executed[1].startswith("CREATE TABLE raw.bank.retail_bank__clients"))

    def test_existing_table_skips_ddl(self):
        self.append(None, {"n": 3}, ["client_id"], exists=True, incremental=False)
        executed = [call.args[0] for call in self.spark.sql.call_args_list]
        self.assertTrue(all("CREATE TABLE" not in sql for sql in executed))

    def test_zero_rows_verify_schema_without_write(self):
        result, clean = self.append(None, {"n": 0, "position": None}, ["client_id"])
        self.assertEqual(result, (0, None))
        clean.withColumn.assert_not_called()

    def test_metadata_collision_rejected(self):
        with self.assertRaisesRegex(ValueError, "collides"):
            self.append(None, {"n": 1, "position": "1"}, ["__process_id"])


class TestManagedLoad(unittest.TestCase):
    """Managed execution: planning, empty windows, finalization, and failure."""

    def setUp(self):
        from ingestion import meta

        self.meta = meta
        self.spark = MagicMock()
        self.source = MagicMock()
        psycopg2 = types.ModuleType("psycopg2")
        psycopg2.connect = MagicMock(return_value=MagicMock())
        self.psycopg2 = psycopg2

    def batch(self, **changes):
        values = {"process_id": 7, "status": "RUNNING", "incremental_type": "bigint",
                  "lower_bound": "5", "lower_bound_op": ">",
                  "upper_bound": "10", "lookback_size": 0,
                  "watermark_before": None, "reason": "bootstrap"}
        values.update(changes)
        return values

    def args(self, *extra):
        return load_entity.parse_args(build_argv(
            "--source-id", "bank", "--dag-id", "dag", "--run-id", "run", "--process-id", "7", *extra
        ))

    def load(self, batch, args=None, appended=(5, "10"), append_error=None):
        """Run managed_load with every collaborator patched; return the patchers' mocks."""
        args = args or self.args("--incremental-column", "_batch_id")
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.object(
            load_entity, "_plan_batch", return_value=batch
        ), patch.object(load_entity, "_read_extract_frame", return_value=self.source) as read_frame, patch.object(
            load_entity, "_append_raw",
            **({"side_effect": append_error} if append_error else {"return_value": appended}),
        ) as append, patch.object(self.meta, "start_batch") as started, patch.object(
            self.meta, "finalize_batch"
        ) as finalize, patch.object(self.meta, "fail_batch") as failed:
            load_entity.managed_load(self.spark, args)
        return types.SimpleNamespace(read_frame=read_frame, append=append, started=started,
                                     finalize=finalize, failed=failed)

    def test_succeeded_batch_is_noop(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.object(
            load_entity, "_plan_batch", return_value=self.batch(status="SUCCEEDED")
        ):
            load_entity.managed_load(self.spark, load_entity.parse_args(build_argv()))
        self.source.unpersist.assert_not_called()

    def test_full_slice_entity_loads_without_registry(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.object(
            load_entity, "_plan_batch", return_value=None
        ), patch.object(load_entity, "_read_extract_frame", return_value=self.source) as read_frame, patch.object(
            load_entity, "_append_raw", return_value=(7, None)
        ) as append, patch.object(self.meta, "start_batch") as started, patch.object(
            self.meta, "finalize_batch"
        ) as finalize, patch.object(self.meta, "fail_batch") as failed:
            load_entity.managed_load(self.spark, load_entity.parse_args(build_argv()))
        read_frame.assert_called_once()
        append.assert_called_once()
        self.source.unpersist.assert_called_once()
        started.assert_not_called()
        finalize.assert_not_called()
        failed.assert_not_called()

    def test_provably_empty_window_finalizes_without_reading(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.object(
            load_entity, "_plan_batch", return_value=self.batch(lower_bound="10")
        ), patch.object(load_entity, "_read_extract_frame") as read_frame, patch.object(
            self.meta, "finalize_batch"
        ) as finalize:
            load_entity.managed_load(self.spark, load_entity.parse_args(build_argv()))
        read_frame.assert_not_called()
        self.assertEqual(
            (finalize.call_args.kwargs["row_count"], finalize.call_args.kwargs["observed_max"]),
            (0, None),
        )

    def test_lookback_window_with_equal_bounds_is_read(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.object(
            load_entity, "_plan_batch",
            return_value=self.batch(lower_bound="10", lookback_size=3, reason="regular",
                                    watermark_before="10")
        ), patch.object(load_entity, "_read_extract_frame", return_value=self.source) as read_frame, patch.object(
            load_entity, "_append_raw", return_value=(2, "10")
        ), patch.object(self.meta, "start_batch"), patch.object(self.meta, "finalize_batch"):
            load_entity.managed_load(self.spark, load_entity.parse_args(build_argv()))
        read_frame.assert_called_once()

    def test_success_finalizes_with_typed_maximum(self):
        run = self.load(self.batch())
        run.started.assert_called_once_with(process_id=7, source_id="bank",
                                            entity_fqn="retail_bank.clients", db_connect=ANY)
        run.failed.assert_not_called()
        self.assertEqual(run.finalize.call_args.kwargs["watermark_after"], "10")
        self.source.unpersist.assert_called_once()

    def test_regular_window_never_regresses_watermark(self):
        run = self.load(self.batch(watermark_before="12", reason="regular"))
        self.assertEqual(run.finalize.call_args.kwargs["watermark_after"], "12")

    def test_failure_path_records_failed_status_and_reraises(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.object(
            load_entity, "_plan_batch", return_value=self.batch()
        ), patch.object(load_entity, "_read_extract_frame", return_value=self.source), patch.object(
            load_entity, "_append_raw", side_effect=RuntimeError("secret boom")
        ), patch.object(self.meta, "start_batch"), patch.object(self.meta, "fail_batch") as failed:
            with self.assertRaisesRegex(RuntimeError, "secret boom"):
                load_entity.managed_load(self.spark, self.args("--incremental-column", "_batch_id"))
        self.assertEqual(
            {key: failed.call_args.kwargs[key] for key in ("process_id", "source_id", "entity_fqn")},
            {"process_id": 7, "source_id": "bank", "entity_fqn": "retail_bank.clients"},
        )
        self.assertNotIn("error", failed.call_args.kwargs)
        self.source.unpersist.assert_called_once()

    def test_failure_recording_failure_still_reraises(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.object(
            load_entity, "_plan_batch", return_value=self.batch()
        ), patch.object(load_entity, "_read_extract_frame", return_value=self.source), patch.object(
            load_entity, "_append_raw", side_effect=RuntimeError("secret boom")
        ), patch.object(self.meta, "start_batch"), patch.object(
            self.meta, "fail_batch", side_effect=RuntimeError("meta down")
        ):
            with self.assertRaisesRegex(RuntimeError, "secret boom"), self.assertLogs("load_entity", level="WARNING"):
                load_entity.managed_load(self.spark, self.args("--incremental-column", "_batch_id"))


if __name__ == "__main__":
    unittest.main()
