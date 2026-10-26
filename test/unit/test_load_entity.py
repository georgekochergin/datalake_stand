"""Unit tests for spark/raw/load_entity.py (pure functions)."""

import importlib.util
import sys
import types
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "spark" / "raw" / "load_entity.py"
_SPEC = importlib.util.spec_from_file_location("load_entity", MODULE_PATH)
load_entity = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(load_entity)

BASE_ARGS = [
    "--jdbc-url",
    "jdbc:sqlserver://host:1433;databaseName=retail_bank",
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


class TestResolveDriver(unittest.TestCase):
    """JDBC driver resolution by URL prefix."""

    def test_sqlserver(self):
        self.assertEqual(
            load_entity.resolve_driver("jdbc:sqlserver://host:1433"),
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        )

    def test_oracle(self):
        self.assertEqual(
            load_entity.resolve_driver("jdbc:oracle:thin:@//host:1521/svc"),
            "oracle.jdbc.OracleDriver",
        )

    def test_unknown(self):
        with self.assertRaises(ValueError):
            load_entity.resolve_driver("jdbc:postgresql://host:5432/db")


class TestValidateTargetTable(unittest.TestCase):
    """Target table identifier validation."""

    def test_valid(self):
        self.assertEqual(
            load_entity.validate_target_table("raw.bank.retail_bank__clients"),
            ("bank", "retail_bank__clients"),
        )

    def test_wrong_catalog(self):
        with self.assertRaises(ValueError):
            load_entity.validate_target_table("iceberg.bank.retail_bank__clients")

    def test_two_parts(self):
        with self.assertRaises(ValueError):
            load_entity.validate_target_table("bank.retail_bank__clients")

    def test_four_parts(self):
        with self.assertRaises(ValueError):
            load_entity.validate_target_table("raw.bank.a.b")

    def test_uppercase_namespace(self):
        with self.assertRaises(ValueError):
            load_entity.validate_target_table("raw.Bank.retail_bank__clients")

    def test_empty_namespace(self):
        with self.assertRaises(ValueError):
            load_entity.validate_target_table("raw..clients")


class TestSqlReference(unittest.TestCase):
    """Dialect-aware column references for source SQL."""

    def test_mssql_underscore_column_unquoted(self):
        self.assertEqual(
            load_entity.sql_reference("jdbc:sqlserver://host:1433", "_batch_id"),
            "_batch_id",
        )

    def test_oracle_underscore_column_quoted(self):
        self.assertEqual(
            load_entity.sql_reference("jdbc:oracle:thin:@//host:1521/sydafa", "_batch_id"),
            '"_batch_id"',
        )

    def test_regular_column_unquoted_everywhere(self):
        self.assertEqual(load_entity.sql_reference("jdbc:oracle:thin:@//h:1521/s", "updated_at"), "updated_at")
        self.assertEqual(load_entity.sql_reference("jdbc:sqlserver://h:1433", "updated_at"), "updated_at")


class TestBuildReadQuery(unittest.TestCase):
    """JDBC dbtable expression construction."""

    def test_full_snapshot_returns_bare_table(self):
        self.assertEqual(
            load_entity.build_read_query("retail_bank.clients", None, None),
            "retail_bank.clients",
        )

    def test_incremental_builds_subquery_mssql(self):
        self.assertEqual(
            load_entity.build_read_query("retail_bank.clients", "_batch_id", "100", "jdbc:sqlserver://h:1433"),
            "(SELECT * FROM retail_bank.clients WHERE _batch_id > 100) q",
        )

    def test_incremental_builds_subquery_oracle_quotes_underscore(self):
        self.assertEqual(
            load_entity.build_read_query("trading_core.transactions", "_batch_id", "100", "jdbc:oracle:thin:@//h:1521/sydafa"),
            '(SELECT * FROM trading_core.transactions WHERE "_batch_id" > 100) q',
        )

    def test_bootstrap_without_watermark_reads_everything(self):
        self.assertEqual(
            load_entity.build_read_query("retail_bank.clients", "_batch_id", None),
            "retail_bank.clients",
        )

    def test_non_numeric_watermark_rejected(self):
        with self.assertRaises(ValueError):
            load_entity.build_read_query("retail_bank.clients", "_batch_id", "1; DROP TABLE x")

    def test_injection_in_source_table_rejected(self):
        with self.assertRaises(ValueError):
            load_entity.build_read_query("retail_bank.clients; DROP TABLE x", None, None)

    def test_quoted_fragment_allowed(self):
        self.assertEqual(
            load_entity.build_read_query('retail_bank."sub.clients"', None, None),
            'retail_bank."sub.clients"',
        )


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


class TestBuildCreateTableSql(unittest.TestCase):
    """First-load DDL construction."""

    def test_full_ddl(self):
        ddl = load_entity.build_create_table_sql(
            "raw.bank.retail_bank__clients",
            [("client_id", "bigint"), ("full_name", "string")],
        )
        self.assertEqual(
            ddl,
            "CREATE TABLE raw.bank.retail_bank__clients "
            "(`client_id` bigint, `full_name` string, "
            "`__load_dttm` timestamp_ltz, `__process_id` bigint) USING iceberg "
            "PARTITIONED BY (days(`__load_dttm`)) "
            "TBLPROPERTIES ('write.spark.accept-any-schema'='true')",
        )

    def test_backtick_in_column_name_escaped(self):
        ddl = load_entity.build_create_table_sql(
            "raw.bank.t",
            [("a`b", "string")],
        )
        self.assertIn("`a``b` string", ddl)

    def test_metadata_collision_rejected(self):
        with self.assertRaises(ValueError):
            load_entity.build_create_table_sql(
                "raw.bank.t",
                [("__load_dttm", "timestamptz")],
            )

    def test_invalid_target_rejected(self):
        with self.assertRaises(ValueError):
            load_entity.build_create_table_sql("bank.t", [("a", "int")])


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


class TestMetadata(unittest.TestCase):
    """Quote-aware metadata resolution and observable fallback."""

    def test_quoted_dots_case_and_escaped_quotes(self):
        self.assertEqual(
            load_entity.parse_source_table('"Mixed.Schema"."Ta""ble.part"'),
            [("Mixed.Schema", True), ('Ta"ble.part', True)],
        )

    def test_invalid_fqn(self):
        for value in ('a."b', "a..b", "a.b.", 'a."b"x', "a.b.c", ""):
            with self.subTest(value=value), self.assertRaises(ValueError):
                load_entity.build_read_query(value, None, None)

    def test_only_unquoted_fragments_fold(self):
        spark = MagicMock()
        connection = spark._jvm.java.sql.DriverManager.getConnection.return_value
        metadata = connection.getMetaData.return_value
        with patch.object(load_entity, "_ordered_key_columns", side_effect=[[], ["id"]]) as keys, patch.object(
            load_entity, "_column_types", return_value={"id": "INTEGER"}
        ):
            self.assertEqual(
                load_entity.detect_key_columns(spark, "jdbc:oracle:", "u", "p", 'schema."Mixed.Table"'),
                [("id", "INTEGER")],
            )
        self.assertEqual(keys.call_args_list[0].args, (metadata, "schema", "Mixed.Table"))
        self.assertEqual(keys.call_args_list[1].args, (metadata, "SCHEMA", "Mixed.Table"))
        connection.close.assert_called_once()

    def test_fully_quoted_names_never_fold(self):
        spark = MagicMock()
        connection = spark._jvm.java.sql.DriverManager.getConnection.return_value
        with patch.object(load_entity, "_ordered_key_columns", return_value=[]) as keys, self.assertLogs(
            load_entity.__name__, level="WARNING"
        ) as logs:
            self.assertEqual(load_entity.detect_key_columns(spark, "jdbc:oracle:", "u", "p", '"Schema"."Table"'), [])
        keys.assert_called_once_with(connection.getMetaData.return_value, "Schema", "Table")
        self.assertIn("falling back", logs.output[0])

    def test_metadata_error_warns(self):
        spark = MagicMock()
        spark._jvm.java.sql.DriverManager.getConnection.side_effect = RuntimeError("metadata offline")
        with self.assertLogs(load_entity.__name__, level="WARNING") as logs:
            self.assertEqual(load_entity.detect_key_columns(spark, "jdbc:oracle:", "u", "p", "s.t"), [])
        self.assertIn("metadata offline", logs.output[0])


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
        ), patch.object(load_entity, "_schema_columns", return_value=[(field.name, "bigint") for field in source.schema.fields]), patch.object(
            load_entity, "detect_key_columns", return_value=[]
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
        for name in ("__process_id", "__load_dttm"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "collides"):
                self.run_loader(("--no-parallelism",), _FakeSchema(_FakeField(name, _FakeType("long"))))


class TestValidateIncrementalColumn(unittest.TestCase):
    """Existence and integral-type checks on the incremental column."""

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

    def test_non_numeric_types_rejected(self):
        for type_name in ("string", "date", "timestamp", "boolean", "double"):
            with self.assertRaises(ValueError):
                load_entity.validate_incremental_column(
                    self.schema_with(_FakeType(type_name)), "_batch_id"
                )


if __name__ == "__main__":
    unittest.main()
