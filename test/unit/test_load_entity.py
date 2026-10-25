"""Unit tests for spark/raw/load_entity.py (pure functions)."""

import importlib.util
import unittest
from pathlib import Path

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


class TestBuildReadQuery(unittest.TestCase):
    """JDBC dbtable expression construction."""

    def test_full_snapshot_returns_bare_table(self):
        self.assertEqual(
            load_entity.build_read_query("retail_bank.clients", None, None),
            "retail_bank.clients",
        )

    def test_incremental_builds_subquery(self):
        self.assertEqual(
            load_entity.build_read_query("retail_bank.clients", "_batch_id", "100"),
            "(SELECT * FROM retail_bank.clients WHERE _batch_id > 100) AS q",
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


if __name__ == "__main__":
    unittest.main()
