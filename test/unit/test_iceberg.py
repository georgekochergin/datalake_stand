"""Unit tests for the target-side Iceberg DDL helpers (dlh_ingestion.iceberg)."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dlh_ingestion import iceberg


class TestValidateTargetTable(unittest.TestCase):
    """Target table identifier validation."""

    def test_valid(self):
        self.assertEqual(
            iceberg.validate_target_table("raw.bank.retail_bank__clients"),
            ("bank", "retail_bank__clients"),
        )

    def test_wrong_catalog(self):
        with self.assertRaises(ValueError):
            iceberg.validate_target_table("iceberg.bank.retail_bank__clients")

    def test_two_parts(self):
        with self.assertRaises(ValueError):
            iceberg.validate_target_table("bank.retail_bank__clients")

    def test_four_parts(self):
        with self.assertRaises(ValueError):
            iceberg.validate_target_table("raw.bank.a.b")

    def test_uppercase_namespace(self):
        with self.assertRaises(ValueError):
            iceberg.validate_target_table("raw.Bank.retail_bank__clients")

    def test_empty_namespace(self):
        with self.assertRaises(ValueError):
            iceberg.validate_target_table("raw..clients")


class TestBuildCreateTableSql(unittest.TestCase):
    """First-load DDL construction."""

    def test_full_ddl(self):
        ddl = iceberg.build_create_table_sql(
            "raw.bank.retail_bank__clients",
            [("client_id", "bigint"), ("full_name", "string")],
        )
        self.assertEqual(
            ddl,
            "CREATE TABLE raw.bank.retail_bank__clients "
            "(`client_id` bigint, `full_name` string, "
            "`__load_dttm` timestamp_ltz, `__process_id` bigint, `__batch_id` bigint) USING iceberg "
            "PARTITIONED BY (days(`__load_dttm`)) "
            "LOCATION 's3://datalake/raw/bank/retail_bank__clients' "
            "TBLPROPERTIES ('write.spark.accept-any-schema'='true')",
        )

    def test_backtick_in_column_name_escaped(self):
        ddl = iceberg.build_create_table_sql(
            "raw.bank.t",
            [("a`b", "string")],
        )
        self.assertIn("`a``b` string", ddl)

    def test_exact_location_for_each_namespace(self):
        for namespace, table in (("bank", "retail_bank__accounts"), ("broker", "trading_core__transactions")):
            with self.subTest(namespace=namespace):
                ddl = iceberg.build_create_table_sql(f"raw.{namespace}.{table}", [("id", "bigint")])
                self.assertIn(f"LOCATION 's3://datalake/raw/{namespace}/{table}'", ddl)

    def test_unsafe_location_parts_rejected(self):
        for target in ("raw.bank.a/b", "raw.bank.a'b", "raw../accounts", "raw.bank.a-b"):
            with self.subTest(target=target), self.assertRaises(ValueError):
                iceberg.build_create_table_sql(target, [("id", "bigint")])

    def test_metadata_collision_rejected(self):
        with self.assertRaises(ValueError):
            iceberg.build_create_table_sql(
                "raw.bank.t",
                [("__load_dttm", "timestamptz")],
            )

    def test_invalid_target_rejected(self):
        with self.assertRaises(ValueError):
            iceberg.build_create_table_sql("bank.t", [("a", "int")])


if __name__ == "__main__":
    unittest.main()
