"""Unit tests for the target-side Iceberg DDL helpers (ingestion.iceberg)."""

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion import iceberg


def fake_pyspark_types():
    """A pyspark.sql.types double with the real sibling type hierarchy.

    PySpark declares TimestampType and TimestampNTZType as parallel
    AtomicType subclasses, so the doubles stay independent classes too.
    """
    module = types.ModuleType("pyspark.sql.types")
    for name in (
        "BinaryType", "BooleanType", "ByteType", "DateType", "DoubleType",
        "FloatType", "IntegerType", "LongType", "ShortType", "StringType",
        "TimestampNTZType", "TimestampType",
    ):
        setattr(module, name, type(name, (), {}))

    class DecimalType:
        """Decimal column type carrying precision and scale."""

        def __init__(self, precision, scale):
            self.precision = precision
            self.scale = scale

    module.DecimalType = DecimalType

    class StructField:
        """A named schema field."""

        def __init__(self, name, data_type):
            self.name = name
            self.dataType = data_type

    class StructType:
        """A schema over a field list."""

        def __init__(self, fields):
            self.fields = fields

    module.StructField = StructField
    module.StructType = StructType
    return module


class SchemaDouble:
    """Build schemas against an injected pyspark.sql.types double."""

    def __init__(self):
        self.types = fake_pyspark_types()

    def schema(self, *pairs):
        """A StructType from (name, type instance or class) pairs."""
        fields = [
            self.types.StructField(name, value() if isinstance(value, type) else value)
            for name, value in pairs
        ]
        return self.types.StructType(fields)


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


class TestSchemaColumns(unittest.TestCase):
    """PySpark schema to Iceberg DDL type mapping."""

    def setUp(self):
        self.double = SchemaDouble()
        root = types.ModuleType("pyspark")
        sql = types.ModuleType("pyspark.sql")
        sql.types = self.double.types
        root.sql = sql
        self.modules = {"pyspark": root, "pyspark.sql": sql, "pyspark.sql.types": self.double.types}

    def columns(self, *pairs):
        """Run schema_columns against the injected type module."""
        resolved = [
            (name, value if not isinstance(value, str) else getattr(self.double.types, value))
            for name, value in pairs
        ]
        with patch.dict(sys.modules, self.modules):
            return iceberg.schema_columns(self.double.schema(*resolved))

    def test_simple_type_mapping(self):
        cases = (
            ("name", "StringType", "string"),
            ("flag", "BooleanType", "boolean"),
            ("byte", "ByteType", "tinyint"),
            ("short", "ShortType", "smallint"),
            ("int", "IntegerType", "int"),
            ("long", "LongType", "bigint"),
            ("float", "FloatType", "float"),
            ("double", "DoubleType", "double"),
            ("day", "DateType", "date"),
            ("ts", "TimestampType", "timestamp_ltz"),
            ("ts_ntz", "TimestampNTZType", "timestamp_ntz"),
            ("blob", "BinaryType", "binary"),
        )
        rendered = self.columns(*[(name, kind) for name, kind, _ in cases])
        self.assertEqual([name for name, _ in rendered], [name for name, _, _ in cases])
        self.assertEqual([ddl for _, ddl in rendered], [ddl for _, _, ddl in cases])

    def test_decimal_keeps_precision_and_scale(self):
        self.assertEqual(
            self.columns(("amount", self.double.types.DecimalType(18, 2))),
            [("amount", "decimal(18,2)")],
        )

    def test_unsupported_type_rejected(self):
        with self.assertRaisesRegex(ValueError, "unsupported source column type"):
            self.columns(("weird", object()))

    def test_read_schema_order_preserved(self):
        rendered = self.columns(("b", "StringType"), ("a", "LongType"))
        self.assertEqual(rendered, [("b", "string"), ("a", "bigint")])


if __name__ == "__main__":
    unittest.main()
