"""Unit tests for the shared raw append (spark/raw/rdbms/raw_append.py)."""

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.spark.raw.rdbms import raw_append


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

    def make_args(self, incremental=True):
        return types.SimpleNamespace(
            target_table="raw.bank.retail_bank__clients",
            process_id=42,
            incremental_column="_batch_id" if incremental else None,
        )

    def append(self, summary_values, fields, exists=False, incremental=True):
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
        with patch.dict(sys.modules, doubles):
            return raw_append.append_raw(
                self.spark, self.make_args(incremental), frame, "bigint" if incremental else None
            ), clean

    def test_rows_appended_with_metadata_columns(self):
        result, clean = self.append({"n": 5, "position": "10"}, ["client_id"])
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
        self.append({"n": 0}, ["client_id"], exists=False, incremental=False)
        executed = [call.args[0] for call in self.spark.sql.call_args_list]
        self.assertEqual(executed[0], "CREATE NAMESPACE IF NOT EXISTS raw.bank")
        self.assertTrue(executed[1].startswith("CREATE TABLE raw.bank.retail_bank__clients"))

    def test_existing_table_skips_ddl(self):
        self.append({"n": 3}, ["client_id"], exists=True, incremental=False)
        executed = [call.args[0] for call in self.spark.sql.call_args_list]
        self.assertTrue(all("CREATE TABLE" not in sql for sql in executed))

    def test_zero_rows_verify_schema_without_write(self):
        result, clean = self.append({"n": 0, "position": None}, ["client_id"])
        self.assertEqual(result, (0, None))
        clean.withColumn.assert_not_called()

    def test_metadata_collision_rejected(self):
        with self.assertRaisesRegex(ValueError, "collides"):
            self.append({"n": 1, "position": "1"}, ["__process_id"])


if __name__ == "__main__":
    unittest.main()
