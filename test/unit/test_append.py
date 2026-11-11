"""Unit tests for the raw-layer writer (spark/raw/append.py)."""

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.spark.raw import append as append_module


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


class TestAppendRaw(unittest.TestCase):
    """Raw append: DDL bootstrap, metadata stamping, and collision guard."""

    def setUp(self):
        self.spark = MagicMock()

    def functions_modules(self):
        """Register the functions and types doubles with their parent packages."""
        root = types.ModuleType("pyspark")
        sql = types.ModuleType("pyspark.sql")
        functions = types.ModuleType("pyspark.sql.functions")
        for name in ("lit", "current_timestamp"):
            setattr(functions, name, MagicMock())
        sql.functions = functions
        root.sql = sql
        return {
            "pyspark": root,
            "pyspark.sql": sql,
            "pyspark.sql.functions": functions,
            "pyspark.sql.types": fake_pyspark_types(),
        }

    def make_frame(self, fields, doubles):
        """Build a frame double with schema fields typed by the registered doubles."""
        frame = MagicMock()
        long_type = doubles["pyspark.sql.types"].LongType()
        frame.schema.fields = [types.SimpleNamespace(name=field, dataType=long_type) for field in fields]
        return frame

    def append(self, fields, exists=False, rows=3):
        doubles = self.functions_modules()
        frame = self.make_frame(fields, doubles)
        self.spark.catalog.tableExists.return_value = exists
        with patch.dict(sys.modules, doubles):
            append_module.append_raw(self.spark, "raw.bank.retail_bank__clients", frame, rows, 42)
        return frame

    def test_rows_appended_with_metadata_columns(self):
        frame = self.append(["client_id"], exists=True, rows=5)
        first = frame.withColumn
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
        self.append(["client_id"], exists=False, rows=0)
        executed = [call.args[0] for call in self.spark.sql.call_args_list]
        self.assertEqual(executed[0], "CREATE NAMESPACE IF NOT EXISTS raw.bank")
        self.assertTrue(executed[1].startswith("CREATE TABLE raw.bank.retail_bank__clients"))

    def test_existing_table_skips_ddl(self):
        self.append(["client_id"], exists=True, rows=3)
        executed = [call.args[0] for call in self.spark.sql.call_args_list]
        self.assertTrue(all("CREATE TABLE" not in sql for sql in executed))

    def test_zero_rows_verify_schema_without_write(self):
        frame = self.append(["client_id"], exists=True, rows=0)
        frame.withColumn.assert_not_called()

    def test_metadata_collision_rejected(self):
        with self.assertRaisesRegex(ValueError, "collides"):
            self.append(["__process_id"])


if __name__ == "__main__":
    unittest.main()
