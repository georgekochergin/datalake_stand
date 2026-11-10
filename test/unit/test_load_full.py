"""Unit tests for the full-slice loader (spark/raw/rdbms/full/load_entity.py)."""

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.spark.raw.rdbms.full import load_entity

MSSQL_URL = "jdbc:sqlserver://host:1433;databaseName=retail_bank"

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
    """CLI argument parsing and validation of the full-slice application."""

    def test_happy_path(self):
        args = load_entity.parse_args(build_argv())
        self.assertEqual(args.source_table, "retail_bank.clients")
        self.assertEqual(args.target_table, "raw.bank.retail_bank__clients")
        self.assertEqual(args.process_id, 42)
        self.assertEqual(args.num_partitions, 4)
        self.assertFalse(args.no_jdbc_partitioning)

    def test_incremental_arguments_rejected(self):
        for extra in (
            ["--incremental-column", "_batch_id"],
            ["--lookback-size", "3"],
            ["--lookback-given"],
        ):
            with self.subTest(extra=extra), self.assertRaises(SystemExit):
                load_entity.parse_args(build_argv(*extra))

    def test_missing_required_argument(self):
        with self.assertRaises(SystemExit):
            load_entity.parse_args(build_argv()[2:])

    def test_zero_process_id(self):
        with self.assertRaises(ValueError):
            load_entity.parse_args(build_argv("--process-id", "0"))

    def test_zero_num_partitions(self):
        with self.assertRaises(ValueError):
            load_entity.parse_args(build_argv("--num-jdbc-partitions", "0"))

    def test_jdbc_partitioning_switch(self):
        args = load_entity.parse_args(build_argv("--no-jdbc-partitioning", "--num-jdbc-partitions", "7"))
        self.assertTrue(args.no_jdbc_partitioning)
        self.assertEqual(args.num_partitions, 7)


class TestMain(unittest.TestCase):
    """The entry point requires a managed source and dispatches to managed_load."""

    def test_missing_source_id_rejected(self):
        with self.assertRaisesRegex(ValueError, "source-id"):
            load_entity.main(build_argv())

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


class TestManagedLoad(unittest.TestCase):
    """The full-slice flow: whole-table read, append, no registry access."""

    def setUp(self):
        self.spark = MagicMock()
        self.source = MagicMock()

    def test_reads_whole_frame_and_appends(self):
        with patch.object(load_entity.reader, "read_extract_frame", return_value=self.source) as read_frame, patch.object(
            load_entity.append, "append_raw"
        ) as append:
            load_entity.managed_load(self.spark, load_entity.parse_args(build_argv()))
        read_frame.assert_called_once()
        self.assertEqual(len(read_frame.call_args.args), 2)
        self.assertEqual(read_frame.call_args.kwargs, {})
        append.assert_called_once()
        self.assertIs(append.call_args.args[2], self.source)
        self.assertIs(append.call_args.args[3], self.source.count.return_value)
        self.assertEqual(append.call_args.args[4], 42)
        self.source.unpersist.assert_called_once()

    def test_append_failure_still_releases_frame(self):
        with patch.object(load_entity.reader, "read_extract_frame", return_value=self.source), patch.object(
            load_entity.append, "append_raw", side_effect=RuntimeError("boom")
        ):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                load_entity.managed_load(self.spark, load_entity.parse_args(build_argv()))
        self.source.unpersist.assert_called_once()


if __name__ == "__main__":
    unittest.main()
