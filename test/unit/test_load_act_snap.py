"""Unit tests for the actual-snapshot loader (spark/raw/rdbms/incremental/load_act_snap.py)."""

import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.spark.raw.rdbms.incremental import load_act_snap as act_snap


def cli_args(**overrides):
    """Build a valid CLI namespace for the snapshot loader."""
    values = {
        "jdbc_url": "jdbc:sqlserver://h:1433;d=x",
        "jdbc_user": "reader",
        "jdbc_password": "secret",
        "source_table": "s.t",
        "target_table": "raw.bank.s__t__act_snap",
        "process_id": 7,
        "num_partitions": 4,
        "no_jdbc_partitioning": False,
        "result_marker": "/tmp/marker.json",
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


class TestParseArgs(unittest.TestCase):
    """CLI validation of the snapshot loader."""

    BASE = [
        "--jdbc-url", "jdbc:u", "--jdbc-user", "u", "--jdbc-password", "p",
        "--source-table", "s.t", "--target-table", "raw.bank.s__t__act_snap",
        "--process-id", "7", "--result-marker", "/tmp/m.json",
    ]

    def test_valid_defaults(self):
        args = act_snap.parse_args(self.BASE)
        self.assertEqual(args.process_id, 7)
        self.assertEqual(args.num_partitions, 4)
        self.assertFalse(args.no_jdbc_partitioning)

    def test_process_id_and_partitions_validated(self):
        base = [
            "--jdbc-url", "jdbc:u", "--jdbc-user", "u", "--jdbc-password", "p",
            "--source-table", "s.t", "--target-table", "raw.bank.s__t__act_snap",
            "--process-id", "7", "--result-marker", "/tmp/m.json",
        ]
        for extra in (["--process-id", "0"], ["--process-id", "-3"], ["--num-partitions", "0"]):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                act_snap.parse_args(base + extra)

    def test_result_marker_required(self):
        base = [value for value in self.BASE if value != "/tmp/m.json"]
        with self.assertRaises(SystemExit):
            act_snap.parse_args(base)


class TestMarker(unittest.TestCase):
    """The driver-side result marker."""

    def test_write_result_marker_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "marker.json"
            act_snap.write_result_marker(str(path), True)
            self.assertEqual(path.read_text(encoding="utf-8"), '{"overwritten": true}')

    def test_write_result_marker_false(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "marker.json"
            act_snap.write_result_marker(str(path), False)
            self.assertEqual(path.read_text(encoding="utf-8"), '{"overwritten": false}')


class TestBuildRtasSql(unittest.TestCase):
    """The atomic replace statement."""

    def test_renders_property_location_and_quoted_columns(self):
        statement = act_snap.build_rtas_sql(
            "raw.bank.s__t__act_snap", 42, ["id", "weird name", "__load_dttm"]
        )
        self.assertEqual(
            statement,
            "CREATE OR REPLACE TABLE raw.bank.s__t__act_snap USING iceberg "
            "LOCATION 's3://datalake/raw/bank/s__t__act_snap' "
            "TBLPROPERTIES ('snapshot.process_id'='42') "
            "AS SELECT `id`, `weird name`, `__load_dttm` FROM snapshot_source",
        )


class FakeCatalog:
    """tableExists with scripted answers."""

    def __init__(self, exists):
        self.exists = exists

    def tableExists(self, target):
        return self.exists.pop(0)


class TestCurrentSnapshotAuthor(unittest.TestCase):
    """Reading the publishing author from table properties."""

    def spark_with(self, exists, rows):
        def sql(statement):
            data = list(rows) if "SHOW TBLPROPERTIES" in statement else []
            return types.SimpleNamespace(collect=lambda: data)

        return types.SimpleNamespace(catalog=FakeCatalog(exists), sql=sql)

    def test_missing_table_never_published(self):
        spark = self.spark_with([False], [])
        self.assertIsNone(act_snap.current_snapshot_author(spark, "raw.bank.t"))

    def test_author_read_from_properties(self):
        rows = [{"key": "snapshot.process_id", "value": "77"}, {"key": "format", "value": "iceberg/parquet"}]
        spark = self.spark_with([True], rows)
        self.assertEqual(act_snap.current_snapshot_author(spark, "raw.bank.t"), 77)

    def test_non_numeric_or_missing_property_is_none(self):
        for rows in ([], [{"key": "snapshot.process_id", "value": "soon"}]):
            with self.subTest(rows=rows):
                spark = self.spark_with([True], rows)
                self.assertIsNone(act_snap.current_snapshot_author(spark, "raw.bank.t"))


class TestManagedSnapshot(unittest.TestCase):
    """Decision tree of the snapshot execution."""

    def setUp(self):
        self.marker = tempfile.NamedTemporaryFile(delete=False)
        self.marker.close()
        self.addCleanup(Path(self.marker.name).unlink)
        self.args = cli_args(result_marker=self.marker.name)

    def test_retry_skips_read_when_already_published(self):
        spark = types.SimpleNamespace(catalog=FakeCatalog([True]))
        with patch.object(act_snap, "current_snapshot_author", return_value=7), \
                patch.object(act_snap, "read_extract_frame") as read_frame, \
                patch.object(act_snap, "write_result_marker") as write_marker:
            act_snap.managed_snapshot(spark, self.args)
            read_frame.assert_not_called()
            write_marker.assert_called_once_with(self.args.result_marker, True)

    def test_empty_read_keeps_existing_table(self):
        spark = types.SimpleNamespace(catalog=FakeCatalog([True]))
        frame = types.SimpleNamespace(count=lambda: 0, unpersist=lambda: None)
        with patch.object(act_snap, "current_snapshot_author", return_value=None), \
                patch.object(act_snap, "read_extract_frame", return_value=frame), \
                patch.object(act_snap, "_overwrite_snapshot") as overwrite, \
                patch.object(act_snap, "write_result_marker") as write_marker:
            act_snap.managed_snapshot(spark, self.args)
            overwrite.assert_not_called()
            write_marker.assert_called_once_with(self.args.result_marker, False)

    def test_empty_read_publishes_missing_table(self):
        spark = types.SimpleNamespace(catalog=FakeCatalog([False]))
        frame = types.SimpleNamespace(count=lambda: 0, unpersist=lambda: None)
        with patch.object(act_snap, "current_snapshot_author", return_value=None), \
                patch.object(act_snap, "read_extract_frame", return_value=frame), \
                patch.object(act_snap, "_overwrite_snapshot") as overwrite, \
                patch.object(act_snap, "write_result_marker") as write_marker:
            act_snap.managed_snapshot(spark, self.args)
            overwrite.assert_called_once()
            write_marker.assert_called_once_with(self.args.result_marker, True)

    def test_non_empty_read_overwrites(self):
        spark = types.SimpleNamespace(catalog=FakeCatalog([False]))
        frame = types.SimpleNamespace(count=lambda: 5, unpersist=lambda: None)
        with patch.object(act_snap, "current_snapshot_author", return_value=None), \
                patch.object(act_snap, "read_extract_frame", return_value=frame), \
                patch.object(act_snap, "_overwrite_snapshot") as overwrite, \
                patch.object(act_snap, "write_result_marker") as write_marker:
            act_snap.managed_snapshot(spark, self.args)
            overwrite.assert_called_once()
            write_marker.assert_called_once_with(self.args.result_marker, True)


if __name__ == "__main__":
    unittest.main()
