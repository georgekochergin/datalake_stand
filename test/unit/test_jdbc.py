"""Unit tests for the shared JDBC layer (ingestion.sources.rdbms.jdbc)."""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.sources.rdbms import jdbc, registry
from ingestion.sources.rdbms.contracts import MetadataLocation, RdbmsDialect

ORACLE = registry.by_conn_type("oracle")


def fake_jvm():
    """A MagicMock standing in for spark._jvm with a JDBC DriverManager."""
    spark = MagicMock()
    connection = spark._jvm.java.sql.DriverManager.getConnection.return_value
    connection.getMetaData.return_value = MagicMock()
    return spark, connection


class TestKeyColumns(unittest.TestCase):
    """Quote-aware metadata resolution and observable fallback."""

    def test_only_unquoted_fragments_fold(self):
        spark, connection = fake_jvm()
        metadata = connection.getMetaData.return_value
        with patch.object(jdbc, "_ordered_key_columns", side_effect=[[], ["id"]]) as keys, patch.object(
            jdbc, "_column_types", return_value={"id": "INTEGER"}
        ):
            self.assertEqual(
                jdbc.key_columns(spark._jvm, ORACLE, "jdbc:oracle:", "u", "p", 'schema."Mixed.Table"'),
                [("id", "INTEGER")],
            )
        self.assertEqual(keys.call_args_list[0].args,
                         (metadata, MetadataLocation(None, "schema", "Mixed.Table")))
        self.assertEqual(keys.call_args_list[1].args,
                         (metadata, MetadataLocation(None, "SCHEMA", "Mixed.Table")))
        connection.close.assert_called_once()

    def test_fully_quoted_names_never_fold(self):
        spark, connection = fake_jvm()
        with patch.object(jdbc, "_ordered_key_columns", return_value=[]) as keys, self.assertLogs(
            jdbc.__name__, level="WARNING"
        ) as logs:
            self.assertEqual(
                jdbc.key_columns(spark._jvm, ORACLE, "jdbc:oracle:", "u", "p", '"Schema"."Table"'),
                [],
            )
        keys.assert_called_once_with(connection.getMetaData.return_value,
                                     MetadataLocation(None, "Schema", "Table"))
        self.assertIn("falling back", logs.output[0])

    def test_metadata_error_warns(self):
        spark, _ = fake_jvm()
        spark._jvm.java.sql.DriverManager.getConnection.side_effect = RuntimeError("metadata offline")
        with self.assertLogs(jdbc.__name__, level="WARNING") as logs:
            self.assertEqual(jdbc.key_columns(spark._jvm, ORACLE, "jdbc:oracle:", "u", "p", "s.t"), [])
        self.assertIn("metadata offline", logs.output[0])

    def test_malformed_reference_warns(self):
        spark, _ = fake_jvm()
        with self.assertLogs(jdbc.__name__, level="WARNING"):
            self.assertEqual(jdbc.key_columns(spark._jvm, ORACLE, "jdbc:oracle:", "u", "p", "a..b"), [])


class CatalogDialect(RdbmsDialect):
    """Stub engine addressing objects through a JDBC catalog instead of a schema."""

    def metadata_candidates(self, parts):
        """One exact location: the database is the catalog, no schema level."""
        return (MetadataLocation(parts[0][0], None, parts[1][0]),)


class TestCatalogAddressing(unittest.TestCase):
    """A dialect-controlled catalog reaches every JDBC metadata call."""

    def test_catalog_dialect_passes_full_location(self):
        spark, connection = fake_jvm()
        metadata = connection.getMetaData.return_value
        primary_keys = metadata.getPrimaryKeys.return_value
        primary_keys.next.side_effect = [True, False]
        primary_keys.getInt.return_value = 1
        primary_keys.getString.return_value = "id"
        indexes = metadata.getIndexInfo.return_value
        indexes.next.side_effect = [False]
        columns = metadata.getColumns.return_value
        columns.next.side_effect = [True, False]
        columns.getString.side_effect = ["id", "int8"]
        self.assertEqual(
            jdbc.key_columns(spark._jvm, CatalogDialect(), "jdbc:stub://", "u", "p", "example_db.example_table"),
            [("id", "int8")],
        )
        metadata.getPrimaryKeys.assert_called_once_with("example_db", None, "example_table")
        metadata.getIndexInfo.assert_called_once_with("example_db", None, "example_table", True, True)
        metadata.getColumns.assert_called_once_with("example_db", None, "example_table", None)

    def test_catalog_dialect_error_still_falls_back(self):
        spark, _ = fake_jvm()
        spark._jvm.java.sql.DriverManager.getConnection.side_effect = RuntimeError("catalog engine offline")
        with self.assertLogs(jdbc.__name__, level="WARNING") as logs:
            self.assertEqual(
                jdbc.key_columns(spark._jvm, CatalogDialect(), "jdbc:stub://", "u", "p", "example_db.example_table"),
                [],
            )
        self.assertIn("catalog engine offline", logs.output[0])


class TestCompatibilityProjection(unittest.TestCase):
    """Dialect projections build select items only when a column is rewritten."""

    def setUp(self):
        self.spark, self.connection = fake_jvm()
        result = self.connection.createStatement.return_value.executeQuery.return_value
        metadata = result.getMetaData.return_value
        metadata.getColumnCount.return_value = 2
        metadata.getColumnLabel.side_effect = ["plain_col", "tz_col"]
        metadata.getColumnTypeName.side_effect = ["NUMBER", "TIMESTAMP(9) WITH TIME ZONE"]
        metadata.getPrecision.return_value = 10
        metadata.getScale.return_value = 0

    def test_oracle_projects_timezone_columns(self):
        projection = jdbc.compatibility_projection(
            self.spark._jvm, ORACLE, "jdbc:oracle:", "u", "p", '"s"."t"'
        )
        self.assertEqual(projection, [
            'extract_source."plain_col"',
            'SYS_EXTRACT_UTC(extract_source."tz_col") AS "tz_col"',
        ])

    def test_native_read_returns_none(self):
        mssql = registry.by_conn_type("mssql")
        projection = jdbc.compatibility_projection(
            self.spark._jvm, mssql, "jdbc:sqlserver://h", "u", "p", "[s].[t]"
        )
        self.assertIsNone(projection)


if __name__ == "__main__":
    unittest.main()
