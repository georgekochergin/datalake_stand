"""Unit tests for the shared JDBC layer (ingestion.sources.rdbms.jdbc)."""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.sources.rdbms import jdbc, registry
from ingestion.sources.rdbms.contracts import MetadataLocation
from ingestion.sources.rdbms.dialects.base import RdbmsDialect

ORACLE = registry.by_conn_type("oracle")


def fake_jvm():
    """A MagicMock standing in for spark._jvm with a JDBC DriverManager."""
    spark = MagicMock()
    connection = spark._jvm.java.sql.DriverManager.getConnection.return_value
    connection.getMetaData.return_value = MagicMock()
    return spark, connection


class TestKeyMetadata(unittest.TestCase):
    """Quote-aware metadata resolution and observable fallback."""

    def test_exact_fqn_location_queried_once(self):
        spark, connection = fake_jvm()
        metadata = connection.getMetaData.return_value
        located = {"id": jdbc.ColumnMeta("id", "INTEGER", 10, 0, True)}
        with patch.object(jdbc, "_column_metadata", return_value=located) as columns, patch.object(
            jdbc, "_primary_key", return_value=("id",)
        ) as primary, patch.object(jdbc, "_unique_single_columns", return_value=()):
            result = jdbc.key_metadata(
                spark._jvm, ORACLE, "jdbc:oracle:", "u", "p", 'schema."Mixed.Table"'
            )
        self.assertEqual(result, jdbc.KeyMetadata(("id",), (), located))
        columns.assert_called_once_with(metadata, MetadataLocation(None, "schema", "Mixed.Table"))
        primary.assert_called_once_with(metadata, MetadataLocation(None, "schema", "Mixed.Table"))
        connection.close.assert_called_once()

    def test_fully_quoted_names_never_fold(self):
        spark, connection = fake_jvm()
        with patch.object(jdbc, "_column_metadata", return_value={}) as columns, self.assertLogs(
            jdbc.__name__, level="WARNING"
        ) as logs:
            result = jdbc.key_metadata(
                spark._jvm, ORACLE, "jdbc:oracle:", "u", "p", '"Schema"."Table"'
            )
        self.assertEqual(result, jdbc.KeyMetadata((), (), {}))
        columns.assert_called_once_with(connection.getMetaData.return_value,
                                        MetadataLocation(None, "Schema", "Table"))
        self.assertIn("falling back", logs.output[0])

    def test_metadata_error_warns(self):
        spark, _ = fake_jvm()
        spark._jvm.java.sql.DriverManager.getConnection.side_effect = RuntimeError("metadata offline")
        with self.assertLogs(jdbc.__name__, level="WARNING") as logs:
            result = jdbc.key_metadata(
                spark._jvm, ORACLE, "jdbc:oracle:", "u", "p", "s.t"
            )
        self.assertEqual(result, jdbc.KeyMetadata((), (), {}))
        self.assertIn("metadata offline", logs.output[0])

    def test_malformed_reference_warns(self):
        spark, _ = fake_jvm()
        with self.assertLogs(jdbc.__name__, level="WARNING"):
            result = jdbc.key_metadata(
                spark._jvm, ORACLE, "jdbc:oracle:", "u", "p", "a..b"
            )
        self.assertEqual(result, jdbc.KeyMetadata((), (), {}))


class CatalogDialect(RdbmsDialect):
    """Stub engine addressing objects through a JDBC catalog instead of a schema."""

    def metadata_location(self, parts):
        """One exact location: the database is the catalog, no schema level."""
        return MetadataLocation(parts[0][0], None, parts[1][0])


class ScriptedResult:
    """JDBC result-set double over scripted {label: value} rows."""

    def __init__(self, rows):
        self.rows = rows
        self.index = -1
        self.current: dict = {}
        self.was_null = False
        self.closed = False

    def next(self):
        """Advance one row; False past the end."""
        self.index += 1
        self.current = self.rows[self.index] if self.index < len(self.rows) else {}
        return self.index < len(self.rows)

    def getString(self, label):
        """Return the scripted string value of the current row."""
        return self.current.get(label)

    def getInt(self, label):
        """Return the scripted int value; SQL NULL reads as zero with wasNull."""
        value = self.current.get(label)
        self.was_null = value is None
        return 0 if value is None else value

    def getBoolean(self, label):
        """Return the scripted boolean value."""
        return self.current.get(label, False)

    def wasNull(self):
        """Report whether the last int read was SQL NULL."""
        return self.was_null

    def close(self):
        """Record the close."""
        self.closed = True


class TestCatalogAddressing(unittest.TestCase):
    """A dialect-controlled catalog reaches every JDBC metadata call."""

    def test_catalog_dialect_passes_full_location(self):
        spark, connection = fake_jvm()
        metadata = connection.getMetaData.return_value
        metadata.getPrimaryKeys.return_value = ScriptedResult([{"KEY_SEQ": 1, "COLUMN_NAME": "id"}])
        metadata.getIndexInfo.return_value = ScriptedResult([])
        metadata.getColumns.return_value = ScriptedResult([
            {"COLUMN_NAME": "id", "TYPE_NAME": "int8", "COLUMN_SIZE": 10,
             "DECIMAL_DIGITS": 0, "IS_NULLABLE": "NO"},
        ])
        self.assertEqual(
            jdbc.key_metadata(
                spark._jvm, CatalogDialect(), "jdbc:stub://", "u", "p", "example_db.example_table"
            ),
            jdbc.KeyMetadata(("id",), (), {"id": jdbc.ColumnMeta("id", "INT8", 10, 0, True)}),
        )
        metadata.getPrimaryKeys.assert_called_once_with("example_db", None, "example_table")
        metadata.getIndexInfo.assert_called_once_with("example_db", None, "example_table", True, True)
        metadata.getColumns.assert_called_once_with("example_db", None, "example_table", None)

    def test_catalog_dialect_error_still_falls_back(self):
        spark, _ = fake_jvm()
        spark._jvm.java.sql.DriverManager.getConnection.side_effect = RuntimeError("catalog engine offline")
        with self.assertLogs(jdbc.__name__, level="WARNING") as logs:
            result = jdbc.key_metadata(
                spark._jvm, CatalogDialect(), "jdbc:stub://", "u", "p", "example_db.example_table"
            )
        self.assertEqual(result, jdbc.KeyMetadata((), (), {}))
        self.assertIn("catalog engine offline", logs.output[0])


class TestCompatibilityProjection(unittest.TestCase):
    """Dialect projections build select items only when a column is rewritten."""

    @staticmethod
    def keys():
        return jdbc.KeyMetadata(
            primary_key=(),
            unique_columns=(),
            columns={
                "plain_col": jdbc.ColumnMeta("plain_col", "NUMBER", 10, 0, True),
                "tz_col": jdbc.ColumnMeta("tz_col", "TIMESTAMP(9) WITH TIME ZONE", None, 9, True),
            },
        )

    def test_oracle_projects_timezone_columns(self):
        projection = jdbc.compatibility_projection(ORACLE, self.keys())
        self.assertEqual(projection, [
            'extract_source."plain_col"',
            'SYS_EXTRACT_UTC(extract_source."tz_col") AS "tz_col"',
        ])

    def test_native_read_returns_none(self):
        mssql = registry.by_conn_type("mssql")
        self.assertIsNone(jdbc.compatibility_projection(mssql, self.keys()))


class TestFirstRow(unittest.TestCase):
    """First-row rendering as strings with owned statement lifecycle."""

    def test_renders_each_column_as_string(self):
        _, connection = fake_jvm()
        statement = connection.createStatement.return_value
        result = statement.executeQuery.return_value
        result.getMetaData.return_value.getColumnCount.return_value = 3
        result.getString.side_effect = ["10", "5", "5"]
        self.assertEqual(jdbc.first_row(connection, "SELECT MAX(id) FROM t"), ["10", "5", "5"])
        statement.executeQuery.assert_called_once_with("SELECT MAX(id) FROM t")
        result.close.assert_called_once()
        statement.close.assert_called_once()

    def test_result_close_still_closes_statement(self):
        _, connection = fake_jvm()
        statement = connection.createStatement.return_value
        statement.executeQuery.return_value.close.side_effect = RuntimeError("boom")
        with self.assertRaisesRegex(RuntimeError, "boom"):
            jdbc.first_row(connection, "SELECT 1")
        statement.close.assert_called_once()


class TestKeyStructure(unittest.TestCase):
    """Primary-key ordering and single-column unique-index grouping."""

    LOCATION = MetadataLocation(None, "s", "t")

    def primary(self, rows):
        """Run _primary_key over scripted (KEY_SEQ, COLUMN_NAME) rows."""
        metadata = MagicMock()
        metadata.getPrimaryKeys.return_value = ScriptedResult(
            [{"KEY_SEQ": seq, "COLUMN_NAME": name} for seq, name in rows]
        )
        return metadata, jdbc._primary_key(metadata, self.LOCATION)

    def unique(self, rows):
        """Run _unique_single_columns over scripted index rows."""
        metadata = MagicMock()
        metadata.getIndexInfo.return_value = ScriptedResult([
            {"INDEX_NAME": index, "NON_UNIQUE": non_unique, "COLUMN_NAME": column,
             "FILTER_CONDITION": filter_condition}
            for index, non_unique, column, filter_condition in rows
        ])
        return metadata, jdbc._unique_single_columns(metadata, self.LOCATION)

    def test_primary_keys_sorted_by_sequence(self):
        metadata, primary = self.primary([(2, "b"), (1, "a")])
        self.assertEqual(primary, ("a", "b"))
        self.assertTrue(metadata.getPrimaryKeys.return_value.closed)

    def test_composite_primary_key_preserved(self):
        _, primary = self.primary([(2, "num"), (1, "code")])
        self.assertEqual(primary, ("code", "num"))

    def test_null_primary_key_name_skipped(self):
        _, primary = self.primary([(1, None)])
        self.assertEqual(primary, ())

    def test_single_column_unique_index_kept(self):
        metadata, unique = self.unique([("uk_num", False, "num", None)])
        self.assertEqual(unique, ("num",))
        self.assertTrue(metadata.getIndexInfo.return_value.closed)

    def test_composite_unique_index_dropped(self):
        _, unique = self.unique([
            ("uk_pair", False, "a", None), ("uk_pair", False, "b", None),
        ])
        self.assertEqual(unique, ())

    def test_non_unique_and_functional_and_filtered_dropped(self):
        _, unique = self.unique([
            ("ix_plain", True, "a", None),
            ("uk_expr", False, None, None),
            ("uk_filtered", False, "b", "status = 'A'"),
        ])
        self.assertEqual(unique, ())

    def test_same_column_twice_deduplicated(self):
        _, unique = self.unique([
            ("uk_one", False, "num", None), ("uk_two", False, "num", None),
        ])
        self.assertEqual(unique, ("num",))


class TestColumnMetadata(unittest.TestCase):
    """Column facts mapped from a scripted getColumns result set."""

    def columns(self, rows):
        """Run _column_metadata over scripted label dicts."""
        metadata = MagicMock()
        metadata.getColumns.return_value = ScriptedResult(rows)
        return jdbc._column_metadata(metadata, MetadataLocation(None, "s", "t"))

    def test_types_normalized_and_keyed_lowercase(self):
        result = self.columns([
            {"COLUMN_NAME": "Id", "TYPE_NAME": "int identity", "COLUMN_SIZE": 10,
             "DECIMAL_DIGITS": 0, "IS_NULLABLE": "NO"},
            {"COLUMN_NAME": "note", "TYPE_NAME": "nvarchar", "COLUMN_SIZE": None,
             "DECIMAL_DIGITS": None, "IS_NULLABLE": "YES"},
        ])
        self.assertEqual(result, {
            "id": jdbc.ColumnMeta("Id", "INT IDENTITY", 10, 0, True),
            "note": jdbc.ColumnMeta("note", "NVARCHAR", None, None, False),
        })

    def test_null_names_and_types_skipped(self):
        result = self.columns([
            {"COLUMN_NAME": None, "TYPE_NAME": "int"},
            {"COLUMN_NAME": "ok", "TYPE_NAME": None},
        ])
        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
