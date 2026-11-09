"""Unit tests for the shared JDBC frame reader (spark/raw/rdbms/reader.py)."""

import sys
import types
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.sources.rdbms import jdbc
from ingestion.sources.rdbms import jdbc_partitioning
from ingestion.sources.rdbms import registry
from ingestion.spark.raw.rdbms import reader

MSSQL_URL = "jdbc:sqlserver://host:1433;databaseName=retail_bank"
ORACLE_URL = "jdbc:oracle:thin:@//host:1521/sydafa"


def make_args(**overrides):
    """Build the reader's args namespace (the shared surface of all three CLIs)."""
    values = {
        "jdbc_url": MSSQL_URL,
        "jdbc_user": "sydafa",
        "jdbc_password": "secret",
        "source_table": "retail_bank.clients",
        "num_partitions": 4,
        "no_jdbc_partitioning": False,
        "incremental_column": None,
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


class TestBoundLiteral(unittest.TestCase):
    """Rendering of JDBC partition bounds."""

    def test_integral_decimal_collapses_to_integer(self):
        self.assertEqual(reader.bound_literal(Decimal("1.0000000000")), "1")

    def test_fractional_decimal_kept_as_is(self):
        self.assertEqual(reader.bound_literal(Decimal("12.50")), "12.50")

    def test_int_and_float_untouched(self):
        self.assertEqual(reader.bound_literal(7), "7")
        self.assertEqual(reader.bound_literal(1.5), "1.5")


class TestRenderSourceTable(unittest.TestCase):
    """Dialect-quoted rendering of the source table reference."""

    def test_mssql_brackets(self):
        dialect = registry.by_conn_type("mssql")
        self.assertEqual(
            reader.render_source_table(dialect, "retail_bank.clients"),
            "[retail_bank].[clients]",
        )

    def test_oracle_quotes_preserve_case(self):
        dialect = registry.by_conn_type("oracle")
        self.assertEqual(
            reader.render_source_table(dialect, 'retail_bank."Clients"'),
            '"retail_bank"."Clients"',
        )

    def test_expression_passthrough_only_when_allowed(self):
        dialect = registry.by_conn_type("mssql")
        expression = "(SELECT * FROM s.t WHERE 1=0)"
        self.assertEqual(
            reader.render_source_table(dialect, expression, allow_expression=True),
            expression,
        )
        with self.assertRaises(ValueError):
            reader.render_source_table(dialect, expression)

    def test_malformed_reference_rejected(self):
        with self.assertRaises(ValueError):
            reader.render_source_table(registry.by_conn_type("mssql"), "a..b")


class RecordingReader:
    """A JDBC reader double recording options, drops and RDD partitions."""

    def __init__(self, collected=None, partitions=1):
        self.options = {}
        self.collected = collected
        self.dropped = []
        self.rdd = types.SimpleNamespace(getNumPartitions=lambda: partitions)

    def format(self, _):
        """Accept the format step."""
        return self

    def option(self, key, value):
        """Record one option and keep chaining."""
        self.options[key] = value
        return self

    def load(self):
        """Return the frame double."""
        return self

    def drop(self, column):
        """Record one dropped service column and keep chaining."""
        self.dropped.append(column)
        return self

    def cache(self):
        """Return the cached frame double."""
        return self

    def collect(self):
        """Return the prepared rows."""
        return self.collected


class TestJdbcReader(unittest.TestCase):
    """Reader construction from CLI arguments and dialect options."""

    def test_mssql_reader_options(self):
        spark = MagicMock()
        reader_double = RecordingReader()
        spark.read.format.return_value = reader_double
        built = reader.jdbc_reader(spark, make_args(), "(SELECT 1) q")
        self.assertIs(built, reader_double)
        self.assertEqual(reader_double.options, {
            "url": MSSQL_URL, "dbtable": "(SELECT 1) q", "user": "sydafa",
            "password": "secret", "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "preferTimestampNTZ": "true",
        })

    def test_oracle_driver_selected_by_url(self):
        spark = MagicMock()
        reader_double = RecordingReader()
        spark.read.format.return_value = reader_double
        reader.jdbc_reader(spark, make_args(jdbc_url=ORACLE_URL), "q")
        self.assertEqual(reader_double.options["driver"], "oracle.jdbc.OracleDriver")


class TestReadBounds(unittest.TestCase):
    """MIN/MAX probing over the read query."""

    def test_query_shape_and_tuple(self):
        spark = MagicMock()
        reader_double = RecordingReader(collected=[(1, 100)])
        with patch.object(reader, "jdbc_reader", return_value=reader_double) as builder:
            self.assertEqual(
                reader.read_bounds(spark, make_args(), "(SELECT 1) q", "client_id"), (1, 100)
            )
        query = builder.call_args.args[2]
        self.assertEqual(
            query,
            "(SELECT MIN([client_id]) AS lower_bound, MAX([client_id]) AS upper_bound "
            "FROM (SELECT 1) q) bounds",
        )


class TestWindowFilters(unittest.TestCase):
    """Pure assembly of the frozen-window predicates."""

    def setUp(self):
        self.dialect = registry.by_conn_type("mssql")

    def filters(self, batch, kind="bigint"):
        return reader.window_filters(
            self.dialect, batch, kind, getattr(batch, "incremental_column", "_batch_id")
        )

    def test_strict_window(self):
        batch = {"lower_bound": "5", "lower_bound_op": ">", "upper_bound": "10", "empty_at_plan": False}
        self.assertEqual(self.filters(batch), ["[_batch_id] > 5", "[_batch_id] <= 10"])

    def test_inclusive_lower_and_missing_bounds(self):
        batch = {"lower_bound": None, "lower_bound_op": ">=", "upper_bound": "10", "empty_at_plan": False}
        self.assertEqual(self.filters(batch), ["[_batch_id] <= 10"])
        batch["upper_bound"] = None
        self.assertEqual(self.filters(batch), [])

    def test_empty_at_plan_proves_no_rows(self):
        batch = {"lower_bound": "10", "lower_bound_op": ">", "upper_bound": "10", "empty_at_plan": True}
        self.assertEqual(self.filters(batch), ["[_batch_id] > 10", "[_batch_id] <= 10", "1=0"])

    def test_temporal_literal_rendered_by_kind(self):
        batch = {"lower_bound": "2026-10-23T00:00:00Z", "lower_bound_op": ">=",
                 "upper_bound": "2026-10-26T12:34:56Z", "empty_at_plan": False}
        self.assertEqual(
            self.filters(batch, kind="timestamp_instant(0)"),
            ["[_batch_id] >= CAST('2026-10-23T00:00:00Z' AS datetimeoffset(0))",
             "[_batch_id] <= CAST('2026-10-26T12:34:56Z' AS datetimeoffset(0))"],
        )


class TestReadExtractFrame(unittest.TestCase):
    """Frozen-window query assembly and heuristic partition discovery."""

    def setUp(self):
        self.spark = MagicMock()

    def key_metadata(self, primary_key=(), unique_columns=(), columns=()):
        """Build KeyMetadata from (name, type[, precision, scale, not_null]) tuples."""
        built = {}
        for item in columns:
            padded = tuple(item) + (None, None, True)
            info = jdbc.ColumnMeta(padded[0], padded[1], padded[2], padded[3], padded[4])
            built[item[0].lower()] = info
        return jdbc.KeyMetadata(
            primary_key=tuple(primary_key),
            unique_columns=tuple(unique_columns),
            columns=built,
        )

    def frame(self, args, batch, kind, projection=None, keys=None, bounds=(1, 100),
              partitions=1, reader_double=None):
        reader_double = reader_double if reader_double is not None else RecordingReader(partitions=partitions)
        with patch.object(reader.jdbc, "compatibility_projection", return_value=projection), patch.object(
            reader.jdbc, "key_metadata", return_value=keys or self.key_metadata()
        ) as key_call, patch.object(reader, "read_bounds", return_value=bounds) as probe, patch.object(
            reader, "jdbc_reader", return_value=reader_double
        ) as builder:
            result = reader.read_extract_frame(self.spark, args, batch, kind)
        return result, reader_double, builder.call_args.args[2], key_call

    def test_snapshot_without_keys_reads_single_partition(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        result, reader_double, query, key_call = self.frame(make_args(), batch, None)
        self.assertIs(result, reader_double)
        self.assertEqual(
            query,
            "(SELECT extract_source.* FROM (SELECT * FROM [retail_bank].[clients]) extract_source) extract_batch",
        )
        key_call.assert_called_once()
        self.assertNotIn("partitionColumn", reader_double.options)

    def test_snapshot_with_integer_pk_partitions_read(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(primary_key=["client_id"], columns=[("client_id", "int")])
        _, reader_double, _, _ = self.frame(make_args(), batch, None, keys=keys)
        self.assertEqual(
            {key: reader_double.options[key] for key in ("partitionColumn", "lowerBound", "upperBound", "numPartitions")},
            {"partitionColumn": "client_id", "lowerBound": "1", "upperBound": "100", "numPartitions": 4},
        )

    def test_incremental_column_has_no_partition_priority(self):
        args = make_args(incremental_column="_batch_id")
        batch = {"lower_bound": "5", "lower_bound_op": ">",
                 "upper_bound": "10", "empty_at_plan": False}
        keys = self.key_metadata(primary_key=["transaction_id"],
                                 columns=[("transaction_id", "bigint"), ("_batch_id", "int")])
        _, reader_double, query, _ = self.frame(args, batch, "bigint", keys=keys, bounds=(5, 10))
        self.assertIn("[_batch_id] > 5 AND [_batch_id] <= 10", query)
        self.assertIn(', [_batch_id] AS "__extract_position"', query)
        self.assertEqual(reader_double.options["partitionColumn"], "transaction_id")

    def test_timestamp_column_partitions_by_derived_day(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(columns=[("updated_at", "TIMESTAMP(9) WITH TIME ZONE")])
        result, reader_double, query, _ = self.frame(
            make_args(jdbc_url=ORACLE_URL), batch, None, keys=keys, partitions=4)
        self.assertIn(
            'CAST(TO_NUMBER(TO_CHAR(SYS_EXTRACT_UTC(extract_source."updated_at"), \'J\'))'
            ' AS NUMBER(10,0)) AS "__jdbc_day"',
            query,
        )
        self.assertEqual(reader_double.options["partitionColumn"], "__jdbc_day")
        self.assertIs(result, reader_double)
        self.assertEqual(reader_double.dropped, ["__jdbc_day"])

    def test_day_alias_dropped_even_without_parallel(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(columns=[("updated_at", "DATE")])
        _, reader_double, query, _ = self.frame(
            make_args(jdbc_url=ORACLE_URL), batch, None, keys=keys, bounds=(None, None))
        self.assertIn('"__jdbc_day"', query)
        self.assertNotIn("partitionColumn", reader_double.options)
        self.assertEqual(reader_double.dropped, ["__jdbc_day"])

    def test_day_alias_collision_skips_derived_partitioning(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(
            columns=[("updated_at", "TIMESTAMP(6)"), ("__jdbc_day", "NUMBER", 10, 0)]
        )
        with self.assertLogs(jdbc_partitioning.LOGGER, level="WARNING") as logs:
            _, reader_double, query, _ = self.frame(make_args(jdbc_url=ORACLE_URL), batch, None, keys=keys)
        self.assertNotIn('TO_NUMBER(TO_CHAR', query)
        self.assertNotIn("partitionColumn", reader_double.options)
        self.assertEqual(reader_double.dropped, [])
        self.assertTrue(any("collides with the day alias" in line for line in logs.output), logs.output)
        self.assertFalse(any("no partition column found" in line for line in logs.output))

    def test_date_column_partitions_directly(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(columns=[("posting_date", "date")])
        _, reader_double, query, _ = self.frame(make_args(), batch, None, keys=keys)
        self.assertNotIn("__jdbc_day", query)
        self.assertEqual(reader_double.options["partitionColumn"], "posting_date")

    def test_composite_pk_and_no_candidates_single_partition(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(primary_key=["code", "num"],
                                 columns=[("code", "nvarchar"), ("num", "int")])
        _, reader_double, _, _ = self.frame(make_args(), batch, None, keys=keys)
        self.assertNotIn("partitionColumn", reader_double.options)

    def test_equal_bounds_read_single_partition(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(primary_key=["client_id"], columns=[("client_id", "int")])
        _, reader_double, _, _ = self.frame(make_args(), batch, None, keys=keys, bounds=(7, 7))
        self.assertNotIn("partitionColumn", reader_double.options)

    def test_long_range_bounds_partition(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(primary_key=["id"], columns=[("id", "NUMBER", 38, 0)])
        edge = Decimal(2 ** 63 - 1)
        _, reader_double, _, _ = self.frame(
            make_args(jdbc_url=ORACLE_URL), batch, None, keys=keys,
            bounds=(-edge, edge), partitions=4)
        self.assertEqual(
            {key: reader_double.options[key] for key in ("lowerBound", "upperBound", "numPartitions")},
            {"lowerBound": str(-(2 ** 63 - 1)), "upperBound": str(2 ** 63 - 1), "numPartitions": 4},
        )

    def test_bounds_beyond_long_skip_partitioning_with_warning(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(primary_key=["id"], columns=[("id", "NUMBER", 38, 0)])
        beyond = Decimal(2 ** 63)
        with self.assertLogs(reader.LOGGER, level="WARNING") as logs:
            _, reader_double, _, _ = self.frame(
                make_args(jdbc_url=ORACLE_URL), batch, None, keys=keys,
                bounds=(beyond, beyond + 100), partitions=4)
        self.assertNotIn("partitionColumn", reader_double.options)
        self.assertTrue(any("exceed the signed 64-bit" in line for line in logs.output), logs.output)

    def test_log_reports_actual_partitions_of_loaded_frame(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(primary_key=["id"], columns=[("id", "int")])
        with self.assertLogs(reader.LOGGER, level="INFO") as logs:
            self.frame(make_args(), batch, None, keys=keys, bounds=(11, 12), partitions=1)
        self.assertTrue(any(
            "window=[11..12] partitions=1 of 4 requested" in line for line in logs.output
        ), logs.output)

    def test_aligned_lookback_lower_uses_inclusive_operator(self):
        args = make_args(incremental_column="_batch_id")
        batch = {"lower_bound": "2026-10-23T00:00:00Z",
                 "lower_bound_op": ">=", "upper_bound": "2026-10-26T12:34:56Z", "empty_at_plan": False}
        _, _, query, _ = self.frame(args, batch, "timestamp_instant(0)")
        self.assertIn("[_batch_id] >= ", query)

    def test_missing_lower_bound_fully_dropped(self):
        args = make_args(incremental_column="_batch_id")
        batch = {"lower_bound": None, "lower_bound_op": ">=",
                 "upper_bound": "10", "empty_at_plan": False}
        _, _, query, _ = self.frame(args, batch, "bigint")
        self.assertNotIn(">=", query)
        self.assertIn("[_batch_id] <= 10", query)

    def test_empty_window_proves_no_rows(self):
        args = make_args(incremental_column="_batch_id")
        batch = {"lower_bound": "10", "lower_bound_op": ">",
                 "upper_bound": "10", "empty_at_plan": True}
        _, _, query, _ = self.frame(args, batch, "bigint")
        self.assertIn("1=0", query)

    def test_disabled_partitioning_skips_probing(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        with patch.object(reader, "jdbc_reader", return_value=RecordingReader()), patch.object(
            reader.jdbc, "key_metadata"
        ) as keys, patch.object(reader, "read_bounds") as probe:
            reader.read_extract_frame(self.spark, make_args(no_jdbc_partitioning=True), batch, None)
        keys.assert_not_called()
        probe.assert_not_called()

    def test_no_candidate_column_warns_with_observed_metadata(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        keys = self.key_metadata(primary_key=["code"],
                                 columns=[("code", "nvarchar"), ("num", "int", None, None, False)])
        with self.assertLogs(jdbc_partitioning.LOGGER, level="WARNING") as logs:
            self.frame(make_args(), batch, None, keys=keys)
        self.assertTrue(any(
            "no partition column" in line and "retail_bank.clients" in line
            and "code nvarchar unsupported" in line and "num int integral null" in line
            for line in logs.output
        ), logs.output)

    def test_unavailable_metadata_does_not_duplicate_no_candidate_warning(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        with self.assertNoLogs(jdbc_partitioning.LOGGER, level="WARNING"):
            self.frame(make_args(), batch, None, keys=jdbc.KeyMetadata((), (), {}))

    def test_compatibility_projection_rewrites_select(self):
        batch = {"lower_bound": None, "lower_bound_op": ">",
                 "upper_bound": None, "empty_at_plan": False}
        projection = ['extract_source."a"', 'SYS_EXTRACT_UTC(extract_source."b") AS "b"']
        _, _, query, _ = self.frame(make_args(jdbc_url=ORACLE_URL), batch, None, projection=projection)
        self.assertIn('SELECT extract_source."a", SYS_EXTRACT_UTC(extract_source."b") AS "b"', query)
        self.assertIn('FROM (SELECT * FROM "retail_bank"."clients")', query)


if __name__ == "__main__":
    unittest.main()
