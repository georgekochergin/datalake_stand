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

    def test_expression_reference_rejected(self):
        with self.assertRaises(ValueError):
            reader.render_source_table(
                registry.by_conn_type("mssql"), "(SELECT * FROM s.t WHERE 1=0)"
            )

    def test_malformed_reference_rejected(self):
        with self.assertRaises(ValueError):
            reader.render_source_table(registry.by_conn_type("mssql"), "a..b")


class RecordingReader:
    """A JDBC reader double recording options, frame operations and RDD partitions."""

    def __init__(self, collected=None, partitions=1):
        self.options = {}
        self.collected = collected
        self.dropped = []
        self.frame_operations = []
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
        self.frame_operations.append(("cache",))
        return self

    def withColumns(self, columns):
        """Record column replacements before caching."""
        self.frame_operations.append(("withColumns", columns))
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


class TestProbeBounds(unittest.TestCase):
    """One statement for the requested aggregate set, none for a plain read."""

    def test_four_modes(self):
        dialect = reader.dialects.by_jdbc_url(make_args().jdbc_url)
        partition = jdbc_partitioning.PartitionChoice("id", "integer", jdbc_partitioning.STEP_PRIMARY_KEY)
        for incremental in (None, ("position", "bigint")):
            for choice in (None, partition):
                with self.subTest(incremental=incremental, partition=choice):
                    source = reader.ReadSource(dialect, "[s].[t]", jdbc.KeyMetadata((), (), {}), choice)
                    sql_row = (["97"] if incremental else []) + (["2", "6"] if choice else [])
                    filters = ["[position] > 90"] if incremental else []
                    with patch.object(jdbc, "connection"), patch.object(
                        jdbc, "first_row", return_value=sql_row
                    ) as sql, patch.object(reader, "log_sql") as sql_log, patch.object(reader.LOGGER, "info") as info:
                        result = reader.probe_bounds(MagicMock(), make_args(), source, filters,
                                                     scope=reader.JdbcReadScope.BATCH_WINDOW, incremental=incremental)
                    self.assertEqual(result, ("97" if incremental else None, (2, 6) if choice else (None, None)))
                    self.assertEqual(sql.call_count, int(bool(incremental or choice)))
                    if sql.called:
                        query = sql.call_args.args[1]
                        self.assertEqual("MAX([position])" in query, bool(incremental))
                        self.assertEqual("MIN([id])" in query and "MAX([id])" in query, bool(choice))
                        self.assertEqual(query.endswith("WHERE [position] > 90"), bool(incremental))
                        self.assertNotIn("COUNT(", query)
                        operation = {
                            (True, True): "combined_bounds",
                            (True, False): "source_position",
                            (False, True): "partition_bounds",
                        }[bool(incremental), bool(choice)]
                        self.assertTrue(sql_log.call_args.args[1].endswith(f"operation={operation}"))
                        result_logs = [call.args for call in info.call_args_list if call.args[0].startswith("JDBC result")]
                        self.assertEqual(len(result_logs), 1)
                        self.assertEqual(result_logs[0][1], operation)
                    else:
                        sql_log.assert_not_called()


class TestReadExtractFrame(unittest.TestCase):
    """Frozen-window query assembly and heuristic partition discovery."""

    def setUp(self):
        self.spark = MagicMock()

    def key_metadata(self, primary_key=(), unique_columns=(),
                     columns=(("client_id", "int", None, None, False),)):
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

    def frame(self, args, filters=(), extra_select="", projection=None, keys=None,
              bounds=(1, 100), partitions=1, reader_double=None):
        reader_double = reader_double if reader_double is not None else RecordingReader(partitions=partitions)
        with patch.object(reader.jdbc, "compatibility_projection", return_value=(projection, [])), patch.object(
            reader.jdbc, "key_metadata", return_value=keys or self.key_metadata()
        ) as key_call, patch.object(reader, "probe_bounds", return_value=(None, bounds)), patch.object(
            reader, "jdbc_reader", return_value=reader_double
        ) as builder:
            result = reader.read_extract_frame(self.spark, args, filters=filters, extra_select=extra_select)
        return result, reader_double, builder.call_args.args[2], key_call

    def test_whole_table_read_without_keys_single_partition(self):
        result, reader_double, query, key_call = self.frame(make_args())
        self.assertIs(result, reader_double)
        self.assertEqual(
            query,
            "(SELECT extract_source.* FROM (SELECT * FROM [retail_bank].[clients]) extract_source) extract_batch",
        )
        key_call.assert_called_once()
        self.assertNotIn("partitionColumn", reader_double.options)

    def test_integer_pk_partitions_read(self):
        keys = self.key_metadata(primary_key=["client_id"], columns=[("client_id", "int")])
        _, reader_double, _, _ = self.frame(make_args(), keys=keys)
        self.assertEqual(
            {key: reader_double.options[key] for key in ("partitionColumn", "lowerBound", "upperBound", "numPartitions")},
            {"partitionColumn": "client_id", "lowerBound": "1", "upperBound": "100", "numPartitions": 4},
        )

    def test_explicit_window_overrides_default_partition_priority(self):
        args = make_args()
        keys = self.key_metadata(primary_key=["transaction_id"],
                                 columns=[("transaction_id", "bigint"), ("_batch_id", "int")])
        _, reader_double, query, _ = self.frame(
            args, filters=["[_batch_id] > 5", "[_batch_id] <= 10"],
            extra_select=', [_batch_id] AS "__extract_position"', keys=keys, bounds=(5, 10),
        )
        self.assertIn("[_batch_id] > 5 AND [_batch_id] <= 10", query)
        self.assertIn(', [_batch_id] AS "__extract_position"', query)
        self.assertEqual(reader_double.options["partitionColumn"], "transaction_id")

    def test_precomputed_bounds_do_not_trigger_another_query(self):
        args = make_args()
        keys = self.key_metadata(primary_key=["client_id"], columns=[("client_id", "int")])
        source = reader.ReadSource(reader.dialects.by_jdbc_url(args.jdbc_url), "[s].[t]", keys,
                                   jdbc_partitioning.PartitionChoice("client_id", "integer", "primary_key"))
        jdbc_reader = RecordingReader()
        with patch.object(reader, "jdbc_reader", return_value=jdbc_reader) as build, patch.object(
            reader, "probe_bounds"
        ) as probe:
            reader.read_extract_frame(self.spark, args, filters=("[position] <= 97",), source=source, bounds=(2, 6))
        probe.assert_not_called()
        self.assertIn("[position] <= 97", build.call_args.args[2])
        self.assertEqual(jdbc_reader.options["upperBound"], "6")

    def test_timestamp_column_does_not_partition(self):
        keys = self.key_metadata(columns=[("updated_at", "TIMESTAMP(9) WITH TIME ZONE")])
        _, reader_double, query, _ = self.frame(make_args(jdbc_url=ORACLE_URL), keys=keys)
        self.assertNotIn("__jdbc_day", query)
        self.assertNotIn("partitionColumn", reader_double.options)
        self.assertEqual(reader_double.dropped, [])

    def test_date_column_partitions_directly(self):
        keys = self.key_metadata(columns=[("posting_date", "date")])
        _, reader_double, query, _ = self.frame(make_args(), keys=keys)
        self.assertNotIn("__jdbc_day", query)
        self.assertEqual(reader_double.options["partitionColumn"], "posting_date")

    def test_composite_pk_and_no_candidates_single_partition(self):
        keys = self.key_metadata(primary_key=["code", "num"],
                                 columns=[("code", "nvarchar"), ("num", "int")])
        _, reader_double, _, _ = self.frame(make_args(), keys=keys)
        self.assertNotIn("partitionColumn", reader_double.options)

    def test_equal_bounds_read_single_partition(self):
        keys = self.key_metadata(primary_key=["client_id"], columns=[("client_id", "int")])
        _, reader_double, _, _ = self.frame(make_args(), keys=keys, bounds=(7, 7))
        self.assertNotIn("partitionColumn", reader_double.options)

    def test_long_range_bounds_partition(self):
        keys = self.key_metadata(primary_key=["id"], columns=[("id", "NUMBER", 38, 0)])
        edge = Decimal(2 ** 63 - 1)
        _, reader_double, _, _ = self.frame(
            make_args(jdbc_url=ORACLE_URL), keys=keys,
            bounds=(-edge, edge), partitions=4)
        self.assertEqual(
            {key: reader_double.options[key] for key in ("lowerBound", "upperBound", "numPartitions")},
            {"lowerBound": str(-(2 ** 63 - 1)), "upperBound": str(2 ** 63 - 1), "numPartitions": 4},
        )

    def test_bounds_beyond_long_skip_partitioning_with_warning(self):
        keys = self.key_metadata(primary_key=["id"], columns=[("id", "NUMBER", 38, 0)])
        beyond = Decimal(2 ** 63)
        with self.assertLogs(reader.LOGGER, level="WARNING") as logs:
            _, reader_double, _, _ = self.frame(
                make_args(jdbc_url=ORACLE_URL), keys=keys,
                bounds=(beyond, beyond + 100), partitions=4)
        self.assertNotIn("partitionColumn", reader_double.options)
        self.assertTrue(any("exceed the signed 64-bit" in line for line in logs.output), logs.output)

    def test_log_reports_actual_partitions_of_loaded_frame(self):
        keys = self.key_metadata(primary_key=["id"], columns=[("id", "int")])
        with self.assertLogs(reader.LOGGER, level="INFO") as logs:
            self.frame(make_args(), keys=keys, bounds=(11, 12), partitions=1)
        self.assertTrue(any(
            "window=[11..12] partitions=1 of 4 requested" in line for line in logs.output
        ), logs.output)

    def test_single_requested_partition_has_its_own_reason(self):
        keys = self.key_metadata(primary_key=["id"], columns=[("id", "int")])
        with self.assertLogs(reader.LOGGER, level="INFO") as logs:
            self.frame(make_args(num_partitions=1), keys=keys)
        self.assertTrue(any("Only one JDBC partition was requested. reason=single_partition" in line
                            for line in logs.output), logs.output)

    def test_disabled_partitioning_still_requires_metadata(self):
        keys = self.key_metadata()
        with patch.object(reader, "jdbc_reader", return_value=RecordingReader()), patch.object(
            reader.jdbc, "key_metadata", return_value=keys
        ) as key_call, patch.object(reader, "probe_bounds") as probe:
            reader.read_extract_frame(self.spark, make_args(no_jdbc_partitioning=True))
        key_call.assert_called_once()
        probe.assert_not_called()

    def test_no_candidate_column_warns_with_observed_metadata(self):
        keys = self.key_metadata(primary_key=["code"],
                                 columns=[("code", "nvarchar"), ("num", "int", None, None, False)])
        with self.assertLogs(jdbc_partitioning.LOGGER, level="WARNING") as logs:
            self.frame(make_args(), keys=keys)
        self.assertTrue(any(
            "no partition column" in line and "retail_bank.clients" in line
            and "code nvarchar unsupported" in line and "num int integral null" in line
            for line in logs.output
        ), logs.output)

    def test_unavailable_metadata_fails_fast_without_partitioning_warning(self):
        with self.assertNoLogs(jdbc_partitioning.LOGGER, level="WARNING"):
            with self.assertRaisesRegex(RuntimeError, "catalog metadata unavailable"):
                self.frame(make_args(), keys=jdbc.KeyMetadata((), (), {}))

    def test_compatibility_projection_rewrites_select(self):
        projection = ['extract_source."a"', 'DATEDIFF_BIG(MICROSECOND, epoch, extract_source."b") AS "b"']
        _, _, query, _ = self.frame(make_args(jdbc_url=ORACLE_URL), projection=projection)
        self.assertIn('SELECT ' + ', '.join(projection), query)
        self.assertIn('FROM (SELECT * FROM "retail_bank"."clients")', query)

    def test_converts_only_projected_microseconds_before_cache(self):
        keys = self.key_metadata(columns=[("local_time", "datetime2"), ("event.at`utc", "datetimeoffset")])
        source = reader.ReadSource(registry.by_conn_type("mssql"), "[s].[t]", keys, None)
        frame = RecordingReader()
        functions = MagicMock()
        sql = types.ModuleType("pyspark.sql")
        sql.functions = functions
        pyspark = types.ModuleType("pyspark")
        pyspark.sql = sql
        with patch.dict(sys.modules, {"pyspark": pyspark, "pyspark.sql": sql}), patch.object(
            reader, "jdbc_reader", return_value=frame
        ):
            result = reader.read_extract_frame(self.spark, make_args(), source=source)

        functions.col.assert_called_once_with("`event.at``utc`")
        functions.col.return_value.cast.assert_called_once_with("long")
        functions.timestamp_micros.assert_called_once_with(functions.col.return_value.cast.return_value)
        self.assertIs(result, frame)
        self.assertEqual(frame.frame_operations, [
            ("withColumns", {"event.at`utc": functions.timestamp_micros.return_value}),
            ("cache",),
        ])


if __name__ == "__main__":
    unittest.main()
