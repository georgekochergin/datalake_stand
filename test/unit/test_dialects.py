"""Dialect registry and MSSQL/Oracle adapter contract tests."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.sources.rdbms import positions, registry
from ingestion.sources.rdbms.contracts import ConnectionSpec, MetadataLocation

MSSQL = registry.by_conn_type("mssql")
ORACLE = registry.by_conn_type("oracle")
MSSQL_URL = "jdbc:sqlserver://source"
ORACLE_URL = "jdbc:oracle:thin:@source"


def literal(dialect, value, kind):
    """Render a position literal the way the loader does: canonicalize first."""
    return dialect.position_literal(positions.canonical(value, kind), kind)


class TestRegistry(unittest.TestCase):
    """Resolution by connection type and JDBC URL stays consistent."""

    def test_resolves_aliases_and_prefixes(self):
        self.assertIs(registry.by_conn_type("mssql"), MSSQL)
        self.assertIs(registry.by_jdbc_url("jdbc:sqlserver://h:1433;databaseName=db"), MSSQL)
        self.assertIs(registry.by_conn_type("oracle"), ORACLE)
        self.assertIs(registry.by_jdbc_url("jdbc:oracle:thin:@//h:1521/s"), ORACLE)

    def test_unknown_conn_type_rejected(self):
        with self.assertRaisesRegex(ValueError, "supported JDBC dialect"):
            registry.by_conn_type("postgres")

    def test_unknown_and_empty_urls_rejected(self):
        for url in ("jdbc:postgresql://h/db", "", None, "sqlserver://h"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                registry.by_jdbc_url(url)

    def test_built_urls_resolve_back_to_their_dialect(self):
        spec = ConnectionSpec(host="h", port=None, database="db", user="u", password="p")
        for dialect in registry.all_dialects():
            with self.subTest(dialect=dialect.conn_type_aliases):
                self.assertIs(registry.by_jdbc_url(dialect.build_jdbc_url(spec)), dialect)

    def test_supported_lists_cover_every_dialect(self):
        dialects = registry.all_dialects()
        self.assertTrue(dialects)
        self.assertEqual(registry.supported_conn_types(), ("mssql", "oracle"))
        self.assertEqual(
            registry.supported_url_prefixes(),
            tuple(sorted(prefix for dialect in dialects for prefix in dialect.url_prefixes)),
        )


class TestConnections(unittest.TestCase):
    """URL building, default ports, and driver classes."""

    def test_mssql_url_uses_explicit_or_default_port(self):
        spec = ConnectionSpec(host="h", port=1434, database="retail", user="u", password="p")
        self.assertEqual(MSSQL.build_jdbc_url(spec), "jdbc:sqlserver://h:1434;databaseName=retail;encrypt=false")
        spec = ConnectionSpec(host="h", port=None, database="retail", user="u", password="p")
        self.assertEqual(MSSQL.build_jdbc_url(spec), "jdbc:sqlserver://h:1433;databaseName=retail;encrypt=false")

    def test_oracle_url_uses_service_name(self):
        spec = ConnectionSpec(host="h", port=None, database="sydafa", user="u", password="p")
        self.assertEqual(ORACLE.build_jdbc_url(spec), "jdbc:oracle:thin:@//h:1521/sydafa")
        spec = ConnectionSpec(host="h", port=1522, database="sydafa", user="u", password="p")
        self.assertEqual(ORACLE.build_jdbc_url(spec), "jdbc:oracle:thin:@//h:1522/sydafa")

    def test_driver_classes(self):
        self.assertEqual(MSSQL.driver_class, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        self.assertEqual(ORACLE.driver_class, "oracle.jdbc.OracleDriver")

    def test_reader_options_prefer_timestamp_ntz(self):
        for dialect in registry.all_dialects():
            self.assertEqual(dialect.reader_options(), {"preferTimestampNTZ": "true"})


class TestQuoting(unittest.TestCase):
    """Dialect-aware column and table references for source SQL."""

    def test_mssql_column_bracket_quoted(self):
        self.assertEqual(MSSQL.quote_identifier("_batch_id"), "[_batch_id]")

    def test_mssql_bracket_escaped(self):
        self.assertEqual(MSSQL.quote_identifier("co]lumn"), "[co]]lumn]")

    def test_oracle_columns_quoted(self):
        self.assertEqual(ORACLE.quote_identifier("_batch_id"), '"_batch_id"')
        self.assertEqual(ORACLE.quote_identifier("updated_at"), '"updated_at"')

    def test_oracle_quote_escaped(self):
        self.assertEqual(ORACLE.quote_identifier('na"me'), '"na""me"')

    def test_render_table_quotes_both_parts(self):
        self.assertEqual(MSSQL.render_table([("retail_bank", False), ("clients", False)]),
                         "[retail_bank].[clients]")
        self.assertEqual(ORACLE.render_table([("trading_core", False), ("transactions", False)]),
                         '"trading_core"."transactions"')

    def test_render_table_preserves_quoted_case(self):
        self.assertEqual(ORACLE.render_table([("Mixed.Schema", True), ('Ta"ble.part', True)]),
                         '"Mixed.Schema"."Ta""ble.part"')


class TestPositionKind(unittest.TestCase):
    """Source schema mapping onto the persisted position contract."""

    def test_metadata(self):
        for dialect, name, precision, scale, expected in (
            (MSSQL, "BIGINT", 19, 0, "bigint"),
            (MSSQL, "int", 10, 0, "integer"),
            (MSSQL, "DATE", 10, 0, "date"),
            (ORACLE, "DATE", 7, 0, "timestamp_local(0)"),
            (MSSQL, "datetime2", 27, 7, "timestamp_local(7)"),
            (MSSQL, "datetimeoffset", 34, 7, "timestamp_instant(7)"),
            (MSSQL, "datetime", 23, 3, "timestamp_local(3)"),
            (MSSQL, "smalldatetime", 16, 0, "timestamp_local(0)"),
            (ORACLE, "TIMESTAMP", 29, 9, "timestamp_local(9)"),
            (ORACLE, "TIMESTAMP(9) WITH TIME ZONE", 0, 9, "timestamp_instant(9)"),
            (ORACLE, "TIMESTAMP WITH LOCAL TIME ZONE", 0, 6, "timestamp_instant(6)"),
            (ORACLE, "NUMBER", 38, 0, "decimal(38,0)"),
            (ORACLE, "NUMBER", 12, 0, "decimal(12,0)"),
            (MSSQL, "numeric", 18, 0, "decimal(18,0)"),
        ):
            with self.subTest(name=name):
                self.assertEqual(dialect.position_kind(name, precision, scale), expected)

    def test_unsupported_metadata(self):
        for dialect, name, precision, scale in (
            (MSSQL, "timestamp", 8, 0), (MSSQL, "rowversion", 8, 0),
            (ORACLE, "NUMBER", 0, -127), (ORACLE, "NUMBER", 39, 0),
            (ORACLE, "NUMBER", 12, 2), (ORACLE, "VARCHAR", 50, 0),
            (MSSQL, "datetime2", 28, 8), (ORACLE, "TIMESTAMP", 30, 10),
            (MSSQL, "VARCHAR", 50, 0),
        ):
            with self.subTest(name=name), self.assertRaises(ValueError):
                dialect.position_kind(name, precision, scale)

    def test_non_string_type_name_rejected(self):
        for dialect in registry.all_dialects():
            with self.assertRaises(ValueError):
                dialect.position_kind(None, 10, 0)


class TestPositionLiteral(unittest.TestCase):
    """Injection-safe typed SQL literals for canonicalized positions."""

    def test_sql_literals(self):
        self.assertEqual(literal(MSSQL, "10", "bigint"), "10")
        self.assertEqual(literal(ORACLE, None, "bigint"), "NULL")
        self.assertEqual(literal(MSSQL, "2026-10-26", "date"), "CAST('2026-10-26' AS date)")
        self.assertEqual(literal(ORACLE, "2026-10-26", "date"), "TO_DATE('2026-10-26', 'YYYY-MM-DD')")
        value = "2026-10-26T12:00:00.1234567"
        self.assertEqual(literal(MSSQL, value, "timestamp_local(7)"), f"CAST('{value}' AS datetime2(7))")
        self.assertEqual(literal(MSSQL, value + "Z", "timestamp_instant(7)"), f"CAST('{value}Z' AS datetimeoffset(7))")
        value = "2026-10-26T12:00:00.123456789"
        self.assertEqual(literal(ORACLE, value, "timestamp_local(9)"), f"TO_TIMESTAMP('{value}', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9')")
        self.assertEqual(literal(ORACLE, value + "Z", "timestamp_instant(9)"), f"TO_TIMESTAMP_TZ('{value}+00:00', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM')")

    def test_mssql_precision_limit(self):
        with self.assertRaisesRegex(ValueError, "cannot exceed 7"):
            literal(MSSQL, "2026-10-26T12:00:00.123456789", "timestamp_local(9)")

    def test_injection_rejected_before_sql(self):
        with self.assertRaises(ValueError):
            literal(MSSQL, "1' OR 1=1", "bigint")


class TestPositionExpression(unittest.TestCase):
    """Lossless source-side temporal string rendering per dialect."""

    def test_mssql_local_precision(self):
        self.assertEqual(
            MSSQL.position_expression("updated_at", "timestamp_local(0)"),
            "CONVERT(char(19), CAST(updated_at AS datetime2(0)), 126)",
        )
        self.assertEqual(
            MSSQL.position_expression("updated_at", "timestamp_local(3)"),
            "CONVERT(char(19), CAST(updated_at AS datetime2(3)), 126) "
            "+ '.' + LEFT(RIGHT('000000000' + CAST(DATEPART(nanosecond, updated_at) AS varchar(9)), 9), 3)",
        )

    def test_mssql_instant_uses_switchoffset_and_z_suffix(self):
        offset = "SWITCHOFFSET(updated_at, '+00:00')"
        self.assertEqual(
            MSSQL.position_expression("updated_at", "timestamp_instant(7)"),
            f"CONVERT(char(19), CAST({offset} AS datetime2(7)), 126) "
            f"+ '.' + LEFT(RIGHT('000000000' + CAST(DATEPART(nanosecond, {offset}) AS varchar(9)), 9), 7) + 'Z'",
        )

    def test_oracle_local_and_instant_precision(self):
        self.assertEqual(
            ORACLE.position_expression('"updated_at"', "timestamp_local(0)"),
            "TO_CHAR(\"updated_at\", 'YYYY-MM-DD\"T\"HH24:MI:SS')",
        )
        self.assertEqual(
            ORACLE.position_expression('"updated_at"', "timestamp_local(9)"),
            "TO_CHAR(\"updated_at\", 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9')",
        )
        self.assertEqual(
            ORACLE.position_expression('"updated_at"', "timestamp_instant(9)"),
            "TO_CHAR(SYS_EXTRACT_UTC(\"updated_at\"), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9\"Z\"')",
        )

    def test_non_temporal_kinds_pass_column_through(self):
        for kind in ("bigint", "integer", "decimal(38,0)", "date"):
            self.assertEqual(MSSQL.position_expression("[_batch_id]", kind), "[_batch_id]")
            self.assertEqual(ORACLE.position_expression('"x"', kind), '"x"')


class TestColumnProjection(unittest.TestCase):
    """Special-type read expressions; every dialect except Oracle reads natively."""

    def test_mssql_reads_natively(self):
        for type_name in ("datetime2", "datetimeoffset", "nvarchar", "TIMESTAMP WITH TIME ZONE"):
            self.assertIsNone(MSSQL.column_projection(type_name, "extract_source.[ts]"))

    def test_oracle_rewrites_timezone_columns_only(self):
        for type_name in ("TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"):
            self.assertEqual(
                ORACLE.column_projection(type_name, 'extract_source."ts"'),
                'SYS_EXTRACT_UTC(extract_source."ts")',
            )
        for type_name in ("TIMESTAMP(9)", "DATE", "NUMBER", "VARCHAR2"):
            self.assertIsNone(ORACLE.column_projection(type_name, 'extract_source."ts"'))


class TestMetadataCandidates(unittest.TestCase):
    """JDBC metadata lookup variants fold only unquoted fragments."""

    def test_only_unquoted_fragments_fold(self):
        expected = (MetadataLocation(None, "schema", "Mixed.Table"),
                    MetadataLocation(None, "SCHEMA", "Mixed.Table"))
        for dialect in registry.all_dialects():
            self.assertEqual(
                dialect.metadata_candidates([("schema", False), ("Mixed.Table", True)]),
                expected,
            )

    def test_fully_quoted_names_never_fold(self):
        for dialect in registry.all_dialects():
            self.assertEqual(
                dialect.metadata_candidates([("Schema", True), ("Table", True)]),
                (MetadataLocation(None, "Schema", "Table"),),
            )




class TestPartitionTypes(unittest.TestCase):
    """Partition-column type classification belongs to the dialect, not the loader."""

    def test_standard_names_accepted_for_both_engines(self):
        for dialect in registry.all_dialects():
            for type_name in ("bigint", "int identity", "NUMBER", "datetime2", "TIMESTAMP WITH TIME ZONE"):
                with self.subTest(dialect=dialect.conn_type_aliases, type_name=type_name):
                    self.assertTrue(dialect.is_partition_type(type_name))

    def test_vendor_names_rejected_by_current_engines(self):
        for dialect in registry.all_dialects():
            for type_name in ("int8", "nvarchar", "varchar2", "", "rowversion", "datetimeoffset"):
                with self.subTest(dialect=dialect.conn_type_aliases, type_name=type_name):
                    self.assertFalse(dialect.is_partition_type(type_name))


if __name__ == "__main__":
    unittest.main()
