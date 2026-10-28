"""JDBC access shared by RDBMS loaders: connections, metadata, and projections.

Every helper takes a live JVM handle instead of importing pyspark, so this
layer stays importable and unit-testable without Spark installed. Dialects
define the rules (identifier folding, special-column projections); this layer
executes JDBC operations and owns resource lifecycles.
"""

import logging
from contextlib import contextmanager

from .contracts import normalize_type_name, parse_source_table

_LOGGER = logging.getLogger(__name__)


@contextmanager
def connection(jvm, jdbc_url, user, password):
    """Own a short JDBC connection and close it."""
    jdbc_connection = jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    try:
        yield jdbc_connection
    finally:
        jdbc_connection.close()


def column_metadata(jdbc_connection, sql) -> list[tuple]:
    """Return (label, raw type name, precision, scale) per column of a query."""
    jdbc_statement = jdbc_connection.createStatement()
    try:
        result = jdbc_statement.executeQuery(sql)
        try:
            metadata = result.getMetaData()
            return [
                (metadata.getColumnLabel(index), metadata.getColumnTypeName(index),
                 metadata.getPrecision(index), metadata.getScale(index))
                for index in range(1, metadata.getColumnCount() + 1)
            ]
        finally:
            result.close()
    finally:
        jdbc_statement.close()


def first_row(jdbc_connection, sql) -> list[str]:
    """Execute a query and return its first row rendered as strings."""
    jdbc_statement = jdbc_connection.createStatement()
    try:
        result = jdbc_statement.executeQuery(sql)
        try:
            result.next()
            return [result.getString(index) for index in range(1, _row_size(result) + 1)]
        finally:
            result.close()
    finally:
        jdbc_statement.close()


def _row_size(result) -> int:
    """Return the result set column count."""
    return result.getMetaData().getColumnCount()


def compatibility_projection(jvm, dialect, jdbc_url, user, password, table_sql) -> list[str] | None:
    """Build projected select items when the dialect rewrites special columns.

    Returns None when the dialect reads every column natively, leaving the
    plain extract_source.* select untouched.
    """
    with connection(jvm, jdbc_url, user, password) as jdbc_connection:
        columns = column_metadata(jdbc_connection, f"SELECT * FROM {table_sql} WHERE 1=0")
    projected = []
    rewrote = False
    for label, type_name, _, _ in columns:
        reference = dialect.quote_identifier(str(label))
        expression = dialect.column_projection(
            normalize_type_name(type_name), f"extract_source.{reference}"
        )
        if expression is None:
            projected.append(f"extract_source.{reference}")
        else:
            rewrote = True
            projected.append(f"{expression} AS {reference}")
    return projected if rewrote else None


def key_columns(jvm, dialect, jdbc_url, user, password, source_table) -> list[tuple[str, str]]:
    """Best-effort ordered (column, JDBC type) candidates from PK and unique indexes.

    Unavailable metadata degrades to a warning and an empty list: the loader
    then falls back to an unpartitioned read.
    """
    try:
        parts = parse_source_table(source_table)
        with connection(jvm, jdbc_url, user, password) as jdbc_connection:
            metadata = jdbc_connection.getMetaData()
            for candidate_schema, candidate_table in dialect.metadata_candidates(parts):
                ordered = _ordered_key_columns(metadata, candidate_schema, candidate_table)
                if ordered:
                    types = _column_types(metadata, candidate_schema, candidate_table)
                    return [(name, types.get(name.lower(), "")) for name in ordered]
    except Exception as exc:
        _LOGGER.warning(
            "JDBC metadata unavailable for %s; falling back to an unpartitioned read: %s",
            source_table, exc,
        )
        return []
    _LOGGER.warning(
        "No JDBC key metadata found for %s; falling back to an unpartitioned read",
        source_table,
    )
    return []


def _ordered_key_columns(metadata, schema, table) -> list[str]:
    """Primary-key columns in KEY_SEQ order, then unique-index columns."""
    ordered: list[str] = []
    primary_keys = metadata.getPrimaryKeys(None, schema, table)
    try:
        pairs = []
        while primary_keys.next():
            pairs.append((primary_keys.getInt("KEY_SEQ"), primary_keys.getString("COLUMN_NAME")))
        for _, name in sorted(pairs):
            if name not in ordered:
                ordered.append(name)
    finally:
        primary_keys.close()
    indexes = metadata.getIndexInfo(None, schema, table, True, True)
    try:
        while indexes.next():
            if not indexes.getBoolean("NON_UNIQUE"):
                name = indexes.getString("COLUMN_NAME")
                if name is not None and name not in ordered:
                    ordered.append(name)
    finally:
        indexes.close()
    return ordered


def _column_types(metadata, schema, table) -> dict[str, str]:
    """Map lowercased column names to JDBC TYPE_NAME values."""
    types: dict[str, str] = {}
    columns = metadata.getColumns(None, schema, table, None)
    try:
        while columns.next():
            name = columns.getString("COLUMN_NAME")
            type_name = columns.getString("TYPE_NAME")
            if name is not None and type_name is not None:
                types[name.lower()] = type_name
    finally:
        columns.close()
    return types
