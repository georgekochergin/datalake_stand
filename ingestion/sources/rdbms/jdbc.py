"""JDBC access shared by RDBMS loaders: connections, metadata, and projections.

Every helper takes a live JVM handle instead of importing pyspark, so this
layer stays importable and unit-testable without Spark installed. Dialects
define the rules (identifier folding, special-column projections); this layer
executes JDBC operations and owns resource lifecycles.
"""

import logging
from contextlib import contextmanager
from dataclasses import dataclass

from .contracts import normalize_type_name, parse_source_table

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ColumnMeta:
    """JDBC column facts the partitioning heuristic decides on."""

    name: str
    type_name: str
    precision: int | None
    scale: int | None
    not_null: bool


@dataclass(frozen=True)
class KeyMetadata:
    """Primary key, single-column unique indexes and column facts of a table."""

    primary_key: tuple[str, ...]
    unique_columns: tuple[str, ...]
    columns: dict[str, ColumnMeta]


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


def key_metadata(jvm, dialect, jdbc_url, user, password, source_table) -> KeyMetadata:
    """Best-effort key metadata for the partitioning heuristic.

    A located table without keys still returns its columns: the heuristic
    can partition by a plain date or timestamp column. Unavailable metadata
    degrades to a warning and an empty result: the loader then falls back
    to an unpartitioned read.
    """
    try:
        parts = parse_source_table(source_table)
        with connection(jvm, jdbc_url, user, password) as jdbc_connection:
            metadata = jdbc_connection.getMetaData()
            for location in dialect.metadata_candidates(parts):
                columns = _column_metadata(metadata, location)
                if columns:
                    return KeyMetadata(
                        primary_key=_primary_key(metadata, location),
                        unique_columns=_unique_single_columns(metadata, location),
                        columns=columns,
                    )
    except Exception as exc:
        _LOGGER.warning(
            "JDBC metadata unavailable for %s; falling back to an unpartitioned read: %s",
            source_table, exc,
        )
        return KeyMetadata((), (), {})
    _LOGGER.warning(
        "No JDBC table metadata found for %s; falling back to an unpartitioned read",
        source_table,
    )
    return KeyMetadata((), (), {})


def _primary_key(metadata, location) -> tuple[str, ...]:
    """Primary-key column names in KEY_SEQ order."""
    pairs: list[tuple[int, str]] = []
    primary_keys = metadata.getPrimaryKeys(location.catalog, location.schema, location.table)
    try:
        while primary_keys.next():
            name = primary_keys.getString("COLUMN_NAME")
            if name is not None:
                pairs.append((primary_keys.getInt("KEY_SEQ"), name))
    finally:
        primary_keys.close()
    return tuple(name for _, name in sorted(pairs))


def _unique_single_columns(metadata, location) -> tuple[str, ...]:
    """Columns covered alone by a unique, unfiltered, non-functional index.

    Composite indexes contribute nothing: rows group by index name and only
    single-row groups survive. Functional indexes surface with a NULL
    COLUMN_NAME, filtered ones with a FILTER_CONDITION.
    """
    grouped: dict[str, list[str | None]] = {}
    indexes = metadata.getIndexInfo(location.catalog, location.schema, location.table, True, True)
    try:
        while indexes.next():
            if indexes.getBoolean("NON_UNIQUE"):
                continue
            if indexes.getString("FILTER_CONDITION") is not None:
                continue
            index_name = indexes.getString("INDEX_NAME")
            if index_name is None:
                continue
            grouped.setdefault(index_name, []).append(indexes.getString("COLUMN_NAME"))
    finally:
        indexes.close()
    names: list[str] = []
    for rows in grouped.values():
        if len(rows) == 1 and rows[0] is not None and rows[0] not in names:
            names.append(rows[0])
    return tuple(names)


def _optional_int(result, label) -> int | None:
    """Read an int column of a JDBC result set treating SQL NULL as None."""
    value = result.getInt(label)
    return None if result.wasNull() else value


def _column_metadata(metadata, location) -> dict[str, ColumnMeta]:
    """Map lowercased column names to type facts for one table location."""
    result: dict[str, ColumnMeta] = {}
    columns = metadata.getColumns(location.catalog, location.schema, location.table, None)
    try:
        while columns.next():
            name = columns.getString("COLUMN_NAME")
            type_name = columns.getString("TYPE_NAME")
            if name is None or type_name is None:
                continue
            result[name.lower()] = ColumnMeta(
                name=name,
                type_name=normalize_type_name(type_name),
                precision=_optional_int(columns, "COLUMN_SIZE"),
                scale=_optional_int(columns, "DECIMAL_DIGITS"),
                not_null=columns.getString("IS_NULLABLE") == "NO",
            )
    finally:
        columns.close()
    return result
