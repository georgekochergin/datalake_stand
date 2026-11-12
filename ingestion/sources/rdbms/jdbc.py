"""JDBC access shared by RDBMS loaders: connections, metadata, and projections.

Every helper takes a live JVM handle instead of importing pyspark, so this
layer stays importable and unit-testable without Spark installed. Dialects
define the rules (identifier folding, special-column projections); this layer
executes JDBC operations and owns resource lifecycles.
"""

import logging
from contextlib import contextmanager
from dataclasses import dataclass

from .contracts import parse_source_table
from .dialects.base import normalize_type_name

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


def column_position_kind(dialect, info: ColumnMeta) -> str | None:
    """Canonical kind of a catalog column, or None when the dialect rejects it.

    Drivers decorate catalog type names with engine specifics (MSSQL appends
    "identity" to integer types), so a rejected full name is retried by its
    first token; the dialect contract itself stays strict.
    """
    try:
        return dialect.position_kind(info.type_name, info.precision, info.scale)
    except (ValueError, NotImplementedError):
        pass
    base = info.type_name.split()[0] if info.type_name else ""
    if base != info.type_name:
        try:
            return dialect.position_kind(base, info.precision, info.scale)
        except (ValueError, NotImplementedError):
            return None
    return None


def compatibility_projection(dialect, keys: KeyMetadata) -> list[str] | None:
    """Build projected select items when the dialect rewrites special columns.

    Column facts come from the JDBC catalog (getColumns), the same source as
    the partitioning choice, so no SQL is executed against the source.
    Returns None when the dialect reads every column natively, leaving the
    plain extract_source.* select untouched.
    """
    projected = []
    rewrote = False
    for info in keys.columns.values():
        alias = dialect.quote_identifier(info.name)
        reference = f"extract_source.{alias}"
        expression = dialect.column_projection(info.type_name, reference)
        if expression is None:
            projected.append(reference)
        else:
            rewrote = True
            projected.append(f"{expression} AS {alias}")
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
            location = dialect.metadata_location(parts)
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
