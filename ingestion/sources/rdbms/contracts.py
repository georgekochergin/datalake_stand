"""Connection and SQL contracts shared by every RDBMS dialect adapter."""

import re
from dataclasses import dataclass

SOURCE_PART_PATTERN = re.compile(r'"((?:[A-Za-z0-9_.]|"")+)"|([A-Za-z0-9_]+)')


@dataclass(frozen=True)
class ConnectionSpec:
    """Engine-agnostic connection parameters resolved from an Airflow Connection."""

    host: str
    port: int | None
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class MetadataLocation:
    """Full JDBC metadata lookup address: catalog, schema, and table."""

    catalog: str | None
    schema: str | None
    table: str


def parse_source_table(source_table: str) -> list[tuple[str, bool]]:
    """Parse schema.table, retaining quoted case and embedded dots for JDBC metadata."""
    parts = []
    offset = 0
    while offset < len(source_table):
        match = SOURCE_PART_PATTERN.match(source_table, offset)
        if match is None:
            raise ValueError(f"invalid source table reference: {source_table!r}")
        quoted, unquoted = match.groups()
        parts.append((quoted.replace('""', '"') if quoted is not None else unquoted, quoted is not None))
        offset = match.end()
        if offset == len(source_table):
            break
        if source_table[offset] != "." or offset + 1 == len(source_table):
            raise ValueError(f"invalid source table reference: {source_table!r}")
        offset += 1
    if len(parts) != 2:
        raise ValueError(f"source table must contain schema.table: {source_table!r}")
    return parts


def __getattr__(name):
    """Lazily re-export the dialect base contract without a circular import."""
    if name == "RdbmsDialect":
        from ingestion.sources.rdbms.dialects.base import RdbmsDialect

        return RdbmsDialect
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
