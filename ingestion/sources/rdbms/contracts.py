"""Connection and SQL contracts shared by every RDBMS dialect adapter."""

import re
from dataclasses import dataclass

from . import positions

SOURCE_PART_PATTERN = re.compile(r'"((?:[A-Za-z0-9_.]|"")+)"|([A-Za-z0-9_]+)')


@dataclass(frozen=True)
class ConnectionSpec:
    """Engine-agnostic connection parameters resolved from an Airflow Connection."""

    host: str
    port: int | None
    database: str
    user: str
    password: str


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


class RdbmsDialect:
    """Base class of RDBMS dialect adapters; subclasses define engine rules."""

    conn_type_aliases: tuple[str, ...] = ()
    url_prefixes: tuple[str, ...] = ()
    default_port: int = 0
    driver_class: str = ""

    def build_jdbc_url(self, spec: ConnectionSpec) -> str:
        """Render the JDBC URL for a resolved connection."""
        raise NotImplementedError

    def quote_identifier(self, name: str) -> str:
        """Render a column reference quoted for the source dialect."""
        raise NotImplementedError

    def render_table(self, parts: list[tuple[str, bool]]) -> str:
        """Render a parsed schema.table reference quoted for the source dialect."""
        return ".".join(self.quote_identifier(name) for name, _ in parts)

    def position_kind(self, type_name, precision, scale) -> str:
        """Map JDBC source type metadata to the persisted position contract."""
        raise NotImplementedError

    def position_literal(self, canonical_text, kind: str) -> str:
        """Render an already canonicalized position as a typed SQL literal."""
        if canonical_text is None:
            return "NULL"
        family, precision, _ = positions.kind_info(kind)
        if family == "integral":
            return canonical_text
        return self._typed_literal(canonical_text, family, precision)

    def _typed_literal(self, text: str, family: str, precision: int) -> str:
        """Render a non-integral canonical position as a typed SQL literal."""
        raise NotImplementedError

    def position_expression(self, column: str, kind: str) -> str:
        """Render a lossless source-side temporal string for the position column."""
        return column

    def metadata_candidates(self, parts: list[tuple[str, bool]]) -> tuple:
        """Return (schema, table) lookup variants for JDBC metadata calls."""
        original = tuple(name for name, _ in parts)
        folded = tuple(name if quoted else name.upper() for name, quoted in parts)
        return tuple(dict.fromkeys((original, folded)))

    def column_projection(self, type_name: str, reference: str) -> str | None:
        """Return a source-side read expression for special types, else None."""
        return None

    def reader_options(self) -> dict:
        """JDBC reader options required by the engine."""
        return {"preferTimestampNTZ": "true"}


def normalize_type_name(type_name) -> str:
    """Uppercase and collapse whitespace in a JDBC TYPE_NAME."""
    if not isinstance(type_name, str):
        raise ValueError("JDBC type name must be a string")
    return " ".join(type_name.upper().split())


def integer_kind(name: str) -> str:
    """Return the canonical integral kind for a JDBC integer type name."""
    return {"INT": "integer", "INTEGER": "integer"}.get(name, name.lower())


def decimal_kind(precision, scale) -> str:
    """Return decimal(p,0): only integral decimals are valid positions."""
    if isinstance(precision, bool) or not isinstance(precision, int) or not 1 <= precision <= 38 or isinstance(scale, bool) or scale != 0:
        raise ValueError("Only decimal(p,0), 1 <= p <= 38, is supported")
    return f"decimal({precision},0)"


def fractional_kind(fractional, limit: int) -> int:
    """Validate the JDBC timestamp fractional precision against the engine limit."""
    if isinstance(fractional, bool) or not isinstance(fractional, int) or not 0 <= fractional <= limit:
        raise ValueError("Unsupported JDBC timestamp fractional precision")
    return fractional
