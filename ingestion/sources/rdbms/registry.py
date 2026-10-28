"""Static registry of RDBMS dialects by Airflow connection type and JDBC URL."""

from .contracts import RdbmsDialect
from .dialects.mssql import MssqlDialect
from .dialects.oracle import OracleDialect

_DIALECTS: tuple[RdbmsDialect, ...] = (MssqlDialect(), OracleDialect())


def all_dialects() -> tuple[RdbmsDialect, ...]:
    """Return every registered dialect instance."""
    return _DIALECTS


def supported_conn_types() -> tuple[str, ...]:
    """Return the sorted connection type aliases accepted by by_conn_type."""
    return tuple(sorted(alias for dialect in _DIALECTS for alias in dialect.conn_type_aliases))


def supported_url_prefixes() -> tuple[str, ...]:
    """Return the sorted JDBC URL prefixes accepted by by_jdbc_url."""
    return tuple(sorted(prefix for dialect in _DIALECTS for prefix in dialect.url_prefixes))


def by_conn_type(conn_type) -> RdbmsDialect:
    """Resolve a dialect by an Airflow connection type alias."""
    for dialect in _DIALECTS:
        if conn_type in dialect.conn_type_aliases:
            return dialect
    raise ValueError(
        f"connection type {conn_type!r} is not a supported JDBC dialect "
        f"(expected one of {list(supported_conn_types())})"
    )


def by_jdbc_url(jdbc_url) -> RdbmsDialect:
    """Resolve a dialect by its JDBC URL prefix; unknown and empty URLs are rejected."""
    if isinstance(jdbc_url, str):
        for dialect in _DIALECTS:
            if jdbc_url.startswith(tuple(dialect.url_prefixes)):
                return dialect
    raise ValueError(
        f"unsupported jdbc url (expected one of {sorted(supported_url_prefixes())}): {jdbc_url!r}"
    )
