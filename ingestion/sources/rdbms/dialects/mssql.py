"""MSSQL dialect adapter: bracket quoting and datetime2/datetimeoffset positions."""

from .. import positions
from ..contracts import (
    ConnectionSpec,
    RdbmsDialect,
    decimal_kind,
    fractional_kind,
    integer_kind,
    normalize_type_name,
)


class MssqlDialect(RdbmsDialect):
    """SQL Server rules for URLs, identifiers, temporal literals and expressions."""

    conn_type_aliases = ("mssql",)
    url_prefixes = ("jdbc:sqlserver:",)
    default_port = 1433
    driver_class = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    def build_jdbc_url(self, spec: ConnectionSpec) -> str:
        """Render jdbc:sqlserver://host:port;databaseName=...;encrypt=false."""
        return (
            f"jdbc:sqlserver://{spec.host}:{spec.port or self.default_port};"
            f"databaseName={spec.database};encrypt=false"
        )

    def quote_identifier(self, name: str) -> str:
        """Bracket-quote an identifier, doubling embedded brackets."""
        return "[%s]" % name.replace("]", "]]")

    def position_kind(self, type_name, precision, scale) -> str:
        """Map MSSQL JDBC types onto the persisted position contract."""
        name = normalize_type_name(type_name)
        if name in ("SMALLINT", "INT", "INTEGER", "BIGINT"):
            return integer_kind(name)
        if name in ("DECIMAL", "NUMERIC"):
            return decimal_kind(precision, scale)
        if name == "DATE":
            return "date"
        if name in ("DATETIME", "SMALLDATETIME"):
            return f"timestamp_local({3 if name == 'DATETIME' else 0})"
        if name not in ("DATETIME2", "DATETIMEOFFSET"):
            raise ValueError(f"Unsupported MSSQL position type: {name}")
        family = "instant" if name == "DATETIMEOFFSET" else "local"
        return f"timestamp_{family}({fractional_kind(scale, 7)})"

    def _typed_literal(self, text: str, family: str, precision: int) -> str:
        """Cast a canonical position to date, datetime2(p) or datetimeoffset(p)."""
        if family != "date" and precision > 7:
            raise ValueError("MSSQL timestamp precision cannot exceed 7")
        if family == "date":
            return f"CAST('{text}' AS date)"
        target = f"{'datetimeoffset' if family == 'instant' else 'datetime2'}({precision})"
        return f"CAST('{text}' AS {target})"

    def position_expression(self, column: str, kind: str) -> str:
        """Convert to datetime2(p) with an exact nanosecond fraction; instants in UTC."""
        if not kind.startswith("timestamp"):
            return column
        family, precision, _ = positions.kind_info(kind)
        value = f"SWITCHOFFSET({column}, '+00:00')" if family == "instant" else column
        expression = f"CONVERT(char(19), CAST({value} AS datetime2({precision})), 126)"
        if precision:
            expression += (
                f" + '.' + LEFT(RIGHT('000000000' + "
                f"CAST(DATEPART(nanosecond, {value}) AS varchar(9)), 9), {precision})"
            )
        return expression + (" + 'Z'" if family == "instant" else "")
