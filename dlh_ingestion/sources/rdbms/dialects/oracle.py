"""Oracle dialect adapter: quoted lowercase identifiers and TO_CHAR positions."""

import re

from .. import positions
from ..contracts import (
    ConnectionSpec,
    RdbmsDialect,
    decimal_kind,
    fractional_kind,
    integer_kind,
    normalize_type_name,
)


class OracleDialect(RdbmsDialect):
    """Oracle rules for URLs, identifiers, temporal literals and expressions."""

    conn_type_aliases = ("oracle",)
    url_prefixes = ("jdbc:oracle:",)
    default_port = 1521
    driver_class = "oracle.jdbc.OracleDriver"

    def build_jdbc_url(self, spec: ConnectionSpec) -> str:
        """Render jdbc:oracle:thin:@//host:port/service."""
        return f"jdbc:oracle:thin:@//{spec.host}:{spec.port or self.default_port}/{spec.database}"

    def quote_identifier(self, name: str) -> str:
        """Double-quote an identifier, doubling embedded quotes."""
        return '"%s"' % name.replace('"', '""')

    def position_kind(self, type_name, precision, scale) -> str:
        """Map Oracle JDBC types onto the persisted position contract."""
        name = normalize_type_name(type_name)
        if name in ("SMALLINT", "INT", "INTEGER", "BIGINT"):
            return integer_kind(name)
        if name in ("DECIMAL", "NUMERIC", "NUMBER"):
            return decimal_kind(precision, scale)
        if name == "DATE":
            return "timestamp_local(0)"
        match = re.fullmatch(r"TIMESTAMP(?:\(([0-9])\))?( WITH (?:LOCAL )?TIME ZONE)?", name)
        if not match:
            raise ValueError(f"Unsupported Oracle position type: {name}")
        family = "instant" if match[2] else "local"
        fractional = fractional_kind(int(match[1]) if match[1] is not None else scale, 9)
        return f"timestamp_{family}({fractional})"

    def _typed_literal(self, text: str, family: str, precision: int) -> str:
        """Render TO_DATE / TO_TIMESTAMP / TO_TIMESTAMP_TZ with an exact FF mask."""
        if family == "date":
            return f"TO_DATE('{text}', 'YYYY-MM-DD')"
        mask = 'YYYY-MM-DD"T"HH24:MI:SS' + (f".FF{precision}" if precision else "")
        if family == "instant":
            return f"TO_TIMESTAMP_TZ('{text[:-1]}+00:00', '{mask}TZH:TZM')"
        return f"TO_TIMESTAMP('{text}', '{mask}')"

    def position_expression(self, column: str, kind: str) -> str:
        """Render TO_CHAR with an FF mask; instants are extracted in UTC first."""
        if not kind.startswith("timestamp"):
            return column
        family, precision, _ = positions.kind_info(kind)
        value = f"SYS_EXTRACT_UTC({column})" if family == "instant" else column
        fraction = f".FF{precision}" if precision else ""
        suffix = '"Z"' if family == "instant" else ""
        return f"TO_CHAR({value}, 'YYYY-MM-DD\"T\"HH24:MI:SS{fraction}{suffix}')"
