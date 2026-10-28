"""Dialect SQL codecs over the shared typed extraction positions."""

import re

from dlh_ingestion.sources.rdbms import positions


def _dialect(jdbc_url):
    """Restrict SQL rendering and metadata interpretation to known sources."""
    if isinstance(jdbc_url, str):
        if jdbc_url.startswith("jdbc:sqlserver:"):
            return "mssql"
        if jdbc_url.startswith("jdbc:oracle:"):
            return "oracle"
    raise ValueError("Unsupported JDBC dialect")


def literal(value, kind, jdbc_url):
    """Render a validated source SQL expression without implicit temporal casts."""
    dialect = _dialect(jdbc_url)
    family, precision, _ = positions.kind_info(kind)
    text = positions.canonical(value, kind)
    if text is None:
        return "NULL"
    if family == "integral":
        return text
    if dialect == "mssql":
        if family != "date" and precision > 7:
            raise ValueError("MSSQL timestamp precision cannot exceed 7")
        target = "date" if family == "date" else f"{'datetimeoffset' if family == 'instant' else 'datetime2'}({precision})"
        return f"CAST('{text}' AS {target})"
    if family == "date":
        return f"TO_DATE('{text}', 'YYYY-MM-DD')"
    mask = 'YYYY-MM-DD"T"HH24:MI:SS' + (f".FF{precision}" if precision else "")
    if family == "instant":
        return f"TO_TIMESTAMP_TZ('{text[:-1]}+00:00', '{mask}TZH:TZM')"
    return f"TO_TIMESTAMP('{text}', '{mask}')"


def jdbc_kind(type_name, precision, scale, jdbc_url):
    """Map JDBC source type metadata to the persisted position contract."""
    dialect = _dialect(jdbc_url)
    if not isinstance(type_name, str):
        raise ValueError("JDBC type name must be a string")
    name = " ".join(type_name.upper().split())
    if name in ("SMALLINT", "INT", "INTEGER", "BIGINT"):
        return {"INT": "integer", "INTEGER": "integer"}.get(name, name.lower())
    if name in ("DECIMAL", "NUMERIC", "NUMBER"):
        if isinstance(precision, bool) or not isinstance(precision, int) or not 1 <= precision <= 38 or isinstance(scale, bool) or scale != 0:
            raise ValueError("Only decimal(p,0), 1 <= p <= 38, is supported")
        return f"decimal({precision},0)"
    if name == "DATE":
        return "date" if dialect == "mssql" else "timestamp_local(0)"
    if dialect == "mssql":
        if name in ("DATETIME", "SMALLDATETIME"):
            return f"timestamp_local({3 if name == 'DATETIME' else 0})"
        if name not in ("DATETIME2", "DATETIMEOFFSET"):
            raise ValueError(f"Unsupported MSSQL position type: {name}")
        family = "instant" if name == "DATETIMEOFFSET" else "local"
        fractional, limit = scale, 7
    else:
        match = re.fullmatch(r"TIMESTAMP(?:\(([0-9])\))?( WITH (?:LOCAL )?TIME ZONE)?", name)
        if not match:
            raise ValueError(f"Unsupported Oracle position type: {name}")
        family = "instant" if match[2] else "local"
        fractional, limit = int(match[1]) if match[1] is not None else scale, 9
    if isinstance(fractional, bool) or not isinstance(fractional, int) or not 0 <= fractional <= limit:
        raise ValueError("Unsupported JDBC timestamp fractional precision")
    return f"timestamp_{family}({fractional})"
