"""Lossless typed extraction positions and source SQL codecs, without Spark."""

import re
from datetime import date, datetime
from decimal import Decimal


_NS = 1_000_000_000
_DAY = 86400 * _NS
_INTEGER = re.compile(r"(?:0|-[1-9][0-9]*|[1-9][0-9]*)\Z")
_TIMESTAMP = re.compile(
    r"(\d{4}-\d{2}-\d{2})[T ](\d{2}):(\d{2}):(\d{2})"
    r"(?:\.([0-9]{1,9}))?(?: ?(Z|[+-]\d{2}(?::?\d{2})?))?\Z"
)
_RANGES = {
    "smallint": (-(2**15), 2**15 - 1),
    "integer": (-(2**31), 2**31 - 1),
    "int": (-(2**31), 2**31 - 1),
    "bigint": (-(2**63), 2**63 - 1),
}


def _kind(kind):
    """Return family, fractional precision, and signed numeric bounds."""
    if not isinstance(kind, str):
        raise ValueError("Position kind must be a string")
    if kind in _RANGES:
        return "integral", 0, _RANGES[kind]
    match = re.fullmatch(r"decimal\(([1-9]|[12][0-9]|3[0-8]),0\)", kind)
    if match:
        limit = 10 ** int(match[1]) - 1
        return "integral", 0, (-limit, limit)
    if kind == "date":
        return "date", 0, None
    match = re.fullmatch(r"timestamp_(local|instant)\(([0-9])\)", kind)
    if match:
        return match[1], int(match[2]), None
    raise ValueError(f"Unsupported position kind: {kind!r}")


def _dialect(jdbc_url):
    """Restrict SQL rendering and metadata interpretation to known sources."""
    if isinstance(jdbc_url, str):
        if jdbc_url.startswith("jdbc:sqlserver:"):
            return "mssql"
        if jdbc_url.startswith("jdbc:oracle:"):
            return "oracle"
    raise ValueError("Unsupported JDBC dialect")


def _parse(value, kind):
    """Parse into integer values, ordinal days, or ordinal nanoseconds."""
    family, precision, bounds = _kind(kind)
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("Boolean positions are not supported")
    if family == "integral":
        if isinstance(value, Decimal):
            if not value.is_finite() or value != value.to_integral_value():
                raise ValueError("Position must be integral")
            number = int(value)
        elif isinstance(value, int):
            number = value
        elif isinstance(value, str) and _INTEGER.fullmatch(value):
            number = int(value)
        else:
            raise ValueError("Position must be a canonical integer")
        if not bounds[0] <= number <= bounds[1]:
            raise ValueError("Position is outside its numeric type range")
        return number
    if family == "date":
        if isinstance(value, datetime):
            raise ValueError("Date positions cannot contain time")
        if isinstance(value, date):
            return value.toordinal()
        if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
            raise ValueError("Position must be an ISO date")
        return date.fromisoformat(value).toordinal()
    if isinstance(value, datetime):
        value = value.isoformat()
    if not isinstance(value, str):
        raise ValueError("Position must be a timestamp or JDBC timestamp string")
    match = _TIMESTAMP.fullmatch(value)
    if not match:
        raise ValueError("Invalid timestamp position")
    day, hour, minute, second, fraction, offset = match.groups()
    hour, minute, second = int(hour), int(minute), int(second)
    if hour > 23 or minute > 59 or second > 59:
        raise ValueError("Invalid timestamp time")
    nanos = int((fraction or "").ljust(9, "0"))
    if nanos % 10 ** (9 - precision):
        raise ValueError("Timestamp precision would be lost")
    if (family == "local" and offset is not None) or (family == "instant" and offset is None):
        raise ValueError("Timestamp timezone does not match position kind")
    result = (date.fromisoformat(day).toordinal() - 1) * _DAY
    result += (hour * 3600 + minute * 60 + second) * _NS + nanos
    if offset and offset != "Z":
        digits = offset[1:].replace(":", "")
        hours = int(digits[:2])
        minutes = int(digits[2:] or "0")
        if hours > 18 or minutes > 59 or (hours == 18 and minutes):
            raise ValueError("Invalid UTC offset")
        result -= (1 if offset[0] == "+" else -1) * (hours * 60 + minutes) * 60 * _NS
    if not 0 <= result < date.max.toordinal() * _DAY:
        raise ValueError("Timestamp is outside supported years 0001..9999")
    return result


def _format(value, kind):
    """Render a parsed position at exactly the declared precision."""
    if value is None:
        return None
    family, precision, _ = _kind(kind)
    if family == "integral":
        return str(value)
    if family == "date":
        return date.fromordinal(value).isoformat()
    days, remainder = divmod(value, _DAY)
    seconds, nanos = divmod(remainder, _NS)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    result = f"{date.fromordinal(days + 1).isoformat()}T{hours:02}:{minutes:02}:{seconds:02}"
    if precision:
        result += "." + f"{nanos:09}"[:precision]
    return result + ("Z" if family == "instant" else "")


def canonical(value, kind):
    """Normalize a position without rounding, or preserve NULL."""
    return _format(_parse(value, kind), kind)


def lower_bound(watermark, kind, size=0, unit=None, specified=False):
    """Subtract lookback; return NULL instead of excluding the type minimum.

    specified marks a configured lookback: temporal kinds then require a unit
    even at size 0, matching manifest validation that cannot know the type.
    """
    family, _, bounds = _kind(kind)
    if isinstance(size, bool) or not isinstance(size, int) or not 0 <= size <= 10**38 - 1:
        raise ValueError("Lookback size must be an integer in 0..10^38-1")
    if family == "integral":
        if unit is not None:
            raise ValueError("Integral lookback forbids a unit")
        delta, minimum = size, bounds[0]
    else:
        allowed = ("d",) if family == "date" else ("h", "d")
        if unit is not None and unit not in allowed:
            raise ValueError("Temporal lookback requires a valid unit")
        if unit is None and (specified or size):
            raise ValueError("Temporal lookback requires a unit")
        delta = size if family == "date" else size * (3600 if unit == "h" else 86400) * _NS
        minimum = 1 if family == "date" else 0
    parsed = _parse(watermark, kind)
    if parsed is None or parsed - delta < minimum:
        return None
    return _format(parsed - delta, kind)


def maximum(previous, observed, kind):
    """Return typed maximum, ignoring NULL positions."""
    values = [_parse(value, kind) for value in (previous, observed)]
    values = [value for value in values if value is not None]
    return _format(max(values) if values else None, kind)


def literal(value, kind, jdbc_url):
    """Render a validated source SQL expression without implicit temporal casts."""
    dialect = _dialect(jdbc_url)
    family, precision, _ = _kind(kind)
    text = canonical(value, kind)
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
