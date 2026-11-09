"""Canonical typed extraction positions and lookback arithmetic, without Spark.

Positions are the persisted pg-meta contract: a plain string that preserves
the exact source value of an incremental column at its declared precision,
independent of any particular database or execution engine.
"""

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


def kind_info(kind):
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


def _parse(value, kind):
    """Parse into integer values, ordinal days, or ordinal nanoseconds."""
    family, precision, bounds = kind_info(kind)
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
    family, precision, _ = kind_info(kind)
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


def window_bounds(watermark, kind, size=0, unit=None):
    """Subtract lookback with hour/day alignment; return (lower, op).

    The lower bound is the start of the hour (unit h) or day (unit d) reached
    by the subtraction, computed in the value's own calendar: instants align
    on the UTC grid, local positions on the wall-clock grid of their naive
    digits. Underflow below the type minimum drops the lower filter (None).
    op is '>=' for an aligned temporal lookback and '>' otherwise; at size 0
    the unit is ignored entirely, without validation.
    """
    family, _, bounds = kind_info(kind)
    if isinstance(size, bool) or not isinstance(size, int) or not 0 <= size <= 10**38 - 1:
        raise ValueError("Lookback size must be an integer in 0..10^38-1")
    if family == "integral":
        if unit is not None and size:
            raise ValueError("Integral lookback forbids a unit")
        step, delta, minimum, op = 1, size, bounds[0], ">"
    elif family == "date":
        if size and unit != "d":
            raise ValueError("Temporal lookback requires a valid unit")
        step, delta, minimum, op = 1, size, 1, ">=" if size else ">"
    else:
        if size and unit not in ("h", "d"):
            raise ValueError("Temporal lookback requires a valid unit")
        step = _DAY if unit == "d" else 3600 * _NS
        delta, minimum, op = size * step, 0, ">=" if size else ">"
    parsed = _parse(watermark, kind)
    if parsed is None or parsed - delta < minimum:
        return None, op
    aligned = parsed - delta
    if size and step > 1:
        aligned -= aligned % step
    return _format(aligned, kind), op


def resolve_lookback(kind, size, unit, specified):
    """Resolve a schedule-derived default for the declared canonical position type.

    The manifest parser resolves the temporal default (3 h or 3 d by schedule)
    before generating the DAG; JDBC verifies the declared type at runtime. Explicitly
    configured lookback passes through unchanged and is validated by kind in
    window_bounds; integral defaults collapse to 0 and date defaults are
    always 3 days regardless of schedule.
    """
    family = kind_info(kind)[0]
    if specified:
        return size, unit
    if family == "integral":
        return 0, None
    if family == "date":
        return 3, "d"
    return size, unit


def maximum(previous, observed, kind):
    """Return typed maximum, ignoring NULL positions."""
    values = [_parse(value, kind) for value in (previous, observed)]
    values = [value for value in values if value is not None]
    return _format(max(values) if values else None, kind)
