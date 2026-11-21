"""Validation of the resolved incremental loading configuration."""

import re
from typing import NotRequired, TypedDict, cast

from . import positions


class _Contract(TypedDict):
    """Validated source position definition."""

    source_column: str
    position_type: str
    check_not_null: NotRequired[bool]


class _Lookback(TypedDict):
    """Resolved lookback size and optional time unit."""

    size: int
    unit: str | None


class IncrementalConfig(TypedDict):
    """Validated incremental contract with resolved lookback."""

    contract: _Contract
    lookback: _Lookback


def validate(config: object) -> IncrementalConfig:
    """Return a validated contract and concrete lookback without runtime defaults."""
    if not isinstance(config, dict) or set(config) != {"contract", "lookback"}:
        raise ValueError("incremental config requires exactly contract and lookback")
    contract, lookback = config["contract"], config["lookback"]
    required = {"source_column", "position_type"}
    if not isinstance(contract, dict) or not required <= set(contract) <= required | {"check_not_null"}:
        raise ValueError("incremental.contract requires source_column and position_type, with optional check_not_null")
    if type(contract.get("check_not_null", True)) is not bool:
        raise ValueError("incremental.contract.check_not_null must be a boolean")
    column, kind = contract["source_column"], contract["position_type"]
    if not isinstance(column, str) or not re.fullmatch(r"[a-z_][a-z0-9_]*", column):
        raise ValueError("incremental.contract.source_column must be a lowercase identifier")
    positions.kind_info(kind)
    if kind == "int":
        raise ValueError("Use canonical position_type integer, not int")
    if not isinstance(lookback, dict) or set(lookback) != {"size", "unit"}:
        raise ValueError("incremental.lookback requires size and unit")
    size, unit = lookback["size"], lookback["unit"]
    if type(size) is not int or not 0 <= size < 10 ** 38:
        raise ValueError("lookback.size must be a non-negative integer below 10^38")
    if size == 0 and unit is not None:
        raise ValueError("Zero lookback requires null unit")
    positions.window_bounds(None, kind, size, unit)
    return cast(IncrementalConfig, config)
