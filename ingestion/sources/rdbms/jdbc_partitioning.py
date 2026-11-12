"""Choice of the JDBC partition column shared by every load mode.

Pure strategy over JDBC key metadata: no Spark imports and no SQL predicate
construction — Spark's partitionColumn mode builds the range predicates
itself. The module owns the four-step column heuristic, the calendar-day
derivative of timestamp columns, its read alias and the plan() composition
of the decision for one read; dialects own the SQL rendering rules, and
jdbc.py owns the metadata access.
"""

import logging
from dataclasses import dataclass

from . import jdbc
from . import positions

LOGGER = logging.getLogger(__name__)

STEP_PRIMARY_KEY = "primary_key"
STEP_UNIQUE_INTEGER = "unique_integer"
STEP_DATE = "date"
STEP_TIMESTAMP_DAY = "timestamp_day"

DAY_ALIAS = "__jdbc_day"

TIMESTAMP_FAMILIES = ("local", "instant")


@dataclass(frozen=True)
class PartitionChoice:
    """Chosen partition column with the heuristic step that selected it."""

    column: str
    kind: str
    step: str
    derived: bool


def partition_name(choice: PartitionChoice) -> str:
    """The partitionColumn option value: the day alias or the column itself."""
    return DAY_ALIAS if choice.derived else choice.column


def day_expression(dialect, keys, choice: PartitionChoice) -> str:
    """Render the calendar-day derivative of the chosen timestamp column.

    Time-zone types derive from the dialect's UTC projection, mirroring the
    position expression; every other type derives straight from the column.
    """
    info = keys.columns[choice.column.lower()]
    reference = f"extract_source.{dialect.quote_identifier(info.name)}"
    projected = dialect.column_projection(info.type_name, reference)
    return dialect.day_expression(projected or reference)


def _family(dialect, info) -> str | None:
    """Position family of a column, or None when unsupported."""
    kind = jdbc.column_position_kind(dialect, info)
    return positions.kind_info(kind)[0] if kind is not None else None


def _first_candidate(dialect, infos, families):
    """First NOT NULL column of a family in stable normalized-name order."""
    matched = [
        info for info in infos
        if info.not_null and _family(dialect, info) in families
    ]
    return min(matched, key=lambda info: info.name.lower()) if matched else None


def no_candidate_reason(dialect, keys) -> str:
    """Render the observed metadata that produced an empty heuristic result.

    Every column is listed with its type, heuristic family and nullability,
    so the task log shows exactly why no step matched — a primary key that
    is not integral, a unique column that is nullable, dates behind NULL.
    """
    columns = ", ".join(
        f"{info.name} {info.type_name} {_family(dialect, info) or 'unsupported'}"
        f"{'' if info.not_null else ' null'}"
        for info in sorted(keys.columns.values(), key=lambda info: info.name.lower())
    )
    return (
        f"primary_key={keys.primary_key or 'none'} "
        f"unique_columns={keys.unique_columns or 'none'} columns=[{columns}]"
    )


def choose(dialect, keys) -> PartitionChoice | None:
    """Pick the partition column with one heuristic shared by every load mode.

    Steps, in order: a single-column primary key of integral type; an
    integral NOT NULL column backed by a single-column unique index; a NOT
    NULL date column; a NOT NULL timestamp column partitioned by its
    calendar-day derivative. Candidates inside a step are interchangeable,
    so the first in normalized-name order wins and JDBC-driver row order
    never decides. No candidate means a single-threaded read.
    """
    if len(keys.primary_key) == 1:
        info = keys.columns.get(keys.primary_key[0].lower())
        if info is not None and _family(dialect, info) == "integral":
            return PartitionChoice(info.name, jdbc.column_position_kind(dialect, info), STEP_PRIMARY_KEY, False)
    unique_infos = [
        keys.columns[name.lower()] for name in keys.unique_columns
        if name.lower() in keys.columns
    ]
    for step, infos, families, derived in (
        (STEP_UNIQUE_INTEGER, unique_infos, ("integral",), False),
        (STEP_DATE, keys.columns.values(), ("date",), False),
        (STEP_TIMESTAMP_DAY, keys.columns.values(), TIMESTAMP_FAMILIES, True),
    ):
        info = _first_candidate(dialect, infos, families)
        if info is not None:
            return PartitionChoice(info.name, jdbc.column_position_kind(dialect, info), step, derived)
    return None


def plan(dialect, keys, source_table):
    """Compose the partitioning decision for one read over observed metadata.

    Returns (partition, extra_select): partition is None when the heuristic
    found no candidate (a warning names the table and lists the observed
    metadata) or when a source column collides with the day alias (a
    warning names the alias); the extra_select fragment carries the derived
    day expression appended to the read query. The caller owns the
    partitioning switch and the metadata access.
    """
    partition = choose(dialect, keys)
    if partition is None:
        if keys.columns:
            LOGGER.warning(
                "jdbc partitioning: no partition column found for %s; "
                "single-threaded read; %s",
                source_table, no_candidate_reason(dialect, keys),
            )
        return None, None
    if not partition.derived:
        return partition, None
    if DAY_ALIAS in keys.columns:
        LOGGER.warning(
            "jdbc partitioning: source column %s collides with the day alias;"
            " single-threaded read",
            DAY_ALIAS,
        )
        return None, None
    day_select = (
        f", {day_expression(dialect, keys, partition)}"
        f" AS {dialect.quote_identifier(DAY_ALIAS)}"
    )
    return partition, day_select
