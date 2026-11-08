"""Choice of the JDBC partition column shared by every load mode.

Pure strategy over JDBC key metadata: no Spark imports and no SQL predicate
construction — Spark's partitionColumn mode builds the range predicates
itself. The module owns the four-step column heuristic, the calendar-day
derivative of timestamp columns and its read alias; dialects own the SQL
rendering rules, and jdbc.py owns the metadata access.
"""

from dataclasses import dataclass

from . import positions

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


def _position_kind(dialect, info):
    """Canonical kind of a column, or None when the dialect rejects it.

    Drivers decorate type names with engine specifics (MSSQL appends
    "identity" to integer types), so a rejected full name is retried by its
    first token; the dialect contract itself stays strict.
    """
    try:
        return dialect.position_kind(info.type_name, info.precision, info.scale)
    except (ValueError, NotImplementedError):
        pass
    base = info.type_name.split()[0] if info.type_name else ""
    if base != info.type_name:
        try:
            return dialect.position_kind(base, info.precision, info.scale)
        except (ValueError, NotImplementedError):
            return None
    return None


def _family(dialect, info) -> str | None:
    """Position family of a column, or None when unsupported."""
    kind = _position_kind(dialect, info)
    return positions.kind_info(kind)[0] if kind is not None else None


def _first_candidate(dialect, infos, families):
    """First NOT NULL column of a family in stable normalized-name order."""
    matched = [
        info for info in infos
        if info.not_null and _family(dialect, info) in families
    ]
    return min(matched, key=lambda info: info.name.lower()) if matched else None


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
            return PartitionChoice(info.name, _position_kind(dialect, info), STEP_PRIMARY_KEY, False)
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
            return PartitionChoice(info.name, _position_kind(dialect, info), step, derived)
    return None
