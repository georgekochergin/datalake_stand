"""Choice of the JDBC partition column shared by every load mode.

Pure strategy over JDBC key metadata: no Spark imports and no SQL predicate
construction — Spark's partitionColumn mode builds the range predicates
itself. The module owns the three-step column heuristic.
"""

import logging
from collections.abc import Collection, Iterable
from dataclasses import dataclass

from . import jdbc
from . import positions
from .dialects.base import RdbmsDialect
from ingestion.logging import log_step

LOGGER = logging.getLogger(__name__)

STEP_PRIMARY_KEY = "primary_key"
STEP_UNIQUE_INTEGER = "unique_integer"
STEP_DATE = "date"


@dataclass(frozen=True)
class PartitionChoice:
    """Chosen partition column with the heuristic step that selected it."""

    column: str
    kind: str
    step: str


def _family(dialect: RdbmsDialect, info: jdbc.ColumnMeta) -> str | None:
    """Position family of a column, or None when unsupported."""
    kind = jdbc.column_position_kind(dialect, info)
    return positions.kind_info(kind)[0] if kind is not None else None


def _first_candidate(
    dialect: RdbmsDialect, infos: Iterable[jdbc.ColumnMeta], families: Collection[str],
) -> jdbc.ColumnMeta | None:
    """First NOT NULL column of a family in stable normalized-name order."""
    matched = [
        info for info in infos
        if info.not_null and _family(dialect, info) in families
    ]
    return min(matched, key=lambda info: info.name.lower()) if matched else None


def no_candidate_reason(dialect: RdbmsDialect, keys: jdbc.KeyMetadata) -> str:
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


def choose(dialect: RdbmsDialect, keys: jdbc.KeyMetadata) -> PartitionChoice | None:
    """Pick the partition column with one heuristic shared by every load mode.

    Steps, in order: a single-column primary key of integral type; an
    integral NOT NULL column backed by a single-column unique index; a NOT
    NULL date column. Candidates inside a step are interchangeable,
    so the first in normalized-name order wins and JDBC-driver row order
    never decides. No candidate means a single-threaded read.
    """
    if len(keys.primary_key) == 1:
        info = keys.columns.get(keys.primary_key[0].lower())
        if info is not None and _family(dialect, info) == "integral":
            return PartitionChoice(info.name, jdbc.column_position_kind(dialect, info), STEP_PRIMARY_KEY)
    LOGGER.info("jdbc partitioning: step=%s rejected; requires a single integral PK", STEP_PRIMARY_KEY)
    unique_infos = [
        keys.columns[name.lower()] for name in keys.unique_columns
        if name.lower() in keys.columns
    ]
    for step, infos, families in (
        (STEP_UNIQUE_INTEGER, unique_infos, ("integral",)),
        (STEP_DATE, keys.columns.values(), ("date",)),
    ):
        info = _first_candidate(dialect, infos, families)
        if info is not None:
            return PartitionChoice(info.name, jdbc.column_position_kind(dialect, info), step)
        LOGGER.info("jdbc partitioning: step=%s rejected; no NOT NULL candidate of family %s", step, families)
    return None


def plan(
    dialect: RdbmsDialect, keys: jdbc.KeyMetadata, source_table: str,
) -> PartitionChoice | None:
    """Choose a partition column and log the result of the heuristic."""
    log_step(LOGGER, "choose JDBC partition column")
    partition = choose(dialect, keys)
    if partition is None:
        if keys.columns:
            LOGGER.warning(
                "jdbc partitioning: no partition column found for %s; "
                "single-threaded read; %s",
                source_table, no_candidate_reason(dialect, keys),
            )
        return None
    LOGGER.info("jdbc partitioning: selected column=%s step=%s kind=%s tie_break=normalized_name",
                partition.column, partition.step, partition.kind)
    return partition
