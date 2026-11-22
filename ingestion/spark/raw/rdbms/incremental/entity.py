"""Load a persisted incremental JDBC window into Iceberg with at-least-once append."""

import argparse
import logging
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

from ingestion.logging import log_step

from ingestion.spark.raw import append
from ingestion.spark.raw.rdbms import reader
from ingestion.spark.raw.rdbms.incremental import window
from ingestion.sources.rdbms import jdbc
from ingestion.sources.rdbms import positions

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from ingestion.meta import DbConnect, DbRow

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ResolvedBatch:
    """Persisted window and any JDBC metadata collected while resolving it."""

    batch: "DbRow"
    read_source: reader.ReadSource | None = None
    partition_bounds: reader.PartitionBounds | None = None


@dataclass
class _WindowProbe:
    """Adapt JDBC bounds to the registry callback, retaining reusable partition bounds."""

    spark: "SparkSession"
    args: argparse.Namespace
    source: reader.ReadSource
    column: jdbc.ColumnMeta
    partition_bounds: reader.PartitionBounds | None = None

    def probe_upper(self, lower: str | None, op: str) -> str | None:
        """Return the incremental maximum to the registry and store JDBC partition bounds.

        The registry supplies the lower predicate under the state lock. Without
        a lower bound, exclude possible NULL positions so they don't skew the
        partition bounds, even though MAX already ignores them.
        """
        dialect = self.source.dialect
        column = dialect.quote_identifier(self.column.name)
        kind = self.args.position_type
        filters = []
        scope = reader.JdbcReadScope.SOURCE_TABLE
        if lower is not None:
            filters.append(window.position_filter(dialect, column, op, lower, kind))
            scope = reader.JdbcReadScope.BATCH_WINDOW
        elif not self.column.not_null:
            filters.append(f"{column} IS NOT NULL")
        upper, self.partition_bounds = reader.probe_bounds(
            self.spark, self.args, self.source, filters, scope=scope,
            incremental=(self.column.name, kind),
        )
        return upper


def _empty_incremental_window(batch: "DbRow") -> bool:
    """Recognize an empty probe or an empty strict window from an older planner."""
    return batch["empty_at_plan"] or (
        batch["lower_bound_op"] == ">" and batch["upper_bound"] is not None
        and batch["lower_bound"] == batch["upper_bound"]
    )


def validate_incremental_column(
    source: reader.ReadSource, args: argparse.Namespace, *, check_not_null: bool,
) -> jdbc.ColumnMeta:
    """Validate the incremental column contract against JDBC catalog metadata."""
    info = source.keys.columns.get(args.incremental_column.lower())
    if info is None:
        raise ValueError(
            f"Incremental column {args.incremental_column!r} not found in JDBC metadata for "
            f"{args.source_id}.{args.source_table}"
        )
    kind = jdbc.column_position_kind(source.dialect, info)
    if kind is None:
        raise ValueError(
            f"Unsupported JDBC type of incremental column "
            f"{args.source_id}.{args.source_table}.{args.incremental_column}: "
            f"{info.type_name} precision={info.precision} scale={info.scale}"
        )
    if kind != args.position_type:
        raise ValueError(
            f"Incremental type mismatch for {args.source_id}.{args.source_table}.{args.incremental_column}: "
            f"expected={args.position_type} actual={kind} JDBC={info.type_name} precision={info.precision} scale={info.scale}"
        )
    if check_not_null and not info.not_null:
        raise ValueError(f"JDBC metadata does not confirm NOT NULL for incremental column {args.source_table}.{info.name}")
    return info


def _resolve_batch(spark: "SparkSession", args: argparse.Namespace, db_connect: "DbConnect") -> _ResolvedBatch:
    """Resolve this execution's persisted batch or plan a fresh window."""
    from ingestion.meta import registry

    key = (args.process_id, args.source_id, args.source_table)
    LOGGER.info("load attempt process_id=%s source_id=%s entity_fqn=%s", *key)
    batch = registry.get_batch(
        process_id=args.process_id, source_id=args.source_id,
        entity_fqn=args.source_table, db_connect=db_connect,
    )
    if batch is not None:
        return _reuse_batch(spark, args, batch)
    return _plan_new_batch(spark, args, db_connect)


def _reuse_batch(spark: "SparkSession", args: argparse.Namespace, batch: "DbRow") -> _ResolvedBatch:
    """Verify a retry's contract without probing the incremental maximum again."""
    reason = reader.JdbcReason.PERSISTED_BATCH
    LOGGER.info("JDBC incremental maximum not recalculated: %s. reason=%s", reason.text, reason.code)
    if batch["target_table"] != args.target_table:
        raise ValueError("Persisted batch target does not match")
    if batch["incremental_column"] != args.incremental_column:
        raise ValueError(
            "Persisted batch incremental column does not match the manifest; "
            "stop executors and migrate the contract"
        )
    if batch["incremental_type"] != args.position_type:
        raise ValueError("Persisted batch position type does not match incremental.contract")
    if batch["status"] == "SUCCEEDED":
        LOGGER.info(
            "batch already succeeded; repeat read skipped: process_id=%s "
            "entity_fqn=%s row_count=%s finished_at=%s",
            args.process_id, args.source_table, batch["row_count"], batch["finished_at"],
        )
        return _ResolvedBatch(batch)
    source = reader.inspect_source(spark, args, detailed=False)
    validate_incremental_column(source, args, check_not_null=False)
    return _ResolvedBatch(batch, read_source=source)


def _plan_new_batch(spark: "SparkSession", args: argparse.Namespace, db_connect: "DbConnect") -> _ResolvedBatch:
    """Probe and persist the window under the registry's state lock."""
    from ingestion.meta import registry

    source = reader.inspect_source(spark, args)
    info = validate_incremental_column(source, args, check_not_null=args.check_not_null)
    kind = args.position_type
    size, unit = args.lookback_size, args.lookback_unit
    LOGGER.info("incremental config: kind=%s lookback_size=%s lookback_unit=%s", kind, size, unit)
    probe = _WindowProbe(spark, args, source, info)
    batch = registry.plan_batch(
        process_id=args.process_id, dag_id=args.dag_id, run_id=args.run_id,
        source_id=args.source_id, entity_fqn=args.source_table, target_table=args.target_table,
        incremental_column=args.incremental_column, incremental_type=kind,
        lookback_size=size, lookback_unit=unit, db_connect=db_connect,
        calculate_lower=partial(positions.window_bounds, kind=kind, size=size, unit=unit),
        probe_upper=probe.probe_upper,
    )
    return _ResolvedBatch(batch, read_source=source, partition_bounds=probe.partition_bounds)


def managed_load(
    spark: "SparkSession", args: argparse.Namespace, *, db_connect: "DbConnect",
) -> None:
    """Resolve a batch and skip, finalize, or execute it according to its state."""
    from ingestion.meta import registry

    log_step(LOGGER, "resolve incremental batch")
    plan = _resolve_batch(spark, args, db_connect)
    batch = plan.batch
    if batch["status"] == "SUCCEEDED":
        return
    key = dict(process_id=args.process_id, source_id=args.source_id,
               entity_fqn=args.source_table, db_connect=db_connect)
    if _empty_incremental_window(batch):
        reason = reader.JdbcReason.EMPTY_WINDOW
        LOGGER.info("%s. JDBC read and Iceberg append skipped. No raw table created by this attempt. "
                    "target_table=%s reason=%s", reason.text, args.target_table, reason.code)
        registry.finalize_batch(row_count=0, observed_max=None, **key)
        return
    _execute_batch(spark, args, plan, key)


def _execute_batch(spark: "SparkSession", args: argparse.Namespace, plan: _ResolvedBatch, key: dict) -> None:
    """Append the window at least once and atomically finalize its pg-meta state."""
    from ingestion.meta import registry

    batch = plan.batch
    kind = batch["incremental_type"]
    registry.start_batch(**key)
    try:
        source = window.read_window_frame(
            spark, args, batch, kind, source=plan.read_source, bounds=plan.partition_bounds,
        )
        try:
            log_step(LOGGER, "summarize batch window")
            rows, observed, clean = window.window_summary(source, kind)
            LOGGER.info("batch summary row_count=%s observed_max=%s", rows, observed)
            append.append_raw(spark, args.target_table, clean, rows, args.process_id)
            watermark = positions.maximum(batch["watermark_before"], observed, kind)
            registry.finalize_batch(row_count=rows, observed_max=observed,
                                watermark_after=watermark, **key)
        finally:
            source.unpersist()
    except Exception:
        try:
            registry.fail_batch(**key)
        except Exception:
            LOGGER.warning("Unable to persist batch failure")
        raise
