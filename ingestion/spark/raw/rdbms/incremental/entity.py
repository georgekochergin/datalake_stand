"""Load a persisted incremental JDBC window into Iceberg with at-least-once append."""

import argparse
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ingestion.logging import log_step
from ingestion.meta import registry
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
    """Persisted window and optional hints for reading it without extra JDBC queries.

    read_source holds inspected JDBC catalog metadata. partition_bounds holds
    ranges from the planning probe; None lets the reader probe the frozen
    window itself if needed. Both hints are absent for a SUCCEEDED batch.
    """

    batch: "DbRow"
    read_source: reader.ReadSource | None = None
    partition_bounds: reader.PartitionBounds | None = None


@dataclass
class _WindowProbe:
    """Supply callbacks for planning a fresh batch under the registry's state lock.

    registry.plan_batch computes the lower predicate from the locked watermark
    and calls probe_upper once if it creates a batch. If the batch already
    exists, the registry does not probe again. After plan_batch returns,
    _plan_new_batch passes probed_bounds to the reader to save a JDBC query.

    None means the callback did not run. (None, None) means it ran but did
    not obtain usable partition bounds, so the reader need not probe again.
    """

    spark: "SparkSession"
    args: argparse.Namespace
    source: reader.ReadSource
    column: jdbc.ColumnMeta
    probed_bounds: reader.PartitionBounds | None = None

    def calculate_lower(self, watermark: str | None) -> tuple[str | None, str]:
        """Compute the lower predicate from the committed watermark and lookback."""
        return positions.window_bounds(
            watermark, self.args.position_type, self.args.lookback_size, self.args.lookback_unit,
        )

    def probe_upper(self, lower: str | None, op: str) -> str | None:
        """Return the maximum for the registry and retain bounds for the reader."""
        upper, self.probed_bounds = self._probe_window(lower, op)
        return upper

    def _probe_window(self, lower: str | None, op: str) -> tuple[str | None, reader.PartitionBounds]:
        """Query both aggregates over the same rows; scope labels the JDBC log.

        Without a lower predicate, exclude possible NULL positions so they
        cannot skew partition bounds, even though MAX(position) ignores them.
        """
        dialect = self.source.dialect
        quoted = dialect.quote_identifier(self.column.name)
        kind = self.args.position_type
        if lower is None:
            filters = [] if self.column.not_null else [f"{quoted} IS NOT NULL"]
            scope = reader.JdbcReadScope.SOURCE_TABLE
        else:
            filters = [window.position_filter(dialect, quoted, op, lower, kind)]
            scope = reader.JdbcReadScope.BATCH_WINDOW
        return reader.probe_bounds(
            self.spark, self.args, self.source, filters, scope=scope,
            incremental=(self.column.name, kind),
        )


def empty_incremental_window(batch: "DbRow") -> bool:
    """Recognize an empty probe or an empty strict window from an older planner."""
    return batch["empty_at_plan"] or (
        batch["lower_bound_op"] == ">" and batch["upper_bound"] is not None
        and batch["lower_bound"] == batch["upper_bound"]
    )


def resolve_incremental_column(
    source: reader.ReadSource, args: argparse.Namespace, *, check_not_null: bool,
) -> jdbc.ColumnMeta:
    """Return JDBC catalog metadata for a column matching the incremental contract."""
    table = f"{args.source_id}.{args.source_table}"
    column = f"{table}.{args.incremental_column}"
    info = source.keys.columns.get(args.incremental_column.lower())
    if info is None:
        raise ValueError(
            f"Incremental column {args.incremental_column!r} not found in JDBC metadata for "
            f"{table}"
        )
    kind = jdbc.column_position_kind(source.dialect, info)
    if kind is None:
        raise ValueError(
            f"Unsupported JDBC type of incremental column {column}: "
            f"{info.type_name} precision={info.precision} scale={info.scale}"
        )
    if kind != args.position_type:
        raise ValueError(
            f"Incremental type mismatch for {column}: "
            f"expected={args.position_type} actual={kind} JDBC={info.type_name} precision={info.precision} scale={info.scale}"
        )
    if check_not_null and not info.not_null:
        raise ValueError(f"JDBC metadata does not confirm NOT NULL for incremental column {args.source_table}.{info.name}")
    return info


def _resolve_batch(spark: "SparkSession", args: argparse.Namespace, db_connect: "DbConnect") -> _ResolvedBatch:
    """Resolve this execution's persisted batch or plan a fresh window."""
    LOGGER.info("load attempt process_id=%s source_id=%s entity_fqn=%s",
                args.process_id, args.source_id, args.source_table)
    batch = registry.get_batch(
        process_id=args.process_id, source_id=args.source_id,
        entity_fqn=args.source_table, db_connect=db_connect,
    )
    if batch is not None:
        return _reuse_batch(spark, args, batch)
    return _plan_new_batch(spark, args, db_connect)


def _reuse_batch(spark: "SparkSession", args: argparse.Namespace, batch: "DbRow") -> _ResolvedBatch:
    """Verify a retry's contract without probing the incremental maximum again."""
    if batch["target_table"] != args.target_table:
        raise ValueError("Persisted batch target does not match")
    if batch["incremental_column"] != args.incremental_column:
        raise ValueError(
            "Persisted batch incremental column does not match the manifest; "
            "stop executors and migrate the contract"
        )
    if batch["incremental_type"] != args.position_type:
        raise ValueError("Persisted batch position type does not match incremental.contract")
    reason = reader.JdbcReason.PERSISTED_BATCH
    LOGGER.info("JDBC incremental maximum not recalculated: %s. reason=%s", reason.text, reason.code)
    if batch["status"] == "SUCCEEDED":
        LOGGER.info(
            "batch already succeeded; repeat read skipped: process_id=%s "
            "entity_fqn=%s row_count=%s finished_at=%s",
            args.process_id, args.source_table, batch["row_count"], batch["finished_at"],
        )
        return _ResolvedBatch(batch)
    source = reader.inspect_source(spark, args, detailed=False)
    resolve_incremental_column(source, args, check_not_null=False)
    return _ResolvedBatch(batch, read_source=source)


def _plan_new_batch(spark: "SparkSession", args: argparse.Namespace, db_connect: "DbConnect") -> _ResolvedBatch:
    """Probe and persist the window under the registry's state lock."""
    source = reader.inspect_source(spark, args)
    info = resolve_incremental_column(source, args, check_not_null=args.check_not_null)
    kind = args.position_type
    size, unit = args.lookback_size, args.lookback_unit
    LOGGER.info("incremental config: kind=%s lookback_size=%s lookback_unit=%s", kind, size, unit)
    probe = _WindowProbe(spark, args, source, info)
    batch = registry.plan_batch(
        process_id=args.process_id, dag_id=args.dag_id, run_id=args.run_id,
        source_id=args.source_id, entity_fqn=args.source_table, target_table=args.target_table,
        incremental_column=args.incremental_column, incremental_type=kind,
        lookback_size=size, lookback_unit=unit, db_connect=db_connect,
        calculate_lower=probe.calculate_lower,
        probe_upper=probe.probe_upper,
    )
    return _ResolvedBatch(batch, read_source=source, partition_bounds=probe.probed_bounds)


def managed_load(
    spark: "SparkSession", args: argparse.Namespace, *, db_connect: "DbConnect",
) -> None:
    """Resolve a batch and skip, finalize, or execute it according to its state."""
    log_step(LOGGER, "resolve incremental batch")
    plan = _resolve_batch(spark, args, db_connect)
    batch = plan.batch
    if batch["status"] == "SUCCEEDED":
        return
    registry_args = dict(process_id=args.process_id, source_id=args.source_id,
                         entity_fqn=args.source_table, db_connect=db_connect)
    if empty_incremental_window(batch):
        reason = reader.JdbcReason.EMPTY_WINDOW
        LOGGER.info("%s. JDBC read and Iceberg append skipped. No raw table created by this attempt. "
                    "target_table=%s reason=%s", reason.text, args.target_table, reason.code)
        registry.finalize_batch(row_count=0, observed_max=None, **registry_args)
        return
    _execute_batch(spark, args, plan, registry_args)


def _execute_batch(
    spark: "SparkSession", args: argparse.Namespace, plan: _ResolvedBatch, registry_args: dict[str, object],
) -> None:
    """Append the window at least once and atomically finalize its pg-meta state."""
    batch = plan.batch
    kind = batch["incremental_type"]
    registry.start_batch(**registry_args)
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
                                    watermark_after=watermark, **registry_args)
        finally:
            source.unpersist()
    except Exception:
        try:
            registry.fail_batch(**registry_args)
        except Exception:
            LOGGER.warning("Unable to persist batch failure")
        raise
