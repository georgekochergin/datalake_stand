"""Load one persisted incremental JDBC window into Iceberg with at-least-once append.

One execution of managed_load walks a single pg-meta batch through the flow:
resolve reuses the frozen window of a retry (registry.get_batch) or plans a
fresh one (registry.plan_batch, probing the source maximum inside the
planning transaction); a succeeded batch returns immediately, a provably
empty window finalizes without reading, and an executable window is started,
read, appended, and finalized, with registry.fail_batch recording an abort.
"""

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
from ingestion.sources.rdbms.incremental_config import WindowContract

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
class _WindowPlanner:
    """JDBC implementation of the registry.BatchPlanner protocol for one attempt.

    registry.plan_batch calls calculate_lower with the watermark locked by the
    planning transaction, then probe_upper over the resulting predicate while
    the lock is still held; a retry that finds an existing batch probes
    nothing. probe_upper remembers the partition bounds it observed so the
    reader can reuse them instead of probing the frozen window again.
    """

    spark: "SparkSession"
    args: argparse.Namespace
    contract: WindowContract
    source: reader.ReadSource
    column: jdbc.ColumnMeta
    probed_bounds: reader.PartitionBounds | None = None

    def calculate_lower(self, watermark: str | None) -> tuple[str | None, str]:
        """Compute the lower predicate from the committed watermark and lookback."""
        return positions.window_bounds(
            watermark, self.contract.position_type, self.contract.lookback_size, self.contract.lookback_unit,
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
        kind = self.contract.position_type
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
    contract: WindowContract = args.incremental
    column = f"{table}.{contract.column}"
    info = source.keys.columns.get(contract.column.lower())
    if info is None:
        raise ValueError(
            f"Incremental column {contract.column!r} not found in JDBC metadata for "
            f"{table}"
        )
    kind = jdbc.column_position_kind(source.dialect, info)
    if kind is None:
        raise ValueError(
            f"Unsupported JDBC type of incremental column {column}: "
            f"{info.type_name} precision={info.precision} scale={info.scale}"
        )
    if kind != contract.position_type:
        raise ValueError(
            f"Incremental type mismatch for {column}: "
            f"expected={contract.position_type} actual={kind} JDBC={info.type_name} "
            f"precision={info.precision} scale={info.scale}"
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
    if batch["incremental_column"] != args.incremental.column:
        raise ValueError(
            "Persisted batch incremental column does not match the manifest; "
            "stop executors and migrate the contract"
        )
    if batch["incremental_type"] != args.incremental.position_type:
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
    contract: WindowContract = args.incremental
    info = resolve_incremental_column(source, args, check_not_null=contract.check_not_null)
    kind = contract.position_type
    size, unit = contract.lookback_size, contract.lookback_unit
    LOGGER.info("incremental config: kind=%s lookback_size=%s lookback_unit=%s", kind, size, unit)
    planner = _WindowPlanner(spark, args, contract, source, info)
    batch = registry.plan_batch(
        process_id=args.process_id, dag_id=args.dag_id, run_id=args.run_id,
        source_id=args.source_id, entity_fqn=args.source_table, target_table=args.target_table,
        incremental_column=contract.column, incremental_type=kind,
        lookback_size=size, lookback_unit=unit, planner=planner, db_connect=db_connect,
    )
    return _ResolvedBatch(batch, read_source=source, partition_bounds=planner.probed_bounds)


def managed_load(
    spark: "SparkSession", args: argparse.Namespace, *, db_connect: "DbConnect",
) -> None:
    """Resolve a batch and skip, finalize, or execute it according to its state."""
    log_step(LOGGER, "resolve incremental batch")
    plan = _resolve_batch(spark, args, db_connect)
    batch = plan.batch
    if batch["status"] == "SUCCEEDED":
        return
    if empty_incremental_window(batch):
        reason = reader.JdbcReason.EMPTY_WINDOW
        LOGGER.info("%s. JDBC read and Iceberg append skipped. No raw table created by this attempt. "
                    "target_table=%s reason=%s", reason.text, args.target_table, reason.code)
        registry.finalize_batch(
            process_id=args.process_id, source_id=args.source_id, entity_fqn=args.source_table,
            row_count=0, observed_max=None, watermark_after=batch["watermark_before"],
            db_connect=db_connect,
        )
        return
    _execute_batch(spark, args, plan, db_connect)


def _execute_batch(
    spark: "SparkSession", args: argparse.Namespace, plan: _ResolvedBatch, db_connect: "DbConnect",
) -> None:
    """Append the window at least once and atomically finalize its pg-meta state."""
    batch = plan.batch
    kind = batch["incremental_type"]
    registry.start_batch(
        process_id=args.process_id, source_id=args.source_id,
        entity_fqn=args.source_table, db_connect=db_connect,
    )
    try:
        source = window.read_window_frame(
            spark, args, batch, kind, source=plan.read_source, bounds=plan.partition_bounds,
        )
        try:
            log_step(LOGGER, "summarize batch window")
            rows, observed, clean = window.window_summary(source, kind)
            LOGGER.info("batch summary row_count=%s observed_max=%s", rows, observed)
            append.append_raw(spark, args.target_table, clean, rows, args.process_id)
            registry.finalize_batch(
                process_id=args.process_id, source_id=args.source_id, entity_fqn=args.source_table,
                row_count=rows, observed_max=observed,
                watermark_after=positions.maximum(batch["watermark_before"], observed, kind),
                db_connect=db_connect,
            )
        finally:
            source.unpersist()
    except Exception:
        try:
            registry.fail_batch(
                process_id=args.process_id, source_id=args.source_id,
                entity_fqn=args.source_table, db_connect=db_connect,
            )
        except Exception:
            LOGGER.warning("Unable to persist batch failure")
        raise
