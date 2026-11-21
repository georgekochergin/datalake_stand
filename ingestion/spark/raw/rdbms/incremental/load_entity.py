"""Append JDBC data to Iceberg with persisted, typed extraction batches.

Managed Airflow execution is the only load path: fixed source bounds are
planned in pg-meta, batch identity survives retries and the watermark is
finalized atomically. Repeated appends remain valid at-least-once behavior.
This application serves incremental entities only: the incremental column is
a required CLI argument, and the full-slice stream runs a separate
application. Pure helpers import without Spark; managed execution uses the
shared ingestion package.
"""

import argparse
import json
import logging
import os
from typing import TYPE_CHECKING

from ingestion.logging import configure_driver_logging, log_step

from ingestion.spark.raw import append
from ingestion.spark.raw.iceberg import validate_target_table
from ingestion.spark.raw.rdbms import reader
from ingestion.spark.raw.rdbms.incremental import window
from ingestion.sources.rdbms import jdbc
from ingestion.sources.rdbms import positions

if TYPE_CHECKING:
    from psycopg2.extensions import connection as Psycopg2Connection
    from pyspark.sql import SparkSession

    from ingestion.meta import DbConnect, DbRow

LOGGER = logging.getLogger(__name__)

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and validate the loader CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Append one incremental entity window from a JDBC source into the raw layer."
    )
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--jdbc-user", required=True)
    parser.add_argument("--jdbc-password", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--process-id", required=True, type=int)
    parser.add_argument("--incremental-config", required=True)
    parser.add_argument("--num-jdbc-partitions", dest="num_partitions", type=int, default=4)
    parser.add_argument("--no-jdbc-partitioning", action="store_true")
    parser.add_argument("--source-id")
    parser.add_argument("--dag-id")
    parser.add_argument("--run-id")
    args = parser.parse_args(argv)
    if args.process_id <= 0:
        raise ValueError(f"--process-id must be a positive integer (got {args.process_id})")
    if args.num_partitions <= 0:
        raise ValueError(f"--num-jdbc-partitions must be a positive integer (got {args.num_partitions})")
    from ingestion.sources.rdbms.incremental_config import validate

    config = validate(json.loads(args.incremental_config))
    args.incremental_config = config
    args.incremental_column = config["contract"]["source_column"]
    args.position_type = config["contract"]["position_type"]
    args.check_not_null = config["contract"].get("check_not_null", True)
    args.lookback_size = config["lookback"]["size"]
    args.lookback_unit = config["lookback"]["unit"]
    return args


def main(argv: list[str] | None = None) -> None:
    """Load one entity from a JDBC source into the raw layer."""
    configure_driver_logging()
    args = parse_args(argv)
    if not args.source_id:
        raise ValueError("--source-id is required: extraction runs only as a managed Airflow batch")
    validate_target_table(args.target_table)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    try:
        managed_load(spark, args)
    finally:
        spark.stop()


def empty_incremental_window(batch: "DbRow") -> bool:
    """Recognize an empty probe or an empty strict window from an older planner."""
    return batch["empty_at_plan"] or (
        batch["lower_bound_op"] == ">" and batch["upper_bound"] is not None
        and batch["lower_bound"] == batch["upper_bound"]
    )


def source_column(source: reader.ReadSource, args: argparse.Namespace, *, check_not_null: bool) -> jdbc.ColumnMeta:
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


def _plan_batch(spark: "SparkSession", args: argparse.Namespace, connect: "DbConnect") -> tuple[
    "DbRow", reader.ReadSource | None, tuple[reader.PartitionBound, reader.PartitionBound] | None,
]:
    """Resolve this execution's persisted batch or plan a fresh one with frozen bounds.

    The batch identity is (process_id, source_id, entity_fqn). A retry
    replays the same frozen window with metadata-only type verification: bounds, the
    position kind and empty_at_plan are already fixed in the batch, and a
    SUCCEEDED batch returns as a no-op with its recorded result logged.
    A retry may probe partition bounds but does not probe the incremental maximum.
    """
    from ingestion.meta import registry

    key = (args.process_id, args.source_id, args.source_table)
    LOGGER.info("load attempt process_id=%s source_id=%s entity_fqn=%s", *key)
    batch = registry.get_batch(process_id=args.process_id, source_id=args.source_id,
                           entity_fqn=args.source_table, db_connect=connect)
    if batch is not None:
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
            return batch, None, None
        source = reader.inspect_source(spark, args, detailed=False)
        source_column(source, args, check_not_null=False)
        return batch, source, None
    source = reader.inspect_source(spark, args)
    info = source_column(source, args, check_not_null=args.check_not_null)
    kind = args.position_type
    size, unit = args.lookback_size, args.lookback_unit
    LOGGER.info("incremental config: kind=%s lookback_size=%s lookback_unit=%s", kind, size, unit)
    bounds = None

    def calculate_lower(value: str | None) -> tuple[str | None, str]:
        """Use the committed watermark to compute the shared lower predicate."""
        return positions.window_bounds(value, kind, size, unit)

    def probe_upper(lower: str | None, op: str) -> str | None:
        """Probe the maximum and any partition bounds over the same source rows."""
        nonlocal bounds
        column = source.dialect.quote_identifier(info.name)
        filters = [window.position_filter(source.dialect, column, op, lower, kind)] if lower is not None else (
            [f"{column} IS NOT NULL"] if not info.not_null else []
        )
        upper, bounds = reader.probe_bounds(
            spark, args, source, filters,
            scope=reader.JdbcReadScope.BATCH_WINDOW if lower is not None else reader.JdbcReadScope.SOURCE_TABLE,
            incremental=(info.name, kind),
        )
        return upper

    batch = registry.plan_batch(
        process_id=args.process_id, dag_id=args.dag_id, run_id=args.run_id,
        source_id=args.source_id, entity_fqn=args.source_table, target_table=args.target_table,
        incremental_column=args.incremental_column, incremental_type=kind,
        lookback_size=size, lookback_unit=unit, db_connect=connect,
        calculate_lower=calculate_lower, probe_upper=probe_upper,
    )
    return batch, source, bounds


def managed_load(spark: "SparkSession", args: argparse.Namespace) -> None:
    """Execute a persisted batch with at-least-once append and atomic pg-meta finalization."""
    import psycopg2

    from ingestion.meta import registry

    def connect() -> "Psycopg2Connection":
        """Open the registry connection from the operator-provided DSN."""
        dsn = os.environ.get("INGESTION_META_DSN")
        if not dsn:
            raise RuntimeError(
                "INGESTION_META_DSN is not set: the incremental loader needs pg-meta credentials"
            )
        return psycopg2.connect(dsn)

    log_step(LOGGER, "resolve incremental batch")
    batch, read_source, bounds = _plan_batch(spark, args, connect)
    if batch["status"] == "SUCCEEDED":
        return
    key = dict(process_id=args.process_id, source_id=args.source_id,
               entity_fqn=args.source_table, db_connect=connect)
    kind = batch["incremental_type"]
    if empty_incremental_window(batch):
        reason = reader.JdbcReason.EMPTY_WINDOW
        LOGGER.info("%s. JDBC read and Iceberg append skipped. reason=%s", reason.text, reason.code)
        registry.finalize_batch(row_count=0, observed_max=None, **key)
        return
    registry.start_batch(**key)
    try:
        source = window.read_window_frame(spark, args, batch, kind, source=read_source, bounds=bounds)
        try:
            log_step(LOGGER, "summarize batch window")
            rows, observed, clean = window.window_summary(source, kind)
            LOGGER.info("batch summary row_count=%s observed_max=%s", rows, observed)
            append.append_raw(spark, args.target_table, clean, rows, args.process_id)
            watermark = batch["watermark_before"]
            watermark = positions.maximum(watermark, observed, kind)
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


if __name__ == "__main__":
    main()
