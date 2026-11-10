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

from ingestion.spark.raw.iceberg import validate_target_table
from ingestion.spark.raw.rdbms import reader
from ingestion.spark.raw.rdbms import raw_append
from ingestion.spark.raw.rdbms.reader import render_source_table
from ingestion.sources.rdbms import jdbc
from ingestion.sources.rdbms import positions
from ingestion.sources.rdbms import registry as dialects

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
    args.lookback_size = config["lookback"]["size"]
    args.lookback_unit = config["lookback"]["unit"]
    return args


def main(argv: list[str] | None = None) -> None:
    """Load one entity from a JDBC source into the raw layer."""
    args = parse_args(argv)
    if not args.source_id:
        raise ValueError("--source-id is required: extraction runs only as a managed Airflow batch")
    validate_target_table(args.target_table)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"ingest-{args.target_table}").getOrCreate()
    try:
        managed_load(spark, args)
    finally:
        spark.stop()


def empty_incremental_window(batch) -> bool:
    """True when persisted bounds prove the strict window contains no rows.

    Only a zero-lookback regular batch with equal typed bounds reaches this
    state: lower = watermark - 0 and upper = source MAX = watermark. With a
    nonzero lookback the window is always read, even when the bounds are
    equal: the aligned inclusive operator covers the range between them.
    """
    return (
        not batch["lookback_size"]
        and batch["upper_bound"] is not None
        and batch["lower_bound"] == batch["upper_bound"]
    )


def source_kind(spark, args):
    """Validate only JDBC metadata, without source aggregates or row reads."""
    dialect = dialects.by_jdbc_url(args.jdbc_url)
    column = dialect.quote_identifier(args.incremental_column)
    table = render_source_table(dialect, args.source_table, allow_expression=True)
    with jdbc.connection(spark._jvm, args.jdbc_url, args.jdbc_user, args.jdbc_password) as connection:
        _, type_name, precision, scale = jdbc.column_metadata(
            connection, f"SELECT {column} FROM {table} WHERE 1=0"
        )[0]
    kind = dialect.position_kind(type_name, precision, scale)
    if kind != args.position_type:
        raise ValueError(
            f"Incremental type mismatch for {args.source_id}.{args.source_table}.{args.incremental_column}: "
            f"expected={args.position_type} actual={kind} JDBC={type_name} precision={precision} scale={scale}"
        )
    return kind


def source_position(spark, args):
    """Inspect original JDBC type and obtain exact MAX plus NULL/empty diagnostics."""
    dialect = dialects.by_jdbc_url(args.jdbc_url)
    column = dialect.quote_identifier(args.incremental_column)
    table = render_source_table(dialect, args.source_table, allow_expression=True)
    kind = source_kind(spark, args)
    with jdbc.connection(spark._jvm, args.jdbc_url, args.jdbc_user, args.jdbc_password) as connection:
        expression = dialect.position_expression(f"MAX({column})", kind)
        row = jdbc.first_row(
            connection, f"SELECT {expression}, COUNT(*), COUNT({column}) FROM {table}"
        )
    upper = positions.canonical(row[0], kind)
    count, nonnull = int(row[1]), int(row[2])
    return kind, upper, count == 0, count != nonnull


def _plan_batch(spark, args, connect):
    """Resolve this execution's persisted batch or plan a fresh one with frozen bounds.

    The batch identity is (process_id, source_id, entity_fqn). A retry
    replays the same frozen window with metadata-only type verification: bounds, the
    position kind and empty_at_plan are already fixed in the batch, and a
    SUCCEEDED batch returns as a no-op with its recorded result logged.
    Source aggregates run only when a new batch is being planned.
    """
    from ingestion.meta import registry

    key = (args.process_id, args.source_id, args.source_table)
    LOGGER.info("load attempt process_id=%s source_id=%s entity_fqn=%s", *key)
    batch = registry.get_batch(process_id=args.process_id, source_id=args.source_id,
                           entity_fqn=args.source_table, db_connect=connect)
    if batch is not None:
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
        else:
            source_kind(spark, args)
        return batch
    kind, upper, empty, has_null = source_position(spark, args)
    if has_null:
        raise ValueError("NULL incremental value in source")
    size, unit = args.lookback_size, args.lookback_unit
    LOGGER.info("incremental config: kind=%s lookback_size=%s lookback_unit=%s", kind, size, unit)
    positions.window_bounds(upper, kind, size, unit)

    def calculate_lower(value):
        """Reject source resets before persisting an incremental window."""
        if value is not None:
            if empty or positions.maximum(value, upper, kind) != upper:
                raise ValueError("SOURCE_POSITION_REGRESSION: explicit reset/bootstrap required")
        return positions.window_bounds(value, kind, size, unit)

    return registry.plan_batch(
        process_id=args.process_id, dag_id=args.dag_id, run_id=args.run_id,
        source_id=args.source_id, entity_fqn=args.source_table, target_table=args.target_table,
        incremental_column=args.incremental_column, incremental_type=kind,
        upper_bound=upper, empty_at_plan=empty,
        lookback_size=size, lookback_unit=unit, db_connect=connect,
        calculate_lower=calculate_lower,
        compare_positions=lambda left, right: (
            0 if positions.canonical(left, kind) == positions.canonical(right, kind)
            else 1 if positions.maximum(left, right, kind) == positions.canonical(left, kind) else -1
        ),
    )


def managed_load(spark, args):
    """Execute a persisted batch with at-least-once append and atomic pg-meta finalization."""
    import psycopg2

    from ingestion.meta import registry

    def connect():
        """Open the registry connection from the operator-provided DSN."""
        dsn = os.environ.get("INGESTION_META_DSN")
        if not dsn:
            raise RuntimeError(
                "INGESTION_META_DSN is not set: the incremental loader needs pg-meta credentials"
            )
        return psycopg2.connect(dsn)

    batch = _plan_batch(spark, args, connect)
    if batch["status"] == "SUCCEEDED":
        return
    key = dict(process_id=args.process_id, source_id=args.source_id,
               entity_fqn=args.source_table, db_connect=connect)
    kind = batch["incremental_type"]
    if empty_incremental_window(batch):
        registry.finalize_batch(row_count=0, observed_max=None, **key)
        return
    registry.start_batch(**key)
    try:
        source = reader.read_extract_frame(spark, args, batch, kind)
        try:
            rows, observed = raw_append.append_raw(spark, args, source, kind)
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
