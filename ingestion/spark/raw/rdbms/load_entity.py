"""Append JDBC data to Iceberg with persisted, typed extraction batches.

Managed Airflow execution is the only load path: fixed source bounds are
planned in pg-meta, batch identity survives retries and the watermark is
finalized atomically. Repeated appends remain valid at-least-once behavior.
Pure helpers import without Spark; managed execution uses the shared
ingestion package.
"""

import argparse
import logging
import re
from decimal import Decimal

from ingestion.iceberg import (
    CATALOG,
    METADATA_BATCH,
    METADATA_DTTM,
    METADATA_PID,
    build_create_table_sql,
    schema_columns,
    validate_target_table,
)
from ingestion.sources.rdbms import jdbc
from ingestion.sources.rdbms import positions
from ingestion.sources.rdbms import registry as dialects
from ingestion.sources.rdbms.contracts import parse_source_table

COLUMN_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and validate the loader CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Append one entity from a JDBC source into the raw layer."
    )
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--jdbc-user", required=True)
    parser.add_argument("--jdbc-password", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--process-id", required=True, type=int)
    parser.add_argument("--incremental-column")
    parser.add_argument("--num-partitions", type=int, default=4)
    parser.add_argument("--no-parallelism", action="store_true")
    parser.add_argument("--source-id")
    parser.add_argument("--dag-id")
    parser.add_argument("--run-id")
    parser.add_argument("--lookback-size", type=int, default=0)
    parser.add_argument("--lookback-unit", choices=("h", "d"))
    parser.add_argument("--lookback-given", action="store_true")
    parser.add_argument("--force-full", action="store_true")
    parser.add_argument("--reason")
    args = parser.parse_args(argv)
    if args.process_id <= 0:
        raise ValueError(f"--process-id must be a positive integer (got {args.process_id})")
    if args.num_partitions <= 0:
        raise ValueError(f"--num-partitions must be a positive integer (got {args.num_partitions})")
    if args.incremental_column is not None and not COLUMN_NAME_PATTERN.match(args.incremental_column):
        raise ValueError(
            f"--incremental-column must be a column identifier (got {args.incremental_column!r})"
        )
    return args


def render_source_table(dialect, source_table: str, allow_expression: bool = False) -> str:
    """Render a schema.table reference quoted for the source dialect.

    Every part of the reference is quoted per dialect (Oracle double quotes,
    MSSQL brackets), exactly as the object was created by sydafa. With
    allow_expression a reference that is not schema.table (already a subquery
    expression) is passed through unchanged; otherwise malformed references
    are rejected.
    """
    try:
        parts = parse_source_table(source_table)
    except ValueError:
        if allow_expression:
            return source_table
        raise
    return dialect.render_table(parts)


def pick_partition_column(dialect, candidates: list[tuple[str, str]]) -> str | None:
    """Return the first candidate column the dialect allows for partitioning, else None."""
    for name, jdbc_type in candidates:
        if dialect.is_partition_type(jdbc_type):
            return name
    return None


def _jdbc_reader(spark, args, read_query):
    """Build the JDBC reader with driver and engine options from the dialect."""
    dialect = dialects.by_jdbc_url(args.jdbc_url)
    reader = (
        spark.read.format("jdbc")
        .option("url", args.jdbc_url)
        .option("dbtable", read_query)
        .option("user", args.jdbc_user)
        .option("password", args.jdbc_password)
        .option("driver", dialect.driver_class)
    )
    for key, value in dialect.reader_options().items():
        reader = reader.option(key, value)
    return reader


def _bound_literal(value) -> str:
    """Render a JDBC partition bound; integral Decimals collapse to plain integers.

    Oracle NUMBER bounds arrive as Decimal('1.0000000000'), which the JDBC
    partitioner cannot parse when the partition column itself maps to an
    integral type.
    """
    if isinstance(value, Decimal) and value == value.to_integral_value():
        return str(int(value))
    return str(value)


def _read_bounds(spark, args, read_query, column) -> tuple:
    """Read MIN/MAX of the partition column over the read query (one-row JDBC read)."""
    column_ref = dialects.by_jdbc_url(args.jdbc_url).quote_identifier(column)
    query = (
        f"(SELECT MIN({column_ref}) AS lower_bound, MAX({column_ref}) AS upper_bound "
        f"FROM {read_query}) bounds"
    )
    frame = _jdbc_reader(spark, args, query).load()
    row = frame.collect()[0]
    return row[0], row[1]


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
    state: lower = watermark - 0 and upper = source MAX = watermark.
    """
    return (
        batch["mode"] == "incremental"
        and batch["upper_bound"] is not None
        and batch["lower_bound"] == batch["upper_bound"]
    )


def source_position(spark, args, inspect_only=False):
    """Inspect original JDBC type and obtain exact MAX plus NULL/empty diagnostics."""
    dialect = dialects.by_jdbc_url(args.jdbc_url)
    column = dialect.quote_identifier(args.incremental_column)
    table = render_source_table(dialect, args.source_table, allow_expression=True)
    with jdbc.connection(spark._jvm, args.jdbc_url, args.jdbc_user, args.jdbc_password) as connection:
        label, type_name, precision, scale = jdbc.column_metadata(
            connection, f"SELECT {column} FROM {table} WHERE 1=0"
        )[0]
        kind = dialect.position_kind(type_name, precision, scale)
        expression = "NULL" if inspect_only else dialect.position_expression(f"MAX({column})", kind)
        row = jdbc.first_row(
            connection, f"SELECT {expression}, COUNT(*), COUNT({column}) FROM {table}"
        )
    upper = positions.canonical(row[0], kind)
    count, nonnull = int(row[1]), int(row[2])
    return kind, upper, count == 0, count != nonnull



def _plan_batch(spark, args, connect):
    """Resolve this DAG run's persisted batch or plan a fresh one with frozen bounds.

    A retry first re-reads the existing batch so every attempt replays the
    same window; source inspection then only re-validates the frozen
    contract. The returned batch may already be SUCCEEDED, which the caller
    skips as a completed no-op.
    """
    from ingestion import meta

    batch = meta.get_batch(dag_id=args.dag_id, run_id=args.run_id, source_id=args.source_id,
                           entity_fqn=args.source_table, db_connect=connect)
    if batch and (batch["process_id"] != args.process_id or batch["target_table"] != args.target_table
                  or batch["incremental_column"] != args.incremental_column):
        raise ValueError("Persisted batch identity or extraction contract changed")
    if batch and batch["status"] == "SUCCEEDED":
        return batch
    kind = batch["incremental_type"] if batch else None
    upper = None
    empty = False
    if args.incremental_column:
        detected, detected_upper, detected_empty, has_null = source_position(spark, args, inspect_only=batch is not None)
        if batch is None:
            kind, upper, empty = detected, detected_upper, detected_empty
        elif detected != kind:
            raise ValueError("Incremental source type changed; migrate pg-meta")
        if has_null and not args.force_full:
            raise ValueError("NULL incremental value in source")
        positions.lower_bound(upper, kind, args.lookback_size, args.lookback_unit,
                              specified=args.lookback_given)
    if batch is not None:
        return batch

    def calculate_lower(value):
        """Reject source resets before persisting an incremental window."""
        if value is not None and not args.force_full:
            if empty or positions.maximum(value, upper, kind) != upper:
                raise ValueError("SOURCE_POSITION_REGRESSION: explicit reset/bootstrap required")
        return positions.lower_bound(value, kind, args.lookback_size, args.lookback_unit,
                                     specified=args.lookback_given)

    return meta.plan_batch(
        process_id=args.process_id, dag_id=args.dag_id, run_id=args.run_id,
        source_id=args.source_id, entity_fqn=args.source_table, target_table=args.target_table,
        incremental_column=args.incremental_column, incremental_type=kind,
        upper_bound=upper, empty_at_plan=empty, force_full=args.force_full,
        lookback_size=args.lookback_size, lookback_unit=args.lookback_unit,
        reason_text=args.reason, db_connect=connect,
        calculate_lower=calculate_lower,
        compare_positions=lambda left, right: (
            0 if positions.canonical(left, kind) == positions.canonical(right, kind)
            else 1 if positions.maximum(left, right, kind) == positions.canonical(left, kind) else -1
        ),
    )


def _read_extract_frame(spark, args, batch, kind):
    """Read the frozen batch window over JDBC into a cached frame.

    The frame mirrors the source columns and carries the exact typed
    extraction position as the __extract_position column.
    """
    dialect = dialects.by_jdbc_url(args.jdbc_url)
    filters = []
    column = dialect.quote_identifier(args.incremental_column) if args.incremental_column else None
    for key, op in (("lower_bound", ">"), ("upper_bound", "<=")):
        if batch[key] is not None:
            filters.append(
                f"{column} {op} {dialect.position_literal(positions.canonical(batch[key], kind), kind)}"
            )
    if batch["empty_at_plan"]:
        filters.append("1=0")
    source_table = render_source_table(dialect, args.source_table, allow_expression=True)
    select = "extract_source.*"
    projection = jdbc.compatibility_projection(
        spark._jvm, dialect, args.jdbc_url, args.jdbc_user, args.jdbc_password, source_table
    )
    if projection is not None:
        select = ", ".join(projection)
    if column:
        select += f', {dialect.position_expression(column, kind)} AS "__extract_position"'
    query = (
        f"(SELECT {select} FROM (SELECT * FROM "
        f"{source_table}) extract_source"
        + (" WHERE " + " AND ".join(filters) if filters else "")
        + ") extract_batch"
    )
    reader = _jdbc_reader(spark, args, query)
    if not args.no_parallelism and (not kind or not kind.startswith("timestamp")):
        partition_column = args.incremental_column or pick_partition_column(
            dialect, jdbc.key_columns(
                spark._jvm, dialect, args.jdbc_url, args.jdbc_user, args.jdbc_password, args.source_table))
        if partition_column:
            low, high = _read_bounds(spark, args, query, partition_column)
            if low is not None and high is not None and low != high:
                reader = reader.option("partitionColumn", partition_column).option(
                    "lowerBound", _bound_literal(low)).option("upperBound", _bound_literal(high)).option(
                    "numPartitions", args.num_partitions)
    return reader.load().cache()


def _append_raw(spark, args, batch, frame, kind):
    """Append the read window to the raw table; return (row_count, observed_max).

    The target table is created on first load from the actually read schema;
    an empty window still verifies the schema without writing rows.
    """
    import pyspark.sql.functions as F

    summary = frame.agg(F.count("*").alias("n"), *(
        [F.max("__extract_position").alias("position")] if args.incremental_column else []
    )).first()
    rows = int(summary["n"])
    observed = positions.canonical(summary["position"], kind) if args.incremental_column else None
    clean = frame.drop("__extract_position") if args.incremental_column else frame
    if any(field.name in (METADATA_BATCH, METADATA_PID, METADATA_DTTM) for field in clean.schema.fields):
        raise ValueError("Source column collides with ingestion metadata")
    namespace, _ = validate_target_table(args.target_table)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")
    if not spark.catalog.tableExists(args.target_table):
        spark.sql(build_create_table_sql(args.target_table, schema_columns(clean.schema)))
    if rows:
        portion = clean.withColumn(METADATA_DTTM, F.current_timestamp()).withColumn(
            METADATA_PID, F.lit(args.process_id).cast("long")).withColumn(
            METADATA_BATCH, F.lit(batch["batch_id"]).cast("long"))
        portion.writeTo(args.target_table).option("mergeSchema", "true").option("check-ordering", "false").append()
    return rows, observed


def managed_load(spark, args):
    """Execute a persisted batch with at-least-once append and atomic pg-meta finalization."""
    import psycopg2

    from ingestion import meta

    connect = lambda: psycopg2.connect("")
    batch = _plan_batch(spark, args, connect)
    if batch["status"] == "SUCCEEDED":
        return
    kind = batch["incremental_type"]
    if empty_incremental_window(batch):
        meta.finalize_batch(batch_id=batch["batch_id"], row_count=0, observed_max=None,
                            db_connect=connect)
        return
    meta.start_batch(batch_id=batch["batch_id"], db_connect=connect)
    try:
        source = _read_extract_frame(spark, args, batch, kind)
        try:
            rows, observed = _append_raw(spark, args, batch, source, kind)
            watermark = batch["watermark_before"]
            if args.incremental_column and batch["reason"] != "manual_full":
                watermark = positions.maximum(watermark, observed, kind)
            meta.finalize_batch(batch_id=batch["batch_id"], row_count=rows, observed_max=observed,
                                watermark_after=watermark, db_connect=connect)
        finally:
            source.unpersist()
    except Exception:
        try:
            meta.fail_batch(batch_id=batch["batch_id"], error="Extraction execution failed; see task logs",
                            db_connect=connect)
        except Exception:
            logging.getLogger(__name__).warning("Unable to persist batch failure")
        raise


if __name__ == "__main__":
    main()
