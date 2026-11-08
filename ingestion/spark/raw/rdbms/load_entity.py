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
    METADATA_DTTM,
    METADATA_PID,
    build_create_table_sql,
    schema_columns,
    validate_target_table,
)
from ingestion.sources.rdbms import jdbc
from ingestion.sources.rdbms import jdbc_partitioning
from ingestion.sources.rdbms import positions
from ingestion.sources.rdbms import registry as dialects
from ingestion.sources.rdbms.contracts import parse_source_table

COLUMN_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
LOGGER = logging.getLogger(__name__)

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
    parser.add_argument("--no-jdbc-partitioning", action="store_true")
    parser.add_argument("--source-id")
    parser.add_argument("--dag-id")
    parser.add_argument("--run-id")
    parser.add_argument("--lookback-size", type=int, default=0)
    parser.add_argument("--lookback-unit", choices=("h", "d"))
    parser.add_argument("--lookback-given", action="store_true")
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


def _long_representable(*values) -> bool:
    """Check numeric partition bounds fit the signed 64-bit Spark bound parsing.

    Spark's JDBC partitioning parses lowerBound/upperBound of numeric
    columns as a Long regardless of the column precision: values outside
    the range fail the read, so such bounds disable partitioning.
    """
    return all(-(2 ** 63) <= int(value) <= 2 ** 63 - 1 for value in values)


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
    state: lower = watermark - 0 and upper = source MAX = watermark. With a
    nonzero lookback the window is always read, even when the bounds are
    equal: the aligned inclusive operator covers the range between them.
    """
    return (
        not batch["lookback_size"]
        and batch["upper_bound"] is not None
        and batch["lower_bound"] == batch["upper_bound"]
    )


def source_position(spark, args):
    """Inspect original JDBC type and obtain exact MAX plus NULL/empty diagnostics."""
    dialect = dialects.by_jdbc_url(args.jdbc_url)
    column = dialect.quote_identifier(args.incremental_column)
    table = render_source_table(dialect, args.source_table, allow_expression=True)
    with jdbc.connection(spark._jvm, args.jdbc_url, args.jdbc_user, args.jdbc_password) as connection:
        label, type_name, precision, scale = jdbc.column_metadata(
            connection, f"SELECT {column} FROM {table} WHERE 1=0"
        )[0]
        kind = dialect.position_kind(type_name, precision, scale)
        expression = dialect.position_expression(f"MAX({column})", kind)
        row = jdbc.first_row(
            connection, f"SELECT {expression}, COUNT(*), COUNT({column}) FROM {table}"
        )
    upper = positions.canonical(row[0], kind)
    count, nonnull = int(row[1]), int(row[2])
    return kind, upper, count == 0, count != nonnull



def _plan_batch(spark, args, connect):
    """Resolve this execution's persisted batch or plan a fresh one with frozen bounds.

    Returns None for entities without an incremental column: full-slice
    loads run outside the incremental registry. For incremental entities the
    batch identity is (process_id, source_id, entity_fqn). A retry
    replays the same frozen window without any source access: bounds, the
    position kind and empty_at_plan are already fixed in the batch, and a
    SUCCEEDED batch returns as a no-op with its recorded result logged.
    Source inspection runs only when a new batch is being planned.
    """
    from ingestion import meta

    key = (args.process_id, args.source_id, args.source_table)
    LOGGER.info("load attempt process_id=%s source_id=%s entity_fqn=%s", *key)
    if not args.incremental_column:
        return None
    batch = meta.get_batch(process_id=args.process_id, source_id=args.source_id,
                           entity_fqn=args.source_table, db_connect=connect)
    if batch is not None:
        if batch["target_table"] != args.target_table:
            raise ValueError("Persisted batch target does not match")
        if batch["incremental_column"] != args.incremental_column:
            raise ValueError(
                "Persisted batch incremental column does not match the manifest; "
                "stop executors and migrate the contract"
            )
        if batch["status"] == "SUCCEEDED":
            LOGGER.info(
                "batch already succeeded; repeat read skipped: process_id=%s "
                "entity_fqn=%s row_count=%s finished_at=%s",
                args.process_id, args.source_table, batch["row_count"], batch["finished_at"],
            )
        return batch
    kind = None
    upper = None
    empty = False
    size, unit = args.lookback_size, args.lookback_unit
    if args.incremental_column:
        detected, detected_upper, detected_empty, has_null = source_position(spark, args)
        if has_null:
            raise ValueError("NULL incremental value in source")
        kind, upper, empty = detected, detected_upper, detected_empty
        size, unit = positions.resolve_lookback(kind, args.lookback_size, args.lookback_unit,
                                                args.lookback_given)
        positions.window_bounds(upper, kind, size, unit)

    def calculate_lower(value):
        """Reject source resets before persisting an incremental window."""
        if value is not None:
            if empty or positions.maximum(value, upper, kind) != upper:
                raise ValueError("SOURCE_POSITION_REGRESSION: explicit reset/bootstrap required")
        return positions.window_bounds(value, kind, size, unit)

    return meta.plan_batch(
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


def _read_extract_frame(spark, args, batch, kind):
    """Read the frozen batch window over JDBC into a cached frame.

    The frame mirrors the source columns and carries the exact typed
    extraction position as the __extract_position column.
    """
    dialect = dialects.by_jdbc_url(args.jdbc_url)
    filters = []
    column = dialect.quote_identifier(args.incremental_column) if args.incremental_column else None
    if column:
        for key, op in (("lower_bound", batch["lower_bound_op"]), ("upper_bound", "<=")):
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
    partition = None
    keys = None
    day_alias_blocked = False
    if not args.no_jdbc_partitioning:
        keys = jdbc.key_metadata(
            spark._jvm, dialect, args.jdbc_url, args.jdbc_user, args.jdbc_password, args.source_table
        )
        partition = jdbc_partitioning.choose(dialect, keys)
        if partition is not None and partition.derived:
            if jdbc_partitioning.DAY_ALIAS in keys.columns:
                LOGGER.warning(
                    "jdbc partitioning: source column %s collides with the day alias;"
                    " single-threaded read",
                    jdbc_partitioning.DAY_ALIAS,
                )
                day_alias_blocked = True
                partition = None
            else:
                select += (
                    f", {jdbc_partitioning.day_expression(dialect, keys, partition)}"
                    f' AS {dialect.quote_identifier(jdbc_partitioning.DAY_ALIAS)}'
                )
    query = (
        f"(SELECT {select} FROM (SELECT * FROM "
        f"{source_table}) extract_source"
        + (" WHERE " + " AND ".join(filters) if filters else "")
        + ") extract_batch"
    )
    reader = _jdbc_reader(spark, args, query)
    if (partition is None and not day_alias_blocked
            and not args.no_jdbc_partitioning and keys is not None and keys.columns):
        LOGGER.warning(
            "jdbc partitioning: no partition column found for %s; "
            "single-threaded read; %s",
            args.source_table, jdbc_partitioning.no_candidate_reason(dialect, keys),
        )
    low = high = None
    if partition is not None:
        reference = jdbc_partitioning.partition_name(partition)
        low, high = _read_bounds(spark, args, query, reference)
        parallel = low is not None and high is not None and low != high
        if parallel and positions.kind_info(partition.kind)[0] == "integral" and not _long_representable(low, high):
            LOGGER.warning(
                "jdbc partitioning: bounds of %s [%s..%s] exceed the signed 64-bit"
                " range Spark parses for partition bounds; single-threaded read",
                partition.column, low, high,
            )
            parallel = False
        if parallel:
            reader = reader.option("partitionColumn", reference).option(
                "lowerBound", _bound_literal(low)).option("upperBound", _bound_literal(high)).option(
                "numPartitions", args.num_partitions)
    frame = reader.load()
    if partition is not None and partition.derived:
        frame = frame.drop(jdbc_partitioning.partition_name(partition))
    frame = frame.cache()
    if partition is not None:
        LOGGER.info(
            "jdbc partitioning: column=%s step=%s window=[%s..%s] partitions=%s of %s requested",
            partition.column, partition.step, low, high,
            frame.rdd.getNumPartitions(), args.num_partitions,
        )
    return frame


def _append_raw(spark, args, frame, kind):
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
    if any(field.name in (METADATA_PID, METADATA_DTTM) for field in clean.schema.fields):
        raise ValueError("Source column collides with ingestion metadata")
    namespace, _ = validate_target_table(args.target_table)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")
    if not spark.catalog.tableExists(args.target_table):
        spark.sql(build_create_table_sql(args.target_table, schema_columns(clean.schema)))
    if rows:
        portion = clean.withColumn(METADATA_DTTM, F.current_timestamp()).withColumn(
            METADATA_PID, F.lit(args.process_id).cast("long"))
        portion.writeTo(args.target_table).option("mergeSchema", "true").option("check-ordering", "false").append()
    return rows, observed


def managed_load(spark, args):
    """Execute a persisted batch with at-least-once append and atomic pg-meta finalization.

    Full-slice entities (no incremental column) load outside the registry:
    the window is read and appended with no pg-meta batch at all, and a task
    retry simply repeats the append (at-least-once).
    """
    import psycopg2

    from ingestion import meta

    connect = lambda: psycopg2.connect("")
    batch = _plan_batch(spark, args, connect)
    if batch is None:
        source = _read_extract_frame(spark, args, batch, None)
        try:
            _append_raw(spark, args, source, None)
        finally:
            source.unpersist()
        return
    if batch["status"] == "SUCCEEDED":
        return
    key = dict(process_id=args.process_id, source_id=args.source_id,
               entity_fqn=args.source_table, db_connect=connect)
    kind = batch["incremental_type"]
    if empty_incremental_window(batch):
        meta.finalize_batch(row_count=0, observed_max=None, **key)
        return
    meta.start_batch(**key)
    try:
        source = _read_extract_frame(spark, args, batch, kind)
        try:
            rows, observed = _append_raw(spark, args, source, kind)
            watermark = batch["watermark_before"]
            if args.incremental_column:
                watermark = positions.maximum(watermark, observed, kind)
            meta.finalize_batch(row_count=rows, observed_max=observed,
                                watermark_after=watermark, **key)
        finally:
            source.unpersist()
    except Exception:
        try:
            meta.fail_batch(**key)
        except Exception:
            LOGGER.warning("Unable to persist batch failure")
        raise


if __name__ == "__main__":
    main()
