"""Append JDBC data to Iceberg with persisted, typed extraction batches.

Managed Airflow execution plans fixed source bounds in pg-meta, preserves
batch identity across retries and atomically finalizes its watermark.
Repeated appends remain valid at-least-once behavior. Pure helpers import
without Spark; managed execution uses the shared ingestion package.
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
WATERMARK_PATTERN = re.compile(r"^[0-9]+$")

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
    parser.add_argument("--watermark-value")
    parser.add_argument("--partition-column")
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
    for name in ("incremental_column", "partition_column"):
        value = getattr(args, name)
        if value is not None and not COLUMN_NAME_PATTERN.match(value):
            raise ValueError(
                f"--{name.replace('_', '-')} must be a column identifier (got {value!r})"
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


def build_read_query(
    source_table: str,
    incremental_column: str | None,
    watermark_value: str | None,
    jdbc_url: str,
) -> str:
    """Build the JDBC dbtable expression: bare table, or an incremental subquery."""
    dialect = dialects.by_jdbc_url(jdbc_url)
    table = render_source_table(dialect, source_table)
    if incremental_column is None:
        return table
    if not COLUMN_NAME_PATTERN.match(incremental_column):
        raise ValueError(f"invalid incremental column: {incremental_column!r}")
    if watermark_value is None:
        return table
    if not WATERMARK_PATTERN.match(watermark_value):
        raise ValueError(
            f"watermark must be a plain integer (got {watermark_value!r}); "
            "quoted/ISO watermarks are not supported"
        )
    column = dialect.quote_identifier(incremental_column)
    return f"(SELECT * FROM {table} WHERE {column} > {watermark_value}) q"


def pick_partition_column(dialect, candidates: list[tuple[str, str]]) -> str | None:
    """Return the first candidate column the dialect allows for partitioning, else None."""
    for name, jdbc_type in candidates:
        if dialect.is_partition_type(jdbc_type):
            return name
    return None


def validate_incremental_column(schema, incremental_column: str) -> None:
    """Raise ValueError unless the incremental column exists and is integral."""
    field = next((f for f in schema.fields if f.name == incremental_column), None)
    if field is None:
        raise ValueError(
            f"incremental column {incremental_column!r} not found in source schema"
        )
    data_type = field.dataType
    type_name = data_type.typeName()
    if type_name not in ("byte", "short", "integer", "long", "date", "timestamp", "timestamp_ntz") and not (
        type_name == "decimal" and data_type.scale == 0
    ):
        raise ValueError(
            f"incremental column {incremental_column!r} has unsupported type "
            f"{type_name!r}: numeric (integral) incremental columns are required"
        )


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
    import pyspark.sql.functions as F
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    namespace, _ = validate_target_table(args.target_table)
    spark = SparkSession.builder.appName(f"ingest-{args.target_table}").getOrCreate()
    try:
        if args.source_id:
            managed_load(spark, args)
            return
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")
        read_query = build_read_query(
            args.source_table, args.incremental_column, args.watermark_value, args.jdbc_url
        )
        print(
            f"load started table={args.source_table} target={args.target_table} "
            f"process_id={args.process_id}"
        )
        partition_column = None if args.no_parallelism else args.partition_column or args.incremental_column
        if not args.no_parallelism and partition_column is None:
            dialect = dialects.by_jdbc_url(args.jdbc_url)
            partition_column = pick_partition_column(
                dialect,
                jdbc.key_columns(
                    spark._jvm, dialect, args.jdbc_url,
                    args.jdbc_user, args.jdbc_password, args.source_table,
                ),
            )
        reader = _jdbc_reader(spark, args, read_query)
        if partition_column is not None:
            lower_bound, upper_bound = _read_bounds(spark, args, read_query, partition_column)
            if lower_bound is not None and upper_bound is not None and lower_bound != upper_bound:
                reader = (
                    reader.option("partitionColumn", partition_column)
                    .option("lowerBound", _bound_literal(lower_bound))
                    .option("upperBound", _bound_literal(upper_bound))
                    .option("numPartitions", args.num_partitions)
                )
        source_frame = reader.load()
        if args.incremental_column is not None:
            validate_incremental_column(source_frame.schema, args.incremental_column)
        create_sql = build_create_table_sql(args.target_table, schema_columns(source_frame.schema))
        frame = (
            source_frame.withColumn(METADATA_DTTM, F.current_timestamp())
            .withColumn(METADATA_PID, F.lit(args.process_id).cast("long"))
            .cache()
        )
        if not spark.catalog.tableExists(args.target_table):
            spark.sql(create_sql)
        frame.writeTo(args.target_table).option("mergeSchema", "true").option(
            "check-ordering", "false"
        ).append()
        rows = frame.count()
        message = (
            f"load finished table={args.source_table} target={args.target_table} "
            f"process_id={args.process_id} rows_appended={rows}"
        )
        if args.incremental_column is not None:
            new_watermark = frame.agg(F.max(args.incremental_column)).collect()[0][0]
            message = f"{message} new_watermark={new_watermark}"
        print(message)
        frame.unpersist()
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



def managed_load(spark, args):
    """Execute a persisted batch with at-least-once append and atomic pg-meta finalization."""
    import pyspark.sql.functions as F
    import psycopg2

    from ingestion import meta

    dialect = dialects.by_jdbc_url(args.jdbc_url)
    connect = lambda: psycopg2.connect("")
    batch = meta.get_batch(dag_id=args.dag_id, run_id=args.run_id, source_id=args.source_id,
                           entity_fqn=args.source_table, db_connect=connect)
    if batch and (batch["process_id"] != args.process_id or batch["target_table"] != args.target_table
                  or batch["incremental_column"] != args.incremental_column):
        raise ValueError("Persisted batch identity or extraction contract changed")
    if batch and batch["status"] == "SUCCEEDED":
        return
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
    if batch is None:
        def calculate_lower(value):
            """Reject source resets before persisting an incremental window."""
            if value is not None and not args.force_full:
                if empty or positions.maximum(value, upper, kind) != upper:
                    raise ValueError("SOURCE_POSITION_REGRESSION: explicit reset/bootstrap required")
            return positions.lower_bound(value, kind, args.lookback_size, args.lookback_unit,
                                         specified=args.lookback_given)

        batch = meta.plan_batch(
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
    elif batch["incremental_column"] != args.incremental_column or batch["incremental_type"] != kind:
        raise ValueError("Extraction contract changed; migrate pg-meta and start a new DagRun")
    batch_id = batch["batch_id"]
    if empty_incremental_window(batch):
        meta.finalize_batch(batch_id=batch_id, row_count=0, observed_max=None,
                                db_connect=connect)
        return
    meta.start_batch(batch_id=batch_id, db_connect=connect)
    try:
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
        source = reader.load().cache()
        try:
            summary = source.agg(F.count("*").alias("n"), *(
                [F.max("__extract_position").alias("position")] if column else []
            )).first()
            rows = int(summary["n"])
            observed = positions.canonical(summary["position"], kind) if column else None
            clean = source.drop("__extract_position") if column else source
            if any(field.name in (METADATA_BATCH, METADATA_PID, METADATA_DTTM) for field in clean.schema.fields):
                raise ValueError("Source column collides with ingestion metadata")
            namespace, _ = validate_target_table(args.target_table)
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS raw.{namespace}")
            if not spark.catalog.tableExists(args.target_table):
                spark.sql(build_create_table_sql(args.target_table, schema_columns(clean.schema)))
            if rows:
                frame = clean.withColumn(METADATA_DTTM, F.current_timestamp()).withColumn(
                    METADATA_PID, F.lit(args.process_id).cast("long")).withColumn(
                    METADATA_BATCH, F.lit(batch_id).cast("long"))
                frame.writeTo(args.target_table).option("mergeSchema", "true").option("check-ordering", "false").append()
            watermark = batch["watermark_before"]
            if args.incremental_column and batch["reason"] != "manual_full":
                watermark = positions.maximum(watermark, observed, kind)
            meta.finalize_batch(batch_id=batch_id, row_count=rows, observed_max=observed,
                                    watermark_after=watermark, db_connect=connect)
        finally:
            source.unpersist()
    except Exception:
        try:
            meta.fail_batch(batch_id=batch_id, error="Extraction execution failed; see task logs", db_connect=connect)
        except Exception:
            logging.getLogger(__name__).warning("Unable to persist batch failure")
        raise


if __name__ == "__main__":
    main()
