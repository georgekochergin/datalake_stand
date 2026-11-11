"""Shared JDBC frame reading for the raw-layer loader applications.

All three stream applications — the incremental window load, the full-slice
load and the actual-snapshot load — read the source through this module: it
renders the dialect-quoted table reference, applies compatibility
projections, picks the JDBC partitioning column and returns a cached frame.
Callers pass explicit projections and predicates: the incremental flow builds
the frozen-window filters and the position column in its own window module,
whole-table reads pass none. Pure helpers import without Spark; the args
namespace of each application carries the JDBC credentials.
"""

import logging
from decimal import Decimal

from ingestion.sources.rdbms import jdbc
from ingestion.sources.rdbms import jdbc_partitioning
from ingestion.sources.rdbms import positions
from ingestion.sources.rdbms import registry as dialects
from ingestion.sources.rdbms.contracts import parse_source_table

LOGGER = logging.getLogger(__name__)


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


def jdbc_reader(spark, args, read_query):
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


def bound_literal(value) -> str:
    """Render a JDBC partition bound; integral Decimals collapse to plain integers.

    Oracle NUMBER bounds arrive as Decimal('1.0000000000'), which the JDBC
    partitioner cannot parse when the partition column itself maps to an
    integral type.
    """
    if isinstance(value, Decimal) and value == value.to_integral_value():
        return str(int(value))
    return str(value)


def long_representable(*values) -> bool:
    """Check numeric partition bounds fit the signed 64-bit Spark bound parsing.

    Spark's JDBC partitioning parses lowerBound/upperBound of numeric
    columns as a Long regardless of the column precision: values outside
    the range fail the read, so such bounds disable partitioning.
    """
    return all(-(2 ** 63) <= int(value) <= 2 ** 63 - 1 for value in values)


def read_bounds(spark, args, read_query, column) -> tuple:
    """Read MIN/MAX of the partition column over the read query (one-row JDBC read)."""
    column_ref = dialects.by_jdbc_url(args.jdbc_url).quote_identifier(column)
    query = (
        f"(SELECT MIN({column_ref}) AS lower_bound, MAX({column_ref}) AS upper_bound "
        f"FROM {read_query}) bounds"
    )
    frame = jdbc_reader(spark, args, query).load()
    row = frame.collect()[0]
    return row[0], row[1]


def build_extract_query(spark, args, dialect, source_table, filters=(), extra_select=""):
    """Assemble the wrapping JDBC read query: projection, extra columns, filters."""
    select = "extract_source.*"
    projection = jdbc.compatibility_projection(
        spark._jvm, dialect, args.jdbc_url, args.jdbc_user, args.jdbc_password, source_table
    )
    if projection is not None:
        select = ", ".join(projection)
    if extra_select:
        select += extra_select
    return (
        f"(SELECT {select} FROM (SELECT * FROM "
        f"{source_table}) extract_source"
        + (" WHERE " + " AND ".join(filters) if filters else "")
        + ") extract_batch"
    )


def apply_partition_options(spark, args, reader, query, partition):
    """Probe MIN/MAX of the partition column and enable parallel ranges.

    Returns (reader, low, high): the reader carries the partition options
    when the window allows parallelism; values beyond the signed 64-bit
    range disable partitioning with a warning (Spark parses numeric bounds
    as a Long).
    """
    if partition is None:
        return reader, None, None
    reference = jdbc_partitioning.partition_name(partition)
    low, high = read_bounds(spark, args, query, reference)
    parallel = low is not None and high is not None and low != high
    if parallel and positions.kind_info(partition.kind)[0] == "integral" and not long_representable(low, high):
        LOGGER.warning(
            "jdbc partitioning: bounds of %s [%s..%s] exceed the signed 64-bit"
            " range Spark parses for partition bounds; single-threaded read",
            partition.column, low, high,
        )
        parallel = False
    if not parallel:
        return reader, low, high
    reader = reader.option("partitionColumn", reference).option(
        "lowerBound", bound_literal(low)).option("upperBound", bound_literal(high)).option(
        "numPartitions", args.num_partitions)
    return reader, low, high


def read_extract_frame(spark, args, filters=(), extra_select=""):
    """Read the source over JDBC into a cached frame with explicit predicates.

    Whole-table reads (full-slice and snapshot streams) pass no filters; the
    incremental flow passes the frozen-window predicates and the exact typed
    extraction position as extra_select.
    """
    dialect = dialects.by_jdbc_url(args.jdbc_url)
    source_table = render_source_table(dialect, args.source_table, allow_expression=True)
    partition = partition_select = None
    if not args.no_jdbc_partitioning:
        keys = jdbc.key_metadata(
            spark._jvm, dialect, args.jdbc_url, args.jdbc_user, args.jdbc_password, args.source_table
        )
        partition, partition_select = jdbc_partitioning.plan(dialect, keys, args.source_table)
    if partition_select:
        extra_select = f"{extra_select}{partition_select}"
    query = build_extract_query(spark, args, dialect, source_table, filters, extra_select)
    reader = jdbc_reader(spark, args, query)
    reader, low, high = apply_partition_options(spark, args, reader, query, partition)
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
