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

import argparse
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING

from ingestion.logging import log_sql, log_step

from ingestion.sources.rdbms import jdbc
from ingestion.sources.rdbms import jdbc_partitioning
from ingestion.sources.rdbms import positions
from ingestion.sources.rdbms import registry as dialects
from ingestion.sources.rdbms.contracts import parse_source_table
from ingestion.sources.rdbms.dialects.base import RdbmsDialect

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, DataFrameReader, SparkSession

PartitionBound = int | Decimal | date | None
PartitionBounds = tuple[PartitionBound, PartitionBound]

LOGGER = logging.getLogger(__name__)


class JdbcReadScope(Enum):
    """Describe the query context in JDBC logs without changing the read.

    BATCH_WINDOW marks an extract with caller-supplied filters or additional
    select expressions. SOURCE_TABLE marks an extract without either.
    """

    BATCH_WINDOW = "batch_window"
    SOURCE_TABLE = "source_table"


class JdbcOperation(Enum):
    """Stable log codes and readable names for source bounds queries."""

    INCREMENTAL_MAX = ("source_position", "Get the incremental maximum")
    PARTITION_BOUNDS = ("partition_bounds", "Get JDBC partition bounds")
    COMBINED_BOUNDS = ("combined_bounds", "Get the incremental maximum and JDBC partition bounds")

    def __init__(self, code: str, text: str) -> None:
        self.code = code
        self.text = text


class JdbcReason(Enum):
    """Stable log codes and explanations for skipping JDBC work."""

    PERSISTED_BATCH = ("persisted_batch", "The batch already has a saved window")
    EMPTY_WINDOW = ("empty_window", "No source rows match the incremental window")
    DISABLED = ("disabled", "JDBC partitioning is disabled")
    SINGLE_PARTITION = ("single_partition", "Only one JDBC partition was requested")
    NO_PARTITION_COLUMN = ("no_partition_column", "No suitable JDBC partition column was found")
    NULL_BOUNDS = ("null_bounds", "At least one JDBC partition bound is missing")
    EQUAL_BOUNDS = ("equal_bounds", "The JDBC partition bounds are equal")
    OUT_OF_LONG_RANGE = ("out_of_long_range", "JDBC partition bounds exceed the signed 64-bit range Spark supports")

    def __init__(self, code: str, text: str) -> None:
        self.code = code
        self.text = text


@dataclass(frozen=True)
class ReadSource:
    """Catalog facts and partition choice shared by planning and reading."""

    dialect: RdbmsDialect
    table: str
    keys: jdbc.KeyMetadata
    partition: jdbc_partitioning.PartitionChoice | None


def inspect_source(spark: "SparkSession", args: argparse.Namespace, *, detailed: bool = True) -> ReadSource:
    """Inspect the source before choosing the aggregates needed for this attempt."""
    dialect = dialects.by_jdbc_url(args.jdbc_url)
    table = render_source_table(dialect, args.source_table)
    keys = jdbc.key_metadata(spark._jvm, dialect, args.jdbc_url, args.jdbc_user,
                            args.jdbc_password, args.source_table, detailed=detailed)
    if not keys.columns:
        raise RuntimeError(f"JDBC catalog metadata unavailable for {args.source_table}: cannot build a safe read")
    partition = None if args.no_jdbc_partitioning or args.num_partitions == 1 else jdbc_partitioning.plan(
        dialect, keys, args.source_table
    )
    return ReadSource(dialect, table, keys, partition)


def render_source_table(dialect: RdbmsDialect, source_table: str) -> str:
    """Render a schema.table reference quoted for the source dialect.

    Every part of the reference is quoted per dialect (Oracle double quotes,
    MSSQL brackets), exactly as the object was created by sydafa. References
    that are not schema.table are rejected.
    """
    return dialect.render_table(parse_source_table(source_table))


def jdbc_reader(spark: "SparkSession", args: argparse.Namespace, read_query: str) -> "DataFrameReader":
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


def bound_literal(value: object) -> str:
    """Render a JDBC partition bound; integral Decimals collapse to plain integers.

    Oracle NUMBER bounds arrive as Decimal('1.0000000000'), which the JDBC
    partitioner cannot parse when the partition column itself maps to an
    integral type.
    """
    if isinstance(value, Decimal) and value == value.to_integral_value():
        return str(int(value))
    return str(value)


def long_representable(*values: int | Decimal) -> bool:
    """Check numeric partition bounds fit the signed 64-bit Spark bound parsing.

    Spark's JDBC partitioning parses lowerBound/upperBound of numeric
    columns as a Long regardless of the column precision: values outside
    the range fail the read, so such bounds disable partitioning.
    """
    return all(-(2 ** 63) <= int(value) <= 2 ** 63 - 1 for value in values)


def _integral_bounds_out_of_long_range(kind: str, low: int | Decimal, high: int | Decimal) -> bool:
    """Check whether an integral partition exceeds Spark's signed 64-bit bound range."""
    return positions.kind_info(kind)[0] == "integral" and not long_representable(low, high)


def probe_bounds(
    spark: "SparkSession", args: argparse.Namespace, source: ReadSource, filters: Sequence[str],
    *, scope: JdbcReadScope, incremental: tuple[str, str] | None = None,
) -> tuple[str | None, PartitionBounds]:
    """Fetch the requested bounds with one JDBC statement, or skip the probe."""
    expressions = []
    dialect, partition = source.dialect, source.partition
    if incremental is not None:
        name, kind = incremental
        ref = dialect.quote_identifier(name)
        expressions.append(f"{dialect.position_expression(f'MAX({ref})', kind)} AS max_increment_col")
    if partition is not None:
        ref = dialect.quote_identifier(partition.column)
        expressions.extend((f"MIN({ref}) AS lower_bound", f"MAX({ref}) AS upper_bound"))
    if not expressions:
        return None, (None, None)
    sql = "SELECT " + ",\n  ".join(expressions) + f"\nFROM {source.table}"
    if filters:
        sql += "\nWHERE " + "\n  AND ".join(filters)
    if incremental is None:
        operation = JdbcOperation.PARTITION_BOUNDS
    elif partition is None:
        operation = JdbcOperation.INCREMENTAL_MAX
    else:
        operation = JdbcOperation.COMBINED_BOUNDS
    log_step(LOGGER, operation.text)
    log_sql(LOGGER, f"JDBC SQL scope={scope.value} operation={operation.code}", sql)
    with jdbc.connection(spark._jvm, args.jdbc_url, args.jdbc_user, args.jdbc_password) as connection:
        row = jdbc.first_row(connection, sql)
    upper = positions.canonical(row[0], kind) if incremental is not None else None
    low = high = None
    result_fields = []
    if incremental is not None:
        result_fields.append(f"max_increment_col={upper}")
    if partition is not None:
        convert = date.fromisoformat if partition.kind == "date" else Decimal
        low, high = (convert(value) if value is not None else None for value in row[-2:])
        result_fields.extend((f"lower_bound={low}", f"upper_bound={high}"))
    LOGGER.info("JDBC result operation=%s %s", operation.code, " ".join(result_fields))
    return upper, (low, high)


def build_extract_query(
    source_table: str, projection: Sequence[str] | None, filters: Sequence[str] = (), extra_select: str = "",
) -> str:
    """Assemble the wrapping JDBC read query: projection, extra columns, filters."""
    items = ["extract_source.*"] if projection is None else list(projection)
    if extra_select:
        items.append(extra_select)
    select = ",\n    ".join(items)
    where = "\n    WHERE " + "\n      AND ".join(filters) if filters else ""
    return (
        f"(\n"
        f"  SELECT {select}\n"
        f"  FROM (\n"
        f"    SELECT *\n"
        f"    FROM {source_table}\n"
        f"  ) extract_source{where}\n"
        f") extract_batch"
    )


def apply_partition_options(
    args: argparse.Namespace, reader: "DataFrameReader", partition: jdbc_partitioning.PartitionChoice | None,
    bounds: PartitionBounds,
) -> "DataFrameReader":
    """Enable parallel ranges when the observed bounds fit Spark's options."""
    if partition is None:
        return reader
    low, high = bounds
    reason = None
    if low is None or high is None:
        reason = JdbcReason.NULL_BOUNDS
    elif low == high:
        reason = JdbcReason.EQUAL_BOUNDS
    elif _integral_bounds_out_of_long_range(partition.kind, low, high):
        reason = JdbcReason.OUT_OF_LONG_RANGE
    if reason is not None:
        level = logging.WARNING if reason is JdbcReason.OUT_OF_LONG_RANGE else logging.INFO
        LOGGER.log(level, "Reading with one JDBC partition: %s. column=%s bounds=[%s..%s] reason=%s",
                   reason.text, partition.column, low, high, reason.code)
        return reader
    return (
        reader.option("partitionColumn", partition.column)
        .option("lowerBound", bound_literal(low))
        .option("upperBound", bound_literal(high))
        .option("numPartitions", args.num_partitions)
    )


def read_extract_frame(
    spark: "SparkSession", args: argparse.Namespace, filters: Sequence[str] = (), extra_select: str = "",
    *, detailed_metadata: bool = True, source: ReadSource | None = None,
    bounds: PartitionBounds | None = None,
) -> "DataFrame":
    """Read the source over JDBC into a cached frame with explicit predicates.

    Catalog metadata (columns, keys, indexes) is always required: it feeds
    both the compatibility projection and the JDBC partitioning choice, so
    no metadata SQL is executed against the source. Whole-table reads
    (full-slice and snapshot streams) pass no filters; the incremental flow
    passes the frozen-window predicates and the exact typed extraction
    position as extra_select.
    """
    scope = JdbcReadScope.BATCH_WINDOW if filters or extra_select else JdbcReadScope.SOURCE_TABLE
    source = source or inspect_source(spark, args, detailed=detailed_metadata)
    projection, utc_columns = jdbc.compatibility_projection(source.dialect, source.keys)
    partition = source.partition
    if bounds is None and partition is not None:
        _, bounds = probe_bounds(spark, args, source, filters, scope=scope)
    bounds = bounds or (None, None)
    low, high = bounds
    query = build_extract_query(source.table, projection, filters, extra_select)
    reader = jdbc_reader(spark, args, query)
    reader = apply_partition_options(args, reader, partition, bounds)
    log_step(LOGGER, "read JDBC source")
    log_sql(LOGGER, f"JDBC dbtable scope={scope.value}", query)
    if partition is None:
        if args.no_jdbc_partitioning:
            reason = JdbcReason.DISABLED
        elif args.num_partitions == 1:
            reason = JdbcReason.SINGLE_PARTITION
        else:
            reason = JdbcReason.NO_PARTITION_COLUMN
        LOGGER.info("JDBC partition bounds skipped: %s. reason=%s", reason.text, reason.code)
    frame = reader.load()
    if utc_columns:
        from pyspark.sql import functions as F

        frame = frame.withColumns({
            name: F.timestamp_micros(F.col("`" + name.replace("`", "``") + "`").cast("long"))
            for name in utc_columns
        })
    frame = frame.cache()
    if partition is not None:
        LOGGER.info(
            "jdbc partitioning: column=%s step=%s window=[%s..%s] partitions=%s of %s requested",
            partition.column, partition.step, low, high,
            frame.rdd.getNumPartitions(), args.num_partitions,
        )
    return frame
