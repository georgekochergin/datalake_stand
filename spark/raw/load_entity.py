"""Append-only entity loader: JDBC source -> raw-layer Iceberg with metadata columns.

Loads one entity per invocation. Reads the source over JDBC (full snapshot
by default, or an incremental slice above a numeric watermark), tags every
row with __load_dttm/__process_id and appends into the raw catalog. The
first load creates the table partitioned by days(__load_dttm) with the
write.spark.accept-any-schema property; every later load appends with the
mergeSchema option. Incremental mode requires an integral incremental
column (the watermark is a plain integer); presence and type of the column
are validated on every load. The module is self-contained: it runs inside
Spark containers where only the spark/ tree is mounted, so it must not
import from the ingestion package; PySpark is imported lazily so pure
helpers stay testable without a Spark installation.
"""

import argparse
import logging
import re
from decimal import Decimal

CATALOG = "raw"
METADATA_DTTM = "__load_dttm"
METADATA_PID = "__process_id"

IDENTIFIER_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
COLUMN_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SOURCE_PART_PATTERN = re.compile(r'"((?:[A-Za-z0-9_.]|"")+)"|([A-Za-z0-9_]+)')
WATERMARK_PATTERN = re.compile(r"^[0-9]+$")

PARTITION_TYPES = frozenset(
    {
        "SMALLINT",
        "TINYINT",
        "INT",
        "INTEGER",
        "BIGINT",
        "NUMBER",
        "DECIMAL",
        "NUMERIC",
        "DATE",
        "DATETIME",
        "DATETIME2",
        "SMALLDATETIME",
        "TIMESTAMP",
    }
)

DRIVERS = {
    "jdbc:sqlserver:": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "jdbc:oracle:": "oracle.jdbc.OracleDriver",
}


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


def resolve_driver(jdbc_url: str) -> str:
    """Return the JDBC driver class for a supported jdbc: URL prefix."""
    for prefix, driver in DRIVERS.items():
        if jdbc_url.startswith(prefix):
            return driver
    raise ValueError(f"unsupported jdbc url (expected one of {sorted(DRIVERS)}): {jdbc_url!r}")


def validate_target_table(target: str) -> tuple[str, str]:
    """Validate raw.<namespace>.<table> and return (namespace, table)."""
    parts = target.split(".")
    if len(parts) != 3 or parts[0] != CATALOG:
        raise ValueError(f"target table must be '{CATALOG}.<namespace>.<table>': {target!r}")
    namespace, table = parts[1], parts[2]
    if not IDENTIFIER_PATTERN.match(namespace) or not IDENTIFIER_PATTERN.match(table):
        raise ValueError(
            f"target table parts must match {IDENTIFIER_PATTERN.pattern}: {target!r}"
        )
    return namespace, table


def sql_reference(jdbc_url: str, name: str) -> str:
    """Render a column reference for source SQL.

    '_'-prefixed columns are stored lowercase (quoted identifiers) in Oracle,
    so they must be referenced quoted there; unquoted reference would fold to
    uppercase and miss the column. MSSQL is case-insensitive and needs no
    quoting; an unknown dialect (empty url) is rendered unquoted.
    """
    if name.startswith("_") and jdbc_url.startswith("jdbc:oracle:"):
        return '"%s"' % name.replace('"', '""')
    return name


def build_read_query(
    source_table: str,
    incremental_column: str | None,
    watermark_value: str | None,
    jdbc_url: str = "",
) -> str:
    """Build the JDBC dbtable expression: bare table, or an incremental subquery."""
    parse_source_table(source_table)
    if incremental_column is None:
        return source_table
    if not COLUMN_NAME_PATTERN.match(incremental_column):
        raise ValueError(f"invalid incremental column: {incremental_column!r}")
    if watermark_value is None:
        return source_table
    if not WATERMARK_PATTERN.match(watermark_value):
        raise ValueError(
            f"watermark must be a plain integer (got {watermark_value!r}); "
            "quoted/ISO watermarks are not supported"
        )
    column = sql_reference(jdbc_url, incremental_column)
    return f"(SELECT * FROM {source_table} WHERE {column} > {watermark_value}) q"


def _normalize_type(jdbc_type: str) -> str:
    """Normalize a JDBC TYPE_NAME (e.g. 'int identity') to its base token."""
    stripped = jdbc_type.strip()
    return stripped.upper().split()[0] if stripped else ""


def pick_partition_column(candidates: list[tuple[str, str]]) -> str | None:
    """Return the first numeric/date/timestamp candidate column, else None."""
    for name, jdbc_type in candidates:
        if _normalize_type(jdbc_type) in PARTITION_TYPES:
            return name
    return None


def quote_identifier(name: str) -> str:
    """Quote a column name for Spark SQL DDL (backticks; embedded backticks doubled)."""
    return "`%s`" % name.replace("`", "``")


def build_create_table_sql(target: str, columns: list[tuple[str, str]]) -> str:
    """Build the first-load Iceberg DDL from (column name, DDL type) pairs."""
    validate_target_table(target)
    rendered: list[str] = []
    for name, ddl_type in columns:
        if name in (METADATA_DTTM, METADATA_PID):
            raise ValueError(f"source column {name!r} collides with a metadata column")
        rendered.append(f"{quote_identifier(name)} {ddl_type}")
    rendered.append(f"{quote_identifier(METADATA_DTTM)} timestamp_ltz")
    rendered.append(f"{quote_identifier(METADATA_PID)} bigint")
    joined = ", ".join(rendered)
    return (
        f"CREATE TABLE {target} ({joined}) USING iceberg "
        f"PARTITIONED BY (days({quote_identifier(METADATA_DTTM)})) "
        f"TBLPROPERTIES ('write.spark.accept-any-schema'='true')"
    )


def validate_incremental_column(schema, incremental_column: str) -> None:
    """Raise ValueError unless the incremental column exists and is integral."""
    field = next((f for f in schema.fields if f.name == incremental_column), None)
    if field is None:
        raise ValueError(
            f"incremental column {incremental_column!r} not found in source schema"
        )
    data_type = field.dataType
    type_name = data_type.typeName()
    if type_name not in ("byte", "short", "integer", "long") and not (
        type_name == "decimal" and data_type.scale == 0
    ):
        raise ValueError(
            f"incremental column {incremental_column!r} has unsupported type "
            f"{type_name!r}: numeric (integral) incremental columns are required"
        )


def _schema_columns(schema) -> list[tuple[str, str]]:
    """Map a PySpark schema to (column name, DDL type) pairs."""
    from pyspark.sql.types import (
        BinaryType,
        BooleanType,
        ByteType,
        DateType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
        StringType,
        TimestampNTZType,
        TimestampType,
    )

    def ddl_type(data_type):
        if isinstance(data_type, StringType):
            return "string"
        if isinstance(data_type, BooleanType):
            return "boolean"
        if isinstance(data_type, ByteType):
            return "tinyint"
        if isinstance(data_type, ShortType):
            return "smallint"
        if isinstance(data_type, IntegerType):
            return "int"
        if isinstance(data_type, LongType):
            return "bigint"
        if isinstance(data_type, FloatType):
            return "float"
        if isinstance(data_type, DoubleType):
            return "double"
        if isinstance(data_type, DateType):
            return "date"
        if isinstance(data_type, TimestampType):
            return "timestamp_ltz"
        if isinstance(data_type, TimestampNTZType):
            return "timestamp_ntz"
        if isinstance(data_type, DecimalType):
            return f"decimal({data_type.precision},{data_type.scale})"
        if isinstance(data_type, BinaryType):
            return "binary"
        raise ValueError(f"unsupported source column type for raw DDL: {data_type!r}")

    return [(field.name, ddl_type(field.dataType)) for field in schema.fields]


def parse_source_table(source_table: str) -> list[tuple[str, bool]]:
    """Parse schema.table, retaining quoted case and embedded dots for JDBC metadata."""
    parts = []
    offset = 0
    while offset < len(source_table):
        match = SOURCE_PART_PATTERN.match(source_table, offset)
        if match is None:
            raise ValueError(f"invalid source table reference: {source_table!r}")
        quoted, unquoted = match.groups()
        parts.append((quoted.replace('""', '"') if quoted is not None else unquoted, quoted is not None))
        offset = match.end()
        if offset == len(source_table):
            break
        if source_table[offset] != "." or offset + 1 == len(source_table):
            raise ValueError(f"invalid source table reference: {source_table!r}")
        offset += 1
    if len(parts) != 2:
        raise ValueError(f"source table must contain schema.table: {source_table!r}")
    return parts


def detect_key_columns(spark, jdbc_url, user, password, source_table) -> list[tuple[str, str]]:
    """Best-effort ordered (column, JDBC type) candidates from PK and unique indexes."""
    try:
        parts = parse_source_table(source_table)
        connection = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        try:
            metadata = connection.getMetaData()
            original = tuple(name for name, _ in parts)
            folded = tuple(name if quoted else name.upper() for name, quoted in parts)
            for candidate_schema, candidate_table in dict.fromkeys((original, folded)):
                ordered = _ordered_key_columns(metadata, candidate_schema, candidate_table)
                if ordered:
                    types = _column_types(metadata, candidate_schema, candidate_table)
                    return [(name, types.get(name.lower(), "")) for name in ordered]
        finally:
            connection.close()
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "JDBC metadata unavailable for %s; falling back to an unpartitioned read: %s",
            source_table, exc,
        )
        return []
    logging.getLogger(__name__).warning(
        "No JDBC key metadata found for %s; falling back to an unpartitioned read",
        source_table,
    )
    return []


def _ordered_key_columns(metadata, schema, table) -> list[str]:
    """Primary-key columns in KEY_SEQ order, then unique-index columns."""
    ordered: list[str] = []
    primary_keys = metadata.getPrimaryKeys(None, schema, table)
    try:
        pairs = []
        while primary_keys.next():
            pairs.append((primary_keys.getInt("KEY_SEQ"), primary_keys.getString("COLUMN_NAME")))
        for _, name in sorted(pairs):
            if name not in ordered:
                ordered.append(name)
    finally:
        primary_keys.close()
    indexes = metadata.getIndexInfo(None, schema, table, True, True)
    try:
        while indexes.next():
            if not indexes.getBoolean("NON_UNIQUE"):
                name = indexes.getString("COLUMN_NAME")
                if name is not None and name not in ordered:
                    ordered.append(name)
    finally:
        indexes.close()
    return ordered


def _column_types(metadata, schema, table) -> dict[str, str]:
    """Map lowercased column names to JDBC TYPE_NAME values."""
    types: dict[str, str] = {}
    columns = metadata.getColumns(None, schema, table, None)
    try:
        while columns.next():
            name = columns.getString("COLUMN_NAME")
            type_name = columns.getString("TYPE_NAME")
            if name is not None and type_name is not None:
                types[name.lower()] = type_name
    finally:
        columns.close()
    return types


def _jdbc_reader(spark, args, read_query):
    """Build the JDBC reader with user/password/driver options."""
    return (
        spark.read.format("jdbc")
        .option("url", args.jdbc_url)
        .option("dbtable", read_query)
        .option("user", args.jdbc_user)
        .option("password", args.jdbc_password)
        .option("driver", resolve_driver(args.jdbc_url))
    )


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
    column_ref = sql_reference(args.jdbc_url, column)
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
            partition_column = pick_partition_column(
                detect_key_columns(
                    spark, args.jdbc_url, args.jdbc_user, args.jdbc_password, args.source_table
                )
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
        create_sql = build_create_table_sql(args.target_table, _schema_columns(source_frame.schema))
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


if __name__ == "__main__":
    main()
