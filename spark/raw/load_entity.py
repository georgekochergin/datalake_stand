"""Append-only entity loader: JDBC source -> raw-layer Iceberg with metadata columns.

Loads one entity per invocation. Reads the source over JDBC (full snapshot
by default, or an incremental slice above a numeric watermark), tags every
row with __load_dttm/__process_id and appends into the raw catalog. The
first load creates the table partitioned by days(__load_dttm) with the
write.spark.accept-any-schema property; every later load appends with the
mergeSchema option. The module is self-contained: it runs inside Spark
containers where only the spark/ tree is mounted, so it must not import
from the ingestion package; PySpark is imported lazily so pure helpers
stay testable without a Spark installation.
"""

import argparse
import re

CATALOG = "raw"
METADATA_DTTM = "__load_dttm"
METADATA_PID = "__process_id"

IDENTIFIER_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
COLUMN_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SOURCE_TABLE_PATTERN = re.compile(r'^[A-Za-z0-9_".]+$')
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


def build_read_query(source_table: str, incremental_column: str | None, watermark_value: str | None) -> str:
    """Build the JDBC dbtable expression: bare table, or an incremental subquery."""
    if not SOURCE_TABLE_PATTERN.match(source_table):
        raise ValueError(f"invalid source table reference: {source_table!r}")
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
    return f"(SELECT * FROM {source_table} WHERE {incremental_column} > {watermark_value}) AS q"


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


def build_create_table_sql(target: str, columns: list[tuple[str, str]]) -> str:
    """Build the first-load Iceberg DDL from (column name, DDL type) pairs."""
    validate_target_table(target)
    rendered: list[str] = []
    for name, ddl_type in columns:
        if name in (METADATA_DTTM, METADATA_PID):
            raise ValueError(f"source column {name!r} collides with a metadata column")
        quoted = '"%s"' % name.replace('"', '""')
        rendered.append(f"{quoted} {ddl_type}")
    rendered.append(f'"{METADATA_DTTM}" timestamptz')
    rendered.append(f'"{METADATA_PID}" bigint')
    joined = ", ".join(rendered)
    return (
        f"CREATE TABLE {target} ({joined}) USING iceberg "
        f"PARTITIONED BY (days({METADATA_DTTM})) "
        f"TBLPROPERTIES ('write.spark.accept-any-schema'='true')"
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
            return "timestamptz"
        if isinstance(data_type, TimestampNTZType):
            return "timestamp"
        if isinstance(data_type, DecimalType):
            return f"decimal({data_type.precision},{data_type.scale})"
        if isinstance(data_type, BinaryType):
            return "binary"
        raise ValueError(f"unsupported source column type for raw DDL: {data_type!r}")

    return [(field.name, ddl_type(field.dataType)) for field in schema.fields]


def detect_key_columns(spark, jdbc_url, user, password, source_table) -> list[tuple[str, str]]:
    """Best-effort ordered (column, JDBC type) candidates from PK and unique indexes."""
    try:
        connection = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        try:
            metadata = connection.getMetaData()
            parts = source_table.split(".")
            if len(parts) != 2:
                return []
            schema, table = parts
            for candidate_schema, candidate_table in (
                (schema, table),
                (schema.upper(), table.upper()),
            ):
                ordered = _ordered_key_columns(metadata, candidate_schema, candidate_table)
                if ordered:
                    types = _column_types(metadata, candidate_schema, candidate_table)
                    return [(name, types.get(name.lower(), "")) for name in ordered]
        finally:
            connection.close()
    except Exception:
        return []
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


def _read_bounds(spark, args, read_query, column):
    """Read MIN/MAX of the partition column over the read query (one-row JDBC read)."""
    query = (
        f"(SELECT MIN({column}) AS lower_bound, MAX({column}) AS upper_bound "
        f"FROM {read_query}) AS bounds"
    )
    frame = _jdbc_reader(spark, args, query).load()
    return frame.collect()[0]


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
            args.source_table, args.incremental_column, args.watermark_value
        )
        print(
            f"load started table={args.source_table} target={args.target_table} "
            f"process_id={args.process_id}"
        )
        partition_column = args.partition_column or args.incremental_column
        if partition_column is None:
            partition_column = pick_partition_column(
                detect_key_columns(
                    spark, args.jdbc_url, args.jdbc_user, args.jdbc_password, args.source_table
                )
            )
        reader = _jdbc_reader(spark, args, read_query)
        if partition_column is not None:
            bounds = _read_bounds(spark, args, read_query, partition_column)
            if bounds["lower_bound"] is None:
                print(
                    f"load finished table={args.source_table} target={args.target_table} "
                    f"process_id={args.process_id} rows_appended=0"
                )
                return
            if bounds["lower_bound"] != bounds["upper_bound"]:
                reader = (
                    reader.option("partitionColumn", partition_column)
                    .option("lowerBound", str(bounds["lower_bound"]))
                    .option("upperBound", str(bounds["upper_bound"]))
                    .option("numPartitions", args.num_partitions)
                )
        source_frame = reader.load()
        create_sql = build_create_table_sql(args.target_table, _schema_columns(source_frame.schema))
        frame = (
            source_frame.withColumn(METADATA_DTTM, F.current_timestamp())
            .withColumn(METADATA_PID, F.lit(args.process_id).cast("long"))
            .cache()
        )
        if not spark.catalog.tableExists(args.target_table):
            spark.sql(create_sql)
        frame.writeTo(args.target_table).option("mergeSchema", "true").append()
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
