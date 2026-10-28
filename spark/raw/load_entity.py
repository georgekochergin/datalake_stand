"""Append JDBC data to Iceberg with persisted, typed extraction batches.

Managed Airflow execution plans fixed source bounds in pg-meta, preserves
batch identity across retries and atomically finalizes its watermark.
Repeated appends remain valid at-least-once behavior. Pure helpers import
without Spark; managed execution uses the shared dlh_ingestion package.
"""

import argparse
import logging
import re
from decimal import Decimal

from dlh_ingestion.sources.rdbms import positions
from dlh_ingestion.sources.rdbms import registry as dialects

CATALOG = "raw"
METADATA_DTTM = "__load_dttm"
METADATA_PID = "__process_id"
METADATA_BATCH = "__batch_id"

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
    namespace, table = validate_target_table(target)
    rendered: list[str] = []
    for name, ddl_type in columns:
        if name in (METADATA_DTTM, METADATA_PID, METADATA_BATCH, "__extract_position"):
            raise ValueError(f"source column {name!r} collides with a metadata column")
        rendered.append(f"{quote_identifier(name)} {ddl_type}")
    rendered.append(f"{quote_identifier(METADATA_DTTM)} timestamp_ltz")
    rendered.append(f"{quote_identifier(METADATA_PID)} bigint")
    rendered.append(f"{quote_identifier(METADATA_BATCH)} bigint")
    joined = ", ".join(rendered)
    return (
        f"CREATE TABLE {target} ({joined}) USING iceberg "
        f"PARTITIONED BY (days({quote_identifier(METADATA_DTTM)})) "
        f"LOCATION 's3://datalake/raw/{namespace}/{table}' "
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
    if type_name not in ("byte", "short", "integer", "long", "date", "timestamp", "timestamp_ntz") and not (
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
    connection = spark._jvm.java.sql.DriverManager.getConnection(
        args.jdbc_url, args.jdbc_user, args.jdbc_password
    )
    try:
        statement = connection.createStatement()
        try:
            column = dialect.quote_identifier(args.incremental_column)
            table = render_source_table(dialect, args.source_table, allow_expression=True)
            result = statement.executeQuery(f"SELECT {column} FROM {table} WHERE 1=0")
            try:
                metadata = result.getMetaData()
                kind = dialect.position_kind(metadata.getColumnTypeName(1), metadata.getPrecision(1),
                                             metadata.getScale(1))
            finally:
                result.close()
            expression = "NULL" if inspect_only else dialect.position_expression(f"MAX({column})", kind)
            result = statement.executeQuery(
                f"SELECT {expression}, COUNT(*), COUNT({column}) FROM {table}"
            )
            try:
                result.next()
                upper = positions.canonical(result.getString(1), kind)
                count = int(result.getString(2))
                nonnull = int(result.getString(3))
            finally:
                result.close()
            return kind, upper, count == 0, count != nonnull
        finally:
            statement.close()
    finally:
        connection.close()


def oracle_utc_projection(spark, args):
    """Rewrite Oracle timezone columns, unmappable by Spark JDBC, into UTC instants.

    Spark cannot resolve Oracle TIMESTAMP ... WITH [LOCAL] TIME ZONE columns at
    all (JDBC type -101/-102), regardless of the incremental contract. Such
    columns are projected as SYS_EXTRACT_UTC(...): session-timezone independent
    normalization to UTC required by the position contract. Returns None when
    the source has no timezone columns, leaving extract_source.* untouched.
    """
    if not args.jdbc_url.startswith("jdbc:oracle:"):
        return None
    dialect = dialects.by_jdbc_url(args.jdbc_url)
    connection = spark._jvm.java.sql.DriverManager.getConnection(
        args.jdbc_url, args.jdbc_user, args.jdbc_password
    )
    try:
        statement = connection.createStatement()
        try:
            table = render_source_table(dialect, args.source_table, allow_expression=True)
            result = statement.executeQuery(f"SELECT * FROM {table} WHERE 1=0")
            try:
                metadata = result.getMetaData()
                projected = []
                for index in range(1, metadata.getColumnCount() + 1):
                    name = str(metadata.getColumnLabel(index))
                    type_name = " ".join(str(metadata.getColumnTypeName(index)).upper().split())
                    reference = dialect.quote_identifier(name)
                    if "TIME ZONE" in type_name:
                        projected.append(f"SYS_EXTRACT_UTC(extract_source.{reference}) AS {reference}")
                    else:
                        projected.append(f"extract_source.{reference}")
                return projected if any("SYS_EXTRACT_UTC" in item for item in projected) else None
            finally:
                result.close()
        finally:
            statement.close()
    finally:
        connection.close()


def managed_load(spark, args):
    """Execute a persisted batch with at-least-once append and atomic pg-meta finalization."""
    import pyspark.sql.functions as F
    import psycopg2

    from dlh_ingestion import meta

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
        select = "extract_source.*"
        projection = oracle_utc_projection(spark, args)
        if projection is not None:
            select = ", ".join(projection)
        if column:
            select += f', {dialect.position_expression(column, kind)} AS "__extract_position"'
        query = (
            f"(SELECT {select} FROM (SELECT * FROM "
            f"{render_source_table(dialect, args.source_table, allow_expression=True)}) extract_source"
            + (" WHERE " + " AND ".join(filters) if filters else "")
            + ") extract_batch"
        )
        reader = _jdbc_reader(spark, args, query)
        if not args.no_parallelism and (not kind or not kind.startswith("timestamp")):
            partition_column = args.incremental_column or pick_partition_column(detect_key_columns(
                spark, args.jdbc_url, args.jdbc_user, args.jdbc_password, args.source_table))
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
                spark.sql(build_create_table_sql(args.target_table, _schema_columns(clean.schema)))
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
