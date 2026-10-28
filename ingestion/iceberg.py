"""Target-side raw-layer Iceberg DDL: technical columns, quoting, and first-load DDL."""

import re

CATALOG = "raw"
METADATA_DTTM = "__load_dttm"
METADATA_PID = "__process_id"
METADATA_BATCH = "__batch_id"

IDENTIFIER_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


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


def schema_columns(schema) -> list[tuple[str, str]]:
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
