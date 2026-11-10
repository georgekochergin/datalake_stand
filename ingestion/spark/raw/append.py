"""Raw-layer table writer: schema guard, first-load DDL, and metadata append.

Receives a cleaned DataFrame and its row count; knows nothing about source
types or load modes.
"""

from ingestion.spark.raw.iceberg import (
    CATALOG,
    METADATA_DTTM,
    METADATA_PID,
    build_create_table_sql,
    schema_columns,
    validate_target_table,
)


def append_raw(spark, target_table, frame, rows, process_id):
    """Append a counted, cleaned frame to the raw table.

    The target table is created on first load from the actually read schema;
    an empty frame still verifies the schema without writing rows.
    """
    import pyspark.sql.functions as F

    if any(field.name in (METADATA_PID, METADATA_DTTM) for field in frame.schema.fields):
        raise ValueError("Source column collides with ingestion metadata")
    namespace, _ = validate_target_table(target_table)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")
    if not spark.catalog.tableExists(target_table):
        spark.sql(build_create_table_sql(target_table, schema_columns(frame.schema)))
    if rows:
        portion = frame.withColumn(METADATA_DTTM, F.current_timestamp()).withColumn(
            METADATA_PID, F.lit(process_id).cast("long"))
        portion.writeTo(target_table).option("mergeSchema", "true").option("check-ordering", "false").append()
