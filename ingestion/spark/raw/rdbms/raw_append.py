"""Shared raw-table append for the window and full-slice loader applications."""

from ingestion.iceberg import (
    CATALOG,
    METADATA_DTTM,
    METADATA_PID,
    build_create_table_sql,
    schema_columns,
    validate_target_table,
)
from ingestion.sources.rdbms import positions


def append_raw(spark, args, frame, kind):
    """Append the read window to the raw table; return (row_count, observed_max).

    The target table is created on first load from the actually read schema;
    an empty window still verifies the schema without writing rows.
    """
    import pyspark.sql.functions as F

    incremental_column = getattr(args, "incremental_column", None)
    summary = frame.agg(F.count("*").alias("n"), *(
        [F.max("__extract_position").alias("position")] if incremental_column else []
    )).first()
    rows = int(summary["n"])
    observed = positions.canonical(summary["position"], kind) if incremental_column else None
    clean = frame.drop("__extract_position") if incremental_column else frame
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
