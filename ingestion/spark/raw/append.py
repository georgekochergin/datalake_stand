"""Raw-layer table writer: schema guard, first-load DDL, and metadata append.

Receives a cleaned DataFrame and its row count; knows nothing about source
types or load modes.
"""

import logging
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from ingestion.logging import log_sql, log_step

from ingestion.spark.raw.iceberg import (
    CATALOG,
    METADATA_DTTM,
    METADATA_PID,
    build_create_table_sql,
    schema_columns,
    validate_target_table,
)

LOGGER = logging.getLogger(__name__)


class AppendReason(Enum):
    """Stable reason codes and readable explanations for Iceberg append."""

    NO_ROWS = ("no_rows", "No source rows were read for this load")

    def __init__(self, code: str, text: str) -> None:
        self.code = code
        self.text = text


def append_raw(spark: "SparkSession", target_table: str, frame: "DataFrame", rows: int, process_id: int) -> None:
    """Append a counted, cleaned frame to the raw table.

    The target table is created on first load from the actually read schema;
    an empty frame still verifies the schema without writing rows.
    """
    import pyspark.sql.functions as F

    log_step(LOGGER, "append to Iceberg")
    LOGGER.info("Iceberg append target=%s process_id=%s row_count=%s", target_table, process_id, rows)
    if any(field.name in (METADATA_PID, METADATA_DTTM) for field in frame.schema.fields):
        raise ValueError("Source column collides with ingestion metadata")
    namespace, _ = validate_target_table(target_table)
    namespace_sql = f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}"
    log_sql(LOGGER, "Iceberg SQL", namespace_sql)
    spark.sql(namespace_sql)
    if not spark.catalog.tableExists(target_table):
        table_sql = build_create_table_sql(target_table, schema_columns(frame.schema))
        log_sql(LOGGER, "Iceberg SQL", table_sql)
        spark.sql(table_sql)
    if rows:
        portion = frame.withColumn(METADATA_DTTM, F.current_timestamp()).withColumn(
            METADATA_PID, F.lit(process_id).cast("long"))
        portion.writeTo(target_table).option("mergeSchema", "true").option("check-ordering", "false").append()
        LOGGER.info("Iceberg append committed target=%s process_id=%s row_count=%s", target_table, process_id, rows)
    else:
        reason = AppendReason.NO_ROWS
        LOGGER.info("%s. Iceberg append skipped. target_table=%s row_count=%s reason=%s",
                    reason.text, target_table, rows, reason.code)
