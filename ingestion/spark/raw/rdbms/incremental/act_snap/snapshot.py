"""Overwrite the actual-snapshot Iceberg table from a JDBC source.

The snapshot stream serves incremental entities outside the incremental
registry: every execution reads the whole source and atomically replaces
raw.<namespace>.<table>__act_snap with a single Iceberg commit. The
publishing process id is stamped as a table property so the author of the
current version stays readable from metadata even when the snapshot has no
rows; snapshot-summary properties are not delivered by the pinned Iceberg
version. Airflow independently checks committed rows before publishing the Asset.
"""

import argparse
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from ingestion.logging import log_sql, log_step

from ingestion.spark.raw.iceberg import (
    CATALOG,
    METADATA_DTTM,
    METADATA_PID,
    SNAPSHOT_PROCESS_ID_PROPERTY,
    quote_identifier,
    table_location,
    validate_target_table,
)
from ingestion.spark.raw.rdbms.reader import read_extract_frame

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

_SNAPSHOT_SOURCE_VIEW = "snapshot_source"
LOGGER = logging.getLogger(__name__)


def current_snapshot_author(spark: "SparkSession", target: str) -> int | None:
    """Process id that published the current table version, None if never published."""
    if not spark.catalog.tableExists(target):
        return None
    properties = {row["key"]: row["value"] for row in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()}
    value = properties.get(SNAPSHOT_PROCESS_ID_PROPERTY)
    if value is None or not value.isdigit():
        return None
    return int(value)


def build_rtas_sql(target: str, process_id: int, columns: list[str]) -> str:
    """Render the atomic replacement statement against the snapshot_source temp view.

    The caller must register that view before executing the statement.
    """
    rendered = ",\n  ".join(quote_identifier(column) for column in columns)
    return (
        f"CREATE OR REPLACE TABLE {target} USING iceberg\n"
        f"LOCATION '{table_location(target)}'\n"
        f"TBLPROPERTIES ('{SNAPSHOT_PROCESS_ID_PROPERTY}'='{process_id}')\n"
        f"AS SELECT {rendered}\n"
        f"FROM {_SNAPSHOT_SOURCE_VIEW}"
    )


def _overwrite_snapshot(
    spark: "SparkSession", frame: "DataFrame", *, target: str, process_id: int, load_dttm: datetime,
) -> None:
    """Replace the snapshot table with the whole read frame in one atomic commit."""
    import pyspark.sql.functions as F

    if any(field.name in (METADATA_DTTM, METADATA_PID) for field in frame.schema.fields):
        raise ValueError("Source column collides with ingestion metadata")
    portion = frame.withColumn(METADATA_DTTM, F.lit(load_dttm).cast("timestamp_ltz")).withColumn(
        METADATA_PID, F.lit(process_id).cast("long")
    )
    portion.createOrReplaceTempView(_SNAPSHOT_SOURCE_VIEW)
    namespace, _ = validate_target_table(target)
    log_step(LOGGER, "publish Iceberg snapshot")
    namespace_sql = f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}"
    replace_sql = build_rtas_sql(target, process_id, portion.columns)
    log_sql(LOGGER, "Iceberg SQL", namespace_sql)
    spark.sql(namespace_sql)
    log_sql(LOGGER, "Iceberg SQL", replace_sql)
    spark.sql(replace_sql)


def managed_snapshot(spark: "SparkSession", args: argparse.Namespace) -> None:
    """Read the whole source and atomically replace the snapshot table.

    A retry first checks the publishing property: when the current version of
    the table already belongs to this process id, the publication happened in
    the failed attempt and the source is not read again. An empty read keeps
    an existing table untouched (a warning is logged);
    a missing table is published even when empty.
    """
    author = current_snapshot_author(spark, args.target_table)
    if author == args.process_id:
        LOGGER.info(
            "actual snapshot: %s already published by process_id=%s; repeat read skipped",
            args.target_table, args.process_id,
        )
        return
    frame = read_extract_frame(spark, args)
    try:
        rows = frame.count()
        if rows == 0 and spark.catalog.tableExists(args.target_table):
            LOGGER.warning(
                "actual snapshot: source read for %s is empty; the existing table is kept"
                " without overwrite",
                args.target_table,
            )
            return
        _overwrite_snapshot(
            spark, frame, target=args.target_table, process_id=args.process_id,
            load_dttm=datetime.now(timezone.utc),
        )
        LOGGER.info(
            "actual snapshot: %s row(s) published to %s by process_id=%s",
            rows, args.target_table, args.process_id,
        )
    finally:
        frame.unpersist()
