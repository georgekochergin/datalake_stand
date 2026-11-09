"""Overwrite the actual-snapshot Iceberg table from a JDBC source.

The snapshot stream serves incremental entities outside the incremental
registry: every execution reads the whole source and atomically replaces
raw.<namespace>.<table>__act_snap with a single Iceberg commit. The
publishing process id is stamped as a table property so the author of the
current version stays readable from metadata even when the snapshot has no
rows; snapshot-summary properties are not delivered by the pinned Iceberg
version. Managed Airflow execution is the only load path: the driver reports
whether the table was actually replaced through a result marker that the
operator reads after the submission.
"""

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from ingestion.iceberg import (
    METADATA_DTTM,
    METADATA_PID,
    SNAPSHOT_AUTHOR_PROPERTY,
    quote_identifier,
    table_location,
    validate_target_table,
)
from ingestion.spark.raw.rdbms.load_entity import _read_extract_frame

SNAPSHOT_SOURCE_VIEW = "snapshot_source"
LOGGER = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and validate the snapshot loader CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Overwrite the actual snapshot of one entity from a JDBC source."
    )
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--jdbc-user", required=True)
    parser.add_argument("--jdbc-password", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--process-id", required=True, type=int)
    parser.add_argument("--num-partitions", type=int, default=4)
    parser.add_argument("--no-jdbc-partitioning", action="store_true")
    parser.add_argument("--result-marker", required=True)
    args = parser.parse_args(argv)
    if args.process_id <= 0:
        raise ValueError(f"--process-id must be a positive integer (got {args.process_id})")
    if args.num_partitions <= 0:
        raise ValueError(f"--num-partitions must be a positive integer (got {args.num_partitions})")
    if not args.result_marker:
        raise ValueError("--result-marker must be a non-empty path")
    args.incremental_column = None
    return args


def write_result_marker(path: str, overwritten: bool) -> None:
    """Report whether this execution replaced the snapshot table."""
    Path(path).write_text(json.dumps({"overwritten": bool(overwritten)}), encoding="utf-8")


def current_snapshot_author(spark, target: str) -> int | None:
    """Process id that published the current table version, None if never published."""
    if not spark.catalog.tableExists(target):
        return None
    properties = {row["key"]: row["value"] for row in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()}
    value = properties.get(SNAPSHOT_AUTHOR_PROPERTY)
    if value is None or not value.isdigit():
        return None
    return int(value)


def build_rtas_sql(target: str, process_id: int, columns: list[str]) -> str:
    """Render the atomic CREATE OR REPLACE TABLE ... AS SELECT statement."""
    rendered = ", ".join(quote_identifier(column) for column in columns)
    return (
        f"CREATE OR REPLACE TABLE {target} USING iceberg "
        f"LOCATION '{table_location(target)}' "
        f"TBLPROPERTIES ('{SNAPSHOT_AUTHOR_PROPERTY}'='{process_id}') "
        f"AS SELECT {rendered} FROM {SNAPSHOT_SOURCE_VIEW}"
    )


def _overwrite_snapshot(spark, args, frame, load_dttm) -> None:
    """Replace the snapshot table with the whole read frame in one atomic commit."""
    import pyspark.sql.functions as F

    if any(field.name in (METADATA_DTTM, METADATA_PID) for field in frame.schema.fields):
        raise ValueError("Source column collides with ingestion metadata")
    portion = frame.withColumn(METADATA_DTTM, F.lit(load_dttm).cast("timestamp_ltz")).withColumn(
        METADATA_PID, F.lit(args.process_id).cast("long")
    )
    portion.createOrReplaceTempView(SNAPSHOT_SOURCE_VIEW)
    spark.sql(build_rtas_sql(args.target_table, args.process_id, portion.columns))


def managed_snapshot(spark, args) -> None:
    """Read the whole source and atomically replace the snapshot table.

    A retry first checks the publishing property: when the current version of
    the table already belongs to this process id, the publication happened in
    the failed attempt and the source is not read again. An empty read keeps
    an existing table untouched (a warning is logged, no marker publication);
    a missing table is published even when empty.
    """
    author = current_snapshot_author(spark, args.target_table)
    if author == args.process_id:
        LOGGER.info(
            "actual snapshot: %s already published by process_id=%s; repeat read skipped",
            args.target_table, args.process_id,
        )
        write_result_marker(args.result_marker, True)
        return
    frame = _read_extract_frame(spark, args, None, None)
    try:
        rows = frame.count()
        if rows == 0 and spark.catalog.tableExists(args.target_table):
            LOGGER.warning(
                "actual snapshot: source read for %s is empty; the existing table is kept"
                " without overwrite",
                args.target_table,
            )
            write_result_marker(args.result_marker, False)
            return
        load_dttm = datetime.now(timezone.utc)
        _overwrite_snapshot(spark, args, frame, load_dttm)
        write_result_marker(args.result_marker, True)
        LOGGER.info(
            "actual snapshot: %s row(s) published to %s by process_id=%s",
            rows, args.target_table, args.process_id,
        )
    finally:
        frame.unpersist()


def main(argv: list[str] | None = None) -> None:
    """Overwrite the actual snapshot of one entity from a JDBC source."""
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv)
    validate_target_table(args.target_table)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"snapshot-{args.target_table}").getOrCreate()
    try:
        managed_snapshot(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
