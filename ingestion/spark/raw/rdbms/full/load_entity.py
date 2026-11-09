"""Append a full slice of one JDBC entity into the raw layer.

Full-slice entities (no incremental column) load outside the incremental
registry: every execution reads the whole source and appends one portion,
and a task retry simply repeats the append (at-least-once). Managed Airflow
execution is the only load path; no pg-meta state is planned or finalized.
"""

import argparse
import logging

from ingestion.iceberg import validate_target_table
from ingestion.spark.raw.rdbms import reader
from ingestion.spark.raw.rdbms import raw_append

LOGGER = logging.getLogger(__name__)

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and validate the full-slice loader CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Append one full entity slice from a JDBC source into the raw layer."
    )
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--jdbc-user", required=True)
    parser.add_argument("--jdbc-password", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--process-id", required=True, type=int)
    parser.add_argument("--num-partitions", type=int, default=4)
    parser.add_argument("--no-jdbc-partitioning", action="store_true")
    parser.add_argument("--source-id")
    parser.add_argument("--dag-id")
    parser.add_argument("--run-id")
    args = parser.parse_args(argv)
    if args.process_id <= 0:
        raise ValueError(f"--process-id must be a positive integer (got {args.process_id})")
    if args.num_partitions <= 0:
        raise ValueError(f"--num-partitions must be a positive integer (got {args.num_partitions})")
    return args


def managed_load(spark, args):
    """Read the whole source frame and append it to the raw table."""
    frame = reader.read_extract_frame(spark, args, None, None)
    try:
        raw_append.append_raw(spark, args, frame, None)
    finally:
        frame.unpersist()


def main(argv: list[str] | None = None) -> None:
    """Load one full entity slice from a JDBC source into the raw layer."""
    args = parse_args(argv)
    if not args.source_id:
        raise ValueError("--source-id is required: extraction runs only as a managed Airflow batch")
    validate_target_table(args.target_table)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"ingest-{args.target_table}").getOrCreate()
    try:
        managed_load(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
