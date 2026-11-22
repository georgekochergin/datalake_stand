"""Append a full slice of one JDBC entity into the raw layer.

Full-slice entities (no incremental column) load outside the incremental
registry: every execution reads the whole source and appends one portion,
and a task retry simply repeats the append (at-least-once). Managed Airflow
execution is the only load path; no pg-meta state is planned or finalized.
"""

import argparse
import logging
from typing import TYPE_CHECKING

from ingestion.spark.raw import append
from ingestion.spark.raw.iceberg import validate_target_table
from ingestion.spark.raw.rdbms import cli, reader
from ingestion.logging import configure_driver_logging

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

LOGGER = logging.getLogger(__name__)

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and validate the full-slice loader CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Append one full entity slice from a JDBC source into the raw layer."
    )
    cli.add_jdbc_load_args(parser)
    parser.add_argument("--source-id")
    parser.add_argument("--dag-id")
    parser.add_argument("--run-id")
    args = parser.parse_args(argv)
    cli.validate_jdbc_load_args(args)
    return args


def managed_load(spark: "SparkSession", args: argparse.Namespace) -> None:
    """Read the whole source frame and append it to the raw table."""
    frame = reader.read_extract_frame(spark, args)
    try:
        append.append_raw(spark, args.target_table, frame, frame.count(), args.process_id)
    finally:
        frame.unpersist()


def main(argv: list[str] | None = None) -> None:
    """Load one full entity slice from a JDBC source into the raw layer."""
    configure_driver_logging()
    args = parse_args(argv)
    if not args.source_id:
        raise ValueError("--source-id is required: extraction runs only as a managed Airflow batch")
    validate_target_table(args.target_table)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    try:
        managed_load(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
