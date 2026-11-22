"""Spark entry point for replacing the current snapshot of an incremental entity."""

import argparse

from ingestion.logging import configure_driver_logging
from ingestion.spark.raw.iceberg import validate_target_table
from ingestion.spark.raw.rdbms import cli
from ingestion.spark.raw.rdbms.incremental.act_snap import snapshot


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and validate the snapshot loader CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Overwrite the actual snapshot of one entity from a JDBC source."
    )
    cli.add_jdbc_load_args(parser)
    args = parser.parse_args(argv)
    cli.validate_jdbc_load_args(args)
    return args


def main(argv: list[str] | None = None) -> None:
    """Overwrite the actual snapshot of one entity from a JDBC source."""
    configure_driver_logging()
    args = parse_args(argv)
    validate_target_table(args.target_table)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    try:
        snapshot.managed_snapshot(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
