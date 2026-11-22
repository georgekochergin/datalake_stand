"""Spark entry point for replacing the current snapshot of an incremental entity."""

import argparse

from ingestion.logging import configure_driver_logging
from ingestion.spark.raw.iceberg import validate_target_table
from ingestion.spark.raw.rdbms.incremental.act_snap import snapshot


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
    parser.add_argument("--num-jdbc-partitions", dest="num_partitions", type=int, default=4)
    parser.add_argument("--no-jdbc-partitioning", action="store_true")
    args = parser.parse_args(argv)
    if args.process_id <= 0:
        raise ValueError(f"--process-id must be a positive integer (got {args.process_id})")
    if args.num_partitions <= 0:
        raise ValueError(f"--num-jdbc-partitions must be a positive integer (got {args.num_partitions})")
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
