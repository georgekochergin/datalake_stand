"""Common arguments for the raw JDBC Spark entry points."""

import argparse


def add_jdbc_load_args(parser: argparse.ArgumentParser) -> None:
    """Register JDBC credentials, table names, process ID and partition options."""
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--jdbc-user", required=True)
    parser.add_argument("--jdbc-password", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--process-id", required=True, type=int)
    parser.add_argument("--num-jdbc-partitions", dest="num_partitions", type=int, default=4)
    parser.add_argument("--no-jdbc-partitioning", action="store_true")


def validate_jdbc_load_args(args: argparse.Namespace) -> None:
    """Require positive process and partition counts after parsing."""
    if args.process_id <= 0:
        raise ValueError(f"--process-id must be a positive integer (got {args.process_id})")
    if args.num_partitions <= 0:
        raise ValueError(f"--num-jdbc-partitions must be a positive integer (got {args.num_partitions})")
