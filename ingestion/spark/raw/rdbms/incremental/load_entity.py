"""Spark entry point for managed incremental JDBC ingestion."""

import argparse
import json
import os
from typing import TYPE_CHECKING

from ingestion.logging import configure_driver_logging
from ingestion.spark.raw.iceberg import validate_target_table
from ingestion.spark.raw.rdbms import cli
from ingestion.spark.raw.rdbms.incremental import entity
from ingestion.sources.rdbms.incremental_config import validate, window_contract

if TYPE_CHECKING:
    from psycopg2.extensions import connection as Psycopg2Connection


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and validate the loader CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Append one incremental entity window from a JDBC source into the raw layer."
    )
    cli.add_jdbc_load_args(parser)
    parser.add_argument("--incremental-config", dest="incremental", required=True)
    parser.add_argument("--source-id")
    parser.add_argument("--dag-id")
    parser.add_argument("--run-id")
    args = parser.parse_args(argv)
    cli.validate_jdbc_load_args(args)
    args.incremental = window_contract(validate(json.loads(args.incremental)))
    return args


def _connect_pg_meta() -> "Psycopg2Connection":
    """Open a registry connection using the DSN passed by the Airflow operator."""
    import psycopg2

    dsn = os.environ.get("INGESTION_META_DSN")
    if not dsn:
        raise RuntimeError(
            "INGESTION_META_DSN is not set: the incremental loader needs pg-meta credentials"
        )
    return psycopg2.connect(dsn)


def main(argv: list[str] | None = None) -> None:
    """Load one entity from a JDBC source into the raw layer."""
    configure_driver_logging()
    args = parse_args(argv)
    if not args.source_id:
        raise ValueError("--source-id is required: extraction runs only as a managed Airflow batch")
    validate_target_table(args.target_table)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    try:
        entity.managed_load(spark, args, db_connect=_connect_pg_meta)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
