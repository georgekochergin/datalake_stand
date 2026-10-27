#!/usr/bin/env python3
"""Exercise temporal managed extraction against real JDBC sources and Iceberg."""

import os
import sys
from argparse import Namespace
from uuid import uuid4
from unittest.mock import patch

sys.path[:0] = ["/opt/airflow/plugins", "/opt/spark-jobs/raw"]


def require(condition, message):
    """Keep contract checks active even under Python optimization."""
    if not condition:
        raise AssertionError(message)
    print("PASS  " + message, flush=True)


def metadata_row(hook, table, source_id, entity_fqn):
    """Read one isolated test state or its latest persisted batch."""
    connection = hook.get_conn()
    try:
        with connection.cursor() as cursor:
            order = " ORDER BY batch_id DESC LIMIT 1" if table == "extraction_batches" else ""
            cursor.execute(
                f"SELECT * FROM ingestion.{table} WHERE source_id = %s AND entity_fqn = %s" + order,
                (source_id, entity_fqn),
            )
            row = cursor.fetchone()
            return dict(zip((column[0] for column in cursor.description), row)) if row else None
    finally:
        connection.close()


def source_count(spark, args, lower, upper, kind):
    """Count the exact source predicate without Spark temporal conversion."""
    import extraction
    import load_entity

    column = load_entity.sql_reference(args.jdbc_url, args.incremental_column)
    predicates = []
    for value, operator in ((lower, ">"), (upper, "<=")):
        if value is not None:
            predicates.append(f"{column} {operator} {extraction.literal(value, kind, args.jdbc_url)}")
    query = f"SELECT COUNT(*) FROM {args.source_table}"
    if predicates:
        query += " WHERE " + " AND ".join(predicates)
    connection = spark._jvm.java.sql.DriverManager.getConnection(
        args.jdbc_url, args.jdbc_user, args.jdbc_password
    )
    try:
        statement = connection.createStatement()
        try:
            result = statement.executeQuery(query)
            try:
                result.next()
                return int(result.getString(1))
            finally:
                result.close()
        finally:
            statement.close()
    finally:
        connection.close()


def exercise(spark, hook, source, jdbc, table, target, column, expected_kind, token,
             expected_upper=None):
    """Verify bootstrap, overlap, manual-full, empty resume, and successful retry."""
    import extraction
    import load_entity
    from ingestion import registry

    args = Namespace(
        jdbc_url=jdbc[0], jdbc_user=jdbc[1], jdbc_password=jdbc[2],
        source_table=table, target_table=f"raw.e2e_contract.{target}",
        source_id=f"e2e_{source}", incremental_column=column,
        partition_column=None, no_parallelism=True, num_partitions=1,
        watermark_value=None, dag_id="e2e_temporal_extraction", run_id=None,
        process_id=None, force_full=False, reason=None, lookback_size=1, lookback_unit="d",
        lookback_given=True,
    )
    require(metadata_row(hook, "extraction_state", args.source_id, table) is None,
            target + " starts with fresh isolated metadata")
    require(not spark.catalog.tableExists(args.target_table), target + " starts with fresh raw table")
    kind, upper, empty, has_null = load_entity.source_position(spark, args)
    require(kind == expected_kind, f"{target} exact JDBC kind is {expected_kind}, got {kind}")
    require(not empty and not has_null and upper is not None, target + " has non-null temporal data")
    if expected_upper is not None:
        require(upper == expected_upper, target + " source MAX preserves full fractional precision")
        digits = upper.removesuffix("Z")
        previous = digits[:-1] + str(int(digits[-1]) - 1) + ("Z" if upper.endswith("Z") else "")
        require(source_count(spark, args, previous, upper, kind) == 1,
                target + " source predicate distinguishes adjacent sub-microsecond positions")
    full_count = source_count(spark, args, None, None, kind)
    total = 0
    watermark = None
    for revision, phase in enumerate(("bootstrap", "lookback", "forcefull", "normal"), start=1):
        args.run_id = f"{token}-{target}-{phase}"
        args.process_id = registry.get_or_create_process_id(dag_id=args.dag_id, run_id=args.run_id)
        args.force_full = phase == "forcefull"
        args.reason = "e2e temporal manual-full contract" if args.force_full else None
        args.lookback_size = 1 if phase in ("bootstrap", "lookback") else 0
        args.lookback_unit = "d" if args.lookback_size else None
        args.lookback_given = bool(args.lookback_size)
        lower = extraction.lower_bound(watermark, kind, 1, "d") if phase == "lookback" else (
            watermark if phase == "normal" else None
        )
        bound = None if args.force_full else upper
        expected_count = source_count(spark, args, lower, bound, kind)
        if phase == "normal":
            require(expected_count == 0, target + " unchanged source has empty strict resume")
        if phase == "lookback":
            require(expected_count > 0, target + " lookback includes overlapping source rows")
        load_entity.managed_load(spark, args)
        batch = metadata_row(hook, "extraction_batches", args.source_id, table)
        state = metadata_row(hook, "extraction_state", args.source_id, table)
        label = f"{target}/{phase}"
        require(batch is not None and state is not None, label + " persisted batch and state")
        require(batch["run_id"] == args.run_id and batch["process_id"] == args.process_id,
                label + " stable run/process identity")
        require(batch["status"] == "SUCCEEDED" and batch["attempt_count"] == (1 if expected_count else 0),
                label + " finalized; empty window starts no executor")
        require(batch["finished_at"] is not None and batch["last_error"] is None,
                label + " finalized cleanly")
        require(batch["incremental_type"] == kind and state["incremental_type"] == kind,
                label + " persists original JDBC kind")
        require(batch["lower_bound"] == lower and batch["upper_bound"] == bound,
                label + " frozen exact lower/upper bounds")
        require(batch["watermark_before"] == watermark, label + " retains previous watermark")
        reason = "manual_full" if args.force_full else ("bootstrap" if phase == "bootstrap" else "regular")
        require(batch["reason"] == reason, label + " persisted extraction reason")
        require(batch["mode"] == ("full_snapshot" if phase in ("bootstrap", "forcefull") else "incremental"),
                label + " persisted extraction mode")
        require(batch["row_count"] == expected_count, label + " row count matches exact JDBC predicate")
        require(batch["observed_max"] == (upper if expected_count else None),
                label + " observed MAX retains exact source precision")
        if not args.force_full and expected_count:
            watermark = upper
        require(batch["watermark_after"] == watermark and state["watermark"] == watermark,
                label + " state watermark matches finalized batch")
        require(state["revision"] == revision + 1 and batch["expected_revision"] == revision,
                label + " advances one state revision")
        require(state["last_batch_id"] == batch["batch_id"], label + " state points to latest batch")
        require(state["configured_mode"] == "incremental" and state["incremental_column"] == column,
                label + " preserves configured incremental contract")
        if args.force_full:
            require(batch["reason_text"] == args.reason and expected_count == full_count,
                    label + " audited manual full reloads entire source")
        total += expected_count
        frame = spark.table(args.target_table)
        require(frame.count() == total, label + " raw append-only count")
        require(frame.filter(frame["__batch_id"] == batch["batch_id"]).count() == expected_count,
                label + " raw rows carry persisted batch ID")
        require(frame.filter(frame["__batch_id"].isNull()).count() == 0,
                label + " every raw row has a batch ID")
        load_entity.managed_load(spark, args)
        retried = metadata_row(hook, "extraction_batches", args.source_id, table)
        require(retried == batch and spark.table(args.target_table).count() == total,
                label + " successful retry neither appends nor mutates persisted batch")
        require(metadata_row(hook, "extraction_state", args.source_id, table) == state,
                label + " successful retry preserves state revision")


def precision_fixtures(source):
    """Provide immutable two-row JDBC queries differing below Spark microseconds."""
    if source == "bank":
        for name, sql_type, kind, suffix in (
            ("datetime2", "datetime2(7)", "timestamp_local(7)", ""),
            ("datetimeoffset", "datetimeoffset(7)", "timestamp_instant(7)", "+02:00"),
        ):
            query = " UNION ALL ".join(
                f"SELECT {index} AS id, CAST('2024-06-15T12:34:56.123456{index}{suffix}' AS {sql_type}) AS position"
                for index in (1, 2)
            )
            upper = "2024-06-15T" + ("10" if suffix else "12") + ":34:56.1234562" + ("Z" if suffix else "")
            yield name, f"({query}) temporal_fixture", kind, upper
    else:
        for name, sql_type, function, kind, suffix in (
            ("timestamp9", "TIMESTAMP(9)", "TO_TIMESTAMP", "timestamp_local(9)", ""),
            ("timestamptz9", "TIMESTAMP(9) WITH TIME ZONE", "TO_TIMESTAMP_TZ", "timestamp_instant(9)", "+02:00"),
        ):
            mask = 'YYYY-MM-DD"T"HH24:MI:SS.FF9' + ("TZH:TZM" if suffix else "")
            query = " UNION ALL ".join(
                f"SELECT {index} AS id, CAST({function}('2024-06-15T12:34:56.12345678{index}{suffix}', '{mask}') AS {sql_type}) AS position FROM dual"
                for index in (1, 2)
            )
            upper = "2024-06-15T" + ("10" if suffix else "12") + ":34:56.123456782" + ("Z" if suffix else "")
            yield name, f"({query}) temporal_fixture", kind, upper


def check_post_append_retry(spark, hook, jdbc, token):
    """Inject a real post-append metadata failure and accept a repeated append."""
    import load_entity
    from ingestion import registry

    args = Namespace(
        jdbc_url=jdbc[0], jdbc_user=jdbc[1], jdbc_password=jdbc[2],
        source_table="(SELECT CAST(1 AS bigint) AS position) retry_fixture",
        target_table="raw.e2e_contract.post_append_retry", source_id="e2e_retry",
        incremental_column="position", partition_column=None, no_parallelism=True,
        num_partitions=1, watermark_value=None, dag_id="e2e_post_append_retry",
        run_id=token, process_id=registry.get_or_create_process_id(
            dag_id="e2e_post_append_retry", run_id=token),
        force_full=False, reason=None, lookback_size=0, lookback_unit=None, lookback_given=False,
    )
    with patch.object(registry, "finalize_batch", side_effect=RuntimeError("injected metadata failure")):
        try:
            load_entity.managed_load(spark, args)
        except RuntimeError as exc:
            require(str(exc) == "injected metadata failure", "expected post-append failure")
        else:
            raise AssertionError("fault injection must fail the first execution")
    require(spark.table(args.target_table).count() == 1, "first append survives metadata failure")
    require(metadata_row(hook, "extraction_state", args.source_id, args.source_table)["watermark"] is None,
            "metadata failure does not advance watermark")
    failed = metadata_row(hook, "extraction_batches", args.source_id, args.source_table)
    require(failed["status"] == "FAILED", "ordinary failed batch permits retry")
    load_entity.managed_load(spark, args)
    finished = metadata_row(hook, "extraction_batches", args.source_id, args.source_table)
    require(finished["batch_id"] == failed["batch_id"] and finished["attempt_count"] == 2,
            "post-append retry retains batch identity")
    require(finished["status"] == "SUCCEEDED" and finished["row_count"] == 1,
            "post-append retry finalizes latest execution")
    require(spark.table(args.target_table).count() == 2, "at-least-once retry intentionally appends twice")
    require(spark.table(args.target_table).select("__batch_id").distinct().count() == 1,
            "both commits retain the same logical batch ID")


def main():
    """Run inside airflow-scheduler with Spark defaults and loaded source data."""
    from airflow.hooks.base import BaseHook
    from airflow.models.connection import Connection
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.utils.session import create_session
    from ingestion.operators import _resolve_jdbc
    from pyspark.sql import SparkSession

    meta = BaseHook.get_connection("ingestion_meta")
    os.environ.update(PGHOST=meta.host, PGPORT=str(meta.port or 5432),
                      PGDATABASE=meta.schema, PGUSER=meta.login, PGPASSWORD=meta.password)
    hook = PostgresHook(postgres_conn_id="ingestion_meta")
    with create_session() as session:
        source_jdbc = {
            source: _resolve_jdbc(session.query(Connection).filter_by(conn_id=f"{source}_source").one())
            for source in ("bank", "broker")
        }
    token = "e2e-temporal-" + uuid4().hex
    spark = SparkSession.builder.appName("e2e-temporal-extraction").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        check_post_append_retry(spark, hook, source_jdbc["bank"], token)
        for source, table, kind in (
            ("bank", "retail_bank.transactions", "date"),
            ("broker", "trading_core.transactions", "timestamp_local(0)"),
        ):
            jdbc = source_jdbc[source]
            exercise(spark, hook, source, jdbc, table, f"{source}_temporal", "posting_date", kind, token)
            for name, query, fixture_kind, upper in precision_fixtures(source):
                exercise(spark, hook, source, jdbc, query, f"{source}_{name}", "position",
                         fixture_kind, token, expected_upper=upper)
        print("PASS temporal extraction contracts; FAIL=0", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
