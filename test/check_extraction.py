#!/usr/bin/env python3
"""Inspect managed extraction metadata after the e2e ingestion scenarios."""

from collections import defaultdict
from uuid import uuid4

from airflow.providers.postgres.hooks.postgres import PostgresHook


def require(condition, message):
    """Fail explicitly without relying on Python assertion optimization flags."""
    if not condition:
        raise AssertionError(message)
    print("PASS  " + message)


def records(cursor, sql):
    """Return metadata rows with their declared column names."""
    cursor.execute(sql)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def check_registry_recovery():
    """Exercise real pg-meta retry/finalization without touching JDBC sources."""
    from dlh_ingestion import meta

    connect = PostgresHook(postgres_conn_id="ingestion_meta").get_conn
    source = "e2e-registry-" + uuid4().hex
    process_id = meta.get_or_create_process_id(dag_id="e2e_registry", run_id=source,
                                               execution_date=None, db_connect=connect)
    require(meta.get_or_create_process_id(dag_id="e2e_registry", run_id=source,
                                          execution_date=None, db_connect=connect) == process_id,
            "DagRun process allocation is idempotent")
    arguments = dict(process_id=process_id, dag_id="e2e_registry", run_id=source,
                     source_id=source, entity_fqn="synthetic.transactions",
                     target_table="raw.synthetic.transactions", incremental_column="position",
                     incremental_type="bigint", upper_bound="10", empty_at_plan=False,
                     db_connect=connect)
    planned = meta.plan_batch(**arguments)
    batch_id = planned["batch_id"]
    meta.start_batch(batch_id, db_connect=connect)
    meta.fail_batch(batch_id, "synthetic failure after append before metadata commit", db_connect=connect)
    retry = meta.plan_batch(**dict(arguments, upper_bound="999"))
    require(retry["batch_id"] == batch_id and retry["upper_bound"] == "10", "retry retains identity and frozen upper bound")
    require(retry["status"] == "FAILED" and retry["watermark_after"] is None, "failed attempt does not finalize watermark")
    meta.start_batch(batch_id, db_connect=connect)
    finished = meta.finalize_batch(batch_id, 2, "10", db_connect=connect)
    require(finished["attempt_count"] == 2 and finished["last_error"] is None, "retry records attempts and clears error")
    repeated = meta.finalize_batch(batch_id, 999, "999", db_connect=connect)
    require(repeated == finished, "repeated successful finalization returns original result")
    second = meta.plan_batch(**dict(arguments, run_id=source + "-regression", upper_bound="9"))
    meta.start_batch(second["batch_id"], db_connect=connect)
    regression = meta.finalize_batch(second["batch_id"], 1, "9",
                                     compare_positions=lambda left, right: int(left) - int(right),
                                     db_connect=connect)
    require(regression["watermark_after"] == "10", "typed comparison prevents watermark regression from 10 to 9")
    third = meta.plan_batch(**dict(arguments, run_id=source + "-noop", upper_bound="10"))
    noop = meta.finalize_batch(third["batch_id"], 0, None, db_connect=connect)
    require(noop["watermark_after"] == "10" and noop["attempt_count"] == 0, "no-op finalization requires no Spark execution")
    stale = meta.plan_batch(**dict(arguments, run_id=source + "-stale", upper_bound="11"))
    competing = meta.plan_batch(**dict(arguments, run_id=source + "-newer", upper_bound="12"))
    meta.start_batch(competing["batch_id"], db_connect=connect)
    meta.finalize_batch(competing["batch_id"], 1, "12",
                        compare_positions=lambda left, right: int(left) - int(right), db_connect=connect)
    try:
        meta.finalize_batch(stale["batch_id"], 1, "11",
                            compare_positions=lambda left, right: int(left) - int(right), db_connect=connect)
    except ValueError:
        print("PASS  stale revision finalization rejected")
    else:
        raise AssertionError("stale revision must not finalize")
    require(meta.get_batch(batch_id=competing["batch_id"], db_connect=connect)["watermark_after"] == "12",
            "stale failure preserves newer finalized state")
    try:
        meta.plan_batch(**dict(arguments, run_id=source + "-wrong-contract", incremental_column="other_position"))
    except ValueError:
        print("PASS  changed incremental contract requires migration")
    else:
        raise AssertionError("changed contract must not plan a batch")
    connection = PostgresHook(postgres_conn_id="ingestion_meta").get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT revision FROM ingestion.extraction_state WHERE source_id = %s FOR UPDATE", (source,))
            revision = cursor.fetchone()[0]
            cursor.execute("UPDATE ingestion.extraction_batches SET status = 'FAILED', last_error = 'stopped stale synthetic executor', finished_at = clock_timestamp() WHERE batch_id = %s", (stale["batch_id"],))
            cursor.execute(
                "INSERT INTO ingestion.extraction_state_migrations "
                "(source_id, entity_fqn, old_revision, new_revision, old_mode, new_mode, "
                "old_column, new_column, old_type, new_type, old_watermark, new_watermark, performed_by, reason_text) "
                "VALUES (%s, %s, %s, %s, 'incremental', 'incremental', 'position', 'other_position', "
                "'bigint', 'bigint', '12', NULL, current_user, 'e2e stopped synthetic executor migration')",
                (source, arguments["entity_fqn"], revision, revision + 1),
            )
            cursor.execute(
                "UPDATE ingestion.extraction_state SET incremental_column = 'other_position', "
                "watermark = NULL, revision = revision + 1 WHERE source_id = %s AND revision = %s",
                (source, revision),
            )
            require(cursor.rowcount == 1, "migration state CAS updates one entity")
        connection.commit()
        migrated = meta.plan_batch(**dict(arguments, run_id=source + "-migrated", incremental_column="other_position", upper_bound="90000"))
        require(migrated["reason"] == "bootstrap" and migrated["watermark_before"] is None, "migrated contract reboots from null watermark")
        meta.start_batch(migrated["batch_id"], db_connect=connect)
        finalized = meta.finalize_batch(migrated["batch_id"], 1, "90000", db_connect=connect)
        require(finalized["watermark_after"] == "90000", "new contract bootstrap advances independently")
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM ingestion.extraction_state_migrations WHERE source_id = %s", (source,))
            require(cursor.fetchone()[0] == 1, "manual migration audit retained exactly once")
    finally:
        connection.close()


def main():
    """Check state, frozen bounds, no-op results, and manual-full semantics."""
    connection = PostgresHook(postgres_conn_id="ingestion_meta").get_conn()
    try:
        with connection.cursor() as cursor:
            states = records(cursor, "SELECT * FROM ingestion.extraction_state WHERE source_id IN ('bank', 'broker')")
            batches = records(cursor, "SELECT * FROM ingestion.extraction_batches WHERE source_id IN ('bank', 'broker') ORDER BY batch_id")
            require(len(states) == 8, "eight source entities have extraction state")
            require(bool(batches), "extraction batches exist")
            identities = {(b["dag_id"], b["run_id"], b["source_id"], b["entity_fqn"]) for b in batches}
            require(len(identities) == len(batches), "one stable batch identity per entity and DagRun")
            grouped = defaultdict(list)
            processes = defaultdict(set)
            for batch in batches:
                label = f"batch {batch['batch_id']}"
                grouped[batch["source_id"], batch["entity_fqn"]].append(batch)
                processes[batch["dag_id"], batch["run_id"]].add(batch["process_id"])
                require(batch["status"] == "SUCCEEDED", label + " succeeded")
                require(batch["row_count"] is not None and batch["row_count"] >= 0, label + " has row count")
                require(batch["finished_at"] is not None and batch["last_error"] is None, label + " finalized cleanly")
                require(batch["batch_id"] > 0 and batch["process_id"] > 0, label + " has positive IDs")
                if batch["row_count"]:
                    require(batch["attempt_count"] >= 1, label + " records execution attempt")
                if batch["incremental_column"] and batch["reason"] != "manual_full":
                    require(batch["upper_bound"] is not None or batch["empty_at_plan"], label + " has frozen upper bound")
                    if batch["observed_max"] is not None:
                        require(int(batch["observed_max"]) <= int(batch["upper_bound"]), label + " observed max within bound")
                    if batch["reason"] == "regular":
                        require(batch["lower_bound"] == batch["watermark_before"], label + " zero-lookback lower bound")
                if batch["reason"] == "manual_full":
                    require(batch["mode"] == "full_snapshot", label + " manual full mode")
                    require(batch["lower_bound"] is None and batch["upper_bound"] is None, label + " manual full has no filters")
                    require(bool(batch["reason_text"]), label + " manual reason retained")
                    require(batch["watermark_after"] == batch["watermark_before"], label + " manual full preserves watermark")
                if batch["run_id"].startswith(("e2e-ingest2-", "e2e-after-manual-")) and batch["incremental_column"]:
                    require(batch["row_count"] == 0 and batch["observed_max"] is None, label + " no-op records zero rows")
                    require(batch["watermark_after"] == batch["watermark_before"], label + " no-op preserves watermark")
            require(all(len(ids) == 1 for ids in processes.values()), "each DagRun shares one process ID")
            for state in states:
                history = grouped[state["source_id"], state["entity_fqn"]]
                label = f"{state['source_id']}/{state['entity_fqn']}"
                require(bool(history), label + " has batch history")
                require(state["last_batch_id"] == history[-1]["batch_id"], label + " points at latest succeeded batch")
                require(state["watermark"] == history[-1]["watermark_after"], label + " state matches final watermark")
                require(state["revision"] == history[-1]["expected_revision"] + 1, label + " revision finalized once")
                require(all(right["expected_revision"] == left["expected_revision"] + 1 for left, right in zip(history, history[1:])), label + " batch revisions contiguous")
                if state["configured_mode"] == "incremental":
                    require(state["incremental_column"] == "_batch_id" and state["watermark"] is not None, label + " incremental contract retained")
                    require(any(b["reason"] == "manual_full" for b in history), label + " manual full exercised")
                else:
                    require(state["watermark"] is None and state["incremental_column"] is None and state["incremental_type"] is None, label + " snapshot has no incremental state")
        check_registry_recovery()
        print("PASS extraction metadata checks; FAIL=0")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
