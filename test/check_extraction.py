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
    from ingestion import meta

    connect = PostgresHook(postgres_conn_id="ingestion_meta").get_conn
    source = "e2e-registry-" + uuid4().hex
    first = meta.allocate_process_id(dag_id="e2e_registry", run_id=source,
                                     execution_date=None, db_connect=connect)
    second = meta.allocate_process_id(dag_id="e2e_registry", run_id=source,
                                      execution_date=None, db_connect=connect)
    require(first != second, "every generating-task execution allocates a fresh process_id")

    def arguments(process_id):
        """Planning arguments for one execution of the synthetic entity."""
        return dict(process_id=process_id, dag_id="e2e_registry", run_id=source,
                    source_id=source, entity_fqn="synthetic.transactions",
                    target_table="raw.synthetic.transactions", incremental_column="position",
                    incremental_type="bigint", upper_bound="10", empty_at_plan=False,
                    db_connect=connect)

    def key(batch):
        """The composite batch key."""
        return dict(process_id=batch["process_id"], source_id=batch["source_id"],
                    entity_fqn=batch["entity_fqn"], db_connect=connect)

    planned = meta.plan_batch(**arguments(first))
    meta.start_batch(**key(planned))
    meta.fail_batch(**key(planned))
    retry = meta.plan_batch(**dict(arguments(first), upper_bound="999"))
    require(retry["process_id"] == planned["process_id"] and retry["upper_bound"] == "10"
            and retry["status"] == "FAILED" and retry["watermark_after"] is None,
            "retry of the same execution retains identity and frozen upper bound")
    meta.start_batch(**key(planned))
    finished = meta.finalize_batch(**dict(key(planned), row_count=2, observed_max="10"))
    require(finished["attempt_count"] == 2, "retry records attempts")
    repeated = meta.finalize_batch(**dict(key(planned), row_count=999, observed_max="999"))
    require(repeated == finished, "repeated successful finalization returns original result")
    regression_process = meta.allocate_process_id(dag_id="e2e_registry", run_id=source + "-regression",
                                                  execution_date=None, db_connect=connect)
    second_batch = meta.plan_batch(**dict(arguments(regression_process), upper_bound="9"))
    meta.start_batch(**key(second_batch))
    regression = meta.finalize_batch(
        **dict(key(second_batch), row_count=1, observed_max="9"),
        compare_positions=lambda left, right: int(left) - int(right),
    )
    require(regression["watermark_after"] == "10", "typed comparison prevents watermark regression from 10 to 9")
    noop_process = meta.allocate_process_id(dag_id="e2e_registry", run_id=source + "-noop",
                                            execution_date=None, db_connect=connect)
    third = meta.plan_batch(**dict(arguments(noop_process), upper_bound="10"))
    noop = meta.finalize_batch(**dict(key(third), row_count=0, observed_max=None))
    require(noop["watermark_after"] == "10" and noop["attempt_count"] == 0,
            "no-op finalization requires no Spark execution")
    stale_process = meta.allocate_process_id(dag_id="e2e_registry", run_id=source + "-stale",
                                             execution_date=None, db_connect=connect)
    newer_process = meta.allocate_process_id(dag_id="e2e_registry", run_id=source + "-newer",
                                             execution_date=None, db_connect=connect)
    stale = meta.plan_batch(**dict(arguments(stale_process), upper_bound="11"))
    competing = meta.plan_batch(**dict(arguments(newer_process), upper_bound="12"))
    meta.start_batch(**key(competing))
    meta.finalize_batch(**dict(key(competing), row_count=1, observed_max="12"),
                        compare_positions=lambda left, right: int(left) - int(right))
    try:
        meta.finalize_batch(**dict(key(stale), row_count=1, observed_max="11"),
                            compare_positions=lambda left, right: int(left) - int(right))
    except ValueError:
        print("PASS  stale revision finalization rejected")
    else:
        raise AssertionError("stale revision must not finalize")
    require(meta.get_batch(process_id=competing["process_id"], source_id=competing["source_id"],
                           entity_fqn=competing["entity_fqn"],
                           db_connect=connect)["watermark_after"] == "12",
            "stale failure preserves newer finalized state")
    wrong_process = meta.allocate_process_id(dag_id="e2e_registry", run_id=source + "-wrong-contract",
                                             execution_date=None, db_connect=connect)
    try:
        meta.plan_batch(**dict(arguments(wrong_process), incremental_column="other_position"))
    except ValueError:
        print("PASS  changed incremental contract requires migration")
    else:
        raise AssertionError("changed contract must not plan a batch")
    connection = PostgresHook(postgres_conn_id="ingestion_meta").get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT revision FROM ingestion.extraction_state WHERE source_id = %s FOR UPDATE", (source,))
            revision = cursor.fetchone()[0]
            cursor.execute("UPDATE ingestion.extraction_batches SET status = 'FAILED', "
                           "finished_at = clock_timestamp() WHERE process_id = %s AND source_id = %s "
                           "AND entity_fqn = 'synthetic.transactions'", (stale["process_id"], source))
            cursor.execute(
                "INSERT INTO ingestion.extraction_state_migrations "
                "(source_id, entity_fqn, old_revision, new_revision, old_mode, new_mode, "
                "old_column, new_column, old_type, new_type, old_watermark, new_watermark, performed_by, reason_text) "
                "VALUES (%s, %s, %s, %s, 'incremental', 'incremental', 'position', 'other_position', "
                "'bigint', 'bigint', '12', NULL, current_user, 'e2e stopped synthetic executor migration')",
                (source, "synthetic.transactions", revision, revision + 1),
            )
            cursor.execute(
                "UPDATE ingestion.extraction_state SET incremental_column = 'other_position', "
                "watermark = NULL, revision = revision + 1 WHERE source_id = %s AND revision = %s",
                (source, revision),
            )
            require(cursor.rowcount == 1, "migration state CAS updates one entity")
        connection.commit()
        migrated_process = meta.allocate_process_id(dag_id="e2e_registry", run_id=source + "-migrated",
                                                    execution_date=None, db_connect=connect)
        migrated = meta.plan_batch(**dict(arguments(migrated_process), incremental_column="other_position",
                                          upper_bound="90000"))
        require(migrated["reason"] == "bootstrap" and migrated["mode"] == "incremental"
                and migrated["watermark_before"] is None,
                "migrated contract reboots from null watermark as incremental bootstrap")
        meta.start_batch(**key(migrated))
        finalized = meta.finalize_batch(**dict(key(migrated), row_count=1, observed_max="90000"))
        require(finalized["watermark_after"] == "90000", "new contract bootstrap advances independently")
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM ingestion.extraction_state_migrations WHERE source_id = %s", (source,))
            require(cursor.fetchone()[0] == 1, "manual migration audit retained exactly once")
    finally:
        connection.close()


def main():
    """Check state, frozen bounds, no-op results, and process identity."""
    connection = PostgresHook(postgres_conn_id="ingestion_meta").get_conn()
    try:
        with connection.cursor() as cursor:
            states = records(cursor, "SELECT * FROM ingestion.extraction_state WHERE source_id IN ('bank', 'broker')")
            batches = records(
                cursor,
                "SELECT * FROM ingestion.extraction_batches WHERE source_id IN ('bank', 'broker') "
                "ORDER BY process_id, source_id, entity_fqn",
            )
            require(len(states) == 8, "eight source entities have extraction state")
            require(bool(batches), "extraction batches exist")
            identities = {(b["process_id"], b["source_id"], b["entity_fqn"]) for b in batches}
            require(len(identities) == len(batches), "one stable batch identity per execution and entity")
            grouped = defaultdict(list)
            processes = defaultdict(set)
            for batch in batches:
                label = f"batch {batch['process_id']}/{batch['source_id']}/{batch['entity_fqn']}"
                grouped[batch["source_id"], batch["entity_fqn"]].append(batch)
                processes[batch["dag_id"], batch["run_id"]].add(batch["process_id"])
                require(batch["status"] == "SUCCEEDED", label + " succeeded")
                require(batch["row_count"] is not None and batch["row_count"] >= 0, label + " has row count")
                require(batch["finished_at"] is not None, label + " finalized cleanly")
                require(batch["process_id"] > 0, label + " has positive process id")
                if batch["row_count"]:
                    require(batch["attempt_count"] >= 1, label + " records execution attempt")
                if batch["mode"] == "incremental":
                    require(batch["upper_bound"] is not None or batch["empty_at_plan"],
                            label + " has frozen upper bound")
                    if batch["observed_max"] is not None:
                        require(int(batch["observed_max"]) <= int(batch["upper_bound"]),
                                label + " observed max within bound")
                    if batch["reason"] == "regular":
                        require(batch["watermark_before"] is not None, label + " regular has watermark")
                        if not batch["lookback_size"]:
                            require(batch["lower_bound"] == batch["watermark_before"],
                                    label + " zero-lookback lower bound")
                            require(batch["lower_bound_op"] == ">", label + " zero-lookback strict operator")
                else:
                    require(batch["lower_bound"] is None and batch["upper_bound"] is None,
                            label + " snapshot has no window filters")
                if batch["run_id"].startswith(("e2e-ingest2-",)) and batch["mode"] == "incremental":
                    require(batch["row_count"] == 0 and batch["observed_max"] is None,
                            label + " no-op records zero rows")
                    require(batch["watermark_after"] == batch["watermark_before"],
                            label + " no-op preserves watermark")
            require(all(len(ids) == 1 for ids in processes.values()),
                    "each DagRun in the flow scenario shares one process ID")
            for state in states:
                history = grouped[state["source_id"], state["entity_fqn"]]
                label = f"{state['source_id']}/{state['entity_fqn']}"
                require(bool(history), label + " has batch history")
                if state["configured_mode"] == "incremental":
                    require(state["incremental_column"] == "_batch_id" and state["watermark"] is not None,
                            label + " incremental contract retained")
                    require(state["last_process_id"] == history[-1]["process_id"],
                            label + " points at latest succeeded batch")
                    require(state["watermark"] == history[-1]["watermark_after"],
                            label + " state matches final watermark")
                    require(state["revision"] == history[-1]["expected_revision"] + 1,
                            label + " revision finalized once")
                    require(all(right["expected_revision"] == left["expected_revision"] + 1
                                for left, right in zip(history, history[1:])),
                            label + " batch revisions contiguous")
                else:
                    require(state["watermark"] is None and state["incremental_column"] is None
                            and state["incremental_type"] is None,
                            label + " snapshot has no incremental state")
                    require(state["last_process_id"] is None and state["revision"] == 1,
                            label + " full-slice batches never touch extraction state")
        check_registry_recovery()
        print("PASS extraction metadata checks; FAIL=0")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
