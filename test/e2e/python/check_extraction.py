#!/usr/bin/env python3
"""Inspect managed extraction metadata after the e2e ingestion scenarios."""

from collections import defaultdict
import os
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


def check_snapshot_publication():
    """Exercise the real Trino publication condition for empty, old and current rows."""
    from airflow.providers.trino.hooks.trino import TrinoHook
    from airflow.sdk.exceptions import AirflowSkipException
    from ingestion.operators import publish_asset_event

    hook = TrinoHook(trino_conn_id="trino_default")
    table = "raw." + os.environ["E2E_SOURCE"] + ".e2e_asset_" + uuid4().hex
    hook.run(f"CREATE TABLE {table} (__process_id BIGINT)")
    try:
        class TaskInstance:
            """Provide the already allocated process identity to the callable."""

            def xcom_pull(self, task_ids):
                return 42

        context = {"ti": TaskInstance()}
        for process in (None, 41, 42):
            if process is not None:
                hook.run(f"DELETE FROM {table}")
                hook.run(f"INSERT INTO {table} VALUES ({process})")
            try:
                publish_asset_event(target_table=table, **context)
            except AirflowSkipException:
                require(process != 42, f"snapshot Asset skipped for process {process}")
            else:
                require(process == 42, "snapshot Asset accepted for current process")
        publish_asset_event()
        print("PASS regular publication requires no table query")
    finally:
        hook.run(f"DROP TABLE {table}")


def check_registry_recovery():
    """Exercise real pg-meta retry/finalization without touching JDBC sources."""
    from ingestion import meta
    from ingestion.meta import registry

    connect = PostgresHook(postgres_conn_id="ingestion_meta").get_conn
    source = "e2e-registry-" + uuid4().hex
    first = meta.allocate_process_id(dag_id="e2e_registry", run_id=source,
                                     execution_date=None, db_connect=connect)
    second = meta.allocate_process_id(dag_id="e2e_registry", run_id=source,
                                      execution_date=None, db_connect=connect)
    require(first != second, "every generating-task execution allocates a fresh process_id")
    orphan = meta.allocate_process_id(dag_id="e2e_registry", run_id=source,
                                      execution_date=None, db_connect=connect)

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

    planned = registry.plan_batch(**arguments(first))
    require(planned["process_id"] != orphan,
            "an orphan generating-task START does not prevent another execution from planning")
    registry.start_batch(**key(planned))
    registry.fail_batch(**key(planned))
    retry = registry.plan_batch(**dict(arguments(first), upper_bound="999"))
    require(retry["process_id"] == planned["process_id"] and retry["upper_bound"] == "10"
            and retry["status"] == "FAILED" and retry["watermark_after"] is None,
            "retry of the same execution retains identity and frozen upper bound")
    registry.start_batch(**key(planned))
    finished = registry.finalize_batch(**dict(key(planned), row_count=2, observed_max="10"))
    require(finished["attempt_count"] == 2, "retry records attempts")
    repeated = registry.finalize_batch(**dict(key(planned), row_count=999, observed_max="999"))
    require(repeated == finished, "repeated successful finalization returns original result")
    regression_process = meta.allocate_process_id(dag_id="e2e_registry", run_id=source + "-regression",
                                                  execution_date=None, db_connect=connect)
    second_batch = registry.plan_batch(**dict(arguments(regression_process), upper_bound="9"))
    registry.start_batch(**key(second_batch))
    regression = registry.finalize_batch(
        **dict(key(second_batch), row_count=1, observed_max="9"),
        compare_positions=lambda left, right: int(left) - int(right),
    )
    require(regression["watermark_after"] == "10", "typed comparison prevents watermark regression from 10 to 9")
    noop_process = meta.allocate_process_id(dag_id="e2e_registry", run_id=source + "-noop",
                                            execution_date=None, db_connect=connect)
    third = registry.plan_batch(**dict(arguments(noop_process), upper_bound="10"))
    noop = registry.finalize_batch(**dict(key(third), row_count=0, observed_max=None))
    require(noop["watermark_after"] == "10" and noop["attempt_count"] == 0,
            "no-op finalization requires no Spark execution")
    stale_process = meta.allocate_process_id(dag_id="e2e_registry", run_id=source + "-stale",
                                             execution_date=None, db_connect=connect)
    newer_process = meta.allocate_process_id(dag_id="e2e_registry", run_id=source + "-newer",
                                             execution_date=None, db_connect=connect)
    stale = registry.plan_batch(**dict(arguments(stale_process), upper_bound="11"))
    competing = registry.plan_batch(**dict(arguments(newer_process), upper_bound="12"))
    registry.start_batch(**key(competing))
    registry.finalize_batch(**dict(key(competing), row_count=1, observed_max="12"),
                        compare_positions=lambda left, right: int(left) - int(right))
    try:
        registry.finalize_batch(**dict(key(stale), row_count=1, observed_max="11"),
                            compare_positions=lambda left, right: int(left) - int(right))
    except ValueError:
        print("PASS  stale revision finalization rejected")
    else:
        raise AssertionError("stale revision must not finalize")
    require(registry.get_batch(process_id=competing["process_id"], source_id=competing["source_id"],
                           entity_fqn=competing["entity_fqn"],
                           db_connect=connect)["watermark_after"] == "12",
            "stale failure preserves newer finalized state")
    wrong_process = meta.allocate_process_id(dag_id="e2e_registry", run_id=source + "-wrong-contract",
                                             execution_date=None, db_connect=connect)
    try:
        registry.plan_batch(**dict(arguments(wrong_process), incremental_column="other_position"))
    except ValueError:
        print("PASS  changed incremental contract requires migration")
    else:
        raise AssertionError("changed contract must not plan a batch")
    connection = PostgresHook(postgres_conn_id="ingestion_meta").get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT revision FROM ingestion.incremental_state WHERE source_id = %s FOR UPDATE", (source,))
            revision = cursor.fetchone()[0]
            cursor.execute("UPDATE ingestion.incremental_batches SET status = 'FAILED', "
                           "finished_at = clock_timestamp() WHERE process_id = %s AND source_id = %s "
                           "AND entity_fqn = 'synthetic.transactions'", (stale["process_id"], source))
            cursor.execute(
                "INSERT INTO ingestion.incremental_state_migrations "
                "(source_id, entity_fqn, old_revision, new_revision, "
                "old_column, new_column, old_type, new_type, old_watermark, new_watermark, performed_by, reason_text) "
                "VALUES (%s, %s, %s, %s, 'position', 'other_position', "
                "'bigint', 'bigint', '12', NULL, current_user, 'e2e stopped synthetic executor migration')",
                (source, "synthetic.transactions", revision, revision + 1),
            )
            cursor.execute(
                "UPDATE ingestion.incremental_state SET incremental_column = 'other_position', "
                "watermark = NULL, revision = revision + 1 WHERE source_id = %s AND revision = %s",
                (source, revision),
            )
            require(cursor.rowcount == 1, "migration state CAS updates one entity")
        connection.commit()
        migrated_process = meta.allocate_process_id(dag_id="e2e_registry", run_id=source + "-migrated",
                                                    execution_date=None, db_connect=connect)
        migrated = registry.plan_batch(**dict(arguments(migrated_process), incremental_column="other_position",
                                          upper_bound="90000"))
        require(migrated["reason"] == "bootstrap" and migrated["watermark_before"] is None,
                "migrated contract reboots from null watermark as bootstrap")
        registry.start_batch(**key(migrated))
        finalized = registry.finalize_batch(**dict(key(migrated), row_count=1, observed_max="90000"))
        require(finalized["watermark_after"] == "90000", "new contract bootstrap advances independently")
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM ingestion.incremental_state_migrations WHERE source_id = %s", (source,))
            require(cursor.fetchone()[0] == 1, "manual migration audit retained exactly once")
    finally:
        connection.close()


def main():
    """Check state, frozen bounds, no-op results, and process identity."""
    source = os.environ["E2E_SOURCE"]
    require(source in ("bank", "broker"), "known isolated source")
    connection = PostgresHook(postgres_conn_id="ingestion_meta").get_conn()
    try:
        with connection.cursor() as cursor:
            events = records(
                cursor,
                "SELECT t.*, d.run_id FROM ingestion.airflow_tasks_log t "
                "JOIN ingestion.airflow_dagruns_log d ON d.process_id = t.process_id "
                "AND d.status = 'START' WHERE d.run_id LIKE 'e2e-%' ORDER BY t.id",
            )
            require(bool(events), "load callbacks recorded lifecycle events")
            lifecycle = defaultdict(list)
            initial_loads = defaultdict(list)
            for event in events:
                lifecycle[event["process_id"], event["datalake_table"]].append(event["status"])
                if event["run_id"].startswith(("e2e-ingest1-", "e2e-snap1-")):
                    initial_loads[event["process_id"], event["datalake_table"]].append(event["status"])
            for statuses in lifecycle.values():
                require(statuses[0] == "START", "load lifecycle starts with START")
                require(all(status in {"START", "END", "FAILED"} for status in statuses),
                        "load callbacks use lifecycle statuses")
            require(any("END" in statuses for statuses in lifecycle.values()),
                    "successful load callbacks recorded END")
            require(any("FAILED" in statuses for statuses in lifecycle.values()),
                    "failed load callbacks recorded FAILED")
            namespace = "retail_bank" if source == "bank" else "trading_core"
            expected_targets = {
                f"raw.{source}.{namespace}__{table}"
                for table in ("clients", "deals", "transactions", "accounts", "transactions__act_snap")
            }
            require({target for _, target in initial_loads} == expected_targets,
                    "full, incremental and snapshot loads recorded lifecycle events")
            require(all(statuses[0] == "START" and statuses[-1] == "END"
                        for statuses in initial_loads.values()),
                    "initial successful loads recorded START and END")
            states = records(cursor, f"SELECT * FROM ingestion.incremental_state WHERE source_id = '{source}'")
            batches = records(
                cursor,
                f"SELECT * FROM ingestion.incremental_batches WHERE source_id = '{source}' "
                "ORDER BY process_id, source_id, entity_fqn",
            )
            require(len(states) == 1, "only the selected incremental entity has registry state")
            registered = {state["entity_fqn"] for state in states}
            expected = f"{namespace}.transactions"
            require(registered == {expected},
                    "registry tracks exactly the transactions entities")
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
                if batch["run_id"].startswith(("e2e-ingest2-",)):
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
        check_registry_recovery()
        check_snapshot_publication()
        print("PASS extraction metadata checks; FAIL=0")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
