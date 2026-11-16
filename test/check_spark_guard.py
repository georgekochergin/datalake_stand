"""Exercise the real guard and all three load operators inside the isolated stack."""

import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch
from urllib.request import urlopen
from uuid import uuid4

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseHook

from ingestion import meta
from ingestion.operators import IngestActSnapOperator, IngestFullOperator, IngestIncrementalOperator
from ingestion.operators import raw_rdbms
from ingestion.spark import guard as spark_guard


def require(condition, message):
    """Report a checked runtime invariant and fail the containing test script."""
    if not condition:
        raise AssertionError(message)
    print("PASS " + message, flush=True)


def snapshot():
    """Read only the test network master used by the configured HTTP Connection."""
    master = BaseHook.get_connection(raw_rdbms.MASTER_HTTP_CONN_ID)
    with urlopen(f"{master.schema}://{master.host}:{master.port}/json/", timeout=5) as response:
        return json.load(response)


def orphan(target, directory):
    """Kill a disposable parent process after its Spark child has registered."""
    token = uuid4().hex
    logfile = Path(directory) / (token + ".log")
    pidfile = Path(directory) / (token + ".pid")
    command = [
        "spark-submit", "--master", "spark://spark-master:7077", "--deploy-mode", "client",
        "--conf", "spark.cores.max=1", "--conf", "spark.executor.memory=512m",
        "/opt/spark-jobs/e2e_guard_idle.py", target,
    ]
    code = (
        "import subprocess,sys,time,pathlib; "
        "child=subprocess.Popen(sys.argv[2:]); "
        "pathlib.Path(sys.argv[1]).write_text(str(child.pid)); time.sleep(240)"
    )
    stream = logfile.open("w")
    parent = subprocess.Popen(["python", "-c", code, str(pidfile), *command], stdout=stream, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 60
    try:
        while time.monotonic() < deadline:
            apps = [app for app in snapshot()["activeapps"] if app["name"] == target]
            if apps:
                os.kill(parent.pid, signal.SIGKILL)
                parent.wait(timeout=5)
                require(any(app["id"] == apps[0]["id"] for app in snapshot()["activeapps"]), "registered application remains after parent SIGKILL")
                return apps[0]["id"], int(pidfile.read_text()), stream
            if parent.poll() is not None:
                raise AssertionError("disposable parent exited before registration: " + str(logfile))
            time.sleep(0.2)
        raise TimeoutError("disposable Spark registration: " + str(logfile))
    except BaseException:
        if parent.poll() is None:
            parent.kill()
            parent.wait(timeout=5)
        stream.close()
        raise


def main():
    """Run real JDBC submissions after master-confirmed cleanup, then fail-closed cases."""
    connection = BaseHook.get_connection(raw_rdbms.MASTER_HTTP_CONN_ID)
    require(connection.host == "spark-master", "HTTP Connection addresses isolated compose master")
    connect = PostgresHook(postgres_conn_id="ingestion_meta").get_conn
    config = {"contract": {"source_column": "_batch_id", "position_type": "bigint"}, "lookback": {"size": 0, "unit": None}}
    cases = (
        (IngestFullOperator, "retail_bank.accounts", "raw.bank.retail_bank__accounts", {}),
        (IngestIncrementalOperator, "retail_bank.transactions", "raw.bank.retail_bank__transactions", {"incremental_config": config}),
        (IngestActSnapOperator, "retail_bank.transactions", "raw.bank.retail_bank__transactions__act_snap", {}),
    )
    with tempfile.TemporaryDirectory(prefix="e2e-spark-guard-") as directory:
        other_target = "raw.bank.e2e_guard_other"
        other_id, _, other_stream = orphan(other_target, directory)
        try:
            for cls, fqn, target, extra in cases:
                application_id, _, stream = orphan(target, directory)
                run_id = "e2e_guard_" + uuid4().hex
                process_id = meta.allocate_process_id(dag_id="e2e_guard", run_id=run_id, execution_date=None, db_connect=connect)
                ti = SimpleNamespace(xcom_pull=lambda **kwargs: process_id)
                context = {"ti": ti, "dag_run": SimpleNamespace(dag_id="e2e_guard", run_id=run_id)}
                operator = cls(task_id="guard_load", source_id="bank", airflow_conn_id="bank_source", fqn=fqn, target_table=target, jdbc_partitioning=False, conf={"spark.cores.max": "1", "spark.executor.cores": "1", "spark.executor.memory": "512m"}, **extra)
                try:
                    operator.execute(context)
                    require(application_id in operator._guard_application_ids, cls.__name__ + " cleaned original application before real JDBC submit")
                    state = snapshot()
                    require(not any(app["id"] == application_id for app in state["activeapps"]), cls.__name__ + " original ID inactive")
                    require(any(app["id"] == application_id and app["state"] in ("KILLED", "FINISHED") for app in state["completedapps"]), cls.__name__ + " completion evidence retained")
                    require(any(app["id"] == other_id for app in state["activeapps"]), "other target was not stopped")
                finally:
                    stream.close()
                operator = cls(task_id="refused_load", source_id="bank", airflow_conn_id="bank_source", fqn=fqn, target_table=target, **extra)
                actual_get = BaseHook.get_connection
                def unavailable(key):
                    if key == raw_rdbms.MASTER_HTTP_CONN_ID:
                        return SimpleNamespace(conn_type="http", host="127.0.0.1", port=1, schema="http", login=None, password=None)
                    return actual_get(key)
                with patch.object(BaseHook, "get_connection", side_effect=unavailable), patch.object(SparkSubmitOperator, "execute") as submit:
                    try:
                        operator.execute(context)
                    except spark_guard.SparkGuardError:
                        pass
                    else:
                        raise AssertionError("unavailable master permitted submit")
                    require(submit.call_count == 0, cls.__name__ + " unavailable master prohibits submit")
        finally:
            master = BaseHook.get_connection(raw_rdbms.MASTER_HTTP_CONN_ID)
            spark_guard.guard_spark_submit(f"{master.schema}://{master.host}:{master.port}", other_target)
            other_stream.close()
    print("PASS Spark guard runtime checks; FAIL=0", flush=True)


if __name__ == "__main__":
    main()
