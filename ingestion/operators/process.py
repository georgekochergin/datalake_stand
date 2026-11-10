"""Airflow-side process orchestration: process_id, lifecycle logging, and DAG callbacks.

Knows nothing about source types, load modes, or data layers.
"""

from functools import partial

from airflow.sdk import BaseOperator

from ingestion import meta


def _meta_connect(pg_conn_id: str = "ingestion_meta"):
    """Open a pg-meta connection through the Airflow Postgres provider."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=pg_conn_id).get_conn()


def _pull_process_id(context) -> int | None:
    """Pull the run's process_id from the generate_process_id task, if present."""
    value = context["ti"].xcom_pull(task_ids="generate_process_id")
    return _validate_process_id(value) if value is not None else None


def _validate_process_id(value) -> int:
    """Require a positive integral process ID, including rendered XCom strings."""
    if type(value) is int and value > 0:
        return value
    if isinstance(value, str) and value.isascii() and value.isdecimal() and int(value) > 0:
        return int(value)
    raise ValueError("process_id must be a positive integer from generate_process_id")


def log_task_failed(
    context,
    source_id: str,
    entity_fqn: str,
    datalake_table: str,
    pg_conn_id: str = "ingestion_meta",
) -> None:
    """Append the task FAILED event; attachable via functools.partial (serializable)."""
    process_id = _pull_process_id(context)
    if process_id is None:
        return
    meta.log_task(
        process_id=process_id,
        source_id=source_id,
        entity_fqn=entity_fqn,
        datalake_table=datalake_table,
        status="FAILED",
        db_connect=partial(_meta_connect, pg_conn_id),
    )


class GenerateProcessIdOperator(BaseOperator):
    """Allocate this execution's process_id, log dagrun START, push XCom.

    Every execution, including a retry of this task itself, allocates a fresh
    process_id: a retried task reuses the value from XCom instead.
    """

    def __init__(self, task_id="generate_process_id", pg_conn_id="ingestion_meta", **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        self.pg_conn_id = pg_conn_id

    def execute(self, context):
        dag_run = context["dag_run"]
        return meta.allocate_process_id(
            dag_id=dag_run.dag_id, run_id=dag_run.run_id,
            execution_date=dag_run.logical_date,
            db_connect=partial(_meta_connect, self.pg_conn_id),
        )


def _dagrun_event(status: str, context) -> None:
    """Append a dagrun END/FAILED event for the run's allocated process_id."""
    dag_run = context["dag_run"]
    connect = partial(_meta_connect, "ingestion_meta")
    process_id = meta.find_process_id(dag_run.dag_id, dag_run.run_id, db_connect=connect)
    if process_id is None:
        return
    meta.log_dagrun(
        process_id=process_id,
        dag_id=dag_run.dag_id,
        run_id=dag_run.run_id,
        execution_date=None,
        status=status,
        db_connect=connect,
    )


def dagrun_success_callback(context) -> None:
    """DAG-level success callback: append the dagrun END event."""
    _dagrun_event("END", context)


def dagrun_failure_callback(context) -> None:
    """DAG-level failure callback: append the dagrun FAILED event."""
    _dagrun_event("FAILED", context)


class TableLogOperator(BaseOperator):
    """Append lifecycle events after managed loading has finalized its own state."""

    template_fields = ("process_id",)

    def __init__(
        self,
        source_id: str,
        entity_fqn: str,
        datalake_table: str,
        status: str,
        pg_conn_id: str = "ingestion_meta",
        process_id: str = "{{ ti.xcom_pull(task_ids='generate_process_id') }}",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_id = source_id
        self.entity_fqn = entity_fqn
        self.datalake_table = datalake_table
        self.status = status
        self.pg_conn_id = pg_conn_id
        self.process_id = process_id

    def execute(self, context):
        process_id = _validate_process_id(self.process_id)
        meta.log_task(
            process_id=process_id,
            source_id=self.source_id,
            entity_fqn=self.entity_fqn,
            datalake_table=self.datalake_table,
            status=self.status,
            db_connect=partial(_meta_connect, self.pg_conn_id),
        )
