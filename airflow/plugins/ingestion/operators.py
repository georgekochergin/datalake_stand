"""Airflow operators for the raw-layer ingestion framework."""

from functools import partial

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.sdk import BaseHook, BaseOperator

from . import registry

JDBC_TEMPLATES = {
    "mssql": ("jdbc:sqlserver://{host}:{port};databaseName={schema};encrypt=false", 1433),
    "oracle": ("jdbc:oracle:thin:@//{host}:{port}/{schema}", 1521),
}


def _resolve_jdbc(connection) -> tuple[str, str, str]:
    """Build (jdbc_url, user, password) from an Airflow Connection with a JDBC dialect."""
    template = JDBC_TEMPLATES.get(connection.conn_type)
    if template is None:
        raise ValueError(
            f"connection {connection.conn_id!r} scheme {connection.conn_type!r} is not a "
            f"supported JDBC dialect (expected one of {sorted(JDBC_TEMPLATES)})"
        )
    url_template, default_port = template
    jdbc_url = url_template.format(
        host=connection.host,
        port=connection.port or default_port,
        schema=connection.schema,
    )
    return jdbc_url, connection.login, connection.password


def _pull_process_id(context) -> int | None:
    """Pull the run's process_id from the generate_process_id task, if present."""
    value = context["ti"].xcom_pull(task_ids="generate_process_id")
    return int(value) if value is not None else None


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
    registry.log_task(
        pg_conn_id=pg_conn_id,
        process_id=process_id,
        source_id=source_id,
        entity_fqn=entity_fqn,
        datalake_table=datalake_table,
        status="FAILED",
    )


class GenerateProcessIdOperator(BaseOperator):
    """Bootstrap the registry, allocate the run's __process_id, log dagrun START, push XCom."""

    def __init__(self, task_id="generate_process_id", pg_conn_id="ingestion_meta", **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        self.pg_conn_id = pg_conn_id

    def execute(self, context):
        registry.bootstrap(self.pg_conn_id)
        process_id = registry.next_process_id(self.pg_conn_id)
        dag_run = context["dag_run"]
        registry.log_dagrun(
            pg_conn_id=self.pg_conn_id,
            process_id=process_id,
            dag_id=dag_run.dag_id,
            run_id=dag_run.run_id,
            execution_date=dag_run.logical_date,
            status="START",
        )
        return process_id


def _dagrun_event(status: str, context) -> None:
    """Append a dagrun END/FAILED event for the run's allocated process_id."""
    dag_run = context["dag_run"]
    process_id = registry.find_process_id("ingestion_meta", dag_run.dag_id, dag_run.run_id)
    if process_id is None:
        return
    registry.log_dagrun(
        pg_conn_id="ingestion_meta",
        process_id=process_id,
        dag_id=dag_run.dag_id,
        run_id=dag_run.run_id,
        execution_date=None,
        status=status,
    )


def dagrun_success_callback(context) -> None:
    """DAG-level success callback: append the dagrun END event."""
    _dagrun_event("END", context)


def dagrun_failure_callback(context) -> None:
    """DAG-level failure callback: append the dagrun FAILED event."""
    _dagrun_event("FAILED", context)


class TableLogOperator(BaseOperator):
    """Append a task lifecycle event; on END advance the watermark for incremental entities."""

    template_fields = ("process_id",)

    def __init__(
        self,
        source_id: str,
        entity_fqn: str,
        datalake_table: str,
        status: str,
        incremental_column: str | None = None,
        trino_conn_id: str = "trino_default",
        pg_conn_id: str = "ingestion_meta",
        process_id: str = "{{ ti.xcom_pull(task_ids='generate_process_id') }}",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_id = source_id
        self.entity_fqn = entity_fqn
        self.datalake_table = datalake_table
        self.status = status
        self.incremental_column = incremental_column
        self.trino_conn_id = trino_conn_id
        self.pg_conn_id = pg_conn_id
        self.process_id = process_id

    def execute(self, context):
        process_id = int(self.process_id)
        registry.log_task(
            pg_conn_id=self.pg_conn_id,
            process_id=process_id,
            source_id=self.source_id,
            entity_fqn=self.entity_fqn,
            datalake_table=self.datalake_table,
            status=self.status,
        )
        if self.status == "END" and self.incremental_column:
            watermark = TrinoHook(trino_conn_id=self.trino_conn_id).get_first(
                f"SELECT max({self.incremental_column}) FROM {self.datalake_table} "
                f"WHERE __process_id = {process_id}"
            )[0]
            if watermark is not None:
                registry.set_watermark(
                    pg_conn_id=self.pg_conn_id,
                    source_id=self.source_id,
                    entity_fqn=self.entity_fqn,
                    watermark_value=watermark,
                    process_id=process_id,
                )


class IngestAppendOperator(SparkSubmitOperator):
    """Submit the raw-layer append job for one entity with resolved runtime parameters."""

    def __init__(
        self,
        source_id: str,
        airflow_conn_id: str,
        fqn: str,
        target_table: str,
        incremental_column: str | None = None,
        pg_conn_id: str = "ingestion_meta",
        **kwargs,
    ):
        super().__init__(
            application="/opt/spark-jobs/raw/load_entity.py",
            conn_id="spark_default",
            deploy_mode="client",
            **kwargs,
        )
        self.source_id = source_id
        self.airflow_conn_id = airflow_conn_id
        self.fqn = fqn
        self.target_table = target_table
        self.incremental_column = incremental_column
        self.pg_conn_id = pg_conn_id
        if self.on_failure_callback is None:
            self.on_failure_callback = partial(
                log_task_failed,
                source_id=source_id,
                entity_fqn=fqn,
                datalake_table=target_table,
                pg_conn_id=pg_conn_id,
            )

    def execute(self, context):
        connection = BaseHook.get_connection(self.airflow_conn_id)
        jdbc_url, jdbc_user, jdbc_password = _resolve_jdbc(connection)
        process_id = _pull_process_id(context)
        application_args = [
            "--jdbc-url",
            jdbc_url,
            "--jdbc-user",
            jdbc_user,
            "--jdbc-password",
            jdbc_password,
            "--source-table",
            self.fqn,
            "--target-table",
            self.target_table,
            "--process-id",
            str(process_id),
        ]
        if self.incremental_column:
            application_args += ["--incremental-column", self.incremental_column]
            watermark = registry.get_watermark(self.pg_conn_id, self.source_id, self.fqn)
            if watermark is not None:
                application_args += ["--watermark-value", watermark]
        self.application_args = application_args
        super().execute(context)
