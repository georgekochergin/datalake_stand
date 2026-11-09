"""Airflow operators for the raw-layer ingestion framework."""

import json
from functools import partial
from pathlib import Path

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import BaseHook, BaseOperator
from airflow.sdk.exceptions import AirflowSkipException

from ingestion import meta
from ingestion.sources.rdbms import registry as rdbms_registry
from ingestion.sources.rdbms.contracts import ConnectionSpec


def _meta_connect(pg_conn_id: str = "ingestion_meta"):
    """Open a pg-meta connection through the Airflow Postgres provider."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=pg_conn_id).get_conn()


def resolve_source_connection(connection) -> tuple:
    """Build (jdbc_url, user, password) from an Airflow Connection via the dialect registry."""
    dialect = rdbms_registry.by_conn_type(connection.conn_type)
    jdbc_url = dialect.build_jdbc_url(ConnectionSpec(
        host=connection.host,
        port=connection.port,
        database=connection.schema,
        user=connection.login,
        password=connection.password,
    ))
    return jdbc_url, connection.login, connection.password


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


def read_snapshot_marker(path: str) -> bool:
    """Read the snapshot driver's result marker and return the overwritten flag.

    The marker is written by the Spark driver in client mode, which shares the
    filesystem with this operator. A missing or malformed marker after a
    successful submission is a contract violation: the task fails and the
    retry re-checks publication through the table property.
    """
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ValueError(f"snapshot result marker {path!r} is missing or malformed: {error}") from error
    if not isinstance(payload, dict) or type(payload.get("overwritten")) is not bool:
        raise ValueError(f"snapshot result marker {path!r} must contain the boolean 'overwritten'")
    return payload["overwritten"]


class IngestSnapshotOperator(SparkSubmitOperator):
    """Submit the actual-snapshot job for one entity with resolved runtime parameters.

    The snapshot stream runs outside the incremental registry: no batch is
    planned or finalized in pg-meta, so no pg-meta environment is passed to
    the application. The job reports whether it actually replaced the table
    through a result marker read after the submission.
    """

    def __init__(
        self,
        source_id: str,
        airflow_conn_id: str,
        fqn: str,
        target_table: str,
        source_type: str = "rdbms",
        jdbc_partitioning: bool = True,
        num_partitions: int = 4,
        result_dir: str = "/tmp",
        **kwargs,
    ):
        if not isinstance(source_type, str):
            raise ValueError("source_type must be a string")
        if type(jdbc_partitioning) is not bool:
            raise ValueError("jdbc_partitioning must be a boolean")
        if type(num_partitions) is not int or num_partitions <= 0:
            raise ValueError("num_partitions must be a positive integer")
        super().__init__(
            application="/opt/spark-jobs/raw/rdbms/incremental/load_act_snap.py",
            conn_id="spark_default",
            deploy_mode="client",
            **kwargs,
        )
        self.source_id = source_id
        self.airflow_conn_id = airflow_conn_id
        self.fqn = fqn
        self.target_table = target_table
        self.source_type = source_type
        self.jdbc_partitioning = jdbc_partitioning
        self.num_partitions = num_partitions
        self.result_dir = result_dir
        if self.on_failure_callback is None:
            self.on_failure_callback = partial(
                log_task_failed,
                source_id=source_id,
                entity_fqn=fqn,
                datalake_table=target_table,
            )

    def execute(self, context):
        if self.source_type != "rdbms":
            raise ValueError(f"source_type {self.source_type!r} is not supported by JDBC ingestion")
        connection = BaseHook.get_connection(self.airflow_conn_id)
        jdbc_url, jdbc_user, jdbc_password = resolve_source_connection(connection)
        process_id = _pull_process_id(context)
        if process_id is None:
            raise ValueError("missing process_id from generate_process_id")
        marker_path = f"{self.result_dir}/snapshot_result_{process_id}.json"
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
            "--num-partitions",
            str(self.num_partitions),
            "--result-marker",
            marker_path,
        ]
        if not self.jdbc_partitioning:
            application_args.append("--no-jdbc-partitioning")
        self.application_args = application_args
        super().execute(context)
        return read_snapshot_marker(marker_path)


class PublishSnapshotAssetOperator(BaseOperator):
    """Publish the snapshot Asset only when the load task actually replaced the table.

    A run that verified an empty source without overwriting succeeds without
    publication: this task raises AirflowSkipException so the outlet Asset
    emits no event. The overwrite decision is made by the load task and read
    from its XCom.
    """

    def __init__(self, load_task_id: str, **kwargs):
        super().__init__(**kwargs)
        self.load_task_id = load_task_id

    def execute(self, context):
        overwritten = context["ti"].xcom_pull(task_ids=self.load_task_id)
        if overwritten is not True:
            raise AirflowSkipException(
                f"snapshot table was not overwritten by {self.load_task_id}; asset not published"
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
        source_type: str = "rdbms",
        jdbc_partitioning: bool = True,
        num_partitions: int = 4,
        lookback_size: int = 0,
        lookback_unit: str | None = None,
        lookback_specified: bool = False,
        **kwargs,
    ):
        if not isinstance(source_type, str):
            raise ValueError("source_type must be a string")
        if type(lookback_size) is not int or lookback_size < 0:
            raise ValueError("lookback_size must be a nonnegative integer")
        if type(lookback_specified) is not bool:
            raise ValueError("lookback_specified must be a boolean")
        if type(jdbc_partitioning) is not bool:
            raise ValueError("jdbc_partitioning must be a boolean")
        if type(num_partitions) is not int or num_partitions <= 0:
            raise ValueError("num_partitions must be a positive integer")
        application = (
            "/opt/spark-jobs/raw/rdbms/incremental/load_entity.py"
            if incremental_column
            else "/opt/spark-jobs/raw/rdbms/full/load_entity.py"
        )
        super().__init__(
            application=application,
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
        self.source_type = source_type
        self.jdbc_partitioning = jdbc_partitioning
        self.num_partitions = num_partitions
        self.lookback_size = lookback_size
        self.lookback_unit = lookback_unit
        self.lookback_specified = lookback_specified
        if self.on_failure_callback is None:
            self.on_failure_callback = partial(
                log_task_failed,
                source_id=source_id,
                entity_fqn=fqn,
                datalake_table=target_table,
                pg_conn_id=pg_conn_id,
            )

    def execute(self, context):
        if self.source_type != "rdbms":
            raise ValueError(f"source_type {self.source_type!r} is not supported by JDBC ingestion")
        connection = BaseHook.get_connection(self.airflow_conn_id)
        jdbc_url, jdbc_user, jdbc_password = resolve_source_connection(connection)
        process_id = _pull_process_id(context)
        if process_id is None:
            raise ValueError("missing process_id from generate_process_id")
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
            "--num-partitions",
            str(self.num_partitions),
        ]
        if not self.jdbc_partitioning:
            application_args.append("--no-jdbc-partitioning")
        dag_run = context["dag_run"]
        meta = BaseHook.get_connection(self.pg_conn_id)
        self.env_vars = dict(self.env_vars or {}, PGHOST=meta.host, PGPORT=str(meta.port or 5432),
                             PGDATABASE=meta.schema, PGUSER=meta.login, PGPASSWORD=meta.password)
        application_args += ["--source-id", self.source_id, "--dag-id", dag_run.dag_id,
                             "--run-id", dag_run.run_id]
        if self.incremental_column:
            application_args += ["--incremental-column", self.incremental_column,
                                 "--lookback-size", str(self.lookback_size)]
            if self.lookback_unit:
                application_args += ["--lookback-unit", self.lookback_unit]
            if self.lookback_specified:
                application_args.append("--lookback-given")
        self.application_args = application_args
        super().execute(context)
