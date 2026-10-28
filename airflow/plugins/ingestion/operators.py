"""Airflow operators for the raw-layer ingestion framework."""

from functools import partial

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import BaseHook, BaseOperator

from dlh_ingestion import meta


def _meta_connect(pg_conn_id: str = "ingestion_meta"):
    """Open a pg-meta connection through the Airflow Postgres provider."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=pg_conn_id).get_conn()


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
    return _validate_process_id(value) if value is not None else None


def _validate_process_id(value) -> int:
    """Require a positive integral process ID, including rendered XCom strings."""
    if type(value) is int and value > 0:
        return value
    if isinstance(value, str) and value.isascii() and value.isdecimal() and int(value) > 0:
        return int(value)
    raise ValueError("process_id must be a positive integer from generate_process_id")


def validate_run_conf(conf, entities):
    """Validate manual overrides before source activation, including API triggers."""
    conf = conf or {}
    if not isinstance(conf, dict) or set(conf) - {"force_full_snapshot_fqns", "reason"}:
        raise ValueError("unknown or invalid ingestion run configuration")
    fqns = conf.get("force_full_snapshot_fqns", [])
    reason = conf.get("reason", "")
    if not isinstance(fqns, list) or any(not isinstance(fqn, str) for fqn in fqns):
        raise ValueError("force_full_snapshot_fqns must be an array of FQNs")
    if len(set(fqns)) != len(fqns) or set(fqns) - set(entities):
        raise ValueError("duplicate or unknown FQN in force_full_snapshot_fqns")
    if not isinstance(reason, str) or (fqns and not reason.strip()):
        raise ValueError("manual full snapshot requires a non-empty reason")
    return fqns, reason


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
    """Allocate the run's __process_id, log dagrun START, push XCom."""

    def __init__(self, task_id="generate_process_id", pg_conn_id="ingestion_meta", entities=(), **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        self.pg_conn_id = pg_conn_id
        self.entities = entities

    def execute(self, context):
        dag_run = context["dag_run"]
        validate_run_conf(dag_run.conf, self.entities)
        return meta.get_or_create_process_id(
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
        parallelism: bool = True,
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
        if type(parallelism) is not bool:
            raise ValueError("parallelism must be a boolean")
        if type(num_partitions) is not int or num_partitions <= 0:
            raise ValueError("num_partitions must be a positive integer")
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
        self.source_type = source_type
        self.parallelism = parallelism
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
        jdbc_url, jdbc_user, jdbc_password = _resolve_jdbc(connection)
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
        if not self.parallelism:
            application_args.append("--no-parallelism")
        dag_run = context["dag_run"]
        conf = dag_run.conf or {}
        meta = BaseHook.get_connection(self.pg_conn_id)
        self.env_vars = dict(self.env_vars or {}, PGHOST=meta.host, PGPORT=str(meta.port or 5432),
                             PGDATABASE=meta.schema, PGUSER=meta.login, PGPASSWORD=meta.password)
        application_args += ["--source-id", self.source_id, "--dag-id", dag_run.dag_id,
                             "--run-id", dag_run.run_id, "--lookback-size", str(self.lookback_size)]
        if self.lookback_unit:
            application_args += ["--lookback-unit", self.lookback_unit]
        if self.lookback_specified:
            application_args.append("--lookback-given")
        if self.fqn in conf.get("force_full_snapshot_fqns", []):
            application_args += ["--force-full", "--reason", conf["reason"]]
        if self.incremental_column:
            application_args += ["--incremental-column", self.incremental_column]
        self.application_args = application_args
        super().execute(context)
