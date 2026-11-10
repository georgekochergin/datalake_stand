"""Submit operators for raw-layer loads over JDBC RDBMS sources."""

import json
from functools import partial

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import BaseHook
from psycopg2.extensions import make_dsn

from .process import _pull_process_id, log_task_failed
from ingestion.sources.rdbms import registry as rdbms_registry
from ingestion.sources.rdbms.contracts import ConnectionSpec
from ingestion.sources.rdbms.incremental_config import validate as validate_incremental_config


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


def _pgmeta_dsn(connection) -> str:
    """Serialize the pg-meta Airflow Connection into an escaped libpq connection string."""
    return make_dsn(
        host=connection.host,
        port=connection.port or 5432,
        dbname=connection.schema,
        user=connection.login,
        password=connection.password,
    )


def _strict_submit_options(source_type, jdbc_partitioning, num_partitions) -> None:
    """Reject malformed shared submit options before any Spark wiring."""
    if not isinstance(source_type, str):
        raise ValueError("source_type must be a string")
    if type(jdbc_partitioning) is not bool:
        raise ValueError("jdbc_partitioning must be a boolean")
    if type(num_partitions) is not int or num_partitions <= 0:
        raise ValueError("num_partitions must be a positive integer")


def _submit_args(operator, context) -> tuple:
    """Resolve runtime credentials and build the arguments shared by the submit operators."""
    if operator.source_type != "rdbms":
        raise ValueError(f"source_type {operator.source_type!r} is not supported by JDBC ingestion")
    connection = BaseHook.get_connection(operator.airflow_conn_id)
    jdbc_url, jdbc_user, jdbc_password = resolve_source_connection(connection)
    process_id = _pull_process_id(context)
    if process_id is None:
        raise ValueError("missing process_id from generate_process_id")
    args = [
        "--jdbc-url",
        jdbc_url,
        "--jdbc-user",
        jdbc_user,
        "--jdbc-password",
        jdbc_password,
        "--source-table",
        operator.fqn,
        "--target-table",
        operator.target_table,
        "--process-id",
        str(process_id),
    ]
    if operator.jdbc_partitioning:
        args += ["--num-jdbc-partitions", str(operator.num_partitions)]
    else:
        args.append("--no-jdbc-partitioning")
    return process_id, args


class IngestActSnapOperator(SparkSubmitOperator):
    """Submit the actual-snapshot job for one entity with resolved runtime parameters.

    The snapshot stream runs outside the incremental registry: no batch is
    planned or finalized in pg-meta, so no pg-meta environment is passed to
    the application. Asset publication independently checks the committed table.
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
        **kwargs,
    ):
        _strict_submit_options(source_type, jdbc_partitioning, num_partitions)
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
        if self.on_failure_callback is None:
            self.on_failure_callback = partial(
                log_task_failed,
                source_id=source_id,
                entity_fqn=fqn,
                datalake_table=target_table,
            )

    def execute(self, context):
        _, self.application_args = _submit_args(self, context)
        super().execute(context)


class IngestFullOperator(SparkSubmitOperator):
    """Submit the full-slice job for one entity with resolved runtime parameters.

    The full loader never touches the incremental registry, so the driver
    receives no pg-meta credentials: lifecycle logging (START/END/FAILED)
    stays on the Airflow side through TableLogOperator and the failure
    callback.
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
        **kwargs,
    ):
        _strict_submit_options(source_type, jdbc_partitioning, num_partitions)
        super().__init__(
            application="/opt/spark-jobs/raw/rdbms/full/load_entity.py",
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
        if self.on_failure_callback is None:
            self.on_failure_callback = partial(
                log_task_failed,
                source_id=source_id,
                entity_fqn=fqn,
                datalake_table=target_table,
            )

    def execute(self, context):
        _, args = _submit_args(self, context)
        dag_run = context["dag_run"]
        args += ["--source-id", self.source_id, "--dag-id", dag_run.dag_id,
                 "--run-id", dag_run.run_id]
        self.application_args = args
        super().execute(context)


class IngestIncrementalOperator(SparkSubmitOperator):
    """Submit the fixed-window incremental job with the batch-registry environment.

    The driver reaches the pg-meta registry itself, so the operator hands it
    the credentials as one explicit DSN in INGESTION_META_DSN, built from the
    ingestion_meta Airflow Connection.
    """

    def __init__(
        self,
        source_id: str,
        airflow_conn_id: str,
        fqn: str,
        target_table: str,
        incremental_config: dict,
        pg_conn_id: str = "ingestion_meta",
        source_type: str = "rdbms",
        jdbc_partitioning: bool = True,
        num_partitions: int = 4,
        **kwargs,
    ):
        _strict_submit_options(source_type, jdbc_partitioning, num_partitions)
        validate_incremental_config(incremental_config)
        super().__init__(
            application="/opt/spark-jobs/raw/rdbms/incremental/load_entity.py",
            conn_id="spark_default",
            deploy_mode="client",
            **kwargs,
        )
        self.source_id = source_id
        self.airflow_conn_id = airflow_conn_id
        self.fqn = fqn
        self.target_table = target_table
        self.incremental_config = incremental_config
        self.pg_conn_id = pg_conn_id
        self.source_type = source_type
        self.jdbc_partitioning = jdbc_partitioning
        self.num_partitions = num_partitions
        if self.on_failure_callback is None:
            self.on_failure_callback = partial(
                log_task_failed,
                source_id=source_id,
                entity_fqn=fqn,
                datalake_table=target_table,
                pg_conn_id=pg_conn_id,
            )

    def execute(self, context):
        _, args = _submit_args(self, context)
        dag_run = context["dag_run"]
        args += ["--source-id", self.source_id, "--dag-id", dag_run.dag_id,
                 "--run-id", dag_run.run_id,
                 "--incremental-config", json.dumps(self.incremental_config)]
        meta = BaseHook.get_connection(self.pg_conn_id)
        self.env_vars = dict(self.env_vars or {}, INGESTION_META_DSN=_pgmeta_dsn(meta))
        self.application_args = args
        super().execute(context)
