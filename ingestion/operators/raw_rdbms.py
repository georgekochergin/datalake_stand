"""Submit operators for raw-layer loads over JDBC RDBMS sources."""

import json
import logging
import re
import shlex
from enum import Enum
from functools import partial

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import BaseHook
from psycopg2.extensions import make_dsn

from .process import _pull_process_id, log_task_event
from ingestion.logging import log_step
from ingestion.spark import guard as spark_guard
from ingestion.sources.rdbms import registry as rdbms_registry
from ingestion.sources.rdbms.contracts import ConnectionSpec
from ingestion.sources.rdbms.incremental_config import validate as validate_incremental_config

SUBMIT_CONN_ID = "spark_main_submit"
MASTER_HTTP_CONN_ID = "spark_main_master_http"
LOG = logging.getLogger(__name__)


class SparkGuardStatus(Enum):
    """Describe the per-execution admission check for task diagnostics."""

    CHECKING = "checking"
    ADMITTED = "admitted"
    REFUSED = "refused"
    UNAVAILABLE = "unavailable"


def log_spark_task_event(context):
    """Log guard diagnostics without master requests or exception text."""
    task = context.get("task") or getattr(context.get("ti"), "task", None)
    error = context.get("exception")
    LOG.info(
        "Spark task event state=%s target=%s application_ids=%s guard=%s reason_type=%s",
        getattr(context.get("ti"), "state", None),
        getattr(task, "target_table", None),
        getattr(task, "_guard_application_ids", None),
        getattr(task, "_guard_result", SparkGuardStatus.UNAVAILABLE).value,
        type(error).__name__ if error is not None else None,
    )


def run_spark_guard(operator):
    """Resolve Airflow configuration and record each execution's guard outcome."""
    log_step(LOG, "launch spark guard", number=1)
    operator._guard_application_ids = []
    operator._guard_result = SparkGuardStatus.CHECKING
    try:
        master = BaseHook.get_connection(MASTER_HTTP_CONN_ID)
        master_url = f"{master.schema}://{master.host}:{master.port}"
        operator._guard_application_ids = spark_guard.guard_spark_submit(
            master_url, operator.target_table,
        )
    except Exception as error:
        operator._guard_result = SparkGuardStatus.REFUSED
        LOG.warning("Spark guard target=%s result=refused reason_type=%s",
                    operator.target_table, type(error).__name__)
        raise
    operator._guard_result = SparkGuardStatus.ADMITTED


_VISIBLE_VALUES = frozenset({
    "--master", "--name", "--deploy-mode", "--source-table", "--target-table",
    "--process-id", "--num-jdbc-partitions", "--source-id", "--dag-id", "--run-id",
    "--incremental-config", "--num-executors", "--executor-cores", "--total-executor-cores",
    "--executor-memory", "--driver-memory",
})
_SWITCHES = frozenset({"--verbose", "--no-jdbc-partitioning"})
_FLAG = re.compile(r"--[a-zA-Z0-9][a-zA-Z0-9-]*")


def _format_submit_command(argv, application_index):
    """Render provider argv without splitting values or displaying unknown values."""
    lines = [shlex.quote(argv[0])]
    i = 1
    while i < len(argv):
        flag = argv[i]
        if i == application_index or flag in _SWITCHES:
            lines.append(shlex.quote(flag))
            i += 1
        elif _FLAG.fullmatch(flag) and i + 1 < len(argv) and i + 1 != application_index:
            value = argv[i + 1] if flag in _VISIBLE_VALUES else "<redacted>"
            lines.append(f"{flag} {shlex.quote(value)}")
            i += 2
        else:
            lines.append("<redacted>")
            i += 1
    return "\n  " + "\n  ".join(lines)


class _RdbmsSubmitOperator(SparkSubmitOperator):
    """Keep provider submission intact and replace its argv display only."""

    def _get_hook(self):
        """Format actual argv at the provider's 6.3.2 masking/logging boundary."""
        hook = super()._get_hook()
        provider_mask = hook._mask_cmd
        args = self.application_args or []
        credentials = [args[i + 1] for i, arg in enumerate(args[:-1])
                       if arg in ("--jdbc-url", "--jdbc-user", "--jdbc-password")]
        dsn = (self.env_vars or {}).get("INGESTION_META_DSN")
        secrets = sorted({value for value in [*credentials, dsn] if value}, key=len, reverse=True)

        def mask(argv):
            """Handle full argv and the provider's individual output-tail lines."""
            if isinstance(argv, list) and len(argv) > len(args) + 1:
                index = len(argv) - len(args) - 1
                if argv[index] == self.application:
                    return _format_submit_command(argv, index)
            lines = argv if isinstance(argv, list) else [argv]
            sanitized = []
            for line in lines:
                for secret in secrets:
                    line = line.replace(secret, "<redacted>")
                sanitized.append(line)
            return provider_mask(sanitized)

        hook._mask_cmd = mask
        return hook

    def execute(self, context):
        """Mark submit after guard admission and argument resolution."""
        log_step(LOG, "launch spark-submit cmd", number=2)
        return super().execute(context)


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


def _apply_entity_fields(
    operator,
    source_id: str,
    airflow_conn_id: str,
    fqn: str,
    target_table: str,
    source_type: str,
    jdbc_partitioning: bool,
    num_partitions: int,
    pg_conn_id: str = "ingestion_meta",
) -> None:
    """Assign submit fields and append best-effort lifecycle callbacks."""
    operator.source_id = source_id
    operator.airflow_conn_id = airflow_conn_id
    operator.fqn = fqn
    operator.target_table = target_table
    operator.source_type = source_type
    operator.jdbc_partitioning = jdbc_partitioning
    operator.num_partitions = num_partitions
    for field, status in (
        ("on_execute_callback", "START"),
        ("on_success_callback", "END"),
        ("on_failure_callback", "FAILED"),
    ):
        existing = getattr(operator, field, None)
        callbacks = list(existing) if isinstance(existing, list) else ([existing] if existing else [])
        callback = partial(
            log_task_event,
            source_id=source_id,
            entity_fqn=fqn,
            datalake_table=target_table,
            pg_conn_id=pg_conn_id,
            status=status,
        )
        setattr(operator, field, [*callbacks, callback])
    for field in ("on_retry_callback", "on_failure_callback"):
        existing = getattr(operator, field, None)
        callbacks = list(existing) if isinstance(existing, list) else ([existing] if existing else [])
        setattr(operator, field, [*callbacks, log_spark_task_event])


class IngestActSnapOperator(_RdbmsSubmitOperator):
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
            conn_id=SUBMIT_CONN_ID,
            deploy_mode="client",
            **kwargs,
        )
        _apply_entity_fields(
            self, source_id, airflow_conn_id, fqn, target_table,
            source_type, jdbc_partitioning, num_partitions,
        )

    def execute(self, context):
        run_spark_guard(self)
        _, self.application_args = _submit_args(self, context)
        super().execute(context)


class IngestFullOperator(_RdbmsSubmitOperator):
    """Submit the full-slice job for one entity with resolved runtime parameters.

    The full loader never touches the incremental registry, so the driver
    receives no pg-meta credentials: lifecycle logging (START/END/FAILED)
    stays on the Airflow side through best-effort task callbacks.
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
            conn_id=SUBMIT_CONN_ID,
            deploy_mode="client",
            **kwargs,
        )
        _apply_entity_fields(
            self, source_id, airflow_conn_id, fqn, target_table,
            source_type, jdbc_partitioning, num_partitions,
        )

    def execute(self, context):
        run_spark_guard(self)
        _, args = _submit_args(self, context)
        dag_run = context["dag_run"]
        args += ["--source-id", self.source_id, "--dag-id", dag_run.dag_id,
                 "--run-id", dag_run.run_id]
        self.application_args = args
        super().execute(context)


class IngestIncrementalOperator(_RdbmsSubmitOperator):
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
            conn_id=SUBMIT_CONN_ID,
            deploy_mode="client",
            **kwargs,
        )
        _apply_entity_fields(
            self, source_id, airflow_conn_id, fqn, target_table,
            source_type, jdbc_partitioning, num_partitions, pg_conn_id=pg_conn_id,
        )
        self.incremental_config = incremental_config
        self.pg_conn_id = pg_conn_id

    def execute(self, context):
        run_spark_guard(self)
        _, args = _submit_args(self, context)
        dag_run = context["dag_run"]
        args += ["--source-id", self.source_id, "--dag-id", dag_run.dag_id,
                 "--run-id", dag_run.run_id,
                 "--incremental-config", json.dumps(self.incremental_config)]
        meta = BaseHook.get_connection(self.pg_conn_id)
        self.env_vars = dict(self.env_vars or {}, INGESTION_META_DSN=_pgmeta_dsn(meta))
        self.application_args = args
        super().execute(context)
