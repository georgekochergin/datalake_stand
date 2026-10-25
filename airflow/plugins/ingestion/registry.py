"""Operational registry access on pg-meta: sequence, event logs, watermarks."""

from airflow.providers.postgres.hooks.postgres import PostgresHook

BOOTSTRAP_STATEMENTS = (
    "CREATE SCHEMA IF NOT EXISTS ingestion",
    "CREATE SEQUENCE IF NOT EXISTS ingestion.process_id_seq",
    """CREATE TABLE IF NOT EXISTS ingestion.airflow_dagruns_log (
  id bigserial PRIMARY KEY,
  process_id bigint NOT NULL,
  dag_id text NOT NULL,
  run_id text NOT NULL,
  execution_date timestamptz,
  ts timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL)""",
    """CREATE INDEX IF NOT EXISTS airflow_dagruns_log_process_id_idx
  ON ingestion.airflow_dagruns_log (process_id)""",
    """CREATE TABLE IF NOT EXISTS ingestion.airflow_tasks_log (
  id bigserial PRIMARY KEY,
  process_id bigint NOT NULL,
  source_id text NOT NULL,
  entity_fqn text NOT NULL,
  datalake_table text NOT NULL,
  ts timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL)""",
    """CREATE INDEX IF NOT EXISTS airflow_tasks_log_run_idx
  ON ingestion.airflow_tasks_log (process_id, entity_fqn)""",
    """CREATE TABLE IF NOT EXISTS ingestion.table_watermarks (
  source_id text NOT NULL,
  entity_fqn text NOT NULL,
  watermark_value text NOT NULL,
  process_id bigint NOT NULL,
  updated_dttm timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source_id, entity_fqn))""",
)

DAGRUN_LOG_SQL = (
    "INSERT INTO ingestion.airflow_dagruns_log "
    "(process_id, dag_id, run_id, execution_date, status) "
    "VALUES (%s, %s, %s, %s, %s)"
)

TASK_LOG_SQL = (
    "INSERT INTO ingestion.airflow_tasks_log "
    "(process_id, source_id, entity_fqn, datalake_table, status) "
    "VALUES (%s, %s, %s, %s, %s)"
)

WATERMARK_UPSERT_SQL = (
    "INSERT INTO ingestion.table_watermarks "
    "(source_id, entity_fqn, watermark_value, process_id) "
    "VALUES (%s, %s, %s, %s) "
    "ON CONFLICT (source_id, entity_fqn) DO UPDATE SET "
    "watermark_value = EXCLUDED.watermark_value, "
    "process_id = EXCLUDED.process_id, "
    "updated_dttm = now()"
)

FIND_PROCESS_SQL = (
    "SELECT max(process_id) FROM ingestion.airflow_dagruns_log "
    "WHERE dag_id = %s AND run_id = %s"
)


def _hook(pg_conn_id: str) -> PostgresHook:
    """Return a Postgres hook for a pg-meta connection id."""
    return PostgresHook(postgres_conn_id=pg_conn_id)


def bootstrap(pg_conn_id: str = "ingestion_meta") -> None:
    """Idempotently create the ingestion schema, sequence, tables, and indexes."""
    _hook(pg_conn_id).run(BOOTSTRAP_STATEMENTS)


def next_process_id(pg_conn_id: str = "ingestion_meta") -> int:
    """Allocate the next __process_id from the sequence."""
    return int(_hook(pg_conn_id).get_first("SELECT nextval('ingestion.process_id_seq')")[0])


def log_dagrun(
    pg_conn_id: str,
    process_id: int,
    dag_id: str,
    run_id: str,
    execution_date,
    status: str,
) -> None:
    """Append a dagrun lifecycle event."""
    _hook(pg_conn_id).run(
        DAGRUN_LOG_SQL,
        parameters=(process_id, dag_id, run_id, execution_date, status),
    )


def log_task(
    pg_conn_id: str,
    process_id: int,
    source_id: str,
    entity_fqn: str,
    datalake_table: str,
    status: str,
) -> None:
    """Append a task lifecycle event."""
    _hook(pg_conn_id).run(
        TASK_LOG_SQL,
        parameters=(process_id, source_id, entity_fqn, datalake_table, status),
    )


def find_process_id(pg_conn_id: str, dag_id: str, run_id: str) -> int | None:
    """Return the process_id allocated in the given DAG run, if any."""
    row = _hook(pg_conn_id).get_first(FIND_PROCESS_SQL, parameters=(dag_id, run_id))
    return int(row[0]) if row and row[0] is not None else None


def get_watermark(pg_conn_id: str, source_id: str, entity_fqn: str) -> str | None:
    """Return the stored watermark for an incremental entity, or None before the first load."""
    row = _hook(pg_conn_id).get_first(
        "SELECT watermark_value FROM ingestion.table_watermarks "
        "WHERE source_id = %s AND entity_fqn = %s",
        parameters=(source_id, entity_fqn),
    )
    return str(row[0]) if row and row[0] is not None else None


def set_watermark(
    pg_conn_id: str,
    source_id: str,
    entity_fqn: str,
    watermark_value,
    process_id: int,
) -> None:
    """Persist the watermark advanced by the given run."""
    _hook(pg_conn_id).run(
        WATERMARK_UPSERT_SQL,
        parameters=(source_id, entity_fqn, str(watermark_value), process_id),
    )
