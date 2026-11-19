"""Transactional pg-meta logging client shared by Airflow and the Spark driver.

Pure DB-API code: callers inject db_connect, so the package imports neither
Airflow nor any database driver. Connections are owned per call: one short
transaction, commit or rollback, then close. The incremental batch registry
lives in ingestion.meta.registry.
"""

from __future__ import annotations

from contextlib import contextmanager
import logging

from ingestion.logging import log_sql, log_step

LOG = logging.getLogger(__name__)


@contextmanager
def connection(db_connect):
    """Own a short DB-API transaction and close its connection."""
    conn = db_connect()
    try:
        conn.autocommit = False
        LOG.info("pg-meta transaction: BEGIN (implicit on first statement)")
        yield conn
        conn.commit()
        LOG.info("pg-meta transaction: COMMIT")
    except BaseException:
        conn.rollback()
        LOG.info("pg-meta transaction: ROLLBACK")
        raise
    finally:
        conn.close()


def _row(cursor):
    """Decode ordinary DB-API rows using cursor metadata."""
    row = cursor.fetchone()
    return dict(zip((column[0] for column in cursor.description), row)) if row is not None else None


def _execute(cursor, sql, params):
    """Log only application-owned SQL and its operational parameters."""
    if LOG.isEnabledFor(logging.INFO):
        _log_statement(cursor, sql, params)
    cursor.execute(sql, params)


def _log_statement(cursor, sql, params):
    """Adapt bind values for display only, independently of execution."""
    try:
        mogrify = getattr(cursor, "mogrify", None)
        if mogrify is not None:
            rendered = mogrify(sql, params)
            if isinstance(rendered, bytes):
                rendered = rendered.decode(cursor.connection.encoding or "utf-8")
        else:
            from psycopg import ClientCursor, Connection

            if not isinstance(cursor.connection, Connection):
                raise TypeError("Cursor has no SQL rendering support")
            with ClientCursor(cursor.connection) as renderer:
                rendered = renderer.mogrify(sql, params)
        if not isinstance(rendered, str):
            raise TypeError("Cursor returned no rendered SQL text")
    except Exception:
        log_sql(LOG, "pg-meta SQL (template)", sql)
        LOG.info("pg-meta SQL params=%r", params)
    else:
        log_sql(LOG, "pg-meta SQL", rendered)


@contextmanager
def _operation(db_connect, name, **key):
    """Publish affected rows only after the owning transaction has committed."""
    log_step(LOG, f"pg-meta: {name}")
    LOG.info("pg-meta begin operation=%s key=%r", name, key)
    changes = []
    try:
        with connection(db_connect) as conn:
            with conn.cursor() as cur:
                yield cur, changes
    except Exception as error:
        LOG.warning("pg-meta not committed operation=%s key=%r reason_type=%s",
                    name, key, type(error).__name__)
        raise
    for table, before, after in changes:
        LOG.info("pg-meta %s operation=%s table=ingestion.%s key=%r before=%r row=%r",
                 "committed" if after is not None else "no-op", name, table, key, before, after)
    if not changes:
        LOG.info("pg-meta no writes operation=%s key=%r", name, key)


def allocate_process_id(dag_id=None, run_id=None, execution_date=None, *, db_connect):
    """Insert a START event and return its fresh process_id.

    Every execution of the generating task allocates its own identifier;
    retried downstream tasks reuse the value passed through XCom instead.
    """
    with _operation(db_connect, "allocate_process_id", dag_id=dag_id, run_id=run_id) as (cur, changes):
        _execute(cur,
            "INSERT INTO ingestion.airflow_dagruns_log "
            "(process_id, dag_id, run_id, execution_date, status) "
            "VALUES (nextval('ingestion.process_id_seq'), %s, %s, %s, 'START') "
            "RETURNING *",
            (dag_id, run_id, execution_date),
        )
        row = _row(cur)
        changes.append(("airflow_dagruns_log", None, row))
        return int(row["process_id"])


def log_dagrun(process_id, dag_id, run_id, execution_date, status, *, db_connect):
    """Append a DagRun lifecycle event, ignoring duplicate start events."""
    with _operation(db_connect, "log_dagrun", process_id=process_id, status=status) as (cur, changes):
        _execute(cur,
            "INSERT INTO ingestion.airflow_dagruns_log "
            "(process_id, dag_id, run_id, execution_date, status) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING *",
            (process_id, dag_id, run_id, execution_date, status),
        )
        changes.append(("airflow_dagruns_log", None, _row(cur)))


def log_task(process_id, source_id, entity_fqn, datalake_table, status, *, db_connect):
    """Append a task lifecycle event."""
    with _operation(db_connect, "log_task", process_id=process_id, status=status) as (cur, changes):
        _execute(cur,
            "INSERT INTO ingestion.airflow_tasks_log "
            "(process_id, source_id, entity_fqn, datalake_table, status) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING *",
            (process_id, source_id, entity_fqn, datalake_table, status),
        )
        changes.append(("airflow_tasks_log", None, _row(cur)))


def find_process_id(dag_id, run_id, *, db_connect):
    """Find an identifier already assigned to a DagRun."""
    with _operation(db_connect, "find_process_id", dag_id=dag_id, run_id=run_id) as (cur, _):
        _execute(cur,
            "SELECT max(process_id) FROM ingestion.airflow_dagruns_log "
            "WHERE dag_id = %s AND run_id = %s", (dag_id, run_id),
        )
        value = cur.fetchone()[0]
        LOG.info("pg-meta find_process_id result=%s", value)
        return int(value) if value is not None else None
