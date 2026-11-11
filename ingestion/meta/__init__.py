"""Transactional pg-meta logging client shared by Airflow and the Spark driver.

Pure DB-API code: callers inject db_connect, so the package imports neither
Airflow nor any database driver. Connections are owned per call: one short
transaction, commit or rollback, then close. The incremental batch registry
lives in ingestion.meta.registry.
"""

from __future__ import annotations

from contextlib import contextmanager


@contextmanager
def connection(db_connect):
    """Own a short DB-API transaction and close its connection."""
    conn = db_connect()
    try:
        conn.autocommit = False
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def _row(cursor):
    """Decode ordinary DB-API rows using cursor metadata."""
    row = cursor.fetchone()
    return dict(zip((column[0] for column in cursor.description), row)) if row is not None else None


def allocate_process_id(dag_id=None, run_id=None, execution_date=None, *, db_connect):
    """Insert a START event and return its fresh process_id.

    Every execution of the generating task allocates its own identifier;
    retried downstream tasks reuse the value passed through XCom instead.
    """
    with connection(db_connect) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.airflow_dagruns_log "
            "(process_id, dag_id, run_id, execution_date, status) "
            "VALUES (nextval('ingestion.process_id_seq'), %s, %s, %s, 'START') "
            "RETURNING process_id",
            (dag_id, run_id, execution_date),
        )
        return int(cur.fetchone()[0])


def log_dagrun(process_id, dag_id, run_id, execution_date, status, *, db_connect):
    """Append a DagRun lifecycle event, ignoring duplicate start events."""
    with connection(db_connect) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.airflow_dagruns_log "
            "(process_id, dag_id, run_id, execution_date, status) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (process_id, dag_id, run_id, execution_date, status),
        )


def log_task(process_id, source_id, entity_fqn, datalake_table, status, *, db_connect):
    """Append a task lifecycle event."""
    with connection(db_connect) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.airflow_tasks_log "
            "(process_id, source_id, entity_fqn, datalake_table, status) "
            "VALUES (%s, %s, %s, %s, %s)",
            (process_id, source_id, entity_fqn, datalake_table, status),
        )


def find_process_id(dag_id, run_id, *, db_connect):
    """Find an identifier already assigned to a DagRun."""
    with connection(db_connect) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT max(process_id) FROM ingestion.airflow_dagruns_log "
            "WHERE dag_id = %s AND run_id = %s", (dag_id, run_id),
        )
        value = cur.fetchone()[0]
        return int(value) if value is not None else None
