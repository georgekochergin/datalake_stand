"""Transactional extraction registry, importable by Airflow and Spark."""

from contextlib import contextmanager


def _hook(pg_conn_id):
    """Load Airflow only when its connection management is needed."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=pg_conn_id)


@contextmanager
def connection(pg_conn_id="ingestion_meta", db_connect=None):
    """Own a short DB-API transaction and close its connection."""
    conn = db_connect() if db_connect is not None else _hook(pg_conn_id).get_conn()
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


def next_process_id(pg_conn_id="ingestion_meta", *, db_connect=None):
    """Allocate an identifier for legacy callers."""
    with connection(pg_conn_id, db_connect) as conn, conn.cursor() as cur:
        cur.execute("SELECT nextval('ingestion.process_id_seq')")
        return int(cur.fetchone()[0])


def get_or_create_process_id(pg_conn_id="ingestion_meta", dag_id=None, run_id=None,
                             execution_date=None, *, db_connect=None):
    """Allocate one process and start event per DagRun, including concurrent retries."""
    with connection(pg_conn_id, db_connect) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.airflow_dagruns_log "
            "(process_id, dag_id, run_id, execution_date, status) "
            "VALUES (nextval('ingestion.process_id_seq'), %s, %s, %s, 'START') "
            "ON CONFLICT (dag_id, run_id) WHERE status = 'START' "
            "DO UPDATE SET dag_id = EXCLUDED.dag_id RETURNING process_id",
            (dag_id, run_id, execution_date),
        )
        return int(cur.fetchone()[0])


def log_dagrun(pg_conn_id, process_id, dag_id, run_id, execution_date, status):
    """Append a DagRun lifecycle event, ignoring duplicate start events."""
    with connection(pg_conn_id) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.airflow_dagruns_log "
            "(process_id, dag_id, run_id, execution_date, status) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (process_id, dag_id, run_id, execution_date, status),
        )


def log_task(pg_conn_id, process_id, source_id, entity_fqn, datalake_table, status):
    """Append a task lifecycle event."""
    with connection(pg_conn_id) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.airflow_tasks_log "
            "(process_id, source_id, entity_fqn, datalake_table, status) "
            "VALUES (%s, %s, %s, %s, %s)",
            (process_id, source_id, entity_fqn, datalake_table, status),
        )


def find_process_id(pg_conn_id, dag_id, run_id, *, db_connect=None):
    """Find an identifier already assigned to a DagRun."""
    with connection(pg_conn_id, db_connect) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT max(process_id) FROM ingestion.airflow_dagruns_log "
            "WHERE dag_id = %s AND run_id = %s", (dag_id, run_id),
        )
        value = cur.fetchone()[0]
        return int(value) if value is not None else None


def _check_contract(state, column, incremental_type):
    """Reject contract changes until an audited migration is applied."""
    mode = "incremental" if column is not None else "full_snapshot"
    if (state["configured_mode"], state["incremental_column"], state["incremental_type"]) != (
        mode, column, incremental_type
    ):
        raise ValueError("Extraction contract changed; stop executors and migrate extraction_state")


def _check_batch(state, batch):
    """Fence stale executors while permitting an already-successful retry."""
    if batch["status"] == "SUCCEEDED":
        return
    _check_contract(state, batch["incremental_column"], batch["incremental_type"])
    if state["revision"] != batch["expected_revision"]:
        raise ValueError("Extraction revision mismatch; stale batch cannot be reused")


def _lock_state(cur, source_id, entity_fqn):
    """Acquire the first lock in the shared state-then-batch lock order."""
    cur.execute(
        "SELECT * FROM ingestion.extraction_state "
        "WHERE source_id = %s AND entity_fqn = %s FOR UPDATE", (source_id, entity_fqn),
    )
    state = _row(cur)
    if state is None:
        raise ValueError("Extraction state does not exist")
    return state


def _lock_batch(cur, batch_id):
    """Locate the entity without a lock, then lock state and batch in order."""
    cur.execute("SELECT source_id, entity_fqn FROM ingestion.extraction_batches WHERE batch_id = %s",
                (batch_id,))
    key = cur.fetchone()
    if key is None:
        raise ValueError("Extraction batch does not exist")
    state = _lock_state(cur, *key)
    cur.execute("SELECT * FROM ingestion.extraction_batches WHERE batch_id = %s FOR UPDATE", (batch_id,))
    batch = _row(cur)
    _check_batch(state, batch)
    return state, batch


def get_batch(pg_conn_id="ingestion_meta", *, dag_id=None, run_id=None,
              source_id=None, entity_fqn=None, batch_id=None, db_connect=None):
    """Return a revision-validated persisted batch before accessing the source."""
    with connection(pg_conn_id, db_connect) as conn, conn.cursor() as cur:
        if batch_id is None:
            cur.execute(
                "SELECT batch_id FROM ingestion.extraction_batches "
                "WHERE dag_id = %s AND run_id = %s AND source_id = %s AND entity_fqn = %s",
                (dag_id, run_id, source_id, entity_fqn),
            )
            found = cur.fetchone()
            if found is None:
                return None
            batch_id = found[0]
        return _lock_batch(cur, batch_id)[1]


def plan_batch(pg_conn_id="ingestion_meta", *, process_id, dag_id, run_id, source_id,
               entity_fqn, target_table, incremental_column, incremental_type,
               upper_bound, empty_at_plan, force_full=False, lookback_size=0,
               lookback_unit=None, reason_text=None, calculate_lower=None,
               compare_positions=None, db_connect=None):
    """Persist fixed bounds once and return the original batch on retry.

    calculate_lower(watermark) returns a typed canonical lower bound or None
    on underflow. Call get_batch before querying a new source maximum.
    """
    if (incremental_column is None) != (incremental_type is None):
        raise ValueError("Incremental column and type must be specified together")
    if type(lookback_size) is not int or not 0 <= lookback_size <= 10**38 - 1:
        raise ValueError("lookback_size must be a nonnegative integer of at most 38 digits")
    if force_full and (not isinstance(reason_text, str) or not reason_text.strip()):
        raise ValueError("Manual full snapshot requires reason_text")
    configured_mode = "incremental" if incremental_column is not None else "full_snapshot"
    with connection(pg_conn_id, db_connect) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.extraction_state "
            "(source_id, entity_fqn, configured_mode, incremental_column, incremental_type) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (source_id, entity_fqn, configured_mode, incremental_column, incremental_type),
        )
        state = _lock_state(cur, source_id, entity_fqn)
        _check_contract(state, incremental_column, incremental_type)
        cur.execute(
            "SELECT * FROM ingestion.extraction_batches WHERE "
            "dag_id = %s AND run_id = %s AND source_id = %s AND entity_fqn = %s FOR UPDATE",
            (dag_id, run_id, source_id, entity_fqn),
        )
        batch = _row(cur)
        if batch is not None:
            _check_batch(state, batch)
            if batch["process_id"] != process_id or batch["target_table"] != target_table:
                raise ValueError("Existing batch process or target does not match")
            return batch
        watermark = state["watermark"]
        reason = "manual_full" if force_full else (
            "bootstrap" if incremental_column is not None and watermark is None else "regular"
        )
        mode = "incremental" if reason == "regular" and incremental_column is not None else "full_snapshot"
        lower = None
        if mode == "incremental":
            if upper_bound is not None and compare_positions is not None and compare_positions(upper_bound, watermark) < 0:
                raise ValueError("Source maximum regressed below the committed watermark")
            if lookback_size and calculate_lower is None:
                raise ValueError("Nonzero lookback requires calculate_lower")
            lower = calculate_lower(watermark) if calculate_lower else watermark
        else:
            lookback_size, lookback_unit = 0, None
            if force_full or incremental_column is None:
                upper_bound, empty_at_plan = None, False
        cur.execute(
            "INSERT INTO ingestion.extraction_batches "
            "(process_id, dag_id, run_id, source_id, entity_fqn, target_table, mode, reason, "
            "incremental_column, incremental_type, expected_revision, watermark_before, "
            "lookback_size, lookback_unit, lower_bound, upper_bound, empty_at_plan, reason_text) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *",
            (process_id, dag_id, run_id, source_id, entity_fqn, target_table, mode, reason,
             incremental_column, incremental_type, state["revision"], watermark,
             lookback_size, lookback_unit, lower, upper_bound, empty_at_plan, reason_text),
        )
        return _row(cur)


def start_batch(batch_id, pg_conn_id="ingestion_meta", *, db_connect=None):
    """Start an attempt after the caller has ensured no live executor remains."""
    with connection(pg_conn_id, db_connect) as conn, conn.cursor() as cur:
        _, batch = _lock_batch(cur, batch_id)
        if batch["status"] == "SUCCEEDED":
            return batch
        cur.execute(
            "UPDATE ingestion.extraction_batches SET status = 'RUNNING', "
            "attempt_count = attempt_count + 1, last_error = NULL, finished_at = NULL "
            "WHERE batch_id = %s RETURNING *", (batch_id,),
        )
        return _row(cur)


_UNSET = object()


def finalize_batch(batch_id, row_count, observed_max, pg_conn_id="ingestion_meta", *,
                   compare_positions=None, watermark_after=_UNSET, db_connect=None):
    """Atomically finish a batch and CAS the state revision.

    compare_positions(left, right) returns a negative/zero/positive typed
    comparison. Alternatively supply the typed maximum as watermark_after.
    Manual full and permanent snapshots always preserve the state watermark.
    """
    if type(row_count) is not int or row_count < 0:
        raise ValueError("row_count must be a nonnegative integer")
    with connection(pg_conn_id, db_connect) as conn, conn.cursor() as cur:
        state, batch = _lock_batch(cur, batch_id)
        if batch["status"] == "SUCCEEDED":
            return batch
        before = state["watermark"]
        if batch["reason"] == "manual_full" or batch["incremental_column"] is None:
            after = before
        elif observed_max is None:
            after = before
        elif before is None:
            after = observed_max
        elif compare_positions is not None:
            after = observed_max if compare_positions(observed_max, before) > 0 else before
        elif watermark_after is not _UNSET:
            if watermark_after not in (before, observed_max):
                raise ValueError("watermark_after must be the typed maximum of before and observed_max")
            after = watermark_after
        else:
            raise ValueError("Typed compare_positions or watermark_after is required")
        cur.execute(
            "UPDATE ingestion.extraction_state SET watermark = %s, revision = revision + 1, "
            "last_batch_id = %s, updated_at = clock_timestamp() "
            "WHERE source_id = %s AND entity_fqn = %s AND revision = %s",
            (after, batch_id, batch["source_id"], batch["entity_fqn"], batch["expected_revision"]),
        )
        if cur.rowcount != 1:
            raise ValueError("Extraction revision CAS failed")
        cur.execute(
            "UPDATE ingestion.extraction_batches SET status = 'SUCCEEDED', row_count = %s, "
            "observed_max = %s, watermark_after = %s, last_error = NULL, "
            "finished_at = clock_timestamp() WHERE batch_id = %s RETURNING *",
            (row_count, observed_max, after, batch_id),
        )
        return _row(cur)


def fail_batch(batch_id, error, pg_conn_id="ingestion_meta", *, db_connect=None):
    """Record a caller-sanitized failure without changing extraction state."""
    with connection(pg_conn_id, db_connect) as conn, conn.cursor() as cur:
        _, batch = _lock_batch(cur, batch_id)
        if batch["status"] == "SUCCEEDED":
            return batch
        cur.execute(
            "UPDATE ingestion.extraction_batches SET status = 'FAILED', last_error = %s, "
            "finished_at = clock_timestamp() WHERE batch_id = %s RETURNING *", (str(error), batch_id),
        )
        return _row(cur)
