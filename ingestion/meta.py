"""Transactional extraction metadata client shared by Airflow and the Spark driver.

Pure DB-API code: callers inject db_connect, so the module imports neither
Airflow nor any database driver. Connections are owned per call: one short
transaction, commit or rollback, then close.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class _IncrementalConfig:
    """Configuration for incremental extraction bounds computation."""

    column: str | None
    type: str | None
    lookback_size: int
    lookback_unit: str | None
    calculate_lower: Callable | None
    compare_positions: Callable | None


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


def _check_contract(state, column, incremental_type):
    """Reject contract changes until an audited migration is applied."""
    mode = "incremental" if column is not None else "full_snapshot"
    if (state["configured_mode"], state["incremental_column"], state["incremental_type"]) != (
        mode, column, incremental_type
    ):
        raise ValueError("Extraction contract changed; stop executors and migrate extraction_state")


def _check_batch(state, batch):
    """Fence stale executors while permitting an already-successful retry.

    Only incremental batches carry a state revision expectation: snapshot and
    full-slice batches never touch extraction_state, so their retries are
    always admissible.
    """
    if batch["status"] == "SUCCEEDED":
        return
    _check_contract(state, batch["incremental_column"], batch["incremental_type"])
    if batch["mode"] == "incremental" and state["revision"] != batch["expected_revision"]:
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


def _lock_batch(cur, process_id, source_id, entity_fqn):
    """Lock state and the process's batch, in the shared lock order."""
    state = _lock_state(cur, source_id, entity_fqn)
    cur.execute(
        "SELECT * FROM ingestion.extraction_batches "
        "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s FOR UPDATE",
        (process_id, source_id, entity_fqn),
    )
    batch = _row(cur)
    if batch is None:
        raise ValueError("Extraction batch does not exist")
    _check_batch(state, batch)
    return state, batch


def get_batch(*, process_id, source_id, entity_fqn, db_connect):
    """Return the process's revision-validated batch, or None before planning."""
    with connection(db_connect) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM ingestion.extraction_batches "
            "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s",
            (process_id, source_id, entity_fqn),
        )
        if cur.fetchone() is None:
            return None
        return _lock_batch(cur, process_id, source_id, entity_fqn)[1]


def _batch_mode_and_reason(incremental_column, watermark):
    """Map the entity contract and watermark onto the batch mode and reason.

    Bootstrap is the opening phase of the incremental stream, not a separate
    mode: a NULL watermark plans (incremental, bootstrap) and only the next
    execution becomes a regular windowed batch.
    """
    if incremental_column is None:
        return "full_snapshot", "regular"
    if watermark is None:
        return "incremental", "bootstrap"
    return "incremental", "regular"


def _incremental_bounds(mode, watermark, upper_bound, empty_at_plan, incremental_config):
    """Compute window bounds with the actually applied lower operator.

    Returns (lower, lower_op, upper_bound, empty_at_plan, lookback_size,
    lookback_unit). Raises ValueError on source regression or configuration
    errors. calculate_lower(watermark) returns the (lower, op) pair, with
    None as the lower bound on underflow.
    """
    if mode != "incremental":
        return None, ">", None, False, 0, None
    if upper_bound is not None and incremental_config.compare_positions is not None and \
            incremental_config.compare_positions(upper_bound, watermark) < 0:
        raise ValueError("Source maximum regressed below the committed watermark")
    if incremental_config.calculate_lower is None:
        if incremental_config.lookback_size:
            raise ValueError("Nonzero lookback requires calculate_lower")
        return watermark, ">", upper_bound, empty_at_plan, \
            incremental_config.lookback_size, incremental_config.lookback_unit
    lower, op = incremental_config.calculate_lower(watermark)
    return lower, op, upper_bound, empty_at_plan, \
        incremental_config.lookback_size, incremental_config.lookback_unit


def plan_batch(*, process_id, dag_id, run_id, source_id,
               entity_fqn, target_table, incremental_column, incremental_type,
               upper_bound, empty_at_plan, lookback_size=0,
               lookback_unit=None, calculate_lower=None,
               compare_positions=None, db_connect):
    """Persist fixed bounds once and return the original batch on retry.

    The batch identity is (process_id, source_id, entity_fqn): retries of one
    execution replan into the same frozen window, a fresh execution plans a
    fresh batch. calculate_lower is described in _incremental_bounds. Call
    get_batch before querying a new source maximum.
    """
    if (incremental_column is None) != (incremental_type is None):
        raise ValueError("Incremental column and type must be specified together")
    if type(lookback_size) is not int or not 0 <= lookback_size <= 10**38 - 1:
        raise ValueError("lookback_size must be a nonnegative integer of at most 38 digits")
    configured_mode = "incremental" if incremental_column is not None else "full_snapshot"
    incremental_config = _IncrementalConfig(
        column=incremental_column,
        type=incremental_type,
        lookback_size=lookback_size,
        lookback_unit=lookback_unit,
        calculate_lower=calculate_lower,
        compare_positions=compare_positions,
    )
    with connection(db_connect) as conn, conn.cursor() as cur:
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
            "process_id = %s AND source_id = %s AND entity_fqn = %s FOR UPDATE",
            (process_id, source_id, entity_fqn),
        )
        batch = _row(cur)
        if batch is not None:
            _check_batch(state, batch)
            if batch["target_table"] != target_table:
                raise ValueError("Existing batch target does not match")
            return batch
        watermark = state["watermark"]
        mode, reason = _batch_mode_and_reason(incremental_column, watermark)
        lower, lower_op, upper_bound, empty_at_plan, lookback_size, lookback_unit = _incremental_bounds(
            mode, watermark, upper_bound, empty_at_plan, incremental_config
        )
        cur.execute(
            "INSERT INTO ingestion.extraction_batches "
            "(process_id, dag_id, run_id, source_id, entity_fqn, target_table, mode, reason, "
            "incremental_column, incremental_type, expected_revision, watermark_before, "
            "lookback_size, lookback_unit, lower_bound, lower_bound_op, upper_bound, empty_at_plan) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *",
            (process_id, dag_id, run_id, source_id, entity_fqn, target_table, mode, reason,
             incremental_column, incremental_type, state["revision"], watermark,
             lookback_size, lookback_unit, lower, lower_op, upper_bound, empty_at_plan),
        )
        return _row(cur)


def start_batch(*, process_id, source_id, entity_fqn, db_connect):
    """Start an attempt after the caller has ensured no live executor remains."""
    with connection(db_connect) as conn, conn.cursor() as cur:
        _, batch = _lock_batch(cur, process_id, source_id, entity_fqn)
        if batch["status"] == "SUCCEEDED":
            return batch
        cur.execute(
            "UPDATE ingestion.extraction_batches SET status = 'RUNNING', "
            "attempt_count = attempt_count + 1, finished_at = NULL "
            "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s RETURNING *",
            (process_id, source_id, entity_fqn),
        )
        return _row(cur)


_UNSET = object()


def finalize_batch(*, process_id, source_id, entity_fqn, row_count, observed_max=None,
                   compare_positions=None, watermark_after=_UNSET, published=None,
                   db_connect):
    """Atomically finish a batch; only incremental batches CAS the state.

    compare_positions(left, right) returns a negative/zero/positive typed
    comparison. Alternatively supply the typed maximum as watermark_after.
    Snapshot and full-slice batches never write extraction_state: their
    finalization updates the batch row alone; published records whether at
    least one attempt actually published the snapshot table.
    """
    if type(row_count) is not int or row_count < 0:
        raise ValueError("row_count must be a nonnegative integer")
    with connection(db_connect) as conn, conn.cursor() as cur:
        state, batch = _lock_batch(cur, process_id, source_id, entity_fqn)
        if batch["status"] == "SUCCEEDED":
            return batch
        after = None
        if batch["mode"] == "incremental":
            before = state["watermark"]
            if observed_max is None:
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
                "last_process_id = %s, updated_at = clock_timestamp() "
                "WHERE source_id = %s AND entity_fqn = %s AND revision = %s",
                (after, batch["process_id"], batch["source_id"], batch["entity_fqn"],
                 batch["expected_revision"]),
            )
            if cur.rowcount != 1:
                raise ValueError("Extraction revision CAS failed")
        cur.execute(
            "UPDATE ingestion.extraction_batches SET status = 'SUCCEEDED', row_count = %s, "
            "observed_max = %s, watermark_after = %s, published = %s, "
            "finished_at = clock_timestamp() "
            "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s RETURNING *",
            (row_count, observed_max, after, published, process_id, source_id, entity_fqn),
        )
        return _row(cur)


def fail_batch(*, process_id, source_id, entity_fqn, db_connect):
    """Record a failed attempt without changing extraction state."""
    with connection(db_connect) as conn, conn.cursor() as cur:
        _, batch = _lock_batch(cur, process_id, source_id, entity_fqn)
        if batch["status"] == "SUCCEEDED":
            return batch
        cur.execute(
            "UPDATE ingestion.extraction_batches SET status = 'FAILED', "
            "finished_at = clock_timestamp() "
            "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s RETURNING *",
            (process_id, source_id, entity_fqn),
        )
        return _row(cur)
