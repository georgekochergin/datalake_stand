"""Incremental batch/state registry in pg-meta.

Pure DB-API code on the shared ingestion.meta transaction helper. Owns the
state-then-batch lock order and CAS revision fencing; serves incremental
entities only — full-slice loads and snapshot flows never call it. The
registry stores and validates frozen windows: computing bounds and typed
maxima belongs to the planner callbacks and ingestion.positions.
"""

from __future__ import annotations

from typing import Literal, Protocol

from . import LOG, DbConnect, DbCursor, DbRow, _execute, _operation, _row


class BatchPlanner(Protocol):
    """Source probes that plan_batch runs inside its pg-meta transaction.

    Both methods run while the incremental_state row is locked, so the frozen
    bounds and the checked-out revision always match. probe_upper may issue a
    JDBC query to the source database.
    """

    def calculate_lower(self, watermark: str | None) -> tuple[str | None, str]:
        """Return the lower bound and its operator for the committed watermark."""
        ...

    def probe_upper(self, lower: str | None, op: str) -> str | None:
        """Return the source maximum over the lower predicate, or None when empty."""
        ...


def _check_contract(state: DbRow, column: str | None, incremental_type: str | None) -> None:
    """Reject contract changes until an audited migration is applied."""
    if (state["incremental_column"], state["incremental_type"]) != (column, incremental_type):
        raise ValueError("Incremental contract changed; stop executors and migrate incremental_state")


def _check_batch(state: DbRow, batch: DbRow) -> None:
    """Fence stale executors while permitting an already-successful retry."""
    if batch["status"] == "SUCCEEDED":
        return
    _check_contract(state, batch["incremental_column"], batch["incremental_type"])
    if state["revision"] != batch["expected_revision"]:
        raise ValueError("Incremental revision mismatch; stale batch cannot be reused")


def _lock_state(cur: DbCursor, source_id: str, entity_fqn: str) -> DbRow:
    """Acquire the first lock in the shared state-then-batch lock order."""
    _execute(cur,
        "SELECT * FROM ingestion.incremental_state "
        "WHERE source_id = %s AND entity_fqn = %s FOR UPDATE", (source_id, entity_fqn),
    )
    state = _row(cur)
    if state is None:
        raise ValueError("Incremental state does not exist")
    return state


def _lock_batch(cur: DbCursor, process_id: int, source_id: str, entity_fqn: str) -> tuple[DbRow, DbRow]:
    """Lock state and the process's batch, in the shared lock order."""
    state = _lock_state(cur, source_id, entity_fqn)
    _execute(cur,
        "SELECT * FROM ingestion.incremental_batches "
        "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s FOR UPDATE",
        (process_id, source_id, entity_fqn),
    )
    batch = _row(cur)
    if batch is None:
        raise ValueError("Incremental batch does not exist")
    _check_batch(state, batch)
    return state, batch


def get_batch(*, process_id: int, source_id: str, entity_fqn: str, db_connect: DbConnect) -> DbRow | None:
    """Return the process's revision-validated batch, or None before planning.

    One pg-meta transaction: an unlocked existence probe first, so a fresh
    execution takes no state lock at all; an existing batch is then fenced by
    locking state and batch FOR UPDATE in the shared order.
    """
    with _operation(db_connect, "get_batch", process_id=process_id, source_id=source_id,
                    entity_fqn=entity_fqn) as (cur, _):
        _execute(cur,
            "SELECT 1 FROM ingestion.incremental_batches "
            "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s",
            (process_id, source_id, entity_fqn),
        )
        if cur.fetchone() is None:
            LOG.info("pg-meta get_batch result=missing")
            return None
        batch = _lock_batch(cur, process_id, source_id, entity_fqn)[1]
        LOG.info("pg-meta get_batch row=%r", batch)
        return batch


def _batch_reason(watermark: str | None) -> Literal["bootstrap", "regular"]:
    """Map the watermark onto the batch reason.

    Bootstrap is the opening phase of the incremental stream: a NULL
    watermark plans bootstrap and only the next execution becomes a regular
    windowed batch.
    """
    return "bootstrap" if watermark is None else "regular"


def plan_batch(
    *, process_id: int, dag_id: str, run_id: str, source_id: str,
    entity_fqn: str, target_table: str, incremental_column: str, incremental_type: str,
    lookback_size: int = 0, lookback_unit: str | None = None, planner: BatchPlanner,
    db_connect: DbConnect,
) -> DbRow | None:
    """Persist fixed bounds once and return the original batch on retry.

    One pg-meta transaction: INSERT the state ON CONFLICT DO NOTHING; state
    FOR UPDATE; batch FOR UPDATE — a retry of this execution returns its
    frozen window without probing again. A fresh batch is planned through
    the planner callbacks, which run while the state row stays locked, and
    inserted with the frozen bounds. The batch identity is (process_id,
    source_id, entity_fqn): a fresh execution plans a fresh batch.
    """
    if incremental_column is None or incremental_type is None:
        raise ValueError("The incremental registry requires an incremental column and type")
    if type(lookback_size) is not int or not 0 <= lookback_size <= 10**38 - 1:
        raise ValueError("lookback_size must be a nonnegative integer of at most 38 digits")
    with _operation(db_connect, "plan_batch", process_id=process_id, source_id=source_id,
                    entity_fqn=entity_fqn) as (cur, changes):
        _execute(cur,
            "INSERT INTO ingestion.incremental_state "
            "(source_id, entity_fqn, incremental_column, incremental_type) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING *",
            (source_id, entity_fqn, incremental_column, incremental_type),
        )
        changes.append(("incremental_state", None, _row(cur)))
        state = _lock_state(cur, source_id, entity_fqn)
        _check_contract(state, incremental_column, incremental_type)
        _execute(cur,
            "SELECT * FROM ingestion.incremental_batches WHERE "
            "process_id = %s AND source_id = %s AND entity_fqn = %s FOR UPDATE",
            (process_id, source_id, entity_fqn),
        )
        batch = _row(cur)
        if batch is not None:
            _check_batch(state, batch)
            if batch["target_table"] != target_table:
                raise ValueError("Existing batch target does not match")
            changes.append(("incremental_batches", batch, None))
            return batch
        watermark = state["watermark"]
        reason = _batch_reason(watermark)
        lower, lower_op = planner.calculate_lower(watermark)
        upper_bound = planner.probe_upper(lower, lower_op)
        empty_at_plan = upper_bound is None
        _execute(cur,
            "INSERT INTO ingestion.incremental_batches "
            "(process_id, dag_id, run_id, source_id, entity_fqn, target_table, reason, "
            "incremental_column, incremental_type, expected_revision, watermark_before, "
            "lookback_size, lookback_unit, lower_bound, lower_bound_op, upper_bound, empty_at_plan) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *",
            (process_id, dag_id, run_id, source_id, entity_fqn, target_table, reason,
             incremental_column, incremental_type, state["revision"], watermark,
             lookback_size, lookback_unit, lower, lower_op, upper_bound, empty_at_plan),
        )
        batch = _row(cur)
        changes.append(("incremental_batches", None, batch))
        return batch


def start_batch(*, process_id: int, source_id: str, entity_fqn: str, db_connect: DbConnect) -> DbRow | None:
    """Start an attempt after the caller has ensured no live executor remains.

    One pg-meta transaction: state and batch FOR UPDATE in the shared order;
    an already-succeeded batch returns unchanged, otherwise the batch becomes
    RUNNING with attempt_count incremented.
    """
    with _operation(db_connect, "start_batch", process_id=process_id, source_id=source_id,
                    entity_fqn=entity_fqn) as (cur, changes):
        _, batch = _lock_batch(cur, process_id, source_id, entity_fqn)
        if batch["status"] == "SUCCEEDED":
            changes.append(("incremental_batches", batch, None))
            return batch
        _execute(cur,
            "UPDATE ingestion.incremental_batches SET status = 'RUNNING', "
            "attempt_count = attempt_count + 1, finished_at = NULL "
            "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s RETURNING *",
            (process_id, source_id, entity_fqn),
        )
        started = _row(cur)
        changes.append(("incremental_batches", batch, started))
        return started


def finalize_batch(
    *, process_id: int, source_id: str, entity_fqn: str, row_count: int,
    observed_max: str | None, watermark_after: str | None, db_connect: DbConnect,
) -> DbRow | None:
    """Atomically finish a batch and CAS the state revision.

    One pg-meta transaction: state and batch FOR UPDATE in the shared order;
    UPDATE the state watermark guarded by a revision CAS; UPDATE the batch to
    SUCCEEDED. watermark_after is the typed maximum of the previous watermark
    and observed_max, computed by the caller through ingestion.positions and
    accepted only when it equals one of the two.
    """
    if type(row_count) is not int or row_count < 0:
        raise ValueError("row_count must be a nonnegative integer")
    with _operation(db_connect, "finalize_batch", process_id=process_id, source_id=source_id,
                    entity_fqn=entity_fqn) as (cur, changes):
        state, batch = _lock_batch(cur, process_id, source_id, entity_fqn)
        if batch["status"] == "SUCCEEDED":
            changes.append(("incremental_batches", batch, None))
            return batch
        before = state["watermark"]
        if watermark_after not in (before, observed_max):
            raise ValueError("watermark_after must be the typed maximum of before and observed_max")
        _execute(cur,
            "UPDATE ingestion.incremental_state SET watermark = %s, revision = revision + 1, "
            "last_process_id = %s, updated_at = clock_timestamp() "
            "WHERE source_id = %s AND entity_fqn = %s AND revision = %s RETURNING *",
            (watermark_after, batch["process_id"], batch["source_id"], batch["entity_fqn"],
             batch["expected_revision"]),
        )
        if cur.rowcount != 1:
            raise ValueError("Incremental revision CAS failed")
        changes.append(("incremental_state", state, _row(cur)))
        _execute(cur,
            "UPDATE ingestion.incremental_batches SET status = 'SUCCEEDED', row_count = %s, "
            "observed_max = %s, watermark_after = %s, "
            "finished_at = clock_timestamp() "
            "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s RETURNING *",
            (row_count, observed_max, watermark_after, process_id, source_id, entity_fqn),
        )
        finalized = _row(cur)
        changes.append(("incremental_batches", batch, finalized))
        return finalized


def fail_batch(*, process_id: int, source_id: str, entity_fqn: str, db_connect: DbConnect) -> DbRow | None:
    """Record a failed attempt without changing extraction state.

    One pg-meta transaction: state and batch FOR UPDATE in the shared order;
    an already-succeeded batch returns unchanged, otherwise the batch becomes
    FAILED and the watermark and revision stay as they are.
    """
    with _operation(db_connect, "fail_batch", process_id=process_id, source_id=source_id,
                    entity_fqn=entity_fqn) as (cur, changes):
        _, batch = _lock_batch(cur, process_id, source_id, entity_fqn)
        if batch["status"] == "SUCCEEDED":
            changes.append(("incremental_batches", batch, None))
            return batch
        _execute(cur,
            "UPDATE ingestion.incremental_batches SET status = 'FAILED', "
            "finished_at = clock_timestamp() "
            "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s RETURNING *",
            (process_id, source_id, entity_fqn),
        )
        failed = _row(cur)
        changes.append(("incremental_batches", batch, failed))
        return failed
