"""Incremental batch/state registry in pg-meta.

Pure DB-API code on the shared ingestion.meta transaction helper. Owns the
state-then-batch lock order and CAS revision fencing; serves incremental
entities only — full-slice loads and snapshot flows never call it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from . import _row, connection


@dataclass(frozen=True)
class _IncrementalConfig:
    """Configuration for incremental extraction bounds computation."""

    column: str | None
    type: str | None
    lookback_size: int
    lookback_unit: str | None
    calculate_lower: Callable | None
    compare_positions: Callable | None


def _check_contract(state, column, incremental_type):
    """Reject contract changes until an audited migration is applied."""
    if (state["incremental_column"], state["incremental_type"]) != (column, incremental_type):
        raise ValueError("Incremental contract changed; stop executors and migrate incremental_state")


def _check_batch(state, batch):
    """Fence stale executors while permitting an already-successful retry."""
    if batch["status"] == "SUCCEEDED":
        return
    _check_contract(state, batch["incremental_column"], batch["incremental_type"])
    if state["revision"] != batch["expected_revision"]:
        raise ValueError("Incremental revision mismatch; stale batch cannot be reused")


def _lock_state(cur, source_id, entity_fqn):
    """Acquire the first lock in the shared state-then-batch lock order."""
    cur.execute(
        "SELECT * FROM ingestion.incremental_state "
        "WHERE source_id = %s AND entity_fqn = %s FOR UPDATE", (source_id, entity_fqn),
    )
    state = _row(cur)
    if state is None:
        raise ValueError("Incremental state does not exist")
    return state


def _lock_batch(cur, process_id, source_id, entity_fqn):
    """Lock state and the process's batch, in the shared lock order."""
    state = _lock_state(cur, source_id, entity_fqn)
    cur.execute(
        "SELECT * FROM ingestion.incremental_batches "
        "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s FOR UPDATE",
        (process_id, source_id, entity_fqn),
    )
    batch = _row(cur)
    if batch is None:
        raise ValueError("Incremental batch does not exist")
    _check_batch(state, batch)
    return state, batch


def get_batch(*, process_id, source_id, entity_fqn, db_connect):
    """Return the process's revision-validated batch, or None before planning."""
    with connection(db_connect) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM ingestion.incremental_batches "
            "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s",
            (process_id, source_id, entity_fqn),
        )
        if cur.fetchone() is None:
            return None
        return _lock_batch(cur, process_id, source_id, entity_fqn)[1]


def _batch_reason(watermark):
    """Map the watermark onto the batch reason.

    Bootstrap is the opening phase of the incremental stream: a NULL
    watermark plans bootstrap and only the next execution becomes a regular
    windowed batch.
    """
    return "bootstrap" if watermark is None else "regular"


def _incremental_bounds(watermark, upper_bound, empty_at_plan, incremental_config):
    """Compute window bounds with the actually applied lower operator.

    Returns (lower, lower_op, upper_bound, empty_at_plan, lookback_size,
    lookback_unit). Raises ValueError on source regression or configuration
    errors. calculate_lower(watermark) returns the (lower, op) pair, with
    None as the lower bound on underflow.
    """
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

    The registry serves incremental entities only; full-slice loads and
    snapshot flows never call it. The batch identity is (process_id,
    source_id, entity_fqn): retries of one execution replan into the same
    frozen window, a fresh execution plans a fresh batch. calculate_lower is
    described in _incremental_bounds. Call get_batch before querying a new
    source maximum.
    """
    if incremental_column is None or incremental_type is None:
        raise ValueError("The incremental registry requires an incremental column and type")
    if type(lookback_size) is not int or not 0 <= lookback_size <= 10**38 - 1:
        raise ValueError("lookback_size must be a nonnegative integer of at most 38 digits")
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
            "INSERT INTO ingestion.incremental_state "
            "(source_id, entity_fqn, incremental_column, incremental_type) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (source_id, entity_fqn, incremental_column, incremental_type),
        )
        state = _lock_state(cur, source_id, entity_fqn)
        _check_contract(state, incremental_column, incremental_type)
        cur.execute(
            "SELECT * FROM ingestion.incremental_batches WHERE "
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
        reason = _batch_reason(watermark)
        lower, lower_op, upper_bound, empty_at_plan, lookback_size, lookback_unit = _incremental_bounds(
            watermark, upper_bound, empty_at_plan, incremental_config
        )
        cur.execute(
            "INSERT INTO ingestion.incremental_batches "
            "(process_id, dag_id, run_id, source_id, entity_fqn, target_table, reason, "
            "incremental_column, incremental_type, expected_revision, watermark_before, "
            "lookback_size, lookback_unit, lower_bound, lower_bound_op, upper_bound, empty_at_plan) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *",
            (process_id, dag_id, run_id, source_id, entity_fqn, target_table, reason,
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
            "UPDATE ingestion.incremental_batches SET status = 'RUNNING', "
            "attempt_count = attempt_count + 1, finished_at = NULL "
            "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s RETURNING *",
            (process_id, source_id, entity_fqn),
        )
        return _row(cur)


_UNSET = object()


def finalize_batch(*, process_id, source_id, entity_fqn, row_count, observed_max=None,
                   compare_positions=None, watermark_after=_UNSET,
                   db_connect):
    """Atomically finish a batch and CAS the state revision.

    compare_positions(left, right) returns a negative/zero/positive typed
    comparison. Alternatively supply the typed maximum as watermark_after.
    """
    if type(row_count) is not int or row_count < 0:
        raise ValueError("row_count must be a nonnegative integer")
    with connection(db_connect) as conn, conn.cursor() as cur:
        state, batch = _lock_batch(cur, process_id, source_id, entity_fqn)
        if batch["status"] == "SUCCEEDED":
            return batch
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
            "UPDATE ingestion.incremental_state SET watermark = %s, revision = revision + 1, "
            "last_process_id = %s, updated_at = clock_timestamp() "
            "WHERE source_id = %s AND entity_fqn = %s AND revision = %s",
            (after, batch["process_id"], batch["source_id"], batch["entity_fqn"],
             batch["expected_revision"]),
        )
        if cur.rowcount != 1:
            raise ValueError("Incremental revision CAS failed")
        cur.execute(
            "UPDATE ingestion.incremental_batches SET status = 'SUCCEEDED', row_count = %s, "
            "observed_max = %s, watermark_after = %s, "
            "finished_at = clock_timestamp() "
            "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s RETURNING *",
            (row_count, observed_max, after, process_id, source_id, entity_fqn),
        )
        return _row(cur)


def fail_batch(*, process_id, source_id, entity_fqn, db_connect):
    """Record a failed attempt without changing extraction state."""
    with connection(db_connect) as conn, conn.cursor() as cur:
        _, batch = _lock_batch(cur, process_id, source_id, entity_fqn)
        if batch["status"] == "SUCCEEDED":
            return batch
        cur.execute(
            "UPDATE ingestion.incremental_batches SET status = 'FAILED', "
            "finished_at = clock_timestamp() "
            "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s RETURNING *",
            (process_id, source_id, entity_fqn),
        )
        return _row(cur)
