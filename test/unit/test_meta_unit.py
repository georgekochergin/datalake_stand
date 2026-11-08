"""Hermetic pg-meta client tests over an in-memory DB-API double.

The integration suite in test_meta.py needs EXTRACTION_TEST_DSN; these cases
exercise the same client against a fake database that interprets exactly the
statements meta.py issues, so branch logic runs on every host without
PostgreSQL.
"""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ingestion import meta


STATE_COLUMNS = (
    "source_id", "entity_fqn", "incremental_column", "incremental_type",
    "watermark", "revision", "last_process_id", "updated_at",
)

BATCH_COLUMNS = (
    "process_id", "dag_id", "run_id", "source_id", "entity_fqn",
    "target_table", "reason", "incremental_column", "incremental_type",
    "expected_revision", "watermark_before", "lookback_size", "lookback_unit",
    "lower_bound", "lower_bound_op", "upper_bound", "empty_at_plan",
    "status", "attempt_count", "row_count", "observed_max",
    "watermark_after", "created_at", "finished_at",
)


class FakeCursor:
    """A cursor executing only the statements emitted by ingestion.meta."""

    def __init__(self, db):
        self.db = db
        self.description = None
        self.rowcount = -1
        self._row = None

    def execute(self, sql, params=()):
        """Apply one known statement to the in-memory database."""
        statement = " ".join(sql.split())
        if statement.startswith("INSERT INTO ingestion.airflow_dagruns_log") and "RETURNING" in statement:
            dag_id, run_id, execution_date = params
            self.db.process_seq += 1
            process_id = self.db.process_seq
            self.db.dagruns.append({"id": len(self.db.dagruns) + 1, "process_id": process_id,
                                    "dag_id": dag_id, "run_id": run_id,
                                    "execution_date": execution_date, "status": "START"})
            self.description = [("process_id",)]
            self._row = (process_id,)
            return
        if statement.startswith("INSERT INTO ingestion.airflow_dagruns_log"):
            process_id, dag_id, run_id, execution_date, status = params
            self.db.dagruns.append({"id": len(self.db.dagruns) + 1, "process_id": process_id,
                                    "dag_id": dag_id, "run_id": run_id,
                                    "execution_date": execution_date, "status": status})
            self.rowcount = 1
            return
        if statement.startswith("INSERT INTO ingestion.airflow_tasks_log"):
            process_id, source_id, entity_fqn, datalake_table, status = params
            self.db.tasks.append({"id": len(self.db.tasks) + 1, "process_id": process_id,
                                  "source_id": source_id, "entity_fqn": entity_fqn,
                                  "datalake_table": datalake_table, "status": status})
            self.rowcount = 1
            return
        if statement.startswith("SELECT max(process_id)"):
            dag_id, run_id = params
            values = [row["process_id"] for row in self.db.dagruns
                      if row["dag_id"] == dag_id and row["run_id"] == run_id]
            self.description = [("max",)]
            self._row = (max(values) if values else None,)
            return
        if statement.startswith("INSERT INTO ingestion.incremental_state"):
            source_id, entity_fqn = params[0], params[1]
            if (source_id, entity_fqn) not in self.db.state:
                self.db.state[(source_id, entity_fqn)] = {
                    "source_id": source_id, "entity_fqn": entity_fqn,
                    "incremental_column": params[2], "incremental_type": params[3],
                    "watermark": None, "revision": 1,
                    "last_process_id": None, "updated_at": None,
                }
            self.rowcount = 1
            return
        if statement.startswith("SELECT * FROM ingestion.incremental_state"):
            state = self.db.state.get((params[0], params[1]))
            self.description = [(name,) for name in STATE_COLUMNS]
            self._row = tuple(state[name] for name in STATE_COLUMNS) if state else None
            return
        if statement.startswith("SELECT 1 FROM ingestion.incremental_batches"):
            self.description = [("one",)]
            self._row = (1,) if params in self.db.batches else None
            return
        if statement.startswith("SELECT * FROM ingestion.incremental_batches"):
            batch = self.db.batches.get(tuple(params))
            self.description = [(name,) for name in BATCH_COLUMNS]
            self._row = tuple(batch[name] for name in BATCH_COLUMNS) if batch else None
            return
        if statement.startswith("INSERT INTO ingestion.incremental_batches"):
            row = dict(zip(
                ("process_id", "dag_id", "run_id", "source_id", "entity_fqn", "target_table",
                 "reason", "incremental_column", "incremental_type", "expected_revision",
                 "watermark_before", "lookback_size", "lookback_unit", "lower_bound",
                 "lower_bound_op", "upper_bound", "empty_at_plan"),
                params,
            ))
            row.update(status="PLANNED", attempt_count=0, row_count=None,
                       observed_max=None, watermark_after=None,
                       created_at="now", finished_at=None)
            self.db.batches[(row["process_id"], row["source_id"], row["entity_fqn"])] = row
            self.description = [(name,) for name in BATCH_COLUMNS]
            self._row = tuple(row[name] for name in BATCH_COLUMNS)
            return
        if statement.startswith("UPDATE ingestion.incremental_batches SET status = 'RUNNING'"):
            batch = self.db.batches[tuple(params)]
            batch.update(status="RUNNING", attempt_count=batch["attempt_count"] + 1, finished_at=None)
            self.description = [(name,) for name in BATCH_COLUMNS]
            self._row = tuple(batch[name] for name in BATCH_COLUMNS)
            self.rowcount = 1
            return
        if statement.startswith("UPDATE ingestion.incremental_state"):
            after, process_id, source_id, entity_fqn, expected_revision = params
            state = self.db.state.get((source_id, entity_fqn))
            if state is None or state["revision"] != expected_revision:
                self.rowcount = 0
                return
            state.update(watermark=after, revision=state["revision"] + 1,
                         last_process_id=process_id, updated_at="now")
            self.rowcount = 1
            return
        if statement.startswith("UPDATE ingestion.incremental_batches SET status = 'SUCCEEDED'"):
            row_count, observed_max, watermark_after, process_id, source_id, entity_fqn = params
            batch = self.db.batches[(process_id, source_id, entity_fqn)]
            batch.update(status="SUCCEEDED", row_count=row_count, observed_max=observed_max,
                         watermark_after=watermark_after, finished_at="now")
            self.description = [(name,) for name in BATCH_COLUMNS]
            self._row = tuple(batch[name] for name in BATCH_COLUMNS)
            self.rowcount = 1
            return
        if statement.startswith("UPDATE ingestion.incremental_batches SET status = 'FAILED'"):
            batch = self.db.batches[tuple(params)]
            batch.update(status="FAILED", finished_at="now")
            self.description = [(name,) for name in BATCH_COLUMNS]
            self._row = tuple(batch[name] for name in BATCH_COLUMNS)
            self.rowcount = 1
            return
        raise AssertionError(f"unexpected statement: {statement!r}")

    def fetchone(self):
        """Return the single row produced by the last SELECT or RETURNING."""
        return self._row

    def close(self):
        """Accept close as a no-op."""

    def __enter__(self):
        """Support with-managed cursors like psycopg2."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close on scope exit."""
        self.close()


class FakeConnection:
    """A DB-API connection backed by the shared in-memory database."""

    def __init__(self, db):
        self.db = db
        self.autocommit = True
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0

    def cursor(self):
        """Open a new cursor."""
        return FakeCursor(self.db)

    def commit(self):
        """Count successful transactions."""
        self.commits += 1

    def rollback(self):
        """Count aborted transactions."""
        self.rollbacks += 1

    def close(self):
        """Count closes."""
        self.closed += 1


class FakePg:
    """The pg-meta tables the client touches, with a shared id sequence."""

    def __init__(self):
        self.dagruns = []
        self.tasks = []
        self.state = {}
        self.batches = {}
        self.process_seq = 100

    def connect(self):
        """Return a fresh client-owned connection."""
        return FakeConnection(self)


class BatchTestCase(unittest.TestCase):
    """Shared planning helpers over a hermetic database."""

    def setUp(self):
        self.db = FakePg()
        self.args = dict(process_id=1, dag_id="dag", run_id="run1", source_id="bank",
                         entity_fqn="s.t", target_table="raw.bank.s__t",
                         incremental_column="id", incremental_type="bigint",
                         upper_bound="10", empty_at_plan=False, db_connect=self.db.connect)
        self.int_compare = lambda left, right: (int(left) > int(right)) - (int(left) < int(right))

    def plan(self, **changes):
        """Plan a batch with test-specific overrides."""
        return meta.plan_batch(**(self.args | changes))

    def finish(self, batch, count=1, observed="10", **changes):
        """Finalize with the integer comparator."""
        return meta.finalize_batch(process_id=batch["process_id"], source_id=batch["source_id"],
                                   entity_fqn=batch["entity_fqn"], row_count=count,
                                   observed_max=observed, db_connect=self.db.connect,
                                   compare_positions=self.int_compare, **changes)

    def state(self):
        """Read committed state for the planned entity."""
        return self.db.state[(self.args["source_id"], self.args["entity_fqn"])]


class TestPlanBatch(BatchTestCase):
    """Planning freezes bounds once and survives retries."""

    def test_bootstrap_plans_incremental_with_frozen_upper(self):
        batch = self.plan()
        self.assertEqual((batch["reason"], batch["status"]), ("bootstrap", "PLANNED"))
        self.assertEqual((batch["expected_revision"], batch["watermark_before"]), (1, None))
        self.assertEqual((batch["lower_bound"], batch["lower_bound_op"], batch["upper_bound"]),
                         (None, ">", "10"))
        self.assertEqual((batch["incremental_column"], batch["incremental_type"]), ("id", "bigint"))
        self.assertEqual(batch["attempt_count"], 0)

    def test_regular_window_applies_lookback_lower_and_op(self):
        self.finish(self.plan())
        batch = self.plan(process_id=2, run_id="run2", lookback_size=2,
                          calculate_lower=lambda value: (str(int(value) - 2), ">="))
        self.assertEqual(batch["reason"], "regular")
        self.assertEqual((batch["lower_bound"], batch["lower_bound_op"], batch["upper_bound"]),
                         ("8", ">=", "10"))
        self.assertEqual((batch["watermark_before"], batch["expected_revision"]), ("10", 2))

    def test_regular_without_lookback_reads_from_watermark(self):
        self.finish(self.plan())
        batch = self.plan(process_id=2, run_id="run2")
        self.assertEqual((batch["lower_bound"], batch["lower_bound_op"]), ("10", ">"))

    def test_registry_serves_incremental_entities_only(self):
        for changes in ({"incremental_column": None}, {"incremental_type": None}):
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, "requires an incremental"):
                self.plan(**changes)

    def test_retry_returns_frozen_batch(self):
        first = self.plan()
        retry = self.plan(upper_bound="99")
        self.assertEqual(retry, first)
        self.assertEqual(retry["upper_bound"], "10")

    def test_new_execution_plans_new_batch(self):
        first = self.plan()
        second = self.plan(process_id=2, run_id="run2", upper_bound="99")
        self.assertEqual(len(self.db.batches), 2)
        self.assertEqual((first["upper_bound"], second["upper_bound"]), ("10", "99"))
        self.assertEqual(self.state()["revision"], 1)

    def test_retry_with_other_target_rejected(self):
        self.plan()
        with self.assertRaisesRegex(ValueError, "target does not match"):
            self.plan(target_table="raw.bank.other__t")

    def test_contract_change_rejected_without_mutation(self):
        self.plan()
        for changes in ({"incremental_column": "other"}, {"incremental_type": "decimal(5,0)"}):
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, "contract"):
                self.plan(**changes)
        self.assertEqual(len(self.db.batches), 1)

    def test_argument_validation(self):
        for changes in (
            {"lookback_size": "2"}, {"lookback_size": -1}, {"lookback_size": 10**38},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.plan(**changes)


class TestBatchLifecycle(BatchTestCase):
    """Start, finalize, and fail transitions with their fencing rules."""

    def key(self, batch):
        """The composite batch key."""
        return dict(process_id=batch["process_id"], source_id=batch["source_id"],
                    entity_fqn=batch["entity_fqn"])

    def test_start_marks_running_and_counts_attempts(self):
        batch = self.plan()
        started = meta.start_batch(**(self.key(batch) | dict(db_connect=self.db.connect)))
        self.assertEqual((started["status"], started["attempt_count"]), ("RUNNING", 1))
        again = meta.start_batch(**(self.key(batch) | dict(db_connect=self.db.connect)))
        self.assertEqual(again["attempt_count"], 2)

    def test_start_after_success_is_noop(self):
        batch = self.plan()
        self.finish(batch)
        started = meta.start_batch(**(self.key(batch) | dict(db_connect=self.db.connect)))
        self.assertEqual((started["status"], started["attempt_count"]), ("SUCCEEDED", 0))

    def test_finalize_bootstrap_sets_watermark(self):
        batch = self.plan()
        result = self.finish(batch, count=5, observed="10")
        self.assertEqual(result["status"], "SUCCEEDED")
        self.assertEqual(result["watermark_after"], "10")
        self.assertEqual((self.state()["watermark"], self.state()["revision"],
                          self.state()["last_process_id"]), ("10", 2, batch["process_id"]))

    def test_finalize_regular_uses_typed_compare(self):
        self.finish(self.plan())
        batch = self.plan(process_id=2, run_id="run2")
        self.assertEqual(self.finish(batch, observed="12")["watermark_after"], "12")
        batch = self.plan(process_id=3, run_id="run3")
        self.assertEqual(self.finish(batch, observed="9")["watermark_after"], "12")
        self.assertEqual(self.state()["watermark"], "12")

    def test_finalize_explicit_watermark_after_validated(self):
        self.finish(self.plan())
        batch = self.plan(process_id=2, run_id="run2")
        with self.assertRaisesRegex(ValueError, "typed maximum"):
            meta.finalize_batch(**(self.key(batch) | dict(row_count=1, observed_max="9",
                                                          watermark_after="999",
                                                          db_connect=self.db.connect)))
        self.assertEqual(meta.finalize_batch(
            **(self.key(batch) | dict(row_count=1, observed_max="9", watermark_after="10",
                                      db_connect=self.db.connect)))["watermark_after"], "10")

    def test_finalize_requires_typed_strategy(self):
        self.finish(self.plan())
        batch = self.plan(process_id=2, run_id="run2")
        with self.assertRaisesRegex(ValueError, "required"):
            meta.finalize_batch(**(self.key(batch) | dict(row_count=1, observed_max="11",
                                                          db_connect=self.db.connect)))

    def test_finalize_stale_revision_rejected_before_update(self):
        self.finish(self.plan())
        batch = self.plan(process_id=2, run_id="run2")
        meta.start_batch(**(self.key(batch) | dict(db_connect=self.db.connect)))
        self.state()["revision"] += 1
        with self.assertRaisesRegex(ValueError, "revision"):
            self.finish(batch)
        self.assertEqual(self.db.batches[(2, "bank", "s.t")]["status"], "RUNNING")
        self.assertEqual(self.state()["watermark"], "10")

    def test_finalize_empty_delta_preserves_watermark(self):
        self.finish(self.plan())
        batch = self.plan(process_id=2, run_id="run2")
        result = self.finish(batch, count=0, observed=None)
        self.assertEqual(result["watermark_after"], "10")
        self.assertEqual(self.state()["watermark"], "10")

    def test_finalize_succeeded_is_idempotent(self):
        batch = self.plan()
        first = self.finish(batch)
        second = self.finish(batch, count=99, observed="99")
        self.assertEqual(first, second)
        self.assertEqual(self.state()["revision"], 2)

    def test_finalize_validates_row_count(self):
        batch = self.plan()
        for bad in ("5", -1, True, 1.0):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                meta.finalize_batch(**(self.key(batch) | dict(row_count=bad, observed_max="10",
                                                              db_connect=self.db.connect)))

    def test_fail_marks_failed_without_state_change(self):
        batch = self.plan()
        meta.start_batch(**(self.key(batch) | dict(db_connect=self.db.connect)))
        failed = meta.fail_batch(**(self.key(batch) | dict(db_connect=self.db.connect)))
        self.assertEqual(failed["status"], "FAILED")
        self.assertIsNone(self.state()["watermark"])
        self.assertEqual(self.state()["revision"], 1)

    def test_fail_after_success_is_noop(self):
        batch = self.plan()
        succeeded = self.finish(batch)
        failed = meta.fail_batch(**(self.key(batch) | dict(db_connect=self.db.connect)))
        self.assertEqual(failed["status"], "SUCCEEDED")
        self.assertEqual(failed, succeeded)


class TestGetBatch(BatchTestCase):
    """Persisted batch lookup and the stale-executor fence."""

    def test_missing_identity_returns_none(self):
        self.assertIsNone(meta.get_batch(process_id=1, source_id="bank", entity_fqn="s.t",
                                         db_connect=self.db.connect))

    def test_lookup_by_composite_key(self):
        planned = self.plan()
        found = meta.get_batch(process_id=1, source_id="bank", entity_fqn="s.t",
                               db_connect=self.db.connect)
        self.assertEqual(found, planned)

    def test_revision_fence_rejects_stale_executor(self):
        self.plan()
        self.state()["revision"] += 1
        with self.assertRaisesRegex(ValueError, "revision"):
            meta.get_batch(process_id=1, source_id="bank", entity_fqn="s.t",
                           db_connect=self.db.connect)
        with self.assertRaisesRegex(ValueError, "revision"):
            meta.start_batch(process_id=1, source_id="bank", entity_fqn="s.t",
                             db_connect=self.db.connect)

    def test_succeeded_batch_escapes_the_fence(self):
        self.plan()
        self.finish(self.plan())
        self.state()["revision"] += 1
        found = meta.get_batch(process_id=1, source_id="bank", entity_fqn="s.t",
                               db_connect=self.db.connect)
        self.assertEqual(found["status"], "SUCCEEDED")

    def test_unknown_batch_key_rejected(self):
        with self.assertRaisesRegex(ValueError, "does not exist"):
            meta.fail_batch(process_id=999, source_id="bank", entity_fqn="s.t",
                            db_connect=self.db.connect)

    def test_missing_state_rejected(self):
        with meta.connection(db_connect=self.db.connect) as conn, conn.cursor() as cur:
            with self.assertRaisesRegex(ValueError, "does not exist"):
                meta._lock_state(cur, "ghost", "s.t")


class TestProcessAndEvents(unittest.TestCase):
    """Process allocation and append-only event logging."""

    def setUp(self):
        self.db = FakePg()

    def test_every_execution_allocates_fresh_process_id(self):
        first = meta.allocate_process_id(dag_id="dag", run_id="run",
                                         execution_date=None, db_connect=self.db.connect)
        second = meta.allocate_process_id(dag_id="dag", run_id="run",
                                          execution_date=None, db_connect=self.db.connect)
        other = meta.allocate_process_id(dag_id="dag", run_id="run2",
                                         execution_date=None, db_connect=self.db.connect)
        self.assertEqual((first < second, other), (True, second + 1))

    def test_find_process_id_follows_latest_execution(self):
        self.assertIsNone(meta.find_process_id("dag", "run", db_connect=self.db.connect))
        first = meta.allocate_process_id(dag_id="dag", run_id="run",
                                         execution_date=None, db_connect=self.db.connect)
        second = meta.allocate_process_id(dag_id="dag", run_id="run",
                                          execution_date=None, db_connect=self.db.connect)
        self.assertEqual(meta.find_process_id("dag", "run", db_connect=self.db.connect), second)
        meta.log_dagrun(process_id=first, dag_id="dag", run_id="run",
                        execution_date=None, status="END", db_connect=self.db.connect)
        self.assertEqual(meta.find_process_id("dag", "run", db_connect=self.db.connect), second)

    def test_log_task_appends_event(self):
        meta.log_task(process_id=7, source_id="bank", entity_fqn="s.t",
                      datalake_table="raw.bank.s__t", status="START",
                      db_connect=self.db.connect)
        self.assertEqual(self.db.tasks[-1]["status"], "START")


class TestPureHelpers(unittest.TestCase):
    """Reason and bounds computation without a database."""

    def config(self, **changes):
        """Build the incremental config with overrides."""
        values = dict(column="id", type="bigint", lookback_size=0, lookback_unit=None,
                      calculate_lower=None, compare_positions=None)
        values.update(changes)
        return meta._IncrementalConfig(**values)

    def test_reason_matrix(self):
        self.assertEqual(meta._batch_reason(None), "bootstrap")
        self.assertEqual(meta._batch_reason("5"), "regular")

    def test_incremental_bounds_regular(self):
        bounds = meta._incremental_bounds("5", "10", False, self.config())
        self.assertEqual(bounds, ("5", ">", "10", False, 0, None))

    def test_incremental_bounds_lookback(self):
        bounds = meta._incremental_bounds(
            "5", "10", False,
            self.config(lookback_size=2, lookback_unit=None,
                        calculate_lower=lambda value: (str(int(value) - 2), ">=")),
        )
        self.assertEqual(bounds, ("3", ">=", "10", False, 2, None))

    def test_incremental_bounds_regression_rejected(self):
        with self.assertRaisesRegex(ValueError, "regressed"):
            meta._incremental_bounds(
                "10", "9", False,
                self.config(compare_positions=lambda left, right: int(left) - int(right)),
            )

    def test_incremental_bounds_lookback_needs_calculator(self):
        with self.assertRaisesRegex(ValueError, "calculate_lower"):
            meta._incremental_bounds("5", "10", False, self.config(lookback_size=1))


if __name__ == "__main__":
    unittest.main()
