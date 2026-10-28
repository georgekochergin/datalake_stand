"""pg-meta client transaction tests, with optional isolated PostgreSQL integration.

Set EXTRACTION_TEST_DSN to a disposable database. The integration tests create
their own database and never replace an existing ingestion schema.
"""

import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ingestion import meta


class TransactionTests(unittest.TestCase):
    """Verify transaction ownership and explicit CAS rollback without a server."""

    def test_import_does_not_load_airflow(self):
        """The Spark driver imports the module without Airflow installed."""
        code = (
            "import sys; "
            f"sys.path.insert(0, {str(ROOT)!r}); "
            "import ingestion.meta; "
            "assert not any(k == 'airflow' or k.startswith('airflow.') for k in sys.modules)"
        )
        subprocess.run([sys.executable, "-c", code], check=True)

    def test_connection_commit_and_close(self):
        """The client commits and releases injected connections."""
        conn = MagicMock()
        with meta.connection(db_connect=lambda: conn):
            pass
        conn.commit.assert_called_once()
        conn.close.assert_called_once()
        conn.rollback.assert_not_called()

    def test_connection_rolls_back_and_closes(self):
        """Exceptions never commit a partial metadata transaction."""
        conn = MagicMock()
        with self.assertRaisesRegex(ValueError, "failure"):
            with meta.connection(db_connect=lambda: conn):
                raise ValueError("failure")
        conn.rollback.assert_called_once()
        conn.close.assert_called_once()
        conn.commit.assert_not_called()

    def test_cas_zero_rolls_back_before_batch_update(self):
        """A zero-row CAS cannot be followed by a successful batch update."""
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.rowcount = 0
        state = {"watermark": "9"}
        batch = {"status": "RUNNING", "reason": "regular", "incremental_column": "id",
                 "source_id": "bank", "entity_fqn": "s.t", "expected_revision": 1}
        with patch.object(meta, "_lock_batch", return_value=(state, batch)):
            with self.assertRaisesRegex(ValueError, "CAS"):
                meta.finalize_batch(1, 1, "10", watermark_after="10", db_connect=lambda: conn)
        self.assertEqual(cur.execute.call_count, 1)
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()


@unittest.skipUnless(os.environ.get("EXTRACTION_TEST_DSN"), "EXTRACTION_TEST_DSN is not set")
class PostgresTests(unittest.TestCase):
    """Exercise real locks, constraints, retries and atomic metadata updates."""

    @classmethod
    def setUpClass(cls):
        """Create a unique disposable database using the supplied admin DSN."""
        import psycopg2
        from psycopg2 import sql

        cls.psycopg2 = psycopg2
        cls.sql = sql
        cls.admin_dsn = os.environ["EXTRACTION_TEST_DSN"]
        cls.database = "registry_test_" + uuid4().hex
        cls.admin = psycopg2.connect(cls.admin_dsn)
        cls.admin.autocommit = True
        with cls.admin.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database)))
        cls.params = psycopg2.extensions.parse_dsn(cls.admin_dsn)
        cls.params["dbname"] = cls.database
        with cls.connect() as conn, conn.cursor() as cur:
            cur.execute((ROOT / "conf/pg-meta/init/01_ingestion.sql").read_text())
        conn.close()

    @classmethod
    def tearDownClass(cls):
        """Remove only the database created by this test class."""
        with cls.admin.cursor() as cur:
            cur.execute(cls.sql.SQL("DROP DATABASE {}").format(sql.Identifier(cls.database)))
        cls.admin.close()

    @classmethod
    def connect(cls):
        """Return a fresh client-owned DB-API connection."""
        return cls.psycopg2.connect(**cls.params)

    def setUp(self):
        """Use a distinct source per case, preserving historical rows in the database."""
        self.source = uuid4().hex
        self.args = dict(process_id=1, dag_id="dag", run_id="run1", source_id=self.source,
                         entity_fqn="s.t", target_table="raw.s.t", incremental_column="id",
                         incremental_type="bigint", upper_bound="10", empty_at_plan=False,
                         db_connect=self.connect)

    def plan(self, **changes):
        """Plan a batch with test-specific overrides."""
        return meta.plan_batch(**(self.args | changes))

    def finish(self, batch, count=1, observed="10"):
        """Finalize using an integer comparator rather than textual ordering."""
        return meta.finalize_batch(
            batch["batch_id"], count, observed, db_connect=self.connect,
            compare_positions=lambda left, right: (int(left) > int(right)) - (int(left) < int(right)),
        )

    def state(self):
        """Read committed state for this case."""
        with meta.connection(db_connect=self.connect) as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM ingestion.extraction_state WHERE source_id = %s", (self.source,))
            return meta._row(cur)

    def test_bootstrap_retry_and_idempotent_success(self):
        """Bounds and IDs survive failures and success is finalized only once."""
        batch = self.plan()
        self.assertEqual((batch["reason"], batch["mode"], batch["expected_revision"]),
                         ("bootstrap", "full_snapshot", 1))
        meta.start_batch(batch["batch_id"], db_connect=self.connect)
        meta.fail_batch(batch["batch_id"], "safe failure", db_connect=self.connect)
        retry = self.plan(upper_bound="99")
        self.assertEqual(retry["batch_id"], batch["batch_id"])
        self.assertEqual(retry["upper_bound"], "10")
        self.assertIsNone(self.state()["watermark"])
        started = meta.start_batch(batch["batch_id"], db_connect=self.connect)
        self.assertEqual(started["attempt_count"], 2)
        result = self.finish(batch)
        self.assertEqual(result["watermark_after"], "10")
        self.assertIsNone(result["last_error"])
        self.assertEqual(self.finish(batch, 99, "99"), result)
        self.assertEqual(self.state()["revision"], 2)
        self.assertEqual(meta.fail_batch(batch["batch_id"], "late", db_connect=self.connect), result)

    def test_typed_max_and_empty_noop(self):
        """A lookback never moves the watermark backwards and no-op advances revision."""
        self.finish(self.plan(), observed="9")
        batch = self.plan(run_id="run2", lookback_size=2, calculate_lower=lambda value: str(int(value) - 2))
        self.assertEqual(batch["lower_bound"], "7")
        self.assertEqual(self.finish(batch, observed="10")["watermark_after"], "10")
        batch = self.plan(run_id="run3", upper_bound="8")
        self.assertEqual(self.finish(batch, observed="8")["watermark_after"], "10")
        batch = self.plan(run_id="run4")
        self.assertEqual(self.finish(batch, count=0, observed=None)["watermark_after"], "10")
        self.assertEqual(self.state()["revision"], 5)

    def test_manual_and_permanent_full_preserve_watermark(self):
        """Manual full ignores upper bounds and never rebases incremental state."""
        self.finish(self.plan())
        batch = self.plan(run_id="manual", force_full=True, reason_text="audit")
        self.assertEqual(batch["reason"], "manual_full")
        self.assertIsNone(batch["upper_bound"])
        self.assertEqual(self.finish(batch, observed="99")["watermark_after"], "10")
        self.source = uuid4().hex
        batch = self.plan(source_id=self.source, incremental_column=None, incremental_type=None)
        self.assertEqual(batch["reason"], "regular")
        self.assertIsNone(self.finish(batch, observed=None)["watermark_after"])

    def test_empty_bootstrap_and_lower_underflow(self):
        """Empty bootstrap is explicit and typed underflow removes the lower filter."""
        batch = self.plan(upper_bound=None, empty_at_plan=True)
        self.assertTrue(batch["empty_at_plan"])
        self.finish(batch, count=0, observed=None)
        self.assertIsNone(self.state()["watermark"])
        batch = self.plan(run_id="run2")
        self.assertEqual(batch["reason"], "bootstrap")
        self.finish(batch)
        batch = self.plan(run_id="run3", lookback_size=20, calculate_lower=lambda value: None)
        self.assertIsNone(batch["lower_bound"])

    def test_contract_and_revision_rejected_without_partial_success(self):
        """Old batches cannot read or finalize after state revision changes."""
        batch = self.plan()
        with self.assertRaisesRegex(ValueError, "contract"):
            self.plan(incremental_column="other")
        with meta.connection(db_connect=self.connect) as conn, conn.cursor() as cur:
            cur.execute("UPDATE ingestion.extraction_state SET revision = revision + 1 WHERE source_id = %s",
                        (self.source,))
        for operation in (
            lambda: meta.get_batch(batch_id=batch["batch_id"], db_connect=self.connect),
            lambda: meta.start_batch(batch["batch_id"], db_connect=self.connect),
            lambda: self.finish(batch),
        ):
            with self.assertRaisesRegex(ValueError, "revision"):
                operation()
        with meta.connection(db_connect=self.connect) as conn, conn.cursor() as cur:
            cur.execute("SELECT status FROM ingestion.extraction_batches WHERE batch_id = %s", (batch["batch_id"],))
            self.assertEqual(cur.fetchone()[0], "PLANNED")

    def test_process_id_is_idempotent(self):
        """Repeated process allocation uses the same unique start event."""
        kwargs = dict(dag_id=self.source, run_id="run", db_connect=self.connect)
        first = meta.get_or_create_process_id(**kwargs)
        self.assertEqual(meta.get_or_create_process_id(**kwargs), first)

    def test_regressed_source_maximum_is_rejected(self):
        """Typed source regression rejects planning without creating a batch."""
        self.finish(self.plan())
        with self.assertRaisesRegex(ValueError, "regressed"):
            self.plan(run_id="run2", upper_bound="9",
                      compare_positions=lambda left, right: int(left) - int(right))
        self.assertIsNone(meta.get_batch(dag_id="dag", run_id="run2", source_id=self.source,
                                         entity_fqn="s.t", db_connect=self.connect))

    def test_manual_full_ignores_incorrect_explicit_watermark(self):
        """An explicit caller result cannot accidentally rebase a manual snapshot."""
        self.finish(self.plan())
        batch = self.plan(run_id="manual", force_full=True, reason_text="audit")
        result = meta.finalize_batch(batch["batch_id"], 1, "99", watermark_after="99",
                                     db_connect=self.connect)
        self.assertEqual(result["watermark_after"], "10")

    def test_last_batch_fk_rejects_unknown_batch(self):
        """The deferred FK rejects last_batch_id pointers to unknown batches at commit."""
        self.plan()
        with self.assertRaises(self.psycopg2.Error):
            with meta.connection(db_connect=self.connect) as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE ingestion.extraction_state SET last_batch_id = %s WHERE source_id = %s",
                    (2147483647, self.source),
                )
        self.assertIsNone(self.state()["last_batch_id"])


if __name__ == "__main__":
    unittest.main()
