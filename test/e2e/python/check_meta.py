"""pg-meta client transaction tests, with optional isolated PostgreSQL integration.

Set EXTRACTION_TEST_DSN to a disposable database. The integration tests create
their own database and never replace an existing ingestion schema.
"""

import os
from pathlib import Path
import unittest
from uuid import uuid4


from ingestion import meta
from ingestion.meta import registry

ROOT = Path("/workspace")


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
            cur.execute(cls.sql.SQL("DROP DATABASE {}").format(cls.sql.Identifier(cls.database)))
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
        return registry.plan_batch(**(self.args | changes))

    def finish(self, batch, count=1, observed="10"):
        """Finalize using an integer comparator rather than textual ordering."""
        return registry.finalize_batch(
            process_id=batch["process_id"], source_id=batch["source_id"],
            entity_fqn=batch["entity_fqn"], row_count=count, observed_max=observed,
            db_connect=self.connect,
            compare_positions=lambda left, right: (int(left) > int(right)) - (int(left) < int(right)),
        )

    def state(self):
        """Read committed state for this case."""
        with meta.connection(db_connect=self.connect) as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM ingestion.incremental_state WHERE source_id = %s", (self.source,))
            return meta._row(cur)

    def test_bootstrap_retry_and_idempotent_success(self):
        """Bounds and IDs survive failures and success is finalized only once."""
        batch = self.plan()
        self.assertEqual((batch["reason"], batch["expected_revision"]),
                         ("bootstrap", 1))
        registry.start_batch(process_id=batch["process_id"], source_id=batch["source_id"],
                         entity_fqn=batch["entity_fqn"], db_connect=self.connect)
        registry.fail_batch(process_id=batch["process_id"], source_id=batch["source_id"],
                        entity_fqn=batch["entity_fqn"], db_connect=self.connect)
        retry = self.plan(upper_bound="99")
        self.assertEqual(retry["process_id"], batch["process_id"])
        self.assertEqual(retry["upper_bound"], "10")
        self.assertEqual(retry["status"], "FAILED")
        self.assertIsNone(self.state()["watermark"])
        started = registry.start_batch(process_id=batch["process_id"], source_id=batch["source_id"],
                                   entity_fqn=batch["entity_fqn"], db_connect=self.connect)
        self.assertEqual(started["attempt_count"], 2)
        result = self.finish(batch)
        self.assertEqual(result["watermark_after"], "10")
        self.assertEqual(self.finish(batch, 99, "99"), result)
        self.assertEqual(self.state()["revision"], 2)
        self.assertEqual(registry.fail_batch(process_id=batch["process_id"],
                                         source_id=batch["source_id"],
                                         entity_fqn=batch["entity_fqn"],
                                         db_connect=self.connect), result)

    def test_typed_max_and_empty_noop(self):
        """A lookback never moves the watermark backwards and no-op advances revision."""
        self.finish(self.plan(), observed="9")
        batch = self.plan(process_id=2, run_id="run2", lookback_size=2,
                          calculate_lower=lambda value: (str(int(value) - 2), ">="))
        self.assertEqual((batch["lower_bound"], batch["lower_bound_op"]), ("7", ">="))
        self.assertEqual(self.finish(batch, observed="10")["watermark_after"], "10")
        batch = self.plan(process_id=3, run_id="run3", upper_bound="8")
        self.assertEqual(self.finish(batch, observed="8")["watermark_after"], "10")
        batch = self.plan(process_id=4, run_id="run4")
        self.assertEqual(self.finish(batch, count=0, observed=None)["watermark_after"], "10")
        self.assertEqual(self.state()["revision"], 5)

    def test_registry_rejects_non_incremental_entities(self):
        """Full-slice entities load outside the incremental registry."""
        with self.assertRaisesRegex(ValueError, "requires an incremental"):
            self.plan(incremental_column=None, incremental_type=None)
        with self.assertRaisesRegex(ValueError, "requires an incremental"):
            self.plan(incremental_type=None)

    def test_empty_bootstrap_and_invalid_lower_bound(self):
        """An invalid lower bound aborts planning before querying the source."""
        from ingestion.sources.rdbms import positions

        batch = self.plan(upper_bound=None, empty_at_plan=True)
        self.assertTrue(batch["empty_at_plan"])
        self.finish(batch, count=0, observed=None)
        self.assertIsNone(self.state()["watermark"])
        batch = self.plan(process_id=2, run_id="run2")
        self.assertEqual(batch["reason"], "bootstrap")
        self.finish(batch)
        def unexpected_probe(lower, op):
            self.fail("Source bounds must not be probed with an invalid lower bound")

        previous = self.state()
        size = 10**20
        with self.assertRaisesRegex(ValueError, "outside its numeric type range"):
            self.plan(process_id=3, run_id="run3", lookback_size=size,
                      calculate_lower=lambda value: positions.window_bounds(value, "bigint", size),
                      probe_upper=unexpected_probe)
        self.assertEqual(self.state(), previous)
        self.assertIsNone(registry.get_batch(process_id=3, source_id=self.source,
                                             entity_fqn="s.t", db_connect=self.connect))

    def test_contract_and_revision_rejected_without_partial_success(self):
        """Old batches cannot read or finalize after state revision changes."""
        batch = self.plan()
        with self.assertRaisesRegex(ValueError, "contract"):
            self.plan(incremental_column="other")
        with meta.connection(db_connect=self.connect) as conn, conn.cursor() as cur:
            cur.execute("UPDATE ingestion.incremental_state SET revision = revision + 1 WHERE source_id = %s",
                        (self.source,))
        for operation in (
            lambda: registry.get_batch(process_id=batch["process_id"], source_id=batch["source_id"],
                                   entity_fqn=batch["entity_fqn"], db_connect=self.connect),
            lambda: registry.start_batch(process_id=batch["process_id"], source_id=batch["source_id"],
                                     entity_fqn=batch["entity_fqn"], db_connect=self.connect),
            lambda: self.finish(batch),
        ):
            with self.assertRaisesRegex(ValueError, "revision"):
                operation()
        with meta.connection(db_connect=self.connect) as conn, conn.cursor() as cur:
            cur.execute("SELECT status FROM ingestion.incremental_batches "
                        "WHERE process_id = %s AND source_id = %s AND entity_fqn = %s",
                        (batch["process_id"], batch["source_id"], batch["entity_fqn"]))
            self.assertEqual(cur.fetchone()[0], "PLANNED")

    def test_process_id_allocated_per_execution(self):
        """Every generating-task execution allocates its own process."""
        kwargs = dict(dag_id=self.source, run_id="run", db_connect=self.connect)
        first = meta.allocate_process_id(**kwargs)
        second = meta.allocate_process_id(**kwargs)
        self.assertLess(first, second)
        with meta.connection(db_connect=self.connect) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM ingestion.airflow_dagruns_log "
                        "WHERE dag_id = %s AND run_id = %s AND status = 'START'", (self.source, "run"))
            self.assertEqual(cur.fetchone()[0], 2)

    def test_lower_source_maximum_keeps_watermark(self):
        """An overlapping slice below the watermark may be committed without moving it back."""
        self.finish(self.plan())
        batch = self.plan(process_id=2, run_id="run2", lookback_size=2,
                          calculate_lower=lambda watermark: (str(int(watermark) - 2), ">"),
                          probe_upper=lambda lower, op: "9")
        self.assertEqual((batch["lower_bound"], batch["upper_bound"]), ("8", "9"))
        self.assertEqual(self.finish(batch, observed="9")["watermark_after"], "10")

    def test_last_process_fk_rejects_unknown_batch(self):
        """The deferred composite FK rejects pointers to unknown batches at commit."""
        self.plan()
        with self.assertRaises(self.psycopg2.Error):
            with meta.connection(db_connect=self.connect) as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE ingestion.incremental_state SET last_process_id = %s WHERE source_id = %s",
                    (2147483647, self.source),
                )
        self.assertIsNone(self.state()["last_process_id"])

    def test_reason_check_rejects_unknown_reasons(self):
        """The registry knows only the bootstrap and regular reasons."""
        self.plan()
        for reason in ("manual_full", "scheduled_full", "full_snapshot"):
            with self.subTest(reason=reason), self.assertRaises(self.psycopg2.Error):
                with meta.connection(db_connect=self.connect) as conn, conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO ingestion.incremental_batches "
                        "(process_id, dag_id, run_id, source_id, entity_fqn, target_table, reason, "
                        "expected_revision, lower_bound_op) VALUES "
                        "(999, 'dag', 'run', %s, 's.t', 'raw.s.t', %s, 1, '>')",
                        (self.source, reason),
                    )


if __name__ == "__main__":
    unittest.main()
