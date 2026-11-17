"""Transaction ownership and CAS rollback without a running database."""

from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch

from ingestion import meta
from ingestion.meta import registry

ROOT = Path(__file__).resolve().parents[2]


class TransactionTests(unittest.TestCase):
    """Verify transaction ownership and explicit CAS rollback without a server."""

    def test_import_does_not_load_airflow(self):
        code = (
            "import sys; "
            f"sys.path.insert(0, {str(ROOT)!r}); "
            "import ingestion.meta, ingestion.meta.registry; "
            "assert not any(k == 'airflow' or k.startswith('airflow.') for k in sys.modules)"
        )
        subprocess.run([sys.executable, "-c", code], check=True)

    def test_connection_commit_and_close(self):
        conn = MagicMock()
        with meta.connection(db_connect=lambda: conn):
            pass
        conn.commit.assert_called_once()
        conn.close.assert_called_once()
        conn.rollback.assert_not_called()

    def test_connection_rolls_back_and_closes(self):
        conn = MagicMock()
        with self.assertRaisesRegex(ValueError, "failure"):
            with meta.connection(db_connect=lambda: conn):
                raise ValueError("failure")
        conn.rollback.assert_called_once()
        conn.close.assert_called_once()
        conn.commit.assert_not_called()

    def test_cas_zero_rolls_back_before_batch_update(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.rowcount = 0
        state = {"watermark": "9"}
        batch = {"status": "RUNNING", "process_id": 1,
                 "incremental_column": "id", "source_id": "bank", "entity_fqn": "s.t",
                 "expected_revision": 1}
        with patch.object(registry, "_lock_batch", return_value=(state, batch)):
            with self.assertRaisesRegex(ValueError, "CAS"):
                registry.finalize_batch(process_id=1, source_id="bank", entity_fqn="s.t",
                                        row_count=1, observed_max="10", watermark_after="10",
                                        db_connect=lambda: conn)
        self.assertEqual(cur.execute.call_count, 1)
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()
