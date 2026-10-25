"""Guards for the pg-meta container-init DDL (conf/pg-meta/init)."""

import unittest
from pathlib import Path

INIT_SQL = Path(__file__).resolve().parents[2] / "conf" / "pg-meta" / "init" / "01_ingestion.sql"

REQUIRED_OBJECTS = (
    "CREATE SCHEMA IF NOT EXISTS ingestion",
    "CREATE SEQUENCE IF NOT EXISTS ingestion.process_id_seq",
    "CREATE TABLE IF NOT EXISTS ingestion.airflow_dagruns_log",
    "CREATE INDEX IF NOT EXISTS airflow_dagruns_log_process_id_idx",
    "CREATE TABLE IF NOT EXISTS ingestion.airflow_tasks_log",
    "CREATE INDEX IF NOT EXISTS airflow_tasks_log_run_idx",
    "CREATE TABLE IF NOT EXISTS ingestion.table_watermarks",
)


class TestPgMetaInitSql(unittest.TestCase):
    """The init DDL must define every object the runtime registry relies on."""

    def test_init_sql_exists(self):
        self.assertTrue(INIT_SQL.exists())

    def test_init_sql_defines_required_objects(self):
        sql = INIT_SQL.read_text(encoding="utf-8")
        for statement in REQUIRED_OBJECTS:
            self.assertIn(statement, sql)

    def test_tables_are_append_only(self):
        sql = INIT_SQL.read_text(encoding="utf-8")
        self.assertNotIn("DROP ", sql.upper())
        self.assertNotIn("UPDATE ", sql.upper())
        self.assertNotIn("DELETE ", sql.upper())


if __name__ == "__main__":
    unittest.main()
