"""Guards for .airflowignore files that keep framework modules out of Airflow scanners."""

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def patterns(path: Path) -> list[str]:
    """Return non-empty ignore lines from an .airflowignore file."""
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


class TestAirflowIgnore(unittest.TestCase):
    """Airflow must not parse framework packages as DAGs or load them as plugins."""

    def setUp(self):
        self.dags_ignore = REPO_ROOT / "airflow" / ".airflowignore"
        self.plugins_ignore = REPO_ROOT / "airflow" / "plugins" / ".airflowignore"

    def test_files_exist(self):
        self.assertTrue(self.dags_ignore.is_file(), "airflow/.airflowignore is required")
        self.assertTrue(self.plugins_ignore.is_file(), "airflow/plugins/.airflowignore is required")

    def test_dag_scanner_excludes_only_nested_plugins(self):
        rules = patterns(self.dags_ignore)
        excluded = "plugins/ingestion/operators.py"
        kept = ["update_source_bank.py", "generated/ingest__rdbms__bank__main.py"]
        self.assertTrue(any(re.search(rule, excluded) for rule in rules))
        for candidate in kept:
            self.assertFalse(
                any(re.search(rule, candidate) for rule in rules),
                f"{candidate} must stay visible to the DAG scanner",
            )

    def test_plugin_loader_excludes_framework_packages(self):
        rules = patterns(self.plugins_ignore)
        excluded = ["ingestion/operators.py", "migrations/__init__.py"]
        for candidate in excluded:
            self.assertTrue(
                any(re.search(rule, candidate) for rule in rules),
                f"{candidate} must be excluded from plugin loading",
            )


if __name__ == "__main__":
    unittest.main()
