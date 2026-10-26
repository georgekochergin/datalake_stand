"""Syntax check for Airflow plugin modules (imports resolve only inside Airflow containers)."""

import ast
import unittest
from pathlib import Path

PLUGIN_ROOT = Path(__file__).resolve().parents[2] / "airflow" / "plugins" / "ingestion"


class TestPluginSyntax(unittest.TestCase):
    """Airflow-dependent plugin modules must at least parse on the host."""

    def test_plugins_parse(self):
        files = sorted(PLUGIN_ROOT.rglob("*.py"))
        self.assertTrue(files)
        for path in files:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


if __name__ == "__main__":
    unittest.main()
