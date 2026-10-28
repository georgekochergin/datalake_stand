"""Guard: runtime code and e2e apps never manipulate sys.path.

Container layout knowledge belongs to compose (PYTHONPATH) or to explicit
importlib loading, never to hardcoded paths inside Python code. Self-locating
inserts remain allowed in host-side tools and unit tests only.
"""

import io
import tokenize
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

RUNTIME_ROOTS = [
    REPO_ROOT / "spark",
    REPO_ROOT / "airflow" / "plugins",
    REPO_ROOT / "dlh_ingestion",
]
RUNTIME_FILES = [
    REPO_ROOT / "test" / "temporal_extraction.py",
    REPO_ROOT / "test" / "schema_evolution.py",
    REPO_ROOT / "test" / "check_extraction.py",
]


def manipulates_sys_path(source: str) -> bool:
    """Detect real sys.path attribute usage, ignoring docstrings and strings."""
    tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    for position, token in enumerate(tokens[:-2]):
        if (token.type, token.string) == (tokenize.NAME, "sys") and (
            tokens[position + 1].type, tokens[position + 1].string
        ) == (tokenize.OP, ".") and (
            tokens[position + 2].type, tokens[position + 2].string
        ) == (tokenize.NAME, "path"):
            return True
    return False


class TestNoSysPathManipulation(unittest.TestCase):
    """Spark jobs, Airflow plugins, the shared package, and e2e apps stay path-clean."""

    def test_runtime_code_has_no_sys_path(self):
        violations = []
        for root in RUNTIME_ROOTS:
            for path in sorted(root.rglob("*.py")):
                if manipulates_sys_path(path.read_text(encoding="utf-8")):
                    violations.append(str(path.relative_to(REPO_ROOT)))
        self.assertEqual(violations, [])

    def test_e2e_apps_have_no_sys_path(self):
        violations = []
        for path in RUNTIME_FILES:
            if manipulates_sys_path(path.read_text(encoding="utf-8")):
                violations.append(str(path.relative_to(REPO_ROOT)))
        self.assertEqual(violations, [])

    def test_detector_ignores_strings_and_catches_usage(self):
        clean = '"""docs mention sys.path"""\nvalue = "sys.path"\n'
        self.assertFalse(manipulates_sys_path(clean))
        self.assertTrue(manipulates_sys_path("import sys\nsys.path.insert(0, x)\n"))


if __name__ == "__main__":
    unittest.main()
