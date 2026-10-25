"""Style guard: framework Python code contains no # comments (docstrings only)."""

import io
import tokenize
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCANNED_ROOTS = [
    REPO_ROOT / "ingestion",
    REPO_ROOT / "spark" / "raw",
    REPO_ROOT / "airflow" / "plugins" / "ingestion",
]


class TestNoHashComments(unittest.TestCase):
    """Enforces the AGENTS.md rule: zero # comments outside a shebang on line 1."""

    def test_no_hash_comments(self):
        violations = []
        for root in SCANNED_ROOTS:
            for path in sorted(root.rglob("*.py")):
                source = path.read_text(encoding="utf-8")
                tokens = tokenize.generate_tokens(io.StringIO(source).readline)
                for token in tokens:
                    if token.type == tokenize.COMMENT:
                        if token.start[0] == 1 and token.string.startswith("#!"):
                            continue
                        violations.append(
                            f"{path.relative_to(REPO_ROOT)}:{token.start[0]}: {token.string}"
                        )
        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
