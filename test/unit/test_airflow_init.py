"""Exercise the Compose Airflow initializer with a controlled CLI double."""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]
CONNECTIONS = (
    "bank_source", "broker_source", "spark_default", "trino_default",
    "sydafa_default", "ingestion_meta",
)


class TestAirflowInit(unittest.TestCase):
    """Preserve existing connections and propagate initialization failures."""

    def run_init(self, existing=False, failure=""):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cli = root / "airflow"
            cli.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, sys\n"
                "with open(os.environ['CALL_LOG'], 'a') as stream:\n"
                "    stream.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                "args = sys.argv[1:]\n"
                "if args[:2] == ['connections', 'get']:\n"
                "    sys.exit(0 if os.environ['EXISTING'] == 'yes' else 1)\n"
                "if args[:2] == ['connections', 'add'] and args[2] == os.environ['FAILURE']:\n"
                "    sys.exit(23)\n"
                "if args == ['db', 'migrate'] and os.environ['FAILURE'] == 'migrate':\n"
                "    sys.exit(24)\n",
                encoding="utf-8",
            )
            cli.chmod(0o755)
            command = yaml.safe_load((ROOT / "docker-compose.yaml").read_text())["services"]["airflow-init"]["command"]
            command[-1] = command[-1].replace("$$", "$").replace(
                "${SYDAFA_CONTAINER_PREFIX:-sydafa}", "sydafa-test"
            )
            credentials = "reader '$literal; spaces & / @"
            result = subprocess.run(command, env=dict(
                os.environ, PATH=f"{root}:{os.environ['PATH']}", CALL_LOG=str(root / "calls"),
                EXISTING="yes" if existing else "no", FAILURE=failure,
                STAND_USER=credentials, STAND_PASSWORD=credentials,
            ), capture_output=True, text=True)
            calls = [json.loads(line) for line in (root / "calls").read_text().splitlines()]
            return result, calls, credentials

    def test_missing_connections_are_created(self):
        result, calls, credentials = self.run_init()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls[0], ["db", "migrate"])
        additions = [call for call in calls if call[:2] == ["connections", "add"]]
        self.assertEqual([call[2] for call in additions], list(CONNECTIONS))
        for call in additions:
            if call[2] in ("trino_default", "ingestion_meta"):
                self.assertEqual(call[call.index("--conn-login") + 1], credentials)
                self.assertEqual(call[call.index("--conn-password") + 1], credentials)
        self.assertEqual(calls[-1][:3], ["pools", "set", "ingest_loads"])

    def test_existing_connections_are_not_overwritten(self):
        result, calls, _ = self.run_init(existing=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([call[2] for call in calls if call[:2] == ["connections", "get"]], list(CONNECTIONS))
        self.assertFalse(any(call[:2] == ["connections", "add"] for call in calls))

    def test_errors_stop_initialization(self):
        for failure, code in (("migrate", 24), ("bank_source", 23), ("ingestion_meta", 23)):
            with self.subTest(failure=failure):
                result, calls, _ = self.run_init(failure=failure)
                self.assertEqual(result.returncode, code, result.stderr)
                self.assertFalse(any(call[:2] == ["pools", "set"] for call in calls))
