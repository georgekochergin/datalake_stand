"""Focused configuration and cleanup isolation contracts for the stand."""

import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]


class StandConfigTests(unittest.TestCase):
    """Verify runtime authentication and project-scoped Docker configuration."""

    def test_airflow_passwords_are_created_at_runtime(self):
        module = runpy.run_path(str(ROOT / "conf/airflow/runtime_auth.py"))
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "passwords.json"
            environment = {
                "STAND_USER": "test-user",
                "STAND_PASSWORD": 'quoted"password',
                "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE": str(target),
            }
            with patch.dict(os.environ, environment), patch("os.execv") as execute:
                module["main"]()
            self.assertEqual(json.loads(target.read_text()), {"test-user": 'quoted"password'})
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)
            self.assertEqual(execute.call_args.args[0], "/entrypoint")

    def test_cleanup_scopes_sources_and_volumes(self):
        script = (ROOT / "test/test.sh").read_text()
        cleanup = script.split("cleanup() {", 1)[1].split("\ntrap cleanup", 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "docker.log"
            harness = r'''
set -euo pipefail
COMPOSE_PROJECT_NAME=dlh-test-unit
SYDAFA_CONTAINER_PREFIX=sydafa-dlh-test-unit
COMPOSE=(docker compose -p "$COMPOSE_PROJECT_NAME")
TEST_WS="$SANDBOX/workspace"
docker() {
  echo "$*" >> "$LOG"
  case "$1 $2" in
    'volume ls') printf '%s\n' sydafa-dlh-test-unit-data-mssql-bank sydafa-dlh-test-other-data-mssql-bank sydafa-data-mssql-bank ;;
  esac
}
cleanup() {
''' + cleanup + "\ncleanup\n"
            result = subprocess.run(
                ["bash", "-c", harness],
                env={**os.environ, "LOG": str(log), "SANDBOX": directory},
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            calls = log.read_text()
            self.assertIn("label=com.docker.compose.project=dlh-test-unit", calls)
            self.assertIn("label=sydafa.prefix=sydafa-dlh-test-unit", calls)
            self.assertIn("label=sydafa.managed=true", calls)
            self.assertIn("volume rm sydafa-dlh-test-unit-data-mssql-bank", calls)
            self.assertNotIn("volume rm sydafa-dlh-test-other", calls)
            self.assertNotIn("volume rm sydafa-data", calls)
            self.assertNotIn("--filter name=", calls)

    def test_spark_uses_environment_credentials(self):
        settings = (ROOT / "conf/spark/spark-defaults.conf").read_text()
        self.assertNotIn("s3.access-key-id", settings)
        self.assertNotIn("s3.secret-access-key", settings)
        self.assertIn("org.apache.iceberg.aws.s3.S3FileIO", settings)

    def test_non_test_project_is_rejected_before_docker(self):
        result = subprocess.run(
            ["bash", str(ROOT / "test/test.sh"), "--help"],
            env={**os.environ, "COMPOSE_PROJECT_NAME": "dlh-poc"},
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("COMPOSE_PROJECT_NAME", result.stdout)

    def test_workspace_source_ports_are_disabled(self):
        script = (ROOT / "test/test.sh").read_text()
        preparation = script.split("<<'PY'\n", 1)[1].split("\nPY", 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "sources.json"
            shutil.copyfile(ROOT / "conf/sydafa/sources.json", manifest)
            with patch("sys.argv", ["prepare", str(manifest)]):
                exec(compile(preparation, "workspace-preparation", "exec"), {})
            for source in json.loads(manifest.read_text())["sources"].values():
                self.assertNotIn("external_port", source)

    def test_runtime_failure_stops_before_schema_evolution(self):
        script = (ROOT / "test/test.sh").read_text()
        helper = script.split("run_runtime_checks() {", 1)[1].split('\nif [ "$MODE"', 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            (workspace / "spark").mkdir()
            (workspace / "spark/e2e_schema_evolution.py").touch()
            log = workspace / "calls"
            harness = r'''
set -euo pipefail
TEST_WS="$SANDBOX"
compose() { echo "$1" >> "$LOG"; return 7; }
COMPOSE=(compose)
run_runtime_checks() {
''' + helper + "\nrun_runtime_checks\n"
            result = subprocess.run(
                ["bash", "-c", harness],
                env={**os.environ, "LOG": str(log), "SANDBOX": directory},
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 7)
            self.assertEqual(log.read_text().splitlines(), ["run"])

    @unittest.skipUnless(shutil.which("docker"), "Docker Compose is unavailable")
    def test_compose_credentials_and_test_dns(self):
        result = subprocess.run(
            ["docker", "compose", "-p", "dlh-test-unit", "-f", "docker-compose.yaml",
             "-f", "test/docker-compose.override.yaml", "--profile", "test", "config", "--format", "json"],
            cwd=ROOT,
            env={**os.environ, "STAND_USER": "unit-user", "STAND_PASSWORD": "unit-password",
                 "SYDAFA_CONTAINER_PREFIX": "sydafa-dlh-test-unit"},
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, "Docker Compose config failed")
        services = json.loads(result.stdout)["services"]
        for name in ("spark-master", "spark-worker", "airflow-scheduler", "trino"):
            self.assertEqual(services[name]["environment"]["AWS_ACCESS_KEY_ID"], "unit-user")
            self.assertEqual(services[name]["environment"]["AWS_SECRET_ACCESS_KEY"], "unit-password")
        for name in ("airflow-init", "airflow-scheduler", "airflow-dag-processor", "airflow-webserver"):
            environment = services[name]["environment"]
            self.assertIn("sydafa-dlh-test-unit-src-mssql-bank", environment["AIRFLOW_CONN_BANK_SOURCE"])
            self.assertIn("sydafa:sydafa@", environment["AIRFLOW_CONN_BROKER_SOURCE"])
        for name in ("AIRFLOW_USER", "TRINO_USER"):
            self.assertEqual(services["test-runner"]["environment"][name], "unit-user")
        for service in services.values():
            self.assertFalse(service.get("ports"))


if __name__ == "__main__":
    unittest.main()
