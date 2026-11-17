"""Static contracts for bounded, source-isolated parallel e2e execution."""

import ast
from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "test"))
from e2e_scale import APPEND_ROWS, disable_retries, prepare_workspace, scale_migration
from ingestion.generator.generate_dags import generate_all
from ingestion.generator.manifest import load_sources


class ParallelE2ETests(unittest.TestCase):
    """Preserve production defaults while preparing bounded test-only copies."""

    def test_scale_invariants_and_input_unchanged(self):
        for source in ("bank", "broker"):
            original = json.loads((ROOT / f"airflow/plugins/migrations/{source}.json").read_text())
            saved = deepcopy(original)
            operations = scale_migration(original)["operations"]
            self.assertEqual(original, saved)
            rows = {op["entity_fqn"]: op["rows"] for op in operations if op["op"] == "append"}
            self.assertEqual(sorted(rows.values()), [10, 10, 100, 100])
            for op in operations:
                for field in ("rows", "limit"):
                    if field in op:
                        self.assertTrue(1 <= op[field] <= 100)
                if "limit" in op:
                    self.assertGreaterEqual(rows[op["entity_fqn"]], op["limit"])
            self.assertGreater(APPEND_ROWS["transactions"], 0)

    def test_invalid_scale_is_rejected(self):
        with self.assertRaises(ValueError):
            scale_migration({"operations": [{"op": "delete", "limit": 101}]})

    def test_retry_override_preserves_other_defaults(self):
        text = 'defaults = {"retries": 2, "retry_delay": timedelta(minutes=5)}\n'
        expected = text.replace('"retries": 2', '"retries": 0')
        self.assertEqual(disable_retries(text), expected)
        self.assertEqual(disable_retries(expected), expected)
        with self.assertRaises(ValueError):
            disable_retries('defaults = {"retries": configured}')

    def test_workspaces_are_filtered_and_do_not_modify_originals(self):
        original = (ROOT / "ingestion/conf/sources.yaml").read_bytes()
        for source in ("bank", "broker"):
            with self.subTest(source=source), tempfile.TemporaryDirectory() as directory:
                workspace = Path(directory) / "workspace"
                prepare_workspace(ROOT, workspace, source)
                sources = json.loads((workspace / "conf/sydafa/sources.json").read_text())["sources"]
                self.assertEqual(set(sources), {source})
                self.assertNotIn("external_port", sources[source])
                self.assertEqual(set(yaml.safe_load((workspace / "ingestion/conf/sources.yaml").read_text())), {source})
                self.assertEqual([p.stem for p in (workspace / "airflow/plugins/migrations").glob("*.json")], [source])
                for path in (workspace / "airflow/dags").rglob("*.py"):
                    if path.name.startswith("ingest__rdbms__"):
                        self.assertIn(f"__{source}__", path.name)
                    for node in ast.walk(ast.parse(path.read_text())):
                        if isinstance(node, ast.Dict):
                            for key, value in zip(node.keys, node.values):
                                if isinstance(key, ast.Constant) and key.value == "retries":
                                    self.assertEqual(ast.literal_eval(value), 0)
        self.assertEqual((ROOT / "ingestion/conf/sources.yaml").read_bytes(), original)

    def test_generated_runtime_structure(self):
        generated = generate_all(load_sources(ROOT / "ingestion/conf/sources.yaml"),
                                 ROOT / "ingestion/conf/pipelines", ROOT / "ingestion/conf/settings.yaml")
        self.assertEqual(len(generated), 6)
        for name, content in generated.items():
            calls = [node for node in ast.walk(ast.parse(content)) if isinstance(node, ast.Call)]
            loads = [node for node in calls if isinstance(node.func, ast.Name)
                     and node.func.id in ("IngestFullOperator", "IngestIncrementalOperator", "IngestActSnapOperator")]
            self.assertEqual(len(loads), 1 if "__act_snap_" in name else 2)
            for call in loads:
                values = {keyword.arg: keyword.value for keyword in call.keywords}
                self.assertEqual(ast.literal_eval(values["pool"]), "ingest_loads")
                table = ast.literal_eval(values["target_table"]) if "target_table" in values else ast.literal_eval(values["datalake_table"])
                self.assertEqual(ast.literal_eval(values["pool_slots"]), 2 if "transactions" in table else 1)

    def test_existing_manifest_contracts_run_as_unit(self):
        with patch.dict(os.environ, {"E2E_SOURCE": "all", "WORKSPACE_DIR": str(ROOT)}), patch.dict(sys.modules):
            sys.path.insert(0, str(ROOT / "test"))
            try:
                spec = importlib.util.spec_from_file_location("e2e_static_contracts", ROOT / "test/run_flow.py")
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                module.validate_manifests_and_dags()
                self.assertEqual(module.FAIL, 0)
            finally:
                sys.path.remove(str(ROOT / "test"))

    def test_source_runtime_inventory(self):
        for selected in ("bank", "broker"):
            with patch.dict(os.environ, {"E2E_SOURCE": selected}):
                spec = importlib.util.spec_from_file_location("isolated_flow", ROOT / "test/run_flow.py")
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                self.assertEqual(module.SOURCES, (selected,))
                self.assertEqual(len(module.INGEST_DAGS), 2)
                self.assertEqual(len(module.SNAPSHOT_DAGS), 1)
                self.assertEqual(len(module.RAW_SNAPSHOT_TABLES), 3)
                self.assertEqual(len(module.RAW_INCREMENTAL_TABLES), 1)
                self.assertEqual(set(module.EVOLUTION_SOURCES), {selected})

    def test_connections_and_pool_static_contract(self):
        import shlex

        services = yaml.safe_load((ROOT / "docker-compose.yaml").read_text())["services"]
        tokens = shlex.split(services["airflow-init"]["command"][2].replace("\\\n", " "))
        connections = {}
        for index in range(len(tokens) - 3):
            if tokens[index:index + 3] == ["airflow", "connections", "add"]:
                name = tokens[index + 3]
                options = {}
                position = index + 4
                while position < len(tokens) and tokens[position].startswith("--conn-"):
                    options[tokens[position]] = tokens[position + 1]
                    position += 2
                connections[name] = options
        expected = {
            "spark_main_submit": ("spark", "spark://spark-master", "7077", None),
            "spark_main_master_http": ("http", "spark-master", "8080", "http"),
            "trino_default": ("trino", "trino", "8443", "raw"),
            "sydafa_default": ("http", "http://sydafa", "8000", None),
            "ingestion_meta": ("postgres", "pg-meta", "5432", "meta"),
        }
        for name, values in expected.items():
            actual = connections[name]
            self.assertEqual(tuple(actual.get(key) for key in (
                "--conn-type", "--conn-host", "--conn-port", "--conn-schema")), values)
        self.assertEqual(json.loads(connections["spark_main_submit"]["--conn-extra"]), {"spark_binary": "spark-submit"})
        self.assertEqual(json.loads(connections["trino_default"]["--conn-extra"]), {"protocol": "https", "verify": "false"})
        for name in ("trino_default", "ingestion_meta"):
            self.assertEqual(connections[name]["--conn-login"], "$${STAND_USER}")
            self.assertEqual(connections[name]["--conn-password"], "$${STAND_PASSWORD}")
        for source, engine, port in (("bank", "mssql", 1433), ("broker", "oracle", 1521)):
            self.assertEqual(connections[source + "_source"]["--conn-uri"],
                             f"{engine}://sydafa:sydafa@${{SYDAFA_CONTAINER_PREFIX:-sydafa}}-src-{engine}-{source}:{port}/sydafa")
        self.assertIn("airflow pools set ingest_loads 4", services["airflow-init"]["command"][2])

    @unittest.skipUnless(shutil.which("docker"), "Docker Compose is unavailable")
    def test_each_compose_stack_uses_its_own_workspace_and_shared_images(self):
        images = []
        for source in ("bank", "broker"):
            environment = {**os.environ, "TEST_WS_DIR": f".workspace-{source}",
                           "SYDAFA_CONTAINER_PREFIX": f"sydafa-dlh-test-{source}",
                           "E2E_SOURCE": source}
            result = subprocess.run(
                ["docker", "compose", "-p", f"dlh-test-{source}", "-f", "docker-compose.yaml",
                 "-f", "test/docker-compose.override.yaml", "--profile", "test", "config", "--format", "json"],
                cwd=ROOT, env=environment, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, "Compose config failed")
            services = json.loads(result.stdout)["services"]
            self.assertEqual(services["test-runner"]["environment"]["E2E_SOURCE"], source)
            for name, service in services.items():
                self.assertFalse(service.get("ports"))
                for volume in service.get("volumes", []):
                    if volume["type"] == "bind" and "/test/" in volume["source"]:
                        self.assertIn(f"/.workspace-{source}", volume["source"])
            images.append({name: service.get("image") for name, service in services.items()})
        self.assertEqual(images[0], images[1])

    def test_orchestrator_rejects_low_memory_without_starting_or_cleaning(self):
        with tempfile.TemporaryDirectory() as directory:
            executable = Path(directory) / "docker"
            log = Path(directory) / "calls"
            executable.write_text('#!/bin/bash\necho "$*" >> "$DOCKER_CALLS"\nif [ "$1" = info ]; then echo 16748032000; else exit 99; fi\n')
            executable.chmod(0o755)
            result = subprocess.run(["bash", str(ROOT / "test/test.sh")],
                                    env={**os.environ, "PATH": directory + ":" + os.environ["PATH"],
                                         "DOCKER_CALLS": str(log), "COMPOSE_PROJECT_NAME": "dlh-test"},
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn("20 GiB", result.stdout)
            self.assertEqual(len(log.read_text().splitlines()), 1)

    def test_parallel_orchestrator_builds_once_and_propagates_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "test").mkdir()
            shutil_script = ROOT / "test/test.sh"
            (root / "test/test.sh").write_bytes(shutil_script.read_bytes())
            (root / "bin").mkdir()
            log = root / "calls"
            docker = root / "bin/docker"
            docker.write_text('''#!/usr/bin/env bash
echo "$*" >> "$DOCKER_CALLS"
if [ "$1" = info ]; then echo 25769803776; exit 0; fi
if [ "$1 $2" = "volume inspect" ]; then exit 1; fi
if [[ "$*" = *" run "* ]]; then
  echo 'PASS=12 FAIL=0'
  if [ "${FAIL_BANK:-0}" = 1 ] && [[ "$*" = *"-p dlh-test-bank "* ]]; then exit 7; fi
fi
exit 0
''')
            docker.chmod(0o755)
            python = root / "bin/python3"
            python.write_text('''#!/usr/bin/env bash
if [ "$1" = - ]; then
  mkdir -p "$3/ingestion/spark" "$3/test/unit" "$3/airflow/plugins"
  touch "$3/ingestion/spark/e2e_schema_evolution.py" "$3/ingestion/spark/e2e_temporal_extraction.py" "$3/ingestion/spark/e2e_guard_idle.py" "$3/test/unit/test_meta.py" "$3/airflow/plugins/e2e_check_spark_guard.py"
fi
exit 0
''')
            python.chmod(0o755)
            for fail, slow in (("0", False), ("1", False), ("0", True)):
                script = shutil_script.read_text()
                if slow:
                    script = script.replace("elapsed=$((SECONDS - started))", "elapsed=421")
                (root / "test/test.sh").write_text(script)
                log.write_text("")
                environment = {**os.environ, "PATH": str(root / "bin") + ":" + os.environ["PATH"],
                               "DOCKER_CALLS": str(log), "FAIL_BANK": fail, "TEST_SKIP_BUILD": "0",
                               "COMPOSE_PROJECT_NAME": "dlh-test", "TEST_FAILURE_LOG_DIR": str(root / "results")}
                result = subprocess.run(["bash", str(root / "test/test.sh")], env=environment,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, int(fail == "1" or slow), result.stdout + result.stderr)
                calls = log.read_text().splitlines()
                self.assertEqual(sum("--profile test build" in call for call in calls), 1)
                self.assertEqual(sum(" up -d --no-build --pull never" in call for call in calls), 2)
                self.assertIn("E2E_TOTAL_SECONDS=", result.stdout)
                if slow:
                    self.assertIn("E2E_TOTAL_SECONDS=421", result.stdout)
                    self.assertIn("Требуется оптимизация теста", result.stdout)
                self.assertFalse((root / "test/.workspace-bank").exists())
                self.assertFalse((root / "test/.workspace-broker").exists())
