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
sys.path.insert(0, str(ROOT / "test/e2e/python"))
from e2e_scale import APPEND_ROWS, disable_retries, prepare_workspace, scale_migration
from ingestion.generator.generate_dags import generate_all
from ingestion.generator.manifest import load_sources


class ParallelE2ETests(unittest.TestCase):
    """Preserve production defaults while preparing bounded test-only copies."""

    def test_test_tree_separates_host_and_runtime_checks(self):
        config = (ROOT / "pytest.ini").read_text()
        self.assertIn("testpaths = test/pytest", config)
        runtime = ROOT / "test/e2e/python"
        self.assertTrue((runtime / "check_meta.py").is_file())
        self.assertFalse(any(runtime.glob("test_*.py")))
        self.assertNotIn("EXTRACTION_TEST_DSN", (ROOT / "test/pytest/test_meta.py").read_text())
        flow = (runtime / "run_flow.py").read_text()
        self.assertNotIn("DROP TABLE", flow)
        self.assertNotIn("rejects injected column", flow)

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

    def test_unknown_workspace_source_is_rejected_before_copying(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "workspace"
            with self.assertRaisesRegex(ValueError, "Unknown e2e source"):
                prepare_workspace(ROOT, destination, "missing")
            self.assertFalse(destination.exists())

    def test_source_selection_validation_precedes_docker(self):
        for arguments in (("--sources",), ("--sources", "missing"),
                          ("--sources", "bank", "bank"),
                          ("--sources", "bank", "broker", "third"),
                          ("--source", "bank"), ("--verify",),
                          ("--verify", "--sources", "bank", "broker")):
            with self.subTest(arguments=arguments), tempfile.TemporaryDirectory() as directory:
                docker = Path(directory) / "docker"
                calls = Path(directory) / "calls"
                docker.write_text('#!/bin/bash\ntouch "$DOCKER_CALLS"\nexit 99\n')
                docker.chmod(0o755)
                result = subprocess.run(["bash", str(ROOT / "test/e2e/shell/test.sh"), *arguments],
                                        env={**os.environ, "PATH": directory + ":" + str(Path(sys.executable).parent) + ":" + os.environ["PATH"],
                                             "DOCKER_CALLS": str(calls)}, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(calls.exists())

    def test_post_checks_and_snapshot_have_no_source_specific_skip(self):
        script = (ROOT / "test/e2e/shell/test.sh").read_text()
        post = script.split("run_post_checks() (", 1)[1].split("run_runtime_checks()", 1)[0]
        self.assertNotIn('"$E2E_SOURCE" =', post)
        self.assertIn("e2e_check_spark_guard.py", post)
        self.assertIn("e2e_check_meta.py", post)
        self.assertIn("e2e_schema_evolution.py", post)
        self.assertNotIn('if "broker" not in SOURCES', (ROOT / "test/e2e/python/run_flow.py").read_text())
        guard = (ROOT / "test/e2e/python/check_spark_guard.py").read_text()
        self.assertIn('entity["incremental"]["contract"]', guard)
        self.assertNotIn('"position_type": "bigint"', guard)

    def test_selection_uses_manifest_keys_and_requires_explicit_subset_above_two(self):
        script = (ROOT / "test/e2e/shell/test.sh").read_text()
        selection = "set -euo pipefail\n" + script.split("MODE=full", 1)[1].split("configure_stack()", 1)[0]
        selection = "MODE=full\n" + selection
        selection += '\nprintf "SELECTED=%s\\n" "${SOURCES[@]}"\n'
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "ingestion/conf/sources.yaml"
            config.parent.mkdir(parents=True)
            config.write_text("alpha: {}\nbeta: {}\ngamma: {}\n")
            cases = (((), 1, ()), (("--sources", "gamma"), 0, ("gamma",)),
                     (("--sources", "beta", "alpha"), 0, ("beta", "alpha")),
                     (("--verify", "--sources", "beta"), 0, ("beta",)))
            for arguments, code, expected in cases:
                with self.subTest(arguments=arguments):
                    result = subprocess.run(["bash", "-c", selection, "selection", *arguments],
                                            cwd=directory, capture_output=True, text=True,
                                            env={**os.environ, "PATH": str(Path(sys.executable).parent) + ":" + os.environ["PATH"]})
                    self.assertEqual(result.returncode, code, result.stderr)
                    self.assertEqual(tuple(line.removeprefix("SELECTED=") for line in result.stdout.splitlines()
                                           if line.startswith("SELECTED=")), expected)

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
            sys.path.insert(0, str(ROOT / "test/e2e/python"))
            try:
                spec = importlib.util.spec_from_file_location("e2e_static_contracts", ROOT / "test/e2e/python/run_flow.py")
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                module.validate_manifests_and_dags()
                self.assertEqual(module.FAIL, 0)
            finally:
                sys.path.remove(str(ROOT / "test/e2e/python"))

    def test_source_runtime_inventory(self):
        for selected in ("bank", "broker"):
            with patch.dict(os.environ, {"E2E_SOURCE": selected}):
                spec = importlib.util.spec_from_file_location("isolated_flow", ROOT / "test/e2e/python/run_flow.py")
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
        self.assertIn("airflow pools set ingest_loads 6", services["airflow-init"]["command"][2])

    @unittest.skipUnless(shutil.which("docker"), "Docker Compose is unavailable")
    def test_each_compose_stack_uses_its_own_workspace_and_shared_images(self):
        images = []
        for source in ("bank", "broker"):
            environment = {**os.environ, "TEST_WS_DIR": f".workspace-{source}",
                           "SYDAFA_CONTAINER_PREFIX": f"sydafa-dlh-test-{source}",
                           "E2E_SOURCE": source}
            result = subprocess.run(
                ["docker", "compose", "-p", f"dlh-test-{source}", "-f", "docker-compose.yaml",
                 "-f", "test/e2e/docker/docker-compose.override.yaml", "--profile", "test", "config", "--format", "json"],
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
            result = subprocess.run(["bash", str(ROOT / "test/e2e/shell/test.sh")],
                                    env={**os.environ, "PATH": directory + ":" + str(Path(sys.executable).parent) + ":" + os.environ["PATH"],
                                         "DOCKER_CALLS": str(log), "COMPOSE_PROJECT_NAME": "dlh-test"},
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn("20 GiB", result.stdout)
            self.assertEqual(len(log.read_text().splitlines()), 1)

    def test_memory_threshold_is_ten_gib_per_selected_stack(self):
        script = (ROOT / "test/e2e/shell/test.sh").read_text()
        check = script.split("memory=$(docker info", 1)[1].split("started=$SECONDS", 1)[0]
        check = "memory=$(docker info" + check
        for count, gib, code in ((1, 9, 1), (1, 10, 0), (2, 19, 1), (2, 20, 0)):
            with self.subTest(count=count, gib=gib):
                harness = f'docker() {{ echo {gib * 1073741824}; }}\nSOURCES=({"bank broker" if count == 2 else "bank"})\n' + check
                result = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
                self.assertEqual(result.returncode, code, result.stderr)

    def test_parallel_post_checks_join_temporal_and_propagate_each_failure(self):
        script = (ROOT / "test/e2e/shell/test.sh").read_text()
        functions = script.split("run_temporal_checks() {", 1)[1].split("run_runtime_checks() {", 1)[0]
        for source in ("bank", "broker"):
            for failure in ("none", "temporal", "schema", "foreground", "meta"):
                with self.subTest(source=source, failure=failure), tempfile.TemporaryDirectory() as directory:
                    harness = r'''
set -euo pipefail
compose() {
  case "$*" in
    *e2e_temporal_extraction.py*)
      touch "$SANDBOX/temporal-started"
      for attempt in {1..100}; do
        [ -f "$SANDBOX/foreground-started" ] && break
        sleep 0.01
      done
      [ -f "$SANDBOX/foreground-started" ] || return 99
      [ "$FAILURE" != temporal ] || return 7
      touch "$SANDBOX/temporal-finished" ;;
    *e2e_schema_evolution.py*)
      touch "$SANDBOX/schema-started"
      [ "$FAILURE" != schema ] || return 9
      touch "$SANDBOX/schema-finished" ;;
    *e2e_check_meta.py*)
      [ "$FAILURE" != meta ] || return 10 ;;
    *)
      touch "$SANDBOX/foreground-started"
      [ "$FAILURE" != foreground ] || return 8 ;;
  esac
}
COMPOSE=(compose)
run_temporal_checks() {
''' + functions + "\nrun_post_checks\n"
                    result = subprocess.run(["bash", "-c", harness],
                                            env={**os.environ, "SANDBOX": directory,
                                                 "E2E_SOURCE": source, "FAILURE": failure},
                                            capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode, {"none": 0, "temporal": 7, "schema": 9, "foreground": 8, "meta": 10}[failure], result.stderr)
                    if failure == "none":
                        self.assertTrue((Path(directory) / "temporal-finished").exists())
                        self.assertTrue((Path(directory) / "schema-finished").exists())

    def test_parallel_orchestrator_builds_once_and_propagates_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "test/e2e/shell").mkdir(parents=True)
            shutil_script = ROOT / "test/e2e/shell/test.sh"
            (root / "test/e2e/shell/test.sh").write_bytes(shutil_script.read_bytes())
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
if [ "$2" = full ] || [ "$2" = verify ]; then
  shift 2
  if [ "$#" = 0 ]; then printf 'bank\\nbroker\\n'; else printf '%s\\n' "$@"; fi
  exit 0
fi
if [ "$1" = - ]; then
  mkdir -p "$3/ingestion/spark" "$3/airflow/plugins"
  touch "$3/ingestion/spark/e2e_schema_evolution.py" "$3/ingestion/spark/e2e_temporal_extraction.py" "$3/ingestion/spark/e2e_guard_idle.py" "$3/airflow/plugins/e2e_check_meta.py" "$3/airflow/plugins/e2e_check_spark_guard.py"
fi
exit 0
''')
            python.chmod(0o755)
            for sources, fail, slow in (((), "0", False), (("bank",), "0", False),
                                        (("broker",), "0", False), (("broker", "bank"), "0", False),
                                        ((), "1", False), ((), "0", True)):
                script = shutil_script.read_text()
                if slow:
                    script = script.replace("elapsed=$((SECONDS - started))", "elapsed=421")
                (root / "test/e2e/shell/test.sh").write_text(script)
                log.write_text("")
                environment = {**os.environ, "PATH": str(root / "bin") + ":" + os.environ["PATH"],
                               "DOCKER_CALLS": str(log), "FAIL_BANK": fail, "TEST_SKIP_BUILD": "0",
                               "COMPOSE_PROJECT_NAME": "dlh-test", "TEST_FAILURE_LOG_DIR": str(root / "results")}
                arguments = ["--sources", *sources] if sources else []
                result = subprocess.run(["bash", str(root / "test/e2e/shell/test.sh"), *arguments], env=environment,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, int(fail == "1" or slow), result.stdout + result.stderr)
                calls = log.read_text().splitlines()
                self.assertEqual(sum("--profile test build" in call for call in calls), 1)
                self.assertEqual(sum(" up -d --no-build --pull never" in call for call in calls), len(sources) or 2)
                for excluded in {"bank", "broker"} - set(sources or ("bank", "broker")):
                    self.assertFalse(any(f"-p dlh-test-{excluded} " in call for call in calls))
                self.assertIn("E2E_TOTAL_SECONDS=", result.stdout)
                if slow:
                    self.assertIn("E2E_TOTAL_SECONDS=421", result.stdout)
                    self.assertIn("Требуется оптимизация теста", result.stdout)
                self.assertFalse((root / "test/.workspace-bank").exists())
                self.assertFalse((root / "test/.workspace-broker").exists())
