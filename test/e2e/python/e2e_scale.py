"""Prepare source-isolated e2e trees without changing production configuration."""

import ast
from copy import deepcopy
import json
from pathlib import Path
import shutil

import yaml

APPEND_ROWS = {"clients": 10, "accounts": 10, "deals": 100, "transactions": 100}
MAX_ROWS = 100


def scale_migration(migration):
    """Return the bounded test profile, preserving production input."""
    result = deepcopy(migration)
    for operation in result["operations"]:
        if operation["op"] == "append":
            operation["rows"] = APPEND_ROWS[operation["entity_fqn"].rsplit(".", 1)[1]]
        for field in ("rows", "limit"):
            if field in operation and not 1 <= operation[field] <= MAX_ROWS:
                raise ValueError(f"Invalid e2e {field}: {operation[field]}")
    return result


def disable_retries(text):
    """Change only literal retries defaults in the isolated DAG copy."""
    tree = ast.parse(text)
    replacements = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                if isinstance(key, ast.Constant) and key.value == "retries":
                    if not isinstance(value, ast.Constant) or not isinstance(value.value, int):
                        raise ValueError("DAG retries must be a literal integer")
                    replacements.append(value)
    lines = text.splitlines(keepends=True)
    for value in sorted(replacements, key=lambda item: (item.lineno, item.col_offset), reverse=True):
        index = value.lineno - 1
        encoded = lines[index].encode()
        lines[index] = (encoded[:value.col_offset] + b"0" + encoded[value.end_col_offset:]).decode()
    return "".join(lines)


def prepare_workspace(root, destination, source):
    """Copy current files and retain exactly one source in disposable configuration."""
    root, destination = Path(root), Path(destination)
    configured = yaml.safe_load((root / "ingestion/conf/sources.yaml").read_text())
    manifest = json.loads((root / "conf/sydafa/sources.json").read_text())
    if source not in configured or source not in manifest["sources"]:
        raise ValueError(f"Unknown e2e source: {source}")
    for name in ("conf", "airflow", "ingestion", "trino"):
        if (root / name).exists():
            shutil.copytree(root / name, destination / name,
                            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "logs"))
    manifest_path = destination / "conf/sydafa/sources.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["sources"] = {source: manifest["sources"][source]}
    manifest["sources"][source].pop("external_port", None)
    manifest_path.write_text(json.dumps(manifest, indent=2))
    config_path = destination / "ingestion/conf/sources.yaml"
    config = yaml.safe_load(config_path.read_text())
    config = {source: config[source]}
    config_path.write_text(yaml.safe_dump(config, sort_keys=False))
    for path in (destination / "airflow/plugins/migrations").glob("*.json"):
        if path.stem != source:
            path.unlink()
        else:
            path.write_text(json.dumps(scale_migration(json.loads(path.read_text())), indent=2))
    for path in (destination / "airflow/dags").rglob("*.py"):
        if (path.name.startswith("update_source_") and path.stem != f"update_source_{source}") or (
            path.name.startswith("ingest__rdbms__") and not path.name.startswith(f"ingest__rdbms__{source}__")
        ):
            path.unlink()
        else:
            path.write_text(disable_retries(path.read_text()))
    copies = {
        "check_extraction.py": "airflow/plugins/e2e_check_extraction.py",
        "check_spark_guard.py": "airflow/plugins/e2e_check_spark_guard.py",
        "spark_guard_idle.py": "ingestion/spark/e2e_guard_idle.py",
        "schema_evolution.py": "ingestion/spark/e2e_schema_evolution.py",
        "temporal_extraction.py": "ingestion/spark/e2e_temporal_extraction.py",
    }
    for original, target in copies.items():
        shutil.copy2(root / "test/e2e/python" / original, destination / target)
    shutil.copy2(root / "test/e2e/python/check_meta.py", destination / "airflow/plugins/e2e_check_meta.py")
