"""Static DAG generator: pipeline manifests -> fully-expanded Airflow DAG files.

Renders one literal .py file per pipeline into airflow/dags/generated/ so
the DAG bag contains no YAML parsing and no runtime lookups at parse time.
Run manually to (re)generate, or with --check to verify committed-state
drift (regenerate in memory, diff against disk, exit non-zero on drift).
"""

import argparse
import sys
from pathlib import Path
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.lib.manifest import (
    PipelineManifest,
    SourceConfig,
    load_pipeline,
    load_sources,
    validate_pipeline,
)
from ingestion.lib.naming import CATALOG, resolve_fully_qualified

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCES = REPO_ROOT / "ingestion" / "sources.yaml"
DEFAULT_PIPELINES = REPO_ROOT / "ingestion" / "pipelines"
DEFAULT_OUTPUT = REPO_ROOT / "airflow" / "dags" / "generated"
ACTIVATE_TTL_MINUTES = 15
LOAD_POOL = "ingest_loads"


def dag_id_for(source_id: str) -> str:
    """Airflow dag_id for a pipeline."""
    return f"ingest_{source_id}"


def file_name_for(source_id: str) -> str:
    """Generated file name for a pipeline."""
    return f"ingest__{source_id}.py"


def asset_uri(source_id: str, fqn: str) -> str:
    """Asset URI: raw entity FQN, percent-encoded where needed."""
    return f"{CATALOG}://{source_id}/{quote(fqn)}"


def _entity_block(source: SourceConfig, entity) -> list[str]:
    """Render the per-entity TaskGroup (log_start -> load -> log_end)."""
    target = resolve_fully_qualified(source.source_id, entity.fqn)
    group_id = f"load_{target.split('.')[-1]}"
    incremental_line = (
        f'incremental_column="{entity.incremental_column}",' if entity.incremental_column else None
    )
    lines = [
        f'        with TaskGroup(group_id="{group_id}") as load_group:',
        '            log_start = TableLogOperator(',
        '                task_id="log_start",',
        f'                source_id="{source.source_id}",',
        f'                entity_fqn="{entity.fqn}",',
        f'                datalake_table="{target}",',
        '                status="START",',
        '            )',
        '            load = IngestAppendOperator(',
        '                task_id="load",',
        f'                pool="{LOAD_POOL}",',
        f'                source_id="{source.source_id}",',
        f'                airflow_conn_id="{source.airflow_conn_id}",',
        f'                fqn="{entity.fqn}",',
        f'                target_table="{target}",',
    ]
    if incremental_line:
        lines.append(f"                {incremental_line}")
    lines.extend(
        [
            f'                outlets=[Asset("{asset_uri(source.source_id, entity.fqn)}")],',
            '            )',
            '            log_end = TableLogOperator(',
            '                task_id="log_end",',
            f'                source_id="{source.source_id}",',
            f'                entity_fqn="{entity.fqn}",',
            f'                datalake_table="{target}",',
            '                status="END",',
        ]
    )
    if incremental_line:
        lines.append(f"                {incremental_line}")
    lines.extend(['            )', '            log_start >> load >> log_end'])
    return lines


def generate_dag_source(source: SourceConfig, pipeline: PipelineManifest) -> str:
    """Render the full DAG module source for one pipeline."""
    description = f"Raw-layer ingestion for source {pipeline.source_id} (generated)"
    lines = [
        f'"""Generated DAG for pipeline "{pipeline.source_id}" — regenerate via '
        'ingestion/generate_dags.py, do not edit."""',
        "from datetime import datetime",
        "",
        "from airflow import DAG",
        "from airflow.sdk import Asset",
        "from airflow.utils.task_group import TaskGroup",
        "from sydafa_airflow.operators import SydafaActivateOperator",
        "",
        "from ingestion.operators import (",
        "    GenerateProcessIdOperator,",
        "    IngestAppendOperator,",
        "    TableLogOperator,",
        "    dagrun_failure_callback,",
        "    dagrun_success_callback,",
        ")",
        "",
        "with DAG(",
        f'    dag_id="{dag_id_for(pipeline.source_id)}",',
        f"    description={description!r},",
        "    start_date=datetime(2026, 1, 1),",
        f"    schedule={pipeline.schedule!r},",
        f"    catchup={pipeline.catchup!r},",
        "    max_active_runs=1,",
        f"    tags={list(pipeline.tags)!r},",
        "    on_success_callback=dagrun_success_callback,",
        "    on_failure_callback=dagrun_failure_callback,",
        ") as dag:",
        '    activate_source = SydafaActivateOperator(',
        '        task_id="activate_source",',
        '        conn_id="sydafa_default",',
        f'        source="{source.source_id}",',
        f"        ttl_minutes={ACTIVATE_TTL_MINUTES},",
        "    )",
        '    generate_process_id = GenerateProcessIdOperator(task_id="generate_process_id")',
        '    with TaskGroup(group_id="ingest_group") as ingest_group:',
    ]
    for entity in pipeline.entities:
        lines.extend(_entity_block(source, entity))
    lines.extend(
        [
            "    activate_source >> generate_process_id >> ingest_group",
            "",
        ]
    )
    return "\n".join(lines)


def generate_all(sources: dict[str, SourceConfig], pipelines_dir: Path) -> dict[str, str]:
    """Render every pipeline manifest into {generated file name: module source}."""
    paths = sorted(pipelines_dir.glob("*.yaml"))
    if not paths:
        raise ValueError(f"no pipeline manifests found in {pipelines_dir}")
    generated: dict[str, str] = {}
    for path in paths:
        pipeline = load_pipeline(path)
        validate_pipeline(sources, pipeline)
        source = sources[pipeline.source_id]
        generated[file_name_for(pipeline.source_id)] = generate_dag_source(source, pipeline)
    return generated


def compute_drift(generated: dict[str, str], output_dir: Path) -> list[str]:
    """Describe differences between rendered sources and files on disk."""
    drift: list[str] = []
    for name, content in sorted(generated.items()):
        path = output_dir / name
        if not path.exists():
            drift.append(f"missing: {path}")
        elif path.read_text(encoding="utf-8") != content:
            drift.append(f"differs: {path}")
    if output_dir.exists():
        expected = set(generated)
        for path in sorted(output_dir.glob("*.py")):
            if path.name not in expected:
                drift.append(f"stale: {path}")
    return drift


def main(argv: list[str] | None = None) -> int:
    """Generate DAG files or verify drift; returns the process exit code."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES)
    parser.add_argument("--pipelines", type=Path, default=DEFAULT_PIPELINES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    sources = load_sources(args.sources)
    generated = generate_all(sources, args.pipelines)
    if args.check:
        drift = compute_drift(generated, args.output)
        for item in drift:
            print(item)
        print(f"check: {len(drift)} difference(s)")
        return 1 if drift else 0
    args.output.mkdir(parents=True, exist_ok=True)
    for name, content in sorted(generated.items()):
        path = args.output / name
        path.write_text(content, encoding="utf-8")
        print(f"generated: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
