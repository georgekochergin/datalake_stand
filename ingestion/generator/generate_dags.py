"""Static DAG generator: pipeline manifests -> fully-expanded Airflow DAG files.

Renders one literal .py file per pipeline into airflow/dags/generated/ so
the DAG bag contains no YAML parsing and no runtime lookups at parse time.
Run manually to (re)generate, or with --check to verify generated-artifact
drift (regenerate in memory, diff against disk, exit non-zero on drift).
"""

import argparse
import json
import re
import sys
from pathlib import Path
from urllib.parse import quote

from ingestion.generator.manifest import (
    PipelineManifest,
    SourceConfig,
    load_pipeline,
    load_sources,
    load_settings,
    validate_pipeline,
)
from ingestion.generator.naming import CATALOG, resolve_fully_qualified

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCES = REPO_ROOT / "ingestion" / "conf" / "sources.yaml"
DEFAULT_PIPELINES = REPO_ROOT / "ingestion" / "conf" / "pipelines"
DEFAULT_OUTPUT = REPO_ROOT / "airflow" / "dags" / "generated"
DEFAULT_SETTINGS = REPO_ROOT / "ingestion" / "conf" / "settings.yaml"
ACTIVATE_TTL_MINUTES = 15


def dag_id_for(source: SourceConfig, pipeline_name: str) -> str:
    """Airflow dag_id for a pipeline."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*", pipeline_name):
        raise ValueError(f"invalid pipeline file stem: {pipeline_name!r}")
    suffix = "" if pipeline_name == source.source_id else f"__{pipeline_name}"
    return f"ingest__{source.type}__{source.source_id}{suffix}"


def file_name_for(source: SourceConfig, pipeline_name: str) -> str:
    """Generated file name for a pipeline."""
    return f"{dag_id_for(source, pipeline_name)}.py"


def asset_uri(source_id: str, fqn: str) -> str:
    """Asset URI: raw entity FQN, percent-encoded where needed."""
    return f"{CATALOG}://{source_id}/{quote(fqn)}"


def _entity_block(source: SourceConfig, entity, num_partitions: int) -> list[str]:
    """Render logging, loading, and separate Asset publication for one entity."""
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
        f'                entity_fqn={json.dumps(entity.fqn)},',
        f'                datalake_table="{target}",',
        '                status="START",',
        '            )',
        '            load = IngestAppendOperator(',
        '                task_id="load",',
        f'                pool={json.dumps(entity.pool)},',
        f'                pool_slots={entity.pool_slots},',
        f'                source_type={json.dumps(source.type)},',
        f'                parallelism={entity.parallelism!r},',
        f'                num_partitions={num_partitions},',
        f'                source_id="{source.source_id}",',
        f'                airflow_conn_id={json.dumps(source.airflow_conn_id)},',
        f'                fqn={json.dumps(entity.fqn)},',
        f'                target_table="{target}",',
    ]
    if incremental_line:
        lines.append(f"                {incremental_line}")
        lines.append(f"                lookback_size={entity.lookback_size},")
        lines.append(f"                lookback_unit={entity.lookback_unit!r},")
        lines.append(f"                lookback_specified={entity.lookback_specified!r},")
    lines.extend(
        [
            '            )',
            '            log_end = TableLogOperator(',
            '                task_id="log_end",',
            f'                source_id="{source.source_id}",',
            f'                entity_fqn={json.dumps(entity.fqn)},',
            f'                datalake_table="{target}",',
            '                status="END",',
        ]
    )
    lines.extend([
        '            )',
        '            publish_asset = EmptyOperator(',
        '                task_id="publish_asset",',
        f'                outlets=[Asset("{asset_uri(source.source_id, entity.fqn)}")],',
        '            )',
        '            log_start >> load >> log_end >> publish_asset',
    ])
    return lines


def generate_dag_source(
    source: SourceConfig, pipeline: PipelineManifest, pipeline_name: str | None = None,
    num_partitions: int | None = None,
) -> str:
    """Render the full DAG module source for one pipeline."""
    description = f"Raw-layer ingestion for source {pipeline.source_id} (generated)"
    if num_partitions is None:
        num_partitions = load_settings(DEFAULT_SETTINGS).num_partitions
    lines = [
        f'"""Generated DAG for pipeline "{pipeline.source_id}" — regenerate via '
        'python3 -m ingestion.generator.generate_dags, do not edit."""',
        "from datetime import datetime",
        "",
        "from airflow import DAG",
        "from airflow.providers.standard.operators.empty import EmptyOperator",
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
        f'    dag_id="{dag_id_for(source, pipeline_name or pipeline.source_id)}",',
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
        '    generate_process_id = GenerateProcessIdOperator(task_id="generate_process_id", entities=' + repr([entity.fqn for entity in pipeline.entities]) + ')',
        '    with TaskGroup(group_id="ingest_group") as ingest_group:',
    ]
    for entity in pipeline.entities:
        lines.extend(_entity_block(source, entity, num_partitions))
    lines.extend(
        [
            "    generate_process_id >> activate_source >> ingest_group",
            "",
        ]
    )
    return "\n".join(lines)


def generate_all(
    sources: dict[str, SourceConfig], pipelines_dir: Path, settings_path: Path = DEFAULT_SETTINGS,
) -> dict[str, str]:
    """Render every pipeline manifest into {generated file name: module source}."""
    settings = load_settings(settings_path)
    paths = sorted(pipelines_dir.rglob("*.yaml"))
    if not paths:
        raise ValueError(f"no pipeline manifests found in {pipelines_dir}")
    generated: dict[str, str] = {}
    owners: dict[str, Path] = {}
    targets: dict[str, Path] = {}
    for path in paths:
        pipeline = load_pipeline(path)
        validate_pipeline(sources, pipeline)
        source = sources[pipeline.source_id]
        name = file_name_for(source, path.stem)
        if name in owners:
            raise ValueError(f"DAG collision {name}: {owners[name]} and {path}")
        owners[name] = path
        for entity in pipeline.entities:
            target = resolve_fully_qualified(source.source_id, entity.fqn)
            if target in targets:
                raise ValueError(f"target collision {target}: {targets[target]} and {path}")
            targets[target] = path
        generated[name] = generate_dag_source(source, pipeline, path.stem, settings.num_partitions)
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
    parser.add_argument("--settings", type=Path, default=DEFAULT_SETTINGS)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    sources = load_sources(args.sources)
    generated = generate_all(sources, args.pipelines, args.settings)
    if args.check:
        drift = compute_drift(generated, args.output)
        for item in drift:
            print(item)
        print(f"check: {len(drift)} difference(s)")
        return 1 if drift else 0
    args.output.mkdir(parents=True, exist_ok=True)
    for name in generated:
        path = args.output / name
        if path.exists() and not path.read_text(encoding="utf-8").startswith('"""Generated DAG for pipeline '):
            raise ValueError(f"refusing to overwrite non-generated file: {path}")
    for path in args.output.glob("ingest__*.py"):
        if path.name not in generated and path.read_text(encoding="utf-8").startswith('"""Generated DAG for pipeline '):
            path.unlink()
    for name, content in sorted(generated.items()):
        path = args.output / name
        path.write_text(content, encoding="utf-8")
        print(f"generated: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
