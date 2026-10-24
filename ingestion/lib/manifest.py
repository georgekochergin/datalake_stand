"""Strict loading and validation of ingestion source and pipeline manifests."""

import re
from dataclasses import dataclass

from ingestion.lib import naming

_ALLOWED_SOURCE_KEYS = frozenset({"type", "airflow_conn_id"})
_ALLOWED_PIPELINE_KEYS = frozenset({"source_id", "schedule", "catchup", "tags", "entities"})
_ALLOWED_ENTITY_KEYS = frozenset({"fqn", "incremental_column"})
_COLUMN_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass(frozen=True)
class SourceConfig:
    """Configuration of a single ingestion source."""

    source_id: str
    type: str
    airflow_conn_id: str


@dataclass(frozen=True)
class EntityConfig:
    """Configuration of a single loaded entity inside a pipeline."""

    fqn: str
    target_table: str
    incremental_column: str | None


@dataclass(frozen=True)
class PipelineManifest:
    """Parsed pipeline manifest: one YAML file defines one DAG."""

    source_id: str
    schedule: str | None
    catchup: bool
    tags: tuple[str, ...]
    entities: tuple[EntityConfig, ...]


def _read_yaml(path) -> object:
    """Load a YAML file, importing yaml lazily so the module imports without PyYAML."""
    import yaml

    with open(path, encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def load_sources(path) -> dict[str, SourceConfig]:
    """Load and validate the sources registry (ingestion/sources.yaml)."""
    raw = _read_yaml(path)
    if not isinstance(raw, dict) or not raw:
        raise ValueError("sources registry must be a non-empty mapping of source id to config")
    sources: dict[str, SourceConfig] = {}
    for source_id, config in raw.items():
        naming.resolve_namespace(source_id)
        if not isinstance(config, dict):
            raise ValueError(f"source {source_id!r}: config must be a mapping")
        unknown = set(config) - _ALLOWED_SOURCE_KEYS
        if unknown:
            raise ValueError(f"source {source_id!r}: unknown keys {sorted(unknown)}")
        source_type = config.get("type")
        if source_type == "kafka":
            raise ValueError(f"source {source_id!r}: type 'kafka' is not implemented yet")
        if source_type != "rdbms":
            raise ValueError(f"source {source_id!r}: type must be 'rdbms' (got {source_type!r})")
        airflow_conn_id = config.get("airflow_conn_id")
        if not isinstance(airflow_conn_id, str) or not airflow_conn_id:
            raise ValueError(f"source {source_id!r}: airflow_conn_id must be a non-empty string")
        sources[source_id] = SourceConfig(
            source_id=source_id,
            type=source_type,
            airflow_conn_id=airflow_conn_id,
        )
    return sources


def load_pipeline(path) -> PipelineManifest:
    """Load and validate a pipeline manifest (ingestion/pipelines/<source_id>.yaml)."""
    raw = _read_yaml(path)
    if not isinstance(raw, dict):
        raise ValueError("pipeline manifest must be a mapping")
    unknown = set(raw) - _ALLOWED_PIPELINE_KEYS
    if unknown:
        raise ValueError(f"pipeline manifest: unknown keys {sorted(unknown)}")
    source_id = raw.get("source_id")
    if not isinstance(source_id, str) or not source_id:
        raise ValueError("pipeline manifest: source_id must be a non-empty string")
    schedule = raw.get("schedule")
    if schedule is not None and (not isinstance(schedule, str) or not schedule):
        raise ValueError("pipeline manifest: schedule must be a non-empty string or null")
    catchup = raw.get("catchup", False)
    if not isinstance(catchup, bool):
        raise ValueError("pipeline manifest: catchup must be a boolean")
    tags = raw.get("tags", [])
    if not isinstance(tags, list) or any(not isinstance(tag, str) or not tag for tag in tags):
        raise ValueError("pipeline manifest: tags must be a list of non-empty strings")
    entities_raw = raw.get("entities")
    if not isinstance(entities_raw, list) or not entities_raw:
        raise ValueError("pipeline manifest: entities must be a non-empty list")
    entities: list[EntityConfig] = []
    seen_fqns: set[str] = set()
    for position, entity in enumerate(entities_raw):
        if not isinstance(entity, dict):
            raise ValueError(f"entity #{position + 1}: must be a mapping")
        unknown = set(entity) - _ALLOWED_ENTITY_KEYS
        if unknown:
            raise ValueError(f"entity #{position + 1}: unknown keys {sorted(unknown)}")
        fqn = entity.get("fqn")
        if not isinstance(fqn, str) or not fqn:
            raise ValueError(f"entity #{position + 1}: fqn must be a non-empty string")
        if fqn in seen_fqns:
            raise ValueError(f"duplicate entity FQN: {fqn!r}")
        seen_fqns.add(fqn)
        incremental_column = entity.get("incremental_column")
        if incremental_column is not None and (
            not isinstance(incremental_column, str) or not _COLUMN_PATTERN.match(incremental_column)
        ):
            raise ValueError(f"entity {fqn!r}: incremental_column must be a lowercase identifier")
        entities.append(
            EntityConfig(
                fqn=fqn,
                target_table=naming.resolve_target_table_name(fqn),
                incremental_column=incremental_column,
            )
        )
    naming.assert_unique_targets([entity.fqn for entity in entities])
    return PipelineManifest(
        source_id=source_id,
        schedule=schedule,
        catchup=catchup,
        tags=tuple(tags),
        entities=tuple(entities),
    )


def validate_pipeline(sources: dict[str, SourceConfig], pipeline: PipelineManifest) -> None:
    """Validate a parsed pipeline manifest against the sources registry."""
    if pipeline.source_id not in sources:
        raise ValueError(
            f"pipeline source_id {pipeline.source_id!r} not found in sources registry"
        )
