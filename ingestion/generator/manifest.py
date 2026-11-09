"""Strict loading and validation of ingestion source and pipeline manifests."""

import re
from dataclasses import dataclass, field

from ingestion.generator import naming

_ALLOWED_SOURCE_KEYS = frozenset({"type", "airflow_conn_id"})
_ALLOWED_PIPELINE_KEYS = frozenset({"source_id", "schedule", "catchup", "tags", "entities"})
_ALLOWED_ENTITY_KEYS = frozenset({"fqn", "incremental", "spark", "airflow"})
_COLUMN_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")
_CRON_FIELD_RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))
_CRON_FIELD_NAMES = ("minute", "hour", "day_of_month", "month", "day_of_week")


@dataclass(frozen=True)
class SourceConfig:
    """Configuration of a single ingestion source."""

    source_id: str
    type: str
    airflow_conn_id: str


@dataclass(frozen=True, kw_only=True)
class DagConfig:
    """Common scheduling and labeling contract of generated DAGs."""

    schedule: str | None = None
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class SparkConfig:
    """JDBC reading options shared by entity and snapshot configuration."""

    jdbc_partitioning: bool = True


@dataclass(frozen=True)
class AirflowConfig:
    """Load-task resource options with project defaults."""

    pool: str = "default_pool"
    pool_slots: int = 1


@dataclass(frozen=True, kw_only=True)
class ActSnapConfig(DagConfig):
    """Restricted snapshot DAG configuration without entity or incremental fields."""

    spark: SparkConfig = field(default_factory=SparkConfig)
    airflow: AirflowConfig = field(default_factory=AirflowConfig)


@dataclass(frozen=True)
class EntityConfig:
    """Configuration of a single loaded entity inside a pipeline."""

    fqn: str
    target_table: str
    incremental_column: str | None
    jdbc_partitioning: bool = True
    pool: str = "default_pool"
    pool_slots: int = 1
    lookback_size: int = 0
    lookback_unit: str | None = None
    lookback_specified: bool = False
    act_snap: ActSnapConfig = field(default_factory=ActSnapConfig)
    incremental_config: dict | None = None


@dataclass(frozen=True)
class IngestionSettings:
    """Technical defaults shared by all generated pipelines."""

    num_partitions: int


@dataclass(frozen=True)
class PipelineManifest(DagConfig):
    """Parsed pipeline manifest: one YAML file defines one DAG."""

    source_id: str
    catchup: bool
    entities: tuple[EntityConfig, ...]


def _read_yaml(path) -> object:
    """Load a YAML file, importing yaml lazily so the module imports without PyYAML."""
    import yaml

    with open(path, encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def _strict_object(value, allowed: set[str], label: str) -> dict:
    """Validate a nested configuration object and reject unknown keys.

    Enforces that value is a dict with only keys from the allowlist.
    """
    if not isinstance(value, dict):
        raise ValueError(f"{label}: must be a mapping")
    unknown = set(value) - allowed
    if unknown:
        raise ValueError(f"{label}: unknown keys {sorted(map(str, unknown))}")
    return value


def _positive_integer(value, label: str) -> int:
    """Reject booleans and non-positive integers."""
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label}: must be a positive integer")
    return value


def load_settings(path) -> IngestionSettings:
    """Load strict, explicit JDBC execution defaults."""
    raw = _strict_object(_read_yaml(path), {"spark"}, "settings")
    spark = _strict_object(raw.get("spark"), {"jdbc"}, "settings.spark")
    jdbc = _strict_object(spark.get("jdbc"), {"num_partitions"}, "settings.spark.jdbc")
    return IngestionSettings(_positive_integer(jdbc.get("num_partitions"), "num_partitions"))


def _expand_cron_field(field: str, low: int, high: int, label: str) -> set[int]:
    """Expand one cron field into its set of integer values.

    Only numeric standard tokens are accepted: '*', values, ranges, steps and
    their comma-separated lists. A numeric base with a step (N/S) expands
    from N to the field maximum with step S, matching cron semantics
    ('0/15' minutes = 0,15,30,45). Month/day names, macros and wrap-around
    ranges are rejected, keeping schedule classification static and free of
    calendar arithmetic.
    """
    if not isinstance(field, str) or not field:
        raise ValueError(f"schedule {label}: must be a non-empty string")
    values: set[int] = set()
    for part in field.split(","):
        if not part:
            raise ValueError(f"schedule {label}: empty list element")
        base, slash, step_text = part.partition("/")
        step = 1
        if slash:
            if not step_text.isdigit() or int(step_text) == 0:
                raise ValueError(f"schedule {label}: invalid step in {part!r}")
            step = int(step_text)
        if base == "*":
            start, stop = low, high
        elif "-" in base:
            start_text, _, stop_text = base.partition("-")
            if not start_text.isdigit() or not stop_text.isdigit():
                raise ValueError(f"schedule {label}: unsupported token {part!r}")
            start, stop = int(start_text), int(stop_text)
            if start > stop:
                raise ValueError(f"schedule {label}: descending range in {part!r}")
        elif base.isdigit():
            start = int(base)
            stop = high if slash else start
        else:
            raise ValueError(f"schedule {label}: unsupported token {part!r}")
        if not low <= start <= high or not low <= stop <= high:
            raise ValueError(f"schedule {label}: value out of range in {part!r}")
        values.update(range(start, stop + 1, step))
    return values


def parse_schedule(schedule: str) -> tuple[set[int], set[int]]:
    """Validate a strict five-field cron expression; return (minutes, hours)."""
    fields = schedule.split()
    if len(fields) != 5:
        raise ValueError("schedule must be a standard five-field cron expression")
    minutes = _expand_cron_field(fields[0], *_CRON_FIELD_RANGES[0], _CRON_FIELD_NAMES[0])
    hours = _expand_cron_field(fields[1], *_CRON_FIELD_RANGES[1], _CRON_FIELD_NAMES[1])
    for field, (low, high), label in zip(fields[2:], _CRON_FIELD_RANGES[2:], _CRON_FIELD_NAMES[2:]):
        _expand_cron_field(field, low, high, label)
    return minutes, hours


def temporal_lookback_default(schedule: str | None) -> tuple[int, str]:
    """Temporal lookback default derived statically from the DAG schedule.

    The number of fires per day equals the product of distinct minute and
    hour values; two or more mean 3 hours, otherwise (including no schedule)
    3 days. This is a re-read volume policy, not an outage coverage promise.
    """
    if schedule is None:
        return 3, "d"
    minutes, hours = parse_schedule(schedule)
    return (3, "h") if len(minutes) * len(hours) >= 2 else (3, "d")


def _parse_dag_fields(raw: dict, label: str) -> dict:
    """Validate common DAG fields without widening either YAML allowlist."""
    schedule = raw.get("schedule")
    if schedule is not None:
        if not isinstance(schedule, str) or not schedule:
            raise ValueError(f"{label}: schedule must be a non-empty string or null")
        parse_schedule(schedule)
    tags = raw.get("tags", [])
    if not isinstance(tags, list) or any(not isinstance(tag, str) or not tag for tag in tags):
        raise ValueError(f"{label}: tags must be a list of non-empty strings")
    return {"schedule": schedule, "tags": tuple(tags)}


def _parse_load_options(raw: dict, label: str) -> tuple[SparkConfig, AirflowConfig]:
    """Parse the same nested YAML paths for entity and snapshot load settings."""
    spark = _strict_object(raw.get("spark", {}), {"jdbc_partitioning"}, f"{label}.spark")
    partitioning = spark.get("jdbc_partitioning", True)
    if type(partitioning) is not bool:
        raise ValueError(f"{label}: spark.jdbc_partitioning must be a boolean")
    airflow = _strict_object(raw.get("airflow", {}), {"pool"}, f"{label}.airflow")
    pool = _strict_object(airflow.get("pool", {}), {"name", "slots"}, f"{label}.airflow.pool")
    name = pool.get("name", "default_pool")
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"{label}: pool.name must be a non-empty string")
    return SparkConfig(partitioning), AirflowConfig(
        name, _positive_integer(pool.get("slots", 1), f"{label}.airflow.pool.slots")
    )


def _parse_act_snap(raw) -> ActSnapConfig:
    """Parse an independent snapshot DAG contract, rejecting unsupported fields."""
    label = "incremental.act_snap"
    raw = _strict_object(raw, {"schedule", "tags", "spark", "airflow"}, label)
    spark, airflow = _parse_load_options(raw, label)
    return ActSnapConfig(**_parse_dag_fields(raw, label), spark=spark, airflow=airflow)


def _parse_entity(entity: dict, position: int, temporal_default: tuple[int, str]) -> EntityConfig:
    """Parse and validate a single entity from a pipeline manifest.

    Args:
        entity: Raw entity dict from YAML.
        position: 1-based position in the entities list for error messages.

    Returns:
        EntityConfig with fully validated fields.

    Raises:
        ValueError: If the entity is invalid.
    """
    if not isinstance(entity, dict):
        raise ValueError(f"entity #{position}: must be a mapping")
    if "act_snap" in entity and "incremental" not in entity:
        raise ValueError(
            f"entity #{position}: act_snap is configured inside the incremental block; "
            "the entity has no incremental block"
        )
    unknown = set(entity) - _ALLOWED_ENTITY_KEYS
    if unknown:
        raise ValueError(f"entity #{position}: unknown keys {sorted(unknown)}")

    fqn = entity.get("fqn")
    if not isinstance(fqn, str) or not fqn:
        raise ValueError(f"entity #{position}: fqn must be a non-empty string")

    incremental_column = None
    lookback_size = 0
    lookback_unit = None
    lookback_specified = False
    act_snap = ActSnapConfig()
    incremental_config = None
    if "incremental" in entity:
        from ingestion.sources.rdbms.incremental_config import validate
        from ingestion.sources.rdbms import positions

        incremental = _strict_object(entity["incremental"], {"contract", "lookback", "act_snap"}, "incremental")
        contract = _strict_object(incremental.get("contract"), {"source_column", "position_type"}, "incremental.contract")
        incremental_column = contract.get("source_column")
        if not isinstance(incremental_column, str) or not _COLUMN_PATTERN.fullmatch(incremental_column):
            raise ValueError("incremental.source_column must be a lowercase identifier")
        if "lookback" in incremental:
            lookback = _strict_object(incremental["lookback"], {"size", "unit"}, "incremental.lookback")
            lookback_size = lookback.get("size")
            if type(lookback_size) is not int or not 0 <= lookback_size < 10 ** 38:
                raise ValueError("lookback.size must be a non-negative integer below 10^38")
            lookback_unit = lookback.get("unit")
            if lookback_size and "unit" in lookback and lookback_unit not in ("h", "d"):
                raise ValueError("lookback.unit must be h or d")
            if not lookback_size:
                lookback_unit = None
            lookback_specified = True
        else:
            family = positions.kind_info(contract.get("position_type"))[0]
            lookback_size, lookback_unit = (
                (0, None) if family == "integral" else
                (3, "d") if family == "date" else temporal_default
            )
        incremental_config = validate({
            "contract": contract,
            "lookback": {"size": lookback_size, "unit": lookback_unit},
        })
        if "act_snap" in incremental:
            act_snap = _parse_act_snap(incremental["act_snap"])

    spark_config, airflow_config = _parse_load_options(entity, f"entity {fqn!r}")

    return EntityConfig(
        fqn=fqn,
        target_table=naming.resolve_target_table_name(fqn),
        incremental_column=incremental_column,
        jdbc_partitioning=spark_config.jdbc_partitioning,
        pool=airflow_config.pool,
        pool_slots=airflow_config.pool_slots,
        lookback_size=lookback_size,
        lookback_unit=lookback_unit,
        lookback_specified=lookback_specified,
        act_snap=act_snap,
        incremental_config=incremental_config,
    )


def load_sources(path) -> dict[str, SourceConfig]:
    """Load and validate the sources registry (ingestion/conf/sources.yaml)."""
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
    """Load and validate one pipeline from the recursive ingestion/conf/pipelines tree."""
    raw = _read_yaml(path)
    if not isinstance(raw, dict):
        raise ValueError("pipeline manifest must be a mapping")
    unknown = set(raw) - _ALLOWED_PIPELINE_KEYS
    if unknown:
        raise ValueError(f"pipeline manifest: unknown keys {sorted(unknown)}")
    source_id = raw.get("source_id")
    if not isinstance(source_id, str) or not source_id:
        raise ValueError("pipeline manifest: source_id must be a non-empty string")
    dag_fields = _parse_dag_fields(raw, "pipeline manifest")
    temporal_default = temporal_lookback_default(dag_fields["schedule"])
    catchup = raw.get("catchup", False)
    if not isinstance(catchup, bool):
        raise ValueError("pipeline manifest: catchup must be a boolean")
    entities_raw = raw.get("entities")
    if not isinstance(entities_raw, list) or not entities_raw:
        raise ValueError("pipeline manifest: entities must be a non-empty list")

    entities: list[EntityConfig] = []
    seen_fqns: set[str] = set()
    for position, entity_raw in enumerate(entities_raw):
        entity = _parse_entity(entity_raw, position + 1, temporal_default)
        if entity.fqn in seen_fqns:
            raise ValueError(f"duplicate entity FQN: {entity.fqn!r}")
        seen_fqns.add(entity.fqn)
        entities.append(entity)

    naming.assert_unique_targets([entity.fqn for entity in entities])
    return PipelineManifest(
        source_id=source_id,
        **dag_fields,
        catchup=catchup,
        entities=tuple(entities),
    )


def validate_pipeline(sources: dict[str, SourceConfig], pipeline: PipelineManifest) -> None:
    """Validate a parsed pipeline manifest against the sources registry."""
    if pipeline.source_id not in sources:
        raise ValueError(
            f"pipeline source_id {pipeline.source_id!r} not found in sources registry"
        )
