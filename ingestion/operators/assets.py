"""Asset publication for raw loads: unconditional for pipelines, checked for snapshots."""

from airflow.sdk.exceptions import AirflowSkipException

from .process import _pull_process_id
from ingestion.spark.raw.iceberg import validate_target_table


def snapshot_has_current_process(target_table: str, process_id: int) -> bool:
    """Check one row of an atomic single-process snapshot; propagate read failures."""
    from airflow.providers.trino.hooks.trino import TrinoHook

    validate_target_table(target_table)
    row = TrinoHook(trino_conn_id="trino_default").get_first(
        f"SELECT __process_id FROM {target_table} LIMIT 1"
    )
    return bool(row) and row[0] == process_id


def publish_asset_event(target_table: str | None = None, **context) -> None:
    """Publish unconditionally for raw loads, or only for this process's nonempty snapshot."""
    if target_table is None:
        return
    process_id = _pull_process_id(context)
    if process_id is None:
        raise ValueError("missing process_id from generate_process_id")
    if not snapshot_has_current_process(target_table, process_id):
        raise AirflowSkipException("current process has no published snapshot rows")
