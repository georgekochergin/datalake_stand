"""Airflow operators and callbacks for the raw-layer ingestion framework.

The package is the single public import point: generated DAGs, tests, and
external consumers import operators and callbacks from ingestion.operators.
"""

from .assets import publish_asset_event, snapshot_has_current_process
from .process import (
    GenerateProcessIdOperator,
    TableLogOperator,
    dagrun_failure_callback,
    dagrun_success_callback,
    log_task_failed,
)
from .raw_rdbms import (
    IngestActSnapOperator,
    IngestFullOperator,
    IngestIncrementalOperator,
    resolve_source_connection,
)
