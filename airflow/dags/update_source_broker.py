"""DAG обновления источника broker (Oracle)."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from migrations import load_migration_operations
from sydafa_airflow.operators import SydafaMigrateOperator


broker_operations = load_migration_operations("broker")

with DAG(
    dag_id="update_source_broker",
    description="Транзакционное обновление источника broker (Oracle) через sydafa migrate API",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sydafa", "broker", "oracle", "migrate"],
) as dag:
    migrate_broker = SydafaMigrateOperator(
        task_id="migrate_broker",
        conn_id="sydafa_default",
        source="broker",
        operations=broker_operations,
    )
