"""DAG обновления источника bank (MSSQL)."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from migrations import load_migration_operations
from sydafa_airflow.operators import SydafaMigrateOperator


bank_operations = load_migration_operations("bank")

with DAG(
    dag_id="update_source_bank",
    description="Транзакционное обновление источника bank (MSSQL) через sydafa migrate API",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sydafa", "bank", "mssql", "migrate"],
) as dag:
    migrate_bank = SydafaMigrateOperator(
        task_id="migrate_bank",
        conn_id="sydafa_default",
        source="bank",
        operations=bank_operations,
    )
