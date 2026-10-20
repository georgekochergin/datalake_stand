"""update_source_broker: транзакционное обновление источника broker (Oracle).

1 Dagrun = 1 вызов POST /sources/broker/migrate (All-or-Nothing в единой транзакции БД):
- Добавление 25000 записей в trading_core.transactions
- Обновление по 100 записей в trading_core.clients, trading_core.accounts, trading_core.deals
- Удаление 1000 случайных транзакций из trading_core.transactions
"""
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
