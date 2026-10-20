"""update_source_bank: транзакционное обновление источника bank (MSSQL).

1 Dagrun = 1 вызов POST /sources/bank/migrate (All-or-Nothing в единой транзакции БД):
- Добавление 25000 записей в retail_bank.transactions (append-only)
- Обновление по 100 записей в retail_bank.clients, retail_bank.accounts, retail_bank.deals
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

# Добавляем каталог plugins в sys.path для прозрачного импорта
plugins_dir = str(Path(__file__).resolve().parent.parent / "plugins")
if plugins_dir not in sys.path:
    sys.path.insert(0, plugins_dir)

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
    is_paused_upon_creation=False,
    tags=["sydafa", "bank", "mssql", "migrate"],
) as dag:
    migrate_bank = SydafaMigrateOperator(
        task_id="migrate_bank",
        conn_id="sydafa_default",
        source="bank",
        operations=bank_operations,
        timeout_seconds=300,
    )
