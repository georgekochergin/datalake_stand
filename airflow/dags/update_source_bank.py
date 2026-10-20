"""update_source_bank: транзакционное обновление источника bank (MSSQL).

1 Dagrun = 1 вызов POST /sources/bank/migrate (All-or-Nothing в единой транзакции БД):
- Добавление 25000 записей в retail_bank.transactions (append-only)
- Обновление по 100 записей в retail_bank.clients, retail_bank.accounts, retail_bank.deals
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from sydafa_airflow.operators import SydafaMigrateOperator


def _load_migration_operations(filename: str) -> list[dict[str, Any]]:
    candidates = [
        Path(__file__).resolve().parent.parent / "migrations" / filename,
        Path(__file__).resolve().parent / "migrations" / filename,
        Path("/opt/airflow/migrations") / filename,
        Path("/opt/airflow/dags/migrations") / filename,
    ]
    for candidate in candidates:
        if candidate.is_file():
            with open(candidate, "r", encoding="utf-8") as f:
                return json.load(f)["operations"]
    raise FileNotFoundError(f"Файл миграции '{filename}' не найден по путям: {candidates}")


bank_operations = _load_migration_operations("bank_update.json")

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
