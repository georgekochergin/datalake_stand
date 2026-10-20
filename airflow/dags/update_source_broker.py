"""update_source_broker: транзакционное обновление источника broker (Oracle).

1 Dagrun = 1 вызов POST /sources/broker/migrate (All-or-Nothing в единой транзакции БД):
- Добавление 25000 записей в trading_core.transactions
- Обновление по 100 записей в trading_core.clients, trading_core.accounts, trading_core.deals
- Удаление 1000 случайных транзакций из trading_core.transactions
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


broker_operations = _load_migration_operations("broker_update.json")

with DAG(
    dag_id="update_source_broker",
    description="Транзакционное обновление источника broker (Oracle) через sydafa migrate API",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["sydafa", "broker", "oracle", "migrate"],
) as dag:
    migrate_broker = SydafaMigrateOperator(
        task_id="migrate_broker",
        conn_id="sydafa_default",
        source="broker",
        operations=broker_operations,
        timeout_seconds=300,
    )
