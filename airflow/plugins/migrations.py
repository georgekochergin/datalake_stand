"""Общие утилиты и плагины для Airflow DAG-ов стенда."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# Стандартный путь к каталогу миграций в контейнерах Airflow и локальной разработке
MIGRATIONS_DIR = Path("/opt/airflow/migrations")


def load_migration_operations(source: str) -> list[dict[str, Any]]:
    """Загружает список операций миграции для указанного источника.
    
    Имя файла совпадает с именем источника: <source>.json.
    """
    path = MIGRATIONS_DIR / f"{source}.json"
    if not path.exists():
        # Fallback для запуска вне Docker-контейнера Airflow
        fallback = Path(__file__).resolve().parent.parent / "migrations" / f"{source}.json"
        if fallback.exists():
            path = fallback
        else:
            raise FileNotFoundError(f"Файл миграции для источника '{source}' не найден: {path}")

    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    operations = payload.get("operations")
    if not isinstance(operations, list):
        raise ValueError(f"Файл миграции {path} должен содержать список 'operations'")

    return operations
