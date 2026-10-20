"""Модуль загрузки описаний миграций источников данных."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

MIGRATIONS_DIR = Path(__file__).resolve().parent


def load_migration_operations(source: str) -> list[dict[str, Any]]:
    """Загружает список операций миграции для указанного источника.

    Имя файла совпадает с именем источника: <source>.json.
    """
    path = MIGRATIONS_DIR / f"{source}.json"
    if not path.is_file():
        raise FileNotFoundError(f"Файл миграции для источника '{source}' не найден: {path}")

    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    operations = payload.get("operations")
    if not isinstance(operations, list):
        raise ValueError(f"Файл миграции {path} должен содержать список 'operations'")

    return operations
