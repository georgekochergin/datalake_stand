#!/usr/bin/env python3
"""Полная миграция демо-источников для шага «Эволюция схемы источника».

Для каждого выбранного источника через API sydafa:
- удаляет, переименовывает и добавляет колонку в таблице клиентов одной
  транзакцией migrate;
- назначает генератор добавленной колонке и перенесённому имени;
- удаляет правило прежнего имени колонки.

Запуск из корня репозитория:

    python3 docs/demo/schema_evolution.py           оба источника
    python3 docs/demo/schema_evolution.py bank      только bank
    python3 docs/demo/schema_evolution.py broker    только broker

Адрес sydafa читается из переменной окружения SYDAFA_URL, по умолчанию
http://localhost:8090. Перед миграцией скрипт ждёт готовности GET /health.
Нужна только стандартная библиотека Python. Повторный запуск на уже
мигрированной схеме завершится ошибкой соответствующей операции.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

SYDAFA_URL = os.environ.get("SYDAFA_URL", "http://localhost:8090")

SOURCES: dict[str, dict[str, str]] = {
    "bank": {
        "entity": "retail_bank.clients",
        "dropped": "client_type",
        "old_name": "full_name",
        "new_name": "client_name",
        "added": "email",
    },
    "broker": {
        "entity": "trading_core.clients",
        "dropped": "is_qualified",
        "old_name": "full_name",
        "new_name": "client_name",
        "added": "email",
    },
}

EMAIL_RULE = {"kind": "faker", "provider": "email", "null_ratio": 0.05}
NAME_RULE = {"kind": "faker", "provider": "name", "locale": "ru_RU", "null_ratio": 0.05}


def request(method: str, path: str, body: dict | None = None) -> tuple[int, str]:
    """Отправить HTTP-запрос к API sydafa и вернуть статус и текст ответа."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = urllib.request.Request(SYDAFA_URL + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            return response.status, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as error:
        return 0, str(error)


def wait_health(tries: int = 30, interval: int = 2) -> bool:
    """Дождаться готовности API sydafa по GET /health."""
    for attempt in range(1, tries + 1):
        code, _ = request("GET", "/health")
        if code == 200:
            return True
        if attempt < tries:
            time.sleep(interval)
    return False


def migrate_source(source: str) -> list[str]:
    """Выполнить полную миграцию одного источника и вернуть список ошибок."""
    cfg = SOURCES[source]
    entity = cfg["entity"]
    operations = [
        {"op": "drop_column", "entity_fqn": entity, "name": cfg["dropped"]},
        {"op": "rename_column", "entity_fqn": entity,
         "from_name": cfg["old_name"], "to_name": cfg["new_name"]},
        {"op": "add_column", "entity_fqn": entity,
         "column": {"name": cfg["added"], "type": "string"}},
    ]
    rule_path = "/sources/%s/rules/%s/columns/%%s" % (source, entity)
    steps = [
        ("POST", "/sources/%s/migrate" % source, {"operations": operations},
         "migrate %s: drop %s, rename %s -> %s, add %s"
         % (entity, cfg["dropped"], cfg["old_name"], cfg["new_name"], cfg["added"])),
        ("PUT", rule_path % cfg["added"], EMAIL_RULE,
         "генератор колонки %s: faker email" % cfg["added"]),
        ("PUT", rule_path % cfg["new_name"], NAME_RULE,
         "генератор колонки %s: faker name ru_RU" % cfg["new_name"]),
        ("DELETE", rule_path % cfg["old_name"], None,
         "удалено правило колонки %s" % cfg["old_name"]),
    ]
    errors: list[str] = []
    for method, path, body, title in steps:
        code, text = request(method, path, body)
        if code == 200:
            print("%s: %s — OK" % (source, title))
        else:
            print("%s: %s — ОШИБКА %s: %s" % (source, title, code, text[:300]))
            errors.append("%s: %s" % (source, title))
    return errors


def main(argv: list[str]) -> int:
    """Разобрать аргументы, дождаться sydafa и выполнить миграцию источников."""
    selected = argv[1:] or list(SOURCES)
    unknown = [name for name in selected if name not in SOURCES]
    if unknown:
        print("неизвестные источники: %s (доступны: %s)"
              % (", ".join(unknown), ", ".join(SOURCES)))
        return 2
    if not wait_health():
        print("sydafa не отвечает: GET %s/health" % SYDAFA_URL)
        return 1
    errors: list[str] = []
    for source in selected:
        errors.extend(migrate_source(source))
    if errors:
        print("миграция выполнена с ошибками:")
        for error in errors:
            print("- %s" % error)
        return 1
    print("миграция завершена: %s" % ", ".join(selected))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
