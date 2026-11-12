"""Прямые DDL/DML над СУБД-источниками изолированного контура для e2e-проверки эволюции схемы.

API sydafa не изменяет типы колонок и не создаёт колонки логических типов
int/float, поэтому e2e-сценарий расширяет физические типы и готовит
контрольные значения прямыми запросами под административным пользователем
контура. Реквизиты читаются только из состояния sydafa текущего контура
(том sydafa_state) и не попадают в логи, аргументы команд и git.
"""
from __future__ import annotations

import json
import os
from typing import Any


def read_dba_secrets() -> dict[str, str]:
    """Реквизиты административного пользователя из состояния sydafa контура."""
    path = os.environ.get("SYDAFA_SECRETS_FILE", "/state/secrets.json")
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def mssql_dba_connect(host: str):
    """Подключение к MSSQL-источнику контура под административным пользователем."""
    import pymssql

    secrets = read_dba_secrets()
    return pymssql.connect(
        server=host,
        user=secrets["dba_user"],
        password=secrets["dba_password"],
        database="sydafa",
        login_timeout=30,
        timeout=300,
        autocommit=True,
    )


def oracle_dba_connect(host: str):
    """Подключение к Oracle-источнику контура под административным пользователем."""
    import oracledb

    secrets = read_dba_secrets()
    return oracledb.connect(
        user=secrets["dba_user"],
        password=secrets["dba_password"],
        dsn="%s/sydafa" % host,
    )


def _connect(engine: str, host: str):
    if engine == "mssql":
        return mssql_dba_connect(host)
    if engine == "oracle":
        return oracle_dba_connect(host)
    raise ValueError("unknown source engine: %s" % engine)


def execute(engine: str, host: str, statements: list[str]) -> int:
    """Выполнить DDL/DML-операторы и вернуть число строк последнего оператора."""
    connection = _connect(engine, host)
    try:
        cursor = connection.cursor()
        affected = 0
        try:
            for statement in statements:
                cursor.execute(statement)
                affected = cursor.rowcount if cursor.rowcount is not None else 0
        finally:
            cursor.close()
        if not getattr(connection, "autocommit", False):
            connection.commit()
        return affected
    finally:
        connection.close()


def query_scalar(engine: str, host: str, sql: str) -> Any:
    """Вернуть первый столбец первой строки запроса к словарю СУБД."""
    connection = _connect(engine, host)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        finally:
            cursor.close()
        return None if row is None else row[0]
    finally:
        connection.close()
