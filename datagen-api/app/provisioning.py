"""Создание фиксированной БД+схема `demo.demo` в каждом источнике.

mssql: CREATE DATABASE/SCHEMA явно здесь (у образа mssql-server нет
штатного initdb.d-механизма).
oracle: pluggable database `demo` и пользователь/схема `demo` создаются
штатным механизмом образа gvenzl/oracle-free через переменные окружения
ORACLE_DATABASE / APP_USER контейнера oracle-source — здесь только
проверка, что схема доступна (no-op по умолчанию).
"""
from __future__ import annotations

from .db_connectors import mssql_connection


def ensure_mssql_demo_schema() -> None:
    with mssql_connection(database="master") as conn:
        cur = conn.cursor()
        cur.execute("IF DB_ID('demo') IS NULL CREATE DATABASE demo")
        conn.commit()

    with mssql_connection(database="demo") as conn:
        cur = conn.cursor()
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'demo') "
            "EXEC('CREATE SCHEMA demo')"
        )
        conn.commit()


def ensure_oracle_demo_schema() -> None:
    # PDB demo + пользователь/схема demo создаются переменными окружения
    # ORACLE_DATABASE=demo / APP_USER=demo контейнера oracle-source при его
    # собственном первом старте — здесь явно ничего не делаем.
    return None
