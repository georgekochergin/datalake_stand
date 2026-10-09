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
        # CREATE DATABASE/LOGIN не выполняются внутри транзакции — переводим
        # соединение в autocommit, иначе pymssql оборачивает их в транзакцию
        # и получаем "CREATE DATABASE statement not allowed within multi-statement transaction".
        conn.autocommit(True)
        cur = conn.cursor()
        cur.execute("IF DB_ID('demo') IS NULL CREATE DATABASE demo")
        # Тестовый стенд: прикладной логин admin/admin (sysadmin) для доступа из
        # DBeaver и т.п. Имя системного 'sa' фиксировано образом, а 'admin' как
        # пароль не проходит политику сложности по умолчанию — поэтому
        # CHECK_POLICY=OFF.
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM sys.sql_logins WHERE name = 'admin') "
            "CREATE LOGIN admin WITH PASSWORD = 'admin', CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF"
        )
        cur.execute(
            "IF NOT EXISTS (SELECT 1 FROM sys.server_role_members srm "
            "JOIN sys.server_principals m ON m.principal_id = srm.member_principal_id "
            "JOIN sys.server_principals r ON r.principal_id = srm.role_principal_id "
            "WHERE m.name = 'admin' AND r.name = 'sysadmin') "
            "ALTER SERVER ROLE sysadmin ADD MEMBER admin"
        )

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
