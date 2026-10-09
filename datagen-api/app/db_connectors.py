"""Подключения к mssql-source / oracle-source.

Параметры читаются из переменных окружения (заданы в docker-compose.yaml).
"""
from __future__ import annotations

import os
from contextlib import contextmanager

import oracledb
import pymssql


@contextmanager
def mssql_connection(database: str | None = None):
    conn = pymssql.connect(
        server=os.environ.get("MSSQL_HOST", "mssql-source"),
        port=int(os.environ.get("MSSQL_PORT", "1433")),
        user="sa",
        password=os.environ["MSSQL_SA_PASSWORD"],
        database=database or "master",
        autocommit=False,
    )
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def oracle_connection(service_name: str | None = None):
    dsn = oracledb.makedsn(
        os.environ.get("ORACLE_HOST", "oracle-source"),
        int(os.environ.get("ORACLE_PORT", "1521")),
        service_name=service_name or os.environ.get("ORACLE_SERVICE", "FREEPDB1"),
    )
    conn = oracledb.connect(
        user=os.environ.get("ORACLE_APP_USER", "demo"),
        password=os.environ["ORACLE_APP_PASSWORD"],
        dsn=dsn,
    )
    try:
        yield conn
    finally:
        conn.close()
