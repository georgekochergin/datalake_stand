"""Подключения к источникам.

host/port/database берутся из реестра (config/sources.yaml),
секреты — из переменных окружения.
"""
from __future__ import annotations

import os
from contextlib import contextmanager

import oracledb
import pymssql

from . import sources


@contextmanager
def mssql_connection(database: str | None = None):
    cfg = sources.get_source("mssql")
    conn = pymssql.connect(
        server=cfg["host"],
        port=int(cfg["port"]),
        user="sa",
        password=os.environ["MSSQL_SA_PASSWORD"],
        database=database or cfg["database"],
        autocommit=False,
    )
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def oracle_connection(service_name: str | None = None):
    cfg = sources.get_source("oracle")
    dsn = oracledb.makedsn(
        cfg["host"],
        int(cfg["port"]),
        service_name=service_name or cfg["database"],
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
