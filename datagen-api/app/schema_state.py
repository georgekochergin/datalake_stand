"""Startup-процедура per-source: hash-реинициализация без архивации.

На каждый (ре)старт контейнера, для каждого источника независимо:
1. sha256(bind-mounted YAML) сравнивается с STATE_DIR/<source>.schema_hash.
2. Совпал -> ничего не трогаем, подхватываем live_schema/<source>.yaml как
   рабочее состояние.
3. Не совпал (или первый запуск) -> DROP всех таблиц demo.demo источника,
   CREATE пустых таблиц по новой схеме (топологический порядок),
   перезапись live_schema/<source>.yaml и schema_evolution_log/<source>.json
   БЕЗ сохранения прежней версии (сознательное упрощение для тестового
   стенда), запись нового хэша.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

from . import provisioning
from .db_connectors import mssql_connection, oracle_connection
from .models import SourceSchema
from .render_ddl import render_all
from .schema_loader import load_source_schema, topological_order

STATE_DIR = Path("/state")
BASE_SCHEMA_DIR = Path("/app/schema/sources")

_loaded_schemas: dict[str, SourceSchema] = {}


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _qualify(source: str, table_name: str) -> str:
    if source == "mssql":
        return f"demo.demo.{table_name}"
    return f"demo.{table_name}"  # oracle: schema == user demo


def _drop_all_tables(source: str, schema: SourceSchema) -> None:
    order = list(reversed(topological_order(schema)))
    if source == "mssql":
        with mssql_connection(database="demo") as conn:
            cur = conn.cursor()
            for table_name in order:
                cur.execute(f"IF OBJECT_ID('demo.{table_name}', 'U') IS NOT NULL DROP TABLE demo.{table_name}")
            conn.commit()
    else:
        with oracle_connection() as conn:
            cur = conn.cursor()
            for table_name in order:
                try:
                    cur.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
                except Exception:
                    pass  # таблицы не было — это и есть "первый запуск"
            conn.commit()


def _create_all_tables(source: str, schema: SourceSchema) -> None:
    dialect = "mssql" if source == "mssql" else "oracle"
    ddl_statements = render_all(schema, dialect, lambda t: _qualify(source, t))
    if source == "mssql":
        with mssql_connection(database="demo") as conn:
            cur = conn.cursor()
            for ddl in ddl_statements:
                cur.execute(ddl)
            conn.commit()
    else:
        with oracle_connection() as conn:
            cur = conn.cursor()
            for ddl in ddl_statements:
                cur.execute(ddl)
            conn.commit()


def init_source(source: str) -> SourceSchema:
    """source: "mssql" | "oracle". Возвращает актуальную схему (live)."""
    base_yaml = BASE_SCHEMA_DIR / f"{source}.yaml"
    schema = load_source_schema(str(base_yaml))  # валидация PK/FK здесь же

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    hash_path = STATE_DIR / f"{source}.schema_hash"
    live_yaml_path = STATE_DIR / f"live_schema.{source}.yaml"
    log_path = STATE_DIR / f"schema_evolution_log.{source}.json"

    current_hash = _hash_file(base_yaml)
    stored_hash = hash_path.read_text().strip() if hash_path.exists() else None

    if source == "mssql":
        provisioning.ensure_mssql_demo_schema()
    else:
        provisioning.ensure_oracle_demo_schema()

    if stored_hash == current_hash:
        _loaded_schemas[source] = schema
        return schema

    # хэш не совпал или первый запуск -> полная пересборка без архивации
    if stored_hash is not None:
        _drop_all_tables(source, schema)
    _create_all_tables(source, schema)

    shutil.copyfile(base_yaml, live_yaml_path)
    log_path.write_text(json.dumps([], ensure_ascii=False, indent=2))
    hash_path.write_text(current_hash)

    _loaded_schemas[source] = schema
    return schema


def get_schema(source: str) -> SourceSchema:
    return _loaded_schemas[source]


def append_evolution_log(source: str, entry: dict) -> None:
    log_path = STATE_DIR / f"schema_evolution_log.{source}.json"
    log = json.loads(log_path.read_text()) if log_path.exists() else []
    log.append(entry)
    log_path.write_text(json.dumps(log, ensure_ascii=False, indent=2))
