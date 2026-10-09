"""YAML-схема -> CREATE TABLE под конкретный диалект (mssql | oracle).

Маппинг типов задокументирован в mapping/type_mapping_mssql.md и
mapping/type_mapping_oracle.md — здесь он механически применяется, а не
просто описан. Отсутствие правила для типа — явная ошибка (см. ValueError),
а не "угаданный" тип (AS IS требование).
"""
from __future__ import annotations

import re

from .models import SourceSchema, TableSchema
from .schema_loader import topological_order

_MSSQL_TYPES = {
    "bigint": "BIGINT",
    "string": "NVARCHAR(400)",
    "date": "DATE",
    "timestamp": "DATETIME2",
}
_ORACLE_TYPES = {
    "bigint": "NUMBER(19)",
    "string": "VARCHAR2(400)",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
}
_DECIMAL_RE = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")


def _map_type(logical_type: str, dialect: str) -> str:
    m = _DECIMAL_RE.match(logical_type)
    if m:
        precision, scale = m.groups()
        if dialect == "mssql":
            return f"DECIMAL({precision},{scale})"
        return f"NUMBER({precision},{scale})"

    table = _MSSQL_TYPES if dialect == "mssql" else _ORACLE_TYPES
    try:
        return table[logical_type]
    except KeyError as exc:
        raise ValueError(
            f"Нет правила маппинга логического типа '{logical_type}' для диалекта '{dialect}'"
        ) from exc


def render_create_table(table: TableSchema, qualified_name: str, dialect: str, qualify: callable) -> str:
    col_lines = [
        f"  {c.name} {_map_type(c.type, dialect)}" for c in table.columns
    ]
    pk = ", ".join(table.primary_key)
    col_lines.append(f"  CONSTRAINT pk_{table.name} PRIMARY KEY ({pk})")
    for fk in table.foreign_keys:
        cols = ", ".join(fk.columns)
        ref_cols = ", ".join(fk.ref_columns)
        fk_name = f"fk_{table.name}_{'_'.join(fk.columns)}"
        col_lines.append(
            f"  CONSTRAINT {fk_name} FOREIGN KEY ({cols}) REFERENCES {qualify(fk.ref_table)} ({ref_cols})"
        )
    body = ",\n".join(col_lines)
    return f"CREATE TABLE {qualified_name} (\n{body}\n)"


def render_all(schema: SourceSchema, dialect: str, qualify: callable) -> list[str]:
    """Возвращает список DDL-строк в топологическом порядке (родители раньше детей).

    qualify(table_name) -> полностью квалифицированное имя (например demo.demo.orders).
    """
    order = topological_order(schema)
    return [
        render_create_table(schema.tables[name], qualify(name), dialect, qualify)
        for name in order
    ]
