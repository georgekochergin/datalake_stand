"""Загрузка и валидация YAML-схемы источника.

Правила (см. docs/README.md):
- у каждой таблицы обязателен primary_key;
- foreign_keys декларируются там, где есть связи;
- граф FK должен быть ацикличным — иначе невозможны ни топологическое
  создание таблиц, ни каскадная генерация.
"""
from __future__ import annotations

import yaml

from .models import ColumnSpec, ForeignKey, SourceSchema, TableSchema


class SchemaValidationError(ValueError):
    pass


def load_source_schema(yaml_path: str) -> SourceSchema:
    with open(yaml_path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    tables: dict[str, TableSchema] = {}
    for table_name, table_raw in raw.get("tables", {}).items():
        pk = table_raw.get("primary_key") or []
        if not pk:
            raise SchemaValidationError(
                f"Таблица '{table_name}': primary_key обязателен и не может быть пустым"
            )
        columns = [ColumnSpec(**c) for c in table_raw.get("columns", [])]
        fks = [ForeignKey(**fk) for fk in table_raw.get("foreign_keys", []) or []]
        tables[table_name] = TableSchema(
            name=table_name, primary_key=pk, columns=columns, foreign_keys=fks
        )

    schema = SourceSchema(database=raw["database"], schema=raw["schema"], tables=tables)
    _validate_fk_targets(schema)
    topological_order(schema)  # бросит SchemaValidationError, если есть цикл
    return schema


def _validate_fk_targets(schema: SourceSchema) -> None:
    for table in schema.tables.values():
        for fk in table.foreign_keys:
            if fk.ref_table not in schema.tables:
                raise SchemaValidationError(
                    f"Таблица '{table.name}': FK ссылается на несуществующую таблицу '{fk.ref_table}'"
                )


def topological_order(schema: SourceSchema) -> list[str]:
    """Порядок создания таблиц: родители (по FK) раньше детей.

    Алгоритм Kahn'а; при обнаружении цикла — SchemaValidationError с
    перечислением таблиц, которые не удалось упорядочить (участники цикла).
    """
    deps: dict[str, set[str]] = {
        name: {fk.ref_table for fk in table.foreign_keys if fk.ref_table != name}
        for name, table in schema.tables.items()
    }

    ordered: list[str] = []
    remaining = dict(deps)
    while remaining:
        ready = [name for name, d in remaining.items() if not d]
        if not ready:
            raise SchemaValidationError(
                "Обнаружен цикл в графе FK, таблицы не могут быть упорядочены: "
                + ", ".join(sorted(remaining))
            )
        ready.sort()
        for name in ready:
            ordered.append(name)
            del remaining[name]
        for d in remaining.values():
            d.difference_update(ready)

    return ordered
