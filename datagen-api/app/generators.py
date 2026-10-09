"""append / update / delete с рекурсивным FK-aware каскадом.

Прототип: вставки делаются по одной строке (не батчем с дедупликацией) —
проще и корректно для объёмов тестового стенда; для продуктивного объёма
имеет смысл батчить и дедуплицировать новые родительские строки перед
вставкой (см. docs/PLAN.md, раздел дизайна).

predicate / set / payload — сырые SQL-фрагменты: осознанный риск
(см. docs/PLAN.md), инструмент предполагается доверенным и внутренним.
"""
from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Any

from faker import Faker

from .db_connectors import mssql_connection, oracle_connection
from .models import ColumnSpec, ForeignKey, SourceSchema, TableSchema

_fake = Faker()


class GeneratorError(ValueError):
    pass


def _qualify(source: str, table_name: str) -> str:
    return f"demo.{table_name}" if source == "mssql" else table_name


def _connection(source: str):
    return mssql_connection(database="demo") if source == "mssql" else oracle_connection()


def _generate_value(col: ColumnSpec) -> Any:
    spec = col.generator or {}
    kind = spec.get("kind")
    if kind == "faker":
        return getattr(_fake, spec.get("provider", "word"))()
    if kind == "choice":
        return random.choice(spec["values"])
    if kind == "random_decimal":
        return round(random.uniform(spec.get("min", 0), spec.get("max", 1)), 2)
    if kind == "random_date":
        start = date.today() - timedelta(days=365)
        return start + timedelta(days=random.randint(0, 365))
    return None


def _fk_for_column(table: TableSchema, column_name: str) -> ForeignKey | None:
    for fk in table.foreign_keys:
        if column_name in fk.columns:
            return fk
    return None


def _next_sequence_value(conn, source: str, qualified: str, pk_col: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT MAX({pk_col}) FROM {qualified}")
    (current_max,) = cur.fetchone()
    return int(current_max) + 1 if current_max is not None else 1


def _pick_existing_pk(conn, qualified: str, pk_cols: list[str]) -> dict[str, Any] | None:
    cols = ", ".join(pk_cols)
    cur = conn.cursor()
    cur.execute(f"SELECT {cols} FROM {qualified}")
    rows = cur.fetchall()
    if not rows:
        return None
    row = random.choice(rows)
    return dict(zip(pk_cols, row))


def _insert_row(conn, qualified: str, values: dict[str, Any]) -> None:
    cols = ", ".join(values.keys())
    placeholders = ", ".join(["%s"] * len(values))
    cur = conn.cursor()
    cur.execute(f"INSERT INTO {qualified} ({cols}) VALUES ({placeholders})", list(values.values()))


def _resolve_fk_values(
    conn, source: str, schema: SourceSchema, fk: ForeignKey, new_key_ratio: float
) -> dict[str, Any]:
    parent_table = schema.tables[fk.ref_table]
    parent_qualified = _qualify(source, parent_table.name)
    existing = _pick_existing_pk(conn, parent_qualified, parent_table.primary_key)
    if existing is not None and random.random() >= new_key_ratio:
        parent_pk_values = existing
    else:
        parent_pk_values = _mint_new_row(conn, source, schema, parent_table, new_key_ratio)
    return dict(zip(fk.columns, [parent_pk_values[c] for c in fk.ref_columns]))


def _mint_new_row(
    conn, source: str, schema: SourceSchema, table: TableSchema, new_key_ratio: float
) -> dict[str, Any]:
    qualified = _qualify(source, table.name)
    values: dict[str, Any] = {}

    for fk in table.foreign_keys:
        values.update(_resolve_fk_values(conn, source, schema, fk, new_key_ratio))

    for col in table.columns:
        if col.name in table.primary_key or _fk_for_column(table, col.name):
            continue
        values[col.name] = _generate_value(col)

    for pk_col in table.primary_key:
        if pk_col not in values:
            values[pk_col] = _next_sequence_value(conn, source, qualified, pk_col)

    _insert_row(conn, qualified, values)
    return {pk: values[pk] for pk in table.primary_key}


def append_rows(source: str, schema: SourceSchema, table_name: str, rows: int, new_key_ratio: float) -> int:
    table = schema.tables[table_name]
    with _connection(source) as conn:
        for _ in range(rows):
            _mint_new_row(conn, source, schema, table, new_key_ratio)
        conn.commit()
    return rows


def update_rows(source: str, schema: SourceSchema, table_name: str, predicate: str, set_values: dict[str, Any]) -> None:
    table = schema.tables[table_name]
    qualified = _qualify(source, table_name)
    with _connection(source) as conn:
        for col_name, value in set_values.items():
            fk = _fk_for_column(table, col_name)
            if fk is not None:
                parent_table = schema.tables[fk.ref_table]
                parent_qualified = _qualify(source, parent_table.name)
                ref_col = fk.ref_columns[fk.columns.index(col_name)]
                cur = conn.cursor()
                cur.execute(f"SELECT 1 FROM {parent_qualified} WHERE {ref_col} = %s", [value])
                if cur.fetchone() is None:
                    _mint_new_row_with_key(conn, source, schema, parent_table, {ref_col: value})

        assignments = ", ".join(f"{c} = %s" for c in set_values)
        cur = conn.cursor()
        cur.execute(f"UPDATE {qualified} SET {assignments} WHERE {predicate}", list(set_values.values()))
        conn.commit()


def _mint_new_row_with_key(conn, source: str, schema: SourceSchema, table: TableSchema, forced_pk: dict[str, Any]) -> None:
    """Как _mint_new_row, но PK/часть PK заданы явно (для update-каскада)."""
    qualified = _qualify(source, table.name)
    values: dict[str, Any] = dict(forced_pk)

    for fk in table.foreign_keys:
        for col in fk.columns:
            if col not in values:
                values.update(_resolve_fk_values(conn, source, schema, fk, new_key_ratio=0.1))

    for col in table.columns:
        if col.name in values or col.name in table.primary_key or _fk_for_column(table, col.name):
            continue
        values[col.name] = _generate_value(col)

    for pk_col in table.primary_key:
        if pk_col not in values:
            values[pk_col] = _next_sequence_value(conn, source, qualified, pk_col)

    _insert_row(conn, qualified, values)


def delete_rows(source: str, schema: SourceSchema, table_name: str, predicate: str, cascade: bool) -> None:
    table = schema.tables[table_name]
    qualified = _qualify(source, table_name)
    dependents = [
        (t, fk) for t in schema.tables.values() for fk in t.foreign_keys if fk.ref_table == table_name
    ]

    with _connection(source) as conn:
        cur = conn.cursor()
        if dependents:
            cur.execute(f"SELECT {', '.join(table.primary_key)} FROM {qualified} WHERE {predicate}")
            target_pks = cur.fetchall()
            if target_pks:
                if not cascade:
                    raise GeneratorError(
                        f"На таблицу '{table_name}' есть ссылки из {[t.name for t, _ in dependents]}; "
                        "удаление заблокировано без cascade=true"
                    )
                for dep_table, fk in dependents:
                    dep_qualified = _qualify(source, dep_table.name)
                    ref_idx = {c: i for i, c in enumerate(table.primary_key)}
                    for pk_row in target_pks:
                        conditions = " AND ".join(
                            f"{fk_col} = %s" for fk_col in fk.columns
                        )
                        values = [pk_row[ref_idx[rc]] for rc in fk.ref_columns]
                        cur.execute(f"DELETE FROM {dep_qualified} WHERE {conditions}", values)

        cur.execute(f"DELETE FROM {qualified} WHERE {predicate}")
        conn.commit()
