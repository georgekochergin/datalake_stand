"""evolve: только rename_column и drop_column (см. docs/PLAN.md).

drop_column запрещён для PK/FK-колонок. add_column/alter_column_type —
не реализованы на этом этапе (backlog), любое иное значение ddl_operation
должно быть отклонено моделью Pydantic (422) раньше, чем дойдёт сюда.
"""
from __future__ import annotations

from datetime import datetime, timezone

from . import schema_state
from .db_connectors import mssql_connection, oracle_connection
from .models import EvolveRequest


class EvolutionError(ValueError):
    pass


def _qualify(source: str, table_name: str) -> str:
    return f"demo.{table_name}" if source == "mssql" else table_name


def _apply_ddl(source: str, sql: str) -> None:
    conn_ctx = mssql_connection(database="demo") if source == "mssql" else oracle_connection()
    with conn_ctx as conn:
        conn.cursor().execute(sql)
        conn.commit()


def evolve_column(source: str, table_name: str, req: EvolveRequest) -> None:
    schema = schema_state.get_schema(source)
    table = schema.tables.get(table_name)
    if table is None:
        raise EvolutionError(f"Таблица '{table_name}' не найдена в live-схеме '{source}'")

    is_pk = req.column_name in table.primary_key
    is_fk = any(req.column_name in fk.columns for fk in table.foreign_keys)
    if req.ddl_operation == "drop_column" and (is_pk or is_fk):
        raise EvolutionError(
            f"drop_column запрещён для колонки '{req.column_name}': она входит в PK или FK таблицы '{table_name}'"
        )

    qualified = _qualify(source, table_name)
    if req.ddl_operation == "rename_column":
        if not req.new_column_name:
            raise EvolutionError("new_column_name обязателен для rename_column")
        if source == "mssql":
            sql = f"EXEC sp_rename '{qualified}.{req.column_name}', '{req.new_column_name}', 'COLUMN'"
        else:
            sql = f"ALTER TABLE {qualified} RENAME COLUMN {req.column_name} TO {req.new_column_name}"
        _apply_ddl(source, sql)
        for col in table.columns:
            if col.name == req.column_name:
                col.name = req.new_column_name
        for fk in table.foreign_keys:
            fk.columns = [req.new_column_name if c == req.column_name else c for c in fk.columns]
        table.primary_key = [req.new_column_name if c == req.column_name else c for c in table.primary_key]

    elif req.ddl_operation == "drop_column":
        sql = f"ALTER TABLE {qualified} DROP COLUMN {req.column_name}"
        _apply_ddl(source, sql)
        table.columns = [c for c in table.columns if c.name != req.column_name]

    schema_state.append_evolution_log(
        source,
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "table": table_name,
            "operation": req.ddl_operation,
            "column_name": req.column_name,
            "new_column_name": req.new_column_name,
        },
    )
