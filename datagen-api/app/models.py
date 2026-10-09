"""Pydantic-схемы запросов/ответов и внутренняя модель схемы таблицы."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


# --- внутренняя модель схемы (результат schema_loader.load) ---------------

class ForeignKey(BaseModel):
    columns: list[str]
    ref_table: str
    ref_columns: list[str]


class ColumnSpec(BaseModel):
    name: str
    type: str
    generator: dict[str, Any] | None = None


class TableSchema(BaseModel):
    name: str
    primary_key: list[str]
    columns: list[ColumnSpec]
    foreign_keys: list[ForeignKey] = Field(default_factory=list)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]


class SourceSchema(BaseModel):
    database: str
    schema_: str = Field(alias="schema")
    tables: dict[str, TableSchema]

    class Config:
        populate_by_name = True


# --- запросы API ------------------------------------------------------------

class AppendRequest(BaseModel):
    rows: int = Field(gt=0, le=100_000)
    new_key_ratio: float = Field(default=0.1, ge=0.0, le=1.0)


class UpdateRequest(BaseModel):
    predicate: str
    set: dict[str, Any]


class DeleteRequest(BaseModel):
    predicate: str
    cascade: bool = False


class EvolveRequest(BaseModel):
    ddl_operation: Literal["rename_column", "drop_column"]
    column_name: str
    new_column_name: str | None = None  # обязателен для rename_column
    apply_to: Literal["mssql", "oracle", "both"] = "both"
