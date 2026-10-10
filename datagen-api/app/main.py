"""datagen-api — REST API без веб-UI (см. docs/README.md).

Адресация таблиц: /tables/{source}/{schema}/{table}[/{action}].
Источники и их подключение задаются в config/sources.yaml.
Единственный интерфейс для ручных вызовов — Swagger UI на /docs.
"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException

from . import generators, schema_evolution, schema_state, sources
from .generators import GeneratorError
from .models import AppendRequest, DeleteRequest, EvolveRequest, UpdateRequest
from .schema_evolution import EvolutionError

app = FastAPI(title="datagen-api", description="Только для тестового стенда — отсутствует в production")


@app.on_event("startup")
def _startup() -> None:
    sources.load_sources()
    for source in sources.source_names():
        schema_state.init_source(source)


def _assert_source(source: str) -> str:
    if source not in sources.source_names():
        raise HTTPException(404, f"Неизвестный источник '{source}'")
    return source


def _resolve(source: str, schema: str) -> str:
    _assert_source(source)
    if schema != sources.get_source(source)["schema"]:
        raise HTTPException(404, f"Неизвестная схема '{schema}' источника '{source}'")
    return source


@app.get("/sources")
def list_sources():
    return {"sources": sources.source_names()}


@app.get("/tables/{source}")
def list_tables(source: str):
    _assert_source(source)
    return {"tables": list(schema_state.get_schema(source).tables.keys())}


@app.get("/tables/{source}/{schema}/{table}")
def get_table(source: str, schema: str, table: str):
    source = _resolve(source, schema)
    tbl = schema_state.get_schema(source).tables.get(table)
    if tbl is None:
        raise HTTPException(404, f"Таблица '{table}' не найдена")
    return tbl.model_dump()


@app.post("/tables/{source}/{schema}/{table}/append")
def append_table(source: str, schema: str, table: str, req: AppendRequest):
    source = _resolve(source, schema)
    live_schema = schema_state.get_schema(source)
    if table not in live_schema.tables:
        raise HTTPException(404, f"Таблица '{table}' не найдена")
    inserted = generators.append_rows(source, live_schema, table, req.rows, req.new_key_ratio)
    return {"inserted": inserted}


@app.post("/tables/{source}/{schema}/{table}/update")
def update_table(source: str, schema: str, table: str, req: UpdateRequest):
    source = _resolve(source, schema)
    live_schema = schema_state.get_schema(source)
    if table not in live_schema.tables:
        raise HTTPException(404, f"Таблица '{table}' не найдена")
    generators.update_rows(source, live_schema, table, req.predicate, req.set)
    return {"status": "ok"}


@app.post("/tables/{source}/{schema}/{table}/delete")
def delete_table(source: str, schema: str, table: str, req: DeleteRequest):
    source = _resolve(source, schema)
    live_schema = schema_state.get_schema(source)
    if table not in live_schema.tables:
        raise HTTPException(404, f"Таблица '{table}' не найдена")
    try:
        generators.delete_rows(source, live_schema, table, req.predicate, req.cascade)
    except GeneratorError as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"status": "ok"}


@app.post("/tables/{source}/{schema}/{table}/evolve")
def evolve_table(source: str, schema: str, table: str, req: EvolveRequest):
    source = _resolve(source, schema)
    targets = ["mssql", "oracle"] if req.apply_to == "both" else [req.apply_to]
    try:
        for target in targets:
            schema_evolution.evolve_column(target, table, req)
    except EvolutionError as exc:
        raise HTTPException(422, str(exc)) from exc
    return {"status": "ok", "applied_to": targets}
