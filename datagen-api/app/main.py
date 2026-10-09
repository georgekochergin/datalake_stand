"""datagen-api — REST API без веб-UI (см. docs/README.md).

Иерархия путей: rel_db -> host -> db -> schema -> tables. На этом этапе
db/schema зафиксированы как demo/demo в обоих источниках — любое другое
значение в пути считается ошибкой 404 (не "параметризуемо", см. план).
Единственный интерфейс для ручных вызовов — Swagger UI на /docs.
"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException

from . import generators, schema_evolution, schema_state
from .generators import GeneratorError
from .models import AppendRequest, DeleteRequest, EvolveRequest, UpdateRequest
from .schema_evolution import EvolutionError

app = FastAPI(title="datagen-api", description="Только для тестового стенда — отсутствует в production")

_HOST_BY_ENGINE = {"mssql": "mssql-source", "oracle": "oracle-source"}


@app.on_event("startup")
def _startup() -> None:
    for source in ("mssql", "oracle"):
        schema_state.init_source(source)


def _resolve_source(engine: str, host: str, db: str, schema: str) -> str:
    if engine not in _HOST_BY_ENGINE:
        raise HTTPException(404, f"Неизвестный engine '{engine}'")
    if host != _HOST_BY_ENGINE[engine] or db != "demo" or schema != "demo":
        raise HTTPException(404, "На этом этапе поддерживается только db=demo, schema=demo")
    return engine


@app.get("/rel_db/{engine}/hosts/{host}/dbs/{db}/schemas/{schema}/tables")
def list_tables(engine: str, host: str, db: str, schema: str):
    source = _resolve_source(engine, host, db, schema)
    return {"tables": list(schema_state.get_schema(source).tables.keys())}


@app.get("/rel_db/{engine}/hosts/{host}/dbs/{db}/schemas/{schema}/tables/{table}")
def get_table(engine: str, host: str, db: str, schema: str, table: str):
    source = _resolve_source(engine, host, db, schema)
    tbl = schema_state.get_schema(source).tables.get(table)
    if tbl is None:
        raise HTTPException(404, f"Таблица '{table}' не найдена")
    return tbl.model_dump()


@app.post("/rel_db/{engine}/hosts/{host}/dbs/{db}/schemas/{schema}/tables/{table}/append")
def append_table(engine: str, host: str, db: str, schema: str, table: str, req: AppendRequest):
    source = _resolve_source(engine, host, db, schema)
    live_schema = schema_state.get_schema(source)
    if table not in live_schema.tables:
        raise HTTPException(404, f"Таблица '{table}' не найдена")
    inserted = generators.append_rows(source, live_schema, table, req.rows, req.new_key_ratio)
    return {"inserted": inserted}


@app.post("/rel_db/{engine}/hosts/{host}/dbs/{db}/schemas/{schema}/tables/{table}/update")
def update_table(engine: str, host: str, db: str, schema: str, table: str, req: UpdateRequest):
    source = _resolve_source(engine, host, db, schema)
    live_schema = schema_state.get_schema(source)
    if table not in live_schema.tables:
        raise HTTPException(404, f"Таблица '{table}' не найдена")
    generators.update_rows(source, live_schema, table, req.predicate, req.set)
    return {"status": "ok"}


@app.post("/rel_db/{engine}/hosts/{host}/dbs/{db}/schemas/{schema}/tables/{table}/delete")
def delete_table(engine: str, host: str, db: str, schema: str, table: str, req: DeleteRequest):
    source = _resolve_source(engine, host, db, schema)
    live_schema = schema_state.get_schema(source)
    if table not in live_schema.tables:
        raise HTTPException(404, f"Таблица '{table}' не найдена")
    try:
        generators.delete_rows(source, live_schema, table, req.predicate, req.cascade)
    except GeneratorError as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"status": "ok"}


@app.post("/rel_db/{engine}/hosts/{host}/dbs/{db}/schemas/{schema}/tables/{table}/evolve")
def evolve_table(engine: str, host: str, db: str, schema: str, table: str, req: EvolveRequest):
    source = _resolve_source(engine, host, db, schema)
    targets = ["mssql", "oracle"] if req.apply_to == "both" else [req.apply_to]
    try:
        for target in targets:
            schema_evolution.evolve_column(target, table, req)
    except EvolutionError as exc:
        raise HTTPException(422, str(exc)) from exc
    return {"status": "ok", "applied_to": targets}
