#!/usr/bin/env python3
"""run_flow.py: Сквозной (e2e) тест стенда datalake_stand.

Проверяет актуальное состояние репозитория:
1. Readiness sydafa API и регистрацию источников (bank, broker).
2. Защиту API sydafa от SQL-инъекций (HTTP 422).
3. Авторизацию и доступность DAG-ов в Airflow 3.x.
4. Исполнение и успешное завершение (state=success) DAG-ов обновления источников:
   - update_source_bank (All-or-Nothing миграция MSSQL)
   - update_source_broker (All-or-Nothing миграция Oracle)
5. Исполнение четырёх DAG-ов ингестии main/refs (raw-слой) и проверку
   данных в Iceberg через Trino REST: append-only семантика, общий __process_id
   на запуск, работа инкрементального режима по _batch_id (watermark).
6. Запуск DAG-ов снимков каждой инкрементальной сущности и проверку
   таблиц-снимков __act_snap: полное соответствие источнику, единый
   __process_id и __load_dttm, атомарная перезапись при повторе (одна порция),
   свежесть и автор публикации по метаданным Iceberg ($snapshots, $properties).
7. Эволюцию схемы реальных источников MSSQL и Oracle: удаление, переименование
   и добавление колонок через API sydafa, расширения типов прямыми DDL под
   административным пользователем контура (int -> bigint, float -> double,
   decimal(p,s) -> decimal(p',s), удлинение строк) с успешной загрузкой,
   promotion схем Iceberg, а затем несовместимое изменение типа с отказом
   load-задачи без нового снапшота Iceberg.
"""
from __future__ import annotations

import ast
import base64
import hashlib
import json
import os
from pathlib import Path
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import source_ddl

SYDAFA = os.environ.get("SYDAFA_URL", "http://sydafa:8000")
AIRFLOW = os.environ.get("AIRFLOW_URL", "http://airflow-webserver:8080")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin123")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin123")
TRINO = os.environ.get("TRINO_URL", "https://trino:8443")
TRINO_USER = os.environ.get("TRINO_USER", "admin123")
TRINO_PASSWORD = os.environ.get("TRINO_PASSWORD", "admin123")

INGEST_DAGS = tuple(
    f"ingest__rdbms__{source}__{pipeline}"
    for source in ("bank", "broker") for pipeline in ("main", "refs")
)


def snapshot_dag_id(source: str, pipeline: str, fqn: str) -> str:
    """DAG id снимка: имя DAG-а пайплайна + __act_snap_<alias>; alias = первые 8 символов base32(sha256(FQN))."""
    digest = hashlib.sha256(fqn.encode("utf-8")).digest()
    alias = base64.b32encode(digest).decode("ascii").lower()[:8]
    return f"ingest__rdbms__{source}__{pipeline}__act_snap_{alias}"


SNAPSHOT_DAGS = (
    snapshot_dag_id("bank", "main", "retail_bank.transactions"),
    snapshot_dag_id("broker", "main", "trading_core.transactions"),
)
ACT_SNAPSHOT_TABLES = {
    "raw.bank.retail_bank__transactions__act_snap": "raw.bank.retail_bank__transactions",
    "raw.broker.trading_core__transactions__act_snap": "raw.broker.trading_core__transactions",
}

RAW_SNAPSHOT_TABLES = [
    "raw.bank.retail_bank__clients",
    "raw.bank.retail_bank__accounts",
    "raw.bank.retail_bank__deals",
    "raw.broker.trading_core__clients",
    "raw.broker.trading_core__accounts",
    "raw.broker.trading_core__deals",
]
RAW_INCREMENTAL_TABLES = [
    "raw.bank.retail_bank__transactions",
    "raw.broker.trading_core__transactions",
]

EVOLUTION_SOURCES = {
    "bank": {
        "engine": "mssql",
        "host_suffix": "-src-mssql-bank:1433",
        "entity": "retail_bank.clients",
        "raw_table": "raw.bank.retail_bank__clients",
        "refs_dag": "ingest__rdbms__bank__refs",
        "dropped": "client_type",
        "renamed": ("full_name", "client_name"),
        "added": "email",
        "added_rule": {"kind": "faker", "provider": "email", "null_ratio": 0.0},
        "renamed_rule": {"kind": "faker", "provider": "name", "locale": "ru_RU", "null_ratio": 0.0},
        "float_seeded_on_first_load": True,
        "create_columns": [
            "ALTER TABLE [retail_bank].[clients] ADD [e2e_int] INT NULL, "
            "[e2e_float] REAL NULL, [e2e_dec] DECIMAL(9,2) NULL, [e2e_danger] INT NULL",
        ],
        "seed_values": [
            "UPDATE [retail_bank].[clients] SET [e2e_int] = 100000 + [client_id], "
            "[e2e_float] = 1.5, [e2e_dec] = 12345.67",
        ],
        "widen_types": [
            "ALTER TABLE [retail_bank].[clients] ALTER COLUMN [e2e_int] BIGINT",
            "ALTER TABLE [retail_bank].[clients] ALTER COLUMN [e2e_float] DOUBLE PRECISION",
            "ALTER TABLE [retail_bank].[clients] ALTER COLUMN [e2e_dec] DECIMAL(14,2)",
            "ALTER TABLE [retail_bank].[clients] ALTER COLUMN [client_name] NVARCHAR(800) NULL",
        ],
        "overflow_values": [
            "UPDATE [retail_bank].[clients] SET [e2e_int] = 3000000000 + [client_id], "
            "[e2e_float] = 1.0e300, [e2e_dec] = 12345678901.50, "
            "[client_name] = N'e2e-widened-' + REPLICATE(N'x', 500)",
        ],
        "widen_checks": [
            ("bank: e2e_int расширен в bigint на источнике",
             "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='retail_bank' "
             "AND TABLE_NAME='clients' AND COLUMN_NAME='e2e_int'", "bigint"),
            ("bank: e2e_float расширен в double на источнике",
             "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='retail_bank' "
             "AND TABLE_NAME='clients' AND COLUMN_NAME='e2e_float'", "float"),
            ("bank: e2e_dec расширен в decimal(14,2) на источнике",
             "SELECT DATA_TYPE + '(' + CAST(NUMERIC_PRECISION AS varchar(10)) + ',' "
             "+ CAST(NUMERIC_SCALE AS varchar(10)) + ')' FROM INFORMATION_SCHEMA.COLUMNS "
             "WHERE TABLE_SCHEMA='retail_bank' AND TABLE_NAME='clients' AND COLUMN_NAME='e2e_dec'",
             "decimal(14,2)"),
            ("bank: client_name расширен до 800 символов на источнике",
             "SELECT DATA_TYPE + '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS varchar(10)) + ')' "
             "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='retail_bank' "
             "AND TABLE_NAME='clients' AND COLUMN_NAME='client_name'", "nvarchar(800)"),
        ],
        "iceberg_first": {
            "e2e_int": "integer", "e2e_float": "real", "e2e_dec": "decimal(9,2)",
            "e2e_danger": "integer", "email": "varchar", "client_name": "varchar",
            "client_type": "varchar",
        },
        "iceberg_widened": {
            "e2e_int": "bigint", "e2e_float": "double", "e2e_dec": "decimal(14,2)",
            "client_name": "varchar",
        },
        "danger_type": (
            "ALTER TABLE [retail_bank].[clients] ALTER COLUMN [e2e_danger] NVARCHAR(64) NULL"
        ),
        "danger_insert": (
            "INSERT INTO [retail_bank].[clients] ([client_id], [client_name], [status], "
            "[_batch_id], [e2e_danger]) VALUES (910000001, N'e2e-danger-row', N'E2E', NULL, "
            "N'not-a-number')"
        ),
        "danger_type_check": (
            "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='retail_bank' "
            "AND TABLE_NAME='clients' AND COLUMN_NAME='e2e_danger'"
        ),
        "danger_type_expected": "nvarchar",
    },
    "broker": {
        "engine": "oracle",
        "host_suffix": "-src-oracle-broker:1521",
        "entity": "trading_core.clients",
        "raw_table": "raw.broker.trading_core__clients",
        "refs_dag": "ingest__rdbms__broker__refs",
        "dropped": "is_qualified",
        "renamed": ("full_name", "client_name"),
        "added": "email",
        "added_rule": {"kind": "faker", "provider": "email", "null_ratio": 0.0},
        "renamed_rule": {"kind": "faker", "provider": "name", "locale": "ru_RU", "null_ratio": 0.0},
        "float_seeded_on_first_load": False,
        "create_columns": [
            'ALTER TABLE "trading_core"."clients" ADD ("e2e_int" NUMBER(9,0), '
            '"e2e_float" BINARY_FLOAT, "e2e_dec" NUMBER(9,2), "e2e_danger" NUMBER(9,0))',
        ],
        "seed_values": [
            'UPDATE "trading_core"."clients" SET "e2e_int" = 100000 + "client_id", '
            '"e2e_dec" = 12345.67',
        ],
        "widen_types": [
            'ALTER TABLE "trading_core"."clients" MODIFY ("e2e_int" NUMBER(18,0))',
            'ALTER TABLE "trading_core"."clients" MODIFY ("e2e_float" BINARY_DOUBLE)',
            'ALTER TABLE "trading_core"."clients" MODIFY ("e2e_dec" NUMBER(14,2))',
            'ALTER TABLE "trading_core"."clients" MODIFY ("client_name" VARCHAR2(800))',
        ],
        "overflow_values": [
            'UPDATE "trading_core"."clients" SET "e2e_int" = 3000000000 + "client_id", '
            '"e2e_float" = 1.0e300D, "e2e_dec" = 12345678901.50, '
            "\"client_name\" = 'e2e-widened-' || RPAD('x', 500, 'x')",
        ],
        "widen_checks": [
            ("broker: e2e_int расширен в NUMBER(18,0) на источнике",
             "SELECT data_type || '(' || data_precision || ',' || data_scale || ')' "
             "FROM all_tab_columns WHERE owner='trading_core' AND table_name='clients' "
             "AND column_name='e2e_int'", "NUMBER(18,0)"),
            ("broker: e2e_float расширен в BINARY_DOUBLE на источнике",
             "SELECT data_type FROM all_tab_columns WHERE owner='trading_core' "
             "AND table_name='clients' AND column_name='e2e_float'", "BINARY_DOUBLE"),
            ("broker: e2e_dec расширен в NUMBER(14,2) на источнике",
             "SELECT data_type || '(' || data_precision || ',' || data_scale || ')' "
             "FROM all_tab_columns WHERE owner='trading_core' AND table_name='clients' "
             "AND column_name='e2e_dec'", "NUMBER(14,2)"),
            ("broker: client_name расширен до 800 символов на источнике",
             "SELECT data_type || '(' || char_length || ')' FROM all_tab_columns "
             "WHERE owner='trading_core' AND table_name='clients' "
             "AND column_name='client_name'", "VARCHAR2(800)"),
        ],
        "iceberg_first": {
            "e2e_int": "decimal(9,0)", "e2e_float": "real", "e2e_dec": "decimal(9,2)",
            "e2e_danger": "decimal(9,0)", "email": "varchar", "client_name": "varchar",
            "is_qualified": "varchar",
        },
        "iceberg_widened": {
            "e2e_int": "decimal(18,0)", "e2e_float": "double", "e2e_dec": "decimal(14,2)",
            "client_name": "varchar",
        },
        "danger_type": 'ALTER TABLE "trading_core"."clients" MODIFY ("e2e_danger" VARCHAR2(64))',
        "danger_insert": (
            'INSERT INTO "trading_core"."clients" ("client_id", "client_name", "status", '
            '"_batch_id", "e2e_danger") VALUES (910000001, \'e2e-danger-row\', \'E2E\', NULL, '
            "'not-a-number')"
        ),
        "danger_type_check": (
            "SELECT data_type FROM all_tab_columns WHERE owner='trading_core' "
            "AND table_name='clients' AND column_name='e2e_danger'"
        ),
        "danger_type_expected": "VARCHAR2",
    },
}

_CTX = ssl.create_default_context()
_CTX.check_hostname = False
_CTX.verify_mode = ssl.CERT_NONE

PASS = 0
FAIL = 0


def check(name: str, got: Any, want: Any) -> bool:
    global PASS, FAIL
    if got == want:
        print("PASS  %s" % name)
        PASS += 1
        return True
    print("FAIL  %s: got %r, want %r" % (name, got, want))
    FAIL += 1
    return False


def http(method: str, url: str, body: Any = None, headers: dict[str, str] | None = None, timeout: int = 30) -> tuple[int, str]:
    h = dict(headers or {})
    data = None
    if body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode("utf-8")
            h.setdefault("Content-Type", "application/json")
        elif isinstance(body, str):
            data = body.encode("utf-8")
        elif isinstance(body, bytes):
            data = body
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_CTX) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except Exception as e:
        return 0, str(e)


def wait_http(url: str, want: int, tries: int = 40, interval: int = 3, body: Any = None, headers: dict[str, str] | None = None) -> tuple[int, str]:
    for _ in range(tries):
        code, raw = http("GET", url, body, headers, timeout=20)
        if code == want:
            return code, raw
        time.sleep(interval)
    return code, raw


def sydafa_request(method: str, path: str, body: Any = None) -> tuple[int, Any]:
    """Отправляет запрос в API sydafa и разбирает JSON-ответ."""
    code, raw = http(method, SYDAFA + path, body, {"Content-Type": "application/json"})
    try:
        parsed = json.loads(raw) if raw else None
    except Exception:
        parsed = raw
    return code, parsed


def sydafa_post(path: str, body: Any) -> tuple[int, Any]:
    """Отправляет POST-запрос в API sydafa."""
    return sydafa_request("POST", path, body)


def sydafa_get(path: str) -> tuple[int, Any]:
    """Отправляет GET-запрос в API sydafa."""
    return sydafa_request("GET", path)


def sydafa_rule_put(source: str, entity: str, column: str, rule: dict[str, Any]) -> tuple[int, Any]:
    """Назначает генератор одной колонке через rules API."""
    return sydafa_request("PUT", "/sources/%s/rules/%s/columns/%s" % (source, entity, column), rule)


def sydafa_rule_delete(source: str, entity: str, column: str) -> tuple[int, Any]:
    """Удаляет правило генерации одной колонки через rules API."""
    return sydafa_request("DELETE", "/sources/%s/rules/%s/columns/%s" % (source, entity, column))


def sydafa_migrate(source: str, operations: list[dict[str, Any]]) -> tuple[int, Any]:
    return sydafa_post("/sources/%s/migrate" % source, {"operations": operations})


def airflow_token() -> str:
    """Получает JWT токен авторизации в Airflow."""
    data = urllib.parse.urlencode({"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD})
    code, raw = http(
        "POST",
        AIRFLOW + "/auth/token",
        data,
        {"Content-Type": "application/x-www-form-urlencoded"},
    )
    if code != 201:
        raise RuntimeError("airflow auth %s: %s" % (code, raw[:200]))
    return json.loads(raw)["access_token"]


def airflow_token_retry(timeout: int = 180) -> str | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            return airflow_token()
        except Exception:
            time.sleep(5)
    return None


def airflow_wait_dag_registered(token: str, dag_id: str, timeout: int = 180) -> bool:
    url = "%s/api/v2/dags/%s" % (AIRFLOW, dag_id)
    deadline = time.time() + timeout
    while time.time() < deadline:
        code, raw = http("GET", url, None, {"Authorization": "Bearer " + token})
        if code == 200:
            return True
        time.sleep(3)
    return False


def airflow_unpause(token: str, dag_id: str) -> tuple[int, str]:
    url = "%s/api/v2/dags/%s" % (AIRFLOW, dag_id)
    return http(
        "PATCH",
        url,
        {"is_paused": False},
        {"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )


def airflow_trigger(token: str, dag_id: str, run_id: str, conf: dict[str, Any] | None = None) -> tuple[int, str]:
    url = "%s/api/v2/dags/%s/dagRuns" % (AIRFLOW, dag_id)
    return http(
        "POST",
        url,
        {"dag_run_id": run_id, "logical_date": None, "conf": conf or {}},
        {"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )


def airflow_trigger_retry(token: str, dag_id: str, run_id: str, timeout: int = 180, conf: dict[str, Any] | None = None) -> tuple[int, str]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        airflow_unpause(token, dag_id)
        code, raw = airflow_trigger(token, dag_id, run_id, conf=conf)
        if code == 200:
            return code, raw
        time.sleep(5)
    return code, raw


def airflow_wait_run(token: str, dag_id: str, run_id: str, timeout: int = 300, name: str | None = None) -> bool:
    url = "%s/api/v2/dags/%s/dagRuns/%s" % (AIRFLOW, dag_id, run_id)
    deadline = time.time() + timeout
    last = "unknown"
    while time.time() < deadline:
        code, raw = http("GET", url, None, {"Authorization": "Bearer " + token})
        if code == 200:
            d = json.loads(raw)
            last = d.get("state", "unknown")
            if last == "success":
                check((name or dag_id) + " -> success", "success", "success")
                if dag_id.startswith("ingest__"):
                    airflow_check_asset_publication(token, dag_id, run_id)
                return True
            if last == "failed":
                check((name or dag_id) + " -> success", "failed", "success")
                try:
                    ti_url = "%s/api/v2/dags/%s/dagRuns/%s/taskInstances" % (AIRFLOW, dag_id, run_id)
                    _, ti_raw = http("GET", ti_url, None, {"Authorization": "Bearer " + token})
                    tis = json.loads(ti_raw).get("task_instances", [])
                    for ti in tis:
                        t_id = ti.get("task_id")
                        print("Task %s state: %s" % (t_id, ti.get("state")))
                        log_url = "%s/api/v2/dags/%s/dagRuns/%s/taskInstances/%s/logs/1" % (AIRFLOW, dag_id, run_id, t_id)
                        _, log_raw = http("GET", log_url, None, {"Authorization": "Bearer " + token})
                        print("=== LOG %s ===\n%s\n===============" % (t_id, log_raw[-2000:] if len(log_raw) > 2000 else log_raw))
                except Exception as ex:
                    print("Could not fetch task logs:", ex)
                return False
        time.sleep(5)
    check((name or dag_id) + " -> success", "timeout:" + str(last), "success")
    return False


def airflow_check_asset_publication(token: str, dag_id: str, run_id: str) -> None:
    """Verify each entity publishes only after its END logging task succeeds."""
    url = "%s/api/v2/dags/%s/dagRuns/%s/taskInstances?limit=100" % (AIRFLOW, dag_id, run_id)
    code, raw = http("GET", url, None, {"Authorization": "Bearer " + token})
    if not check(dag_id + ": publication task instances available", code, 200):
        return
    payload = json.loads(raw)
    instances = {item["task_id"]: item for item in payload["task_instances"]}
    check(dag_id + ": all task instances retrieved", len(instances), payload["total_entries"])
    end_tasks = [task_id for task_id in instances if task_id.endswith(".log_end")]
    publishers = [task_id for task_id in instances if task_id.endswith(".publish_asset")]
    check(dag_id + ": one publisher per entity", len(publishers), len(end_tasks))
    check(dag_id + ": entity logging tasks present", bool(end_tasks), True)
    for task_id in end_tasks:
        end = instances[task_id]
        publisher = instances.get(task_id.removesuffix("log_end") + "publish_asset", {})
        check(task_id + ": END logging succeeded", end["state"], "success")
        check(task_id + ": separate publication succeeded", publisher.get("state"), "success")
        end_date = end.get("end_date")
        start_date = publisher.get("start_date")
        check(task_id + ": publication follows END logging",
              bool(end_date and start_date and start_date >= end_date), True)


def airflow_task_states(token: str, dag_id: str, run_id: str) -> dict[str, str]:
    """Состояния задач DagRun: task_id -> state."""
    url = "%s/api/v2/dags/%s/dagRuns/%s/taskInstances?limit=100" % (AIRFLOW, dag_id, run_id)
    code, raw = http("GET", url, None, {"Authorization": "Bearer " + token})
    if code != 200:
        return {}
    payload = json.loads(raw)
    return {item["task_id"]: item.get("state") for item in payload.get("task_instances", [])}


def airflow_task_log(token: str, dag_id: str, run_id: str, task_id: str) -> str:
    """Хвост лога первой попытки задачи."""
    url = "%s/api/v2/dags/%s/dagRuns/%s/taskInstances/%s/logs/1" % (AIRFLOW, dag_id, run_id, task_id)
    _, raw = http("GET", url, None, {"Authorization": "Bearer " + token}, timeout=120)
    return raw[-20000:]


def airflow_wait_run_failed(token: str, dag_id: str, run_id: str, timeout: int = 900, name: str | None = None) -> bool:
    """Дождаться ожидаемого отказа DagRun без печати логов: причину проверяет вызывающий код."""
    url = "%s/api/v2/dags/%s/dagRuns/%s" % (AIRFLOW, dag_id, run_id)
    deadline = time.time() + timeout
    last = "unknown"
    while time.time() < deadline:
        code, raw = http("GET", url, None, {"Authorization": "Bearer " + token})
        if code == 200:
            last = json.loads(raw).get("state", "unknown")
            if last in ("failed", "success"):
                break
        time.sleep(5)
    check((name or dag_id) + " -> failed", last, "failed")
    return last == "failed"


def trino_query(sql: str, timeout: int = 120) -> list[list[Any]]:
    """Выполняет SQL в Trino через REST (/v1/statement) и возвращает строки данных.

    Страницы данных приходят в промежуточных ответах (в терминальном ответе
    поля data нет), поэтому строки накапливаются со всех ответов опроса.
    """
    auth = "Basic " + base64.b64encode(("%s:%s" % (TRINO_USER, TRINO_PASSWORD)).encode("utf-8")).decode("utf-8")
    headers = {"Authorization": auth, "X-Trino-User": TRINO_USER, "Content-Type": "text/plain"}
    code, raw = http("POST", TRINO + "/v1/statement", sql, headers, timeout=60)
    if code != 200:
        raise RuntimeError("trino statement %s: %s" % (code, raw[:300]))
    deadline = time.time() + timeout
    payload = json.loads(raw)
    rows: list[list[Any]] = list(payload.get("data") or [])
    while payload.get("nextUri"):
        if time.time() > deadline:
            raise RuntimeError("trino query timeout: %s" % sql[:200])
        state = payload.get("stats", {}).get("state")
        if state == "FAILED":
            error = payload.get("error", {}).get("message", "unknown trino error")
            raise RuntimeError("trino failed: %s (%s)" % (error, sql[:200]))
        code, raw = http("GET", payload["nextUri"], None, headers, timeout=60)
        if code != 200:
            raise RuntimeError("trino poll %s: %s" % (code, raw[:300]))
        payload = json.loads(raw)
        rows.extend(payload.get("data") or [])
        time.sleep(0.5)
    if payload.get("stats", {}).get("state") == "FAILED":
        error = payload.get("error", {}).get("message", "unknown trino error")
        raise RuntimeError("trino failed: %s (%s)" % (error, sql[:200]))
    return rows


def raw_stats(table: str) -> tuple[int, int, int]:
    """(count, count distinct __process_id, max __process_id) сырой таблицы."""
    namespace, name = table.split(".")[1:]
    expected = f"s3://datalake/raw/{namespace}/{name}/"
    paths = trino_query(f'SELECT DISTINCT "$path" FROM {table}')
    check(f"{table}: exact storage prefix without UUID", bool(paths) and all(row[0].startswith(expected) for row in paths), True)
    rows = trino_query(
        "SELECT count(*), count(DISTINCT __process_id), max(__process_id) FROM %s" % table
    )
    return int(rows[0][0]), int(rows[0][1]), int(rows[0][2])


def raw_rows_with_process(table: str, process_id: int) -> int:
    """Число строк, загруженных конкретным запуском."""
    rows = trino_query("SELECT count(*) FROM %s WHERE __process_id = %d" % (table, process_id))
    return int(rows[0][0])


def raw_column_types(table: str) -> dict[str, str]:
    """Типы колонок raw-таблицы Iceberg из information_schema Trino."""
    namespace, name = table.split(".")[1:]
    rows = trino_query(
        "SELECT column_name, data_type FROM raw.information_schema.columns "
        "WHERE table_schema = '%s' AND table_name = '%s'" % (namespace, name)
    )
    return {row[0]: row[1] for row in rows}


def raw_snapshot_count(table: str) -> int:
    """Число снапшотов Iceberg у raw-таблицы."""
    namespace, name = table.split(".")[1:]
    rows = trino_query('SELECT count(*) FROM "raw"."%s"."%s$snapshots"' % (namespace, name))
    return int(rows[0][0])


def validate_manifests_and_dags() -> None:
    """Выполняет валидацию манифестов источников, файлов миграций и синтаксиса DAG-ов."""
    print("==> Шаг 0: Проверка структуры манифестов источников, миграций и DAG-ов...")
    candidates = [
        Path(os.environ.get("WORKSPACE_DIR", "/workspace")),
        Path(__file__).resolve().parent.parent,
    ]
    ws = next((p for p in candidates if (p / "conf" / "sydafa" / "sources.json").exists()), None)
    if not ws:
        print("WARN: Рабочий каталог с conf/sydafa/sources.json не найден, пропуск статических проверок")
        return

    conf_file = ws / "conf" / "sydafa" / "sources.json"
    with open(conf_file, "r", encoding="utf-8") as f:
        conf = json.load(f)
    sources = conf.get("sources", {})

    check("sources.json has bank source", "bank" in sources, True)
    check("sources.json has broker source", "broker" in sources, True)

    bank = sources.get("bank", {})
    check("bank type is mssql", bank.get("type"), "mssql")
    check("bank external_port is isolated or configured", bank.get("external_port") in (None, 14330), True)

    bank_tables = bank.get("schema", {}).get("tables", {})
    bank_tbl_names = sorted(bank_tables.keys())
    check("bank star schema tables", ",".join(bank_tbl_names), "retail_bank.accounts,retail_bank.clients,retail_bank.deals,retail_bank.transactions")

    tx_fks = bank_tables.get("retail_bank.transactions", {}).get("foreign_keys", [])
    ref_entities = sorted(fk["ref_entity_fqn"] for fk in tx_fks)
    check("bank transactions foreign keys", ",".join(ref_entities), "retail_bank.accounts,retail_bank.clients,retail_bank.deals")

    bank_rules = bank.get("rules", {})
    tx_rules = bank_rules.get("retail_bank.transactions", {}).get("columns", {})
    check("bank currency choice RUB", tx_rules.get("currency", {}).get("values"), ["RUB"])
    check("bank currency null_ratio 0.0", tx_rules.get("currency", {}).get("null_ratio"), 0.0)
    check("bank amount null_ratio 0.0", tx_rules.get("amount", {}).get("null_ratio"), 0.0)
    check("bank posting_date null_ratio 0.0", tx_rules.get("posting_date", {}).get("null_ratio"), 0.0)
    check("bank description null_ratio 0.05", tx_rules.get("description", {}).get("null_ratio"), 0.05)

    deals_rules = bank_rules.get("retail_bank.deals", {}).get("columns", {})
    check("bank deals end_date date_offset", deals_rules.get("end_date", {}).get("kind"), "date_offset")
    check("bank deals end_date ref start_date", deals_rules.get("end_date", {}).get("ref_column"), "start_date")

    bank_invs = bank_rules.get("retail_bank.transactions", {}).get("invariants", [])
    has_bank_bal = any(
        inv.get("kind") == "cumulative_balance"
        and inv.get("amount_column") == "amount"
        and inv.get("partition_by") == ["account_id"]
        and inv.get("min_balance") == 0.0
        and inv.get("validate_existing") is False
        for inv in bank_invs
    )
    check("bank cumulative_balance invariant (validate_existing=False)", has_bank_bal, True)

    broker = sources.get("broker", {})
    check("broker type is oracle", broker.get("type"), "oracle")
    check("broker external_port is isolated or configured", broker.get("external_port") in (None, 15211), True)

    broker_tables = broker.get("schema", {}).get("tables", {})
    broker_tbl_names = sorted(broker_tables.keys())
    check("broker star schema tables", ",".join(broker_tbl_names), "trading_core.accounts,trading_core.clients,trading_core.deals,trading_core.transactions")

    broker_rules = broker.get("rules", {})
    b_tx_rules = broker_rules.get("trading_core.transactions", {}).get("columns", {})
    check("broker security_type choice", b_tx_rules.get("security_type", {}).get("kind"), "choice")
    check("broker security_type null_ratio 0.0", b_tx_rules.get("security_type", {}).get("null_ratio"), 0.0)
    check("broker quantity null_ratio 0.0", b_tx_rules.get("quantity", {}).get("null_ratio"), 0.0)
    check("broker posting_date null_ratio 0.0", b_tx_rules.get("posting_date", {}).get("null_ratio"), 0.0)
    check("broker ticker null_ratio 0.05", b_tx_rules.get("ticker", {}).get("null_ratio"), 0.05)

    broker_invs = broker_rules.get("trading_core.transactions", {}).get("invariants", [])
    has_broker_bal = any(
        inv.get("kind") == "cumulative_balance"
        and inv.get("amount_column") == "quantity"
        and inv.get("partition_by") == ["account_id"]
        and inv.get("min_balance") == 0.0
        and inv.get("validate_existing") is False
        for inv in broker_invs
    )
    check("broker cumulative_balance invariant (validate_existing=False)", has_broker_bal, True)

    bank_mig = ws / "airflow" / "plugins" / "migrations" / "bank.json"
    broker_mig = ws / "airflow" / "plugins" / "migrations" / "broker.json"
    check("bank.json exists", bank_mig.exists(), True)
    check("broker.json exists", broker_mig.exists(), True)

    with open(bank_mig, "r", encoding="utf-8") as f:
        bank_ops = json.load(f).get("operations", [])
    bank_appends = [op for op in bank_ops if op.get("op") == "append"]
    bank_updates = [op for op in bank_ops if op.get("op") == "update"]
    bank_deletes = [op for op in bank_ops if op.get("op") == "delete"]
    check("bank migration has 4 appends (dims 20, 20, 200 + fact 5k)", (
        len(bank_appends) == 4
        and bank_appends[0].get("entity_fqn") == "retail_bank.clients" and bank_appends[0].get("rows") == 20
        and bank_appends[1].get("entity_fqn") == "retail_bank.accounts" and bank_appends[1].get("rows") == 20
        and bank_appends[2].get("entity_fqn") == "retail_bank.deals" and bank_appends[2].get("rows") == 200
        and bank_appends[3].get("entity_fqn") == "retail_bank.transactions"
        and bank_appends[3].get("rows") == 5000
        and "new_key_ratio" not in bank_appends[3]
    ), True)
    check("bank migration has 1 cascade delete of 10 deals", (
        len(bank_deletes) == 1
        and bank_deletes[0].get("entity_fqn") == "retail_bank.deals"
        and bank_deletes[0].get("limit") == 10
        and bank_deletes[0].get("cascade") is True
    ), True)
    check("bank migration has 3 updates with predicates (limits 2, 2, 10)", (
        len(bank_updates) == 3
        and bank_updates[0].get("entity_fqn") == "retail_bank.clients"
        and bank_updates[0].get("limit") == 2
        and bank_updates[0].get("predicate") == [{"column": "status", "operator": "!=", "value": "VIP"}]
        and bank_updates[1].get("entity_fqn") == "retail_bank.accounts"
        and bank_updates[1].get("limit") == 2
        and bank_updates[1].get("predicate") == [{"column": "status", "operator": "!=", "value": "Frozen"}]
        and bank_updates[2].get("entity_fqn") == "retail_bank.deals"
        and bank_updates[2].get("limit") == 10
        and bank_updates[2].get("predicate") == [{"column": "status", "operator": "!=", "value": "Closed"}]
    ), True)

    with open(broker_mig, "r", encoding="utf-8") as f:
        broker_ops = json.load(f).get("operations", [])
    b_appends = [op for op in broker_ops if op.get("op") == "append"]
    b_updates = [op for op in broker_ops if op.get("op") == "update"]
    b_deletes = [op for op in broker_ops if op.get("op") == "delete"]
    check("broker migration has 4 appends (dims 20, 20, 200 + fact 5k)", (
        len(b_appends) == 4
        and b_appends[0].get("entity_fqn") == "trading_core.clients" and b_appends[0].get("rows") == 20
        and b_appends[1].get("entity_fqn") == "trading_core.accounts" and b_appends[1].get("rows") == 20
        and b_appends[2].get("entity_fqn") == "trading_core.deals" and b_appends[2].get("rows") == 200
        and b_appends[3].get("entity_fqn") == "trading_core.transactions"
        and b_appends[3].get("rows") == 5000
        and "new_key_ratio" not in b_appends[3]
    ), True)
    check("broker migration has 1 cascade delete of 10 deals", (
        len(b_deletes) == 1
        and b_deletes[0].get("entity_fqn") == "trading_core.deals"
        and b_deletes[0].get("limit") == 10
        and b_deletes[0].get("cascade") is True
    ), True)
    check("broker migration has 3 updates with predicates (limits 2, 2, 10)", (
        len(b_updates) == 3
        and b_updates[0].get("entity_fqn") == "trading_core.clients"
        and b_updates[0].get("limit") == 2
        and b_updates[0].get("predicate") == [{"column": "risk_profile", "operator": "!=", "value": "Aggressive"}]
        and b_updates[1].get("entity_fqn") == "trading_core.accounts"
        and b_updates[1].get("limit") == 2
        and b_updates[1].get("predicate") == [{"column": "status", "operator": "!=", "value": "Closed"}]
        and b_updates[2].get("entity_fqn") == "trading_core.deals"
        and b_updates[2].get("limit") == 10
        and b_updates[2].get("predicate") == [{"column": "market", "operator": "!=", "value": "OTC"}]
    ), True)

    plugin_path = ws / "airflow" / "plugins" / "migrations" / "__init__.py"
    check("airflow/plugins/migrations/__init__.py exists", plugin_path.exists(), True)
    try:
        import ast
        ast.parse(plugin_path.read_text(encoding="utf-8"))
        check("plugin migrations package python syntax valid", True, True)
    except Exception as e:
        check("plugin migrations package python syntax valid", str(e), True)

    dag_bank_path = ws / "airflow" / "dags" / "update_source_bank.py"
    dag_broker_path = ws / "airflow" / "dags" / "update_source_broker.py"
    check("update_source_bank.py exists", dag_bank_path.exists(), True)
    check("update_source_broker.py exists", dag_broker_path.exists(), True)

    try:
        import ast
        ast.parse(dag_bank_path.read_text(encoding="utf-8"))
        ast.parse(dag_broker_path.read_text(encoding="utf-8"))
        check("DAG files python syntax valid", True, True)
    except Exception as e:
        check("DAG files python syntax valid", str(e), True)

    c_bank = dag_bank_path.read_text(encoding="utf-8")
    check("bank DAG imports load_migration_operations and uses source bank", (
        "load_migration_operations" in c_bank and 'load_migration_operations("bank")' in c_bank and "SydafaMigrateOperator" in c_bank
    ), True)
    c_broker = dag_broker_path.read_text(encoding="utf-8")
    check("broker DAG imports load_migration_operations and uses source broker", (
        "load_migration_operations" in c_broker and 'load_migration_operations("broker")' in c_broker and "SydafaMigrateOperator" in c_broker
    ), True)

    sys.path.insert(0, str(ws))
    from ingestion.generator.generate_dags import generate_all
    from ingestion.generator.manifest import load_sources

    import yaml

    for source in ("bank", "broker"):
        pipeline_path = ws / "ingestion" / "conf" / "pipelines" / source / "main.yaml"
        pipeline = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
        entities = pipeline.get("entities", [])
        transactions = [entity for entity in entities if entity.get("fqn", "").endswith(".transactions")]
        expected_type = "bigint" if source == "bank" else "decimal(19,0)"
        check(source + " transactions uses structured incremental config", len(transactions) == 1 and transactions[0].get("incremental", {}).get("contract") == {"source_column": "_batch_id", "position_type": expected_type}, True)
        check(source + " main has no legacy incremental keys", all("incremental_column" not in entity and "incremental_lookback" not in entity for entity in entities), True)

    generated = generate_all(
        load_sources(ws / "ingestion" / "conf" / "sources.yaml"),
        ws / "ingestion" / "conf" / "pipelines", ws / "ingestion" / "conf" / "settings.yaml",
    )
    check("generated DAG set", sorted(generated), sorted(name + ".py" for name in INGEST_DAGS + SNAPSHOT_DAGS))
    for name, content in generated.items():
        ast.parse(content, filename=name)
        check("generated DAG %s syntax valid" % name, True, True)
    for dag_id in SNAPSHOT_DAGS:
        content = generated[dag_id + ".py"]
        check("snapshot DAG %s renders snapshot loader" % dag_id, "IngestActSnapOperator(" in content, True)
        check("snapshot DAG %s has no schedule" % dag_id, "schedule=None" in content, True)
        check("snapshot DAG %s tagged act_snap" % dag_id, "'act_snap'" in content, True)
        check("snapshot DAG %s uses Python publication task" % dag_id,
              "python_callable=publish_asset_event" in content, True)
        check("snapshot DAG %s uses slash Asset URI" % dag_id, "/act_snap\")" in content, True)
        check("snapshot DAG %s uses explicitly configured shared pool" % dag_id, 'pool="ingest_loads"' in content, True)
        check("snapshot DAG %s uses configured pool weight" % dag_id, "pool_slots=2" in content, True)


def evolution_host(cfg: dict[str, Any]) -> str:
    """Внутренний адрес контейнера источника текущего контура."""
    prefix = os.environ.get("SYDAFA_CONTAINER_PREFIX", "sydafa-dlh-test")
    return prefix + cfg["host_suffix"]


def evolution_ddl(source: str, cfg: dict[str, Any], statements: list[str], label: str) -> int:
    """Выполнить прямые DDL/DML источника; отказ фиксируется проверкой, а не аварией."""
    try:
        affected = source_ddl.execute(cfg["engine"], evolution_host(cfg), statements)
        check("%s: %s" % (source, label), True, True)
        return affected
    except Exception as exc:
        check("%s: %s" % (source, label), "%s: %s" % (type(exc).__name__, exc), True)
        return -1


def evolution_source_check(source: str, cfg: dict[str, Any], sql: str, want: Any, label: str) -> None:
    """Сверить одно значение из словаря СУБД с ожидаемым."""
    try:
        got = source_ddl.query_scalar(cfg["engine"], evolution_host(cfg), sql)
    except Exception as exc:
        got = "%s: %s" % (type(exc).__name__, exc)
    check("%s: %s" % (source, label), got, want)


def evolution_load_refs(token: str, source: str, stamp: str, phase: str) -> bool:
    """Запустить refs-DAG источника и дождаться завершения."""
    cfg = EVOLUTION_SOURCES[source]
    run_id = "e2e-evo%s-%s-%s" % (phase, source, stamp)
    code, _ = airflow_trigger_retry(token, cfg["refs_dag"], run_id)
    check("trigger %s (evolution %s)" % (cfg["refs_dag"], phase), code, 200)
    return airflow_wait_run(token, cfg["refs_dag"], run_id, timeout=900,
                            name="%s evolution load %s" % (source, phase))


def evolution_prepare_sources(token: str, stamp: str) -> None:
    """Безопасные операции схемы через API sydafa и генерация данных новыми колонками.

    Прямые DDL выполняются только после генерации: модель sydafa не знает
    e2e-колонок, и генератор не должен выполняться после расхождения
    физической схемы с сохранённой моделью.
    """
    for source, cfg in EVOLUTION_SOURCES.items():
        old_name, new_name = cfg["renamed"]
        code, _ = sydafa_migrate(source, [
            {"op": "drop_column", "entity_fqn": cfg["entity"], "name": cfg["dropped"]}])
        check("%s: migrate drop_column %s" % (source, cfg["dropped"]), code, 200)
        code, _ = sydafa_migrate(source, [
            {"op": "rename_column", "entity_fqn": cfg["entity"],
             "from_name": old_name, "to_name": new_name}])
        check("%s: migrate rename_column %s -> %s" % (source, old_name, new_name), code, 200)
        code, _ = sydafa_migrate(source, [
            {"op": "add_column", "entity_fqn": cfg["entity"],
             "column": {"name": cfg["added"], "type": "string"}}])
        check("%s: migrate add_column %s" % (source, cfg["added"]), code, 200)
        code, _ = sydafa_rule_put(source, cfg["entity"], cfg["added"], cfg["added_rule"])
        check("%s: генератор добавленной колонки %s" % (source, cfg["added"]), code, 200)
        code, _ = sydafa_rule_put(source, cfg["entity"], new_name, cfg["renamed_rule"])
        check("%s: генератор перенесён на %s" % (source, new_name), code, 200)
        code, _ = sydafa_rule_delete(source, cfg["entity"], old_name)
        check("%s: правило прежнего имени %s удалено" % (source, old_name), code, 200)
    for source in EVOLUTION_SOURCES:
        update_dag = "update_source_%s" % source
        run_id = "e2e-evo-gen-%s-%s" % (source, stamp)
        code, _ = airflow_trigger_retry(token, update_dag, run_id)
        check("trigger %s (evolution data)" % update_dag, code, 200)
        airflow_wait_run(token, update_dag, run_id, timeout=360,
                         name="%s evolution data generation" % source)


def evolution_first_load(token: str, stamp: str) -> dict[str, int]:
    """Создать e2e-колонки прямыми DDL, заполнить контрольные значения и загрузить первый раз."""
    for source, cfg in EVOLUTION_SOURCES.items():
        evolution_ddl(source, cfg, cfg["create_columns"], "e2e-колонки созданы прямым DDL")
        seeded = evolution_ddl(source, cfg, cfg["seed_values"], "контрольные значения до расширения")
        if seeded >= 0:
            check("%s: строк с контрольными значениями до расширения" % source, seeded > 0, True)
    for source in EVOLUTION_SOURCES:
        evolution_load_refs(token, source, stamp, "1")
    first_pids: dict[str, int] = {}
    try:
        for source, cfg in EVOLUTION_SOURCES.items():
            added, new_name, old_name, dropped = (
                cfg["added"], cfg["renamed"][1], cfg["renamed"][0], cfg["dropped"])
            pid = int(trino_query("SELECT max(__process_id) FROM %s" % cfg["raw_table"])[0][0])
            first_pids[source] = pid
            portion = raw_rows_with_process(cfg["raw_table"], pid)
            check("%s: порция эволюции 1 непустая" % source, portion > 0, True)
            counts = trino_query(
                "SELECT count(%s), count(%s), count(%s), count(%s), count(e2e_int), "
                "count(e2e_dec), count(e2e_danger) FROM %s WHERE __process_id = %d"
                % (added, new_name, old_name, dropped, cfg["raw_table"], pid))[0]
            check("%s: добавленная колонка заполнена у новых строк" % source,
                  0 < counts[0] < portion, True)
            check("%s: переименованная колонка пришла со значениями" % source,
                  counts[1] > 0, True)
            check("%s: прежнее имя колонки NULL во всей порции" % source, counts[2], 0)
            check("%s: удалённая колонка NULL во всей порции" % source, counts[3], 0)
            check("%s: e2e_int заполнен у обновлённых строк" % source, counts[4] > 0, True)
            check("%s: e2e_dec заполнен у обновлённых строк" % source, counts[5] > 0, True)
            check("%s: e2e_danger NULL в порции" % source, counts[6], 0)
            history = trino_query(
                "SELECT count(%s), count(%s), count(%s), count(%s) FROM %s WHERE __process_id < %d"
                % (added, new_name, old_name, dropped, cfg["raw_table"], pid))[0]
            check("%s: добавленная колонка NULL в истории" % source, history[0], 0)
            check("%s: переименованная колонка NULL в истории" % source, history[1], 0)
            check("%s: прежнее имя колонки сохранено в истории" % source, history[2] > 0, True)
            check("%s: удалённая колонка сохранена в истории" % source, history[3] > 0, True)
            types = raw_column_types(cfg["raw_table"])
            for column, want in cfg["iceberg_first"].items():
                check("%s: тип Iceberg %s после эволюции" % (source, column),
                      types.get(column), want)
            if cfg["float_seeded_on_first_load"]:
                distinct = trino_query(
                    "SELECT count(DISTINCT e2e_float) FROM %s WHERE __process_id = %d "
                    "AND e2e_float IS NOT NULL" % (cfg["raw_table"], pid))[0][0]
                check("%s: e2e_float посеян значением 1.5" % source, int(distinct), 1)
            else:
                filled = trino_query(
                    "SELECT count(e2e_float) FROM %s WHERE __process_id = %d"
                    % (cfg["raw_table"], pid))[0][0]
                check("%s: e2e_float пустая до расширения (ограничение DDL Oracle)" % source,
                      int(filled), 0)
    except Exception as exc:
        check("evolution first portion checks", "%s: %s" % (type(exc).__name__, exc), "ok")
    return first_pids


def evolution_widen(token: str, stamp: str, first_pids: dict[str, int]) -> None:
    """Расширить типы на источнике прямыми DDL и проверить promotion схемы Iceberg."""
    for source, cfg in EVOLUTION_SOURCES.items():
        evolution_ddl(source, cfg, cfg["widen_types"], "типы расширены прямым DDL")
        for label, sql, want in cfg["widen_checks"]:
            evolution_source_check(source, cfg, sql, want, label)
        affected = evolution_ddl(source, cfg, cfg["overflow_values"],
                                 "контрольные значения после расширения")
        if affected >= 0:
            check("%s: строк с контрольными значениями после расширения" % source,
                  affected > 0, True)
    for source in EVOLUTION_SOURCES:
        evolution_load_refs(token, source, stamp, "2")
    try:
        for source, cfg in EVOLUTION_SOURCES.items():
            new_name = cfg["renamed"][1]
            pid = int(trino_query("SELECT max(__process_id) FROM %s" % cfg["raw_table"])[0][0])
            portion = trino_query(
                "SELECT count(*), count(e2e_int), min(e2e_int), count(e2e_dec), "
                "count(e2e_float), min(e2e_float), min(length(%s)) FROM %s WHERE __process_id = %d"
                % (new_name, cfg["raw_table"], pid))[0]
            total = int(portion[0])
            check("%s: порция эволюции 2 непустая" % source, total > 0, True)
            check("%s: e2e_int заполнен во всей порции" % source, int(portion[1]), total)
            check("%s: e2e_int за пределами int (> 2147483647)" % source,
                  portion[2] is not None and int(portion[2]) > 2147483647, True)
            check("%s: e2e_dec заполнен во всей порции" % source, int(portion[3]), total)
            check("%s: e2e_float заполнен во всей порции" % source, int(portion[4]), total)
            check("%s: e2e_float за пределами real (> 1e38)" % source,
                  portion[5] is not None and float(portion[5]) > 1.0e38, True)
            check("%s: переименованная колонка длиннее 400 символов" % source,
                  portion[6] is not None and int(portion[6]) > 400, True)
            exact = int(trino_query(
                "SELECT count(*) FROM %s WHERE __process_id = %d "
                "AND e2e_dec = DECIMAL '12345678901.50'" % (cfg["raw_table"], pid))[0][0])
            check("%s: значение e2e_dec не потеряно при расширении" % source, exact, total)
            types = raw_column_types(cfg["raw_table"])
            for column, want in cfg["iceberg_widened"].items():
                check("%s: тип Iceberg %s после расширения" % (source, column),
                      types.get(column), want)
            if source in first_pids:
                previous = trino_query(
                    "SELECT max(e2e_int) FROM %s WHERE __process_id = %d "
                    "AND e2e_int IS NOT NULL" % (cfg["raw_table"], first_pids[source]))[0][0]
                check("%s: история порции 1 не перезаписана расширением" % source,
                      previous is not None and int(previous) < 3000000000, True)
    except Exception as exc:
        check("evolution widened portion checks", "%s: %s" % (type(exc).__name__, exc), "ok")


def evolution_incompatible(token: str, stamp: str) -> None:
    """Несовместимое изменение типа на источнике: отказ load-задачи без записи в Iceberg."""
    for source, cfg in EVOLUTION_SOURCES.items():
        try:
            before_rows = int(trino_query("SELECT count(*) FROM %s" % cfg["raw_table"])[0][0])
            before_snapshots = raw_snapshot_count(cfg["raw_table"])
            before_pid = int(trino_query("SELECT max(__process_id) FROM %s" % cfg["raw_table"])[0][0])
        except Exception as exc:
            check("%s: состояние raw до несовместимого изменения" % source,
                  "%s: %s" % (type(exc).__name__, exc), "ok")
            continue
        evolution_ddl(source, cfg, [cfg["danger_type"]],
                      "несовместимый тип применён к e2e_danger прямым DDL")
        evolution_source_check(source, cfg, cfg["danger_type_check"],
                               cfg["danger_type_expected"], "e2e_danger на источнике стал строкой")
        inserted = evolution_ddl(source, cfg, [cfg["danger_insert"]],
                                 "строка с неприводимым значением вставлена напрямую")
        check("%s: вставлена ровно одна строка" % source, inserted, 1)
        cfg_dag = cfg["refs_dag"]
        run_id = "e2e-evo3-%s-%s" % (source, stamp)
        code, _ = airflow_trigger_retry(token, cfg_dag, run_id)
        check("trigger %s (incompatible type)" % cfg_dag, code, 200)
        failed = airflow_wait_run_failed(
            token, cfg_dag, run_id, timeout=900,
            name="%s: загрузка отвергла несовместимый тип" % source)
        if failed:
            states = airflow_task_states(token, cfg_dag, run_id)
            loads = {task_id: state for task_id, state in states.items()
                     if task_id.endswith(".load")}
            check("%s: load задачи выполнены до отказа" % source, len(loads) >= 2, True)
            clients_state = next((state for task_id, state in loads.items()
                                  if "clients" in task_id), None)
            check("%s: load clients failed" % source, clients_state, "failed")
            other_state = next((state for task_id, state in loads.items()
                                if "clients" not in task_id), None)
            check("%s: соседние load не задеты" % source, other_state, "success")
            clients_task = next((task_id for task_id in loads if "clients" in task_id), None)
            if clients_task:
                log = airflow_task_log(token, cfg_dag, run_id, clients_task)
                check("%s: причина отказа — смена типа колонки e2e_danger" % source,
                      "Cannot change column type" in log and "e2e_danger" in log, True)
        try:
            after_rows = int(trino_query("SELECT count(*) FROM %s" % cfg["raw_table"])[0][0])
            after_snapshots = raw_snapshot_count(cfg["raw_table"])
            after_pid = int(trino_query("SELECT max(__process_id) FROM %s" % cfg["raw_table"])[0][0])
            leaked = int(trino_query(
                "SELECT count(*) FROM %s WHERE client_id = 910000001" % cfg["raw_table"])[0][0])
            check("%s: новые строки не добавлены после отказа" % source, after_rows, before_rows)
            check("%s: новый снапшот Iceberg не создан" % source, after_snapshots, before_snapshots)
            check("%s: process_id не вырос после отказа" % source, after_pid, before_pid)
            check("%s: строка с несовместимым значением не попала в raw" % source, leaked, 0)
        except Exception as exc:
            check("%s: состояние raw после несовместимого изменения" % source,
                  "%s: %s" % (type(exc).__name__, exc), "ok")


def run_source_schema_evolution(token: str, stamp: str) -> None:
    """Полный цикл эволюции схемы реальных источников: операции, расширения, отказ."""
    print("\n==> Шаг 14: Эволюция схемы источников: безопасные операции через API sydafa...")
    evolution_prepare_sources(token, stamp)
    print("\n==> Шаг 15: Первая загрузка после эволюции, затем расширения типов и Iceberg promotion...")
    first_pids = evolution_first_load(token, stamp)
    evolution_widen(token, stamp, first_pids)
    print("\n==> Шаг 16: Несовместимое изменение типа на источнике → отказ загрузки без нового снапшота...")
    evolution_incompatible(token, stamp)


def main() -> None:
    validate_manifests_and_dags()
    print("==> Шаг 1: Проверка готовности sydafa API...")
    code, _ = wait_http(SYDAFA + "/health", 200, tries=50, interval=3)
    check("sydafa /health", code, 200)

    code, _ = http("GET", SYDAFA + "/docs", None, None, timeout=10)
    check("sydafa /docs", code, 200)

    code, raw = wait_http(SYDAFA + "/sources", 200, tries=30, interval=3)
    check("sydafa /sources", code, 200)
    try:
        sources_list = json.loads(raw).get("sources", [])
        sources_sorted = sorted(sources_list)
        check("sydafa /sources contains bank", "bank" in sources_sorted, True)
        check("sydafa /sources contains broker", "broker" in sources_sorted, True)
    except Exception as e:
        check("sydafa /sources parse", str(e), "ok")

    for src in ["bank", "broker"]:
        code, meta = sydafa_get("/sources/%s" % src)
        check("GET /sources/%s" % src, code, 200)
        tables = meta.get("tables", []) if isinstance(meta, dict) else []
        table_names = sorted(t["name"] for t in tables if not t["name"].startswith(("_upd_", "_del_")))
        check("GET /sources/%s tables" % src, ",".join(table_names), "accounts,clients,deals,transactions")

    print("\n==> Шаг 2: Проверка безопасности sydafa (отклонение SQL-инъекций)...")
    inj_code, _ = sydafa_migrate("bank", [{
        "op": "update",
        "entity_fqn": "retail_bank.clients",
        "predicate": [{"column": "client_id; DROP TABLE retail_bank.clients --", "operator": "=", "value": 1}],
        "set": {"status": "VIP"},
    }])
    check("sydafa rejects injected column with 422", inj_code, 422)

    print("\n==> Шаг 3: Проверка готовности Airflow...")
    token = airflow_token_retry(timeout=180)
    check("Airflow login with stand credentials", "ok" if token else None, "ok")
    if not token:
        print("FATAL: Не удалось авторизоваться в Airflow")
        return

    for conn_id, conn_type in (("bank_source", "mssql"), ("broker_source", "oracle"),
                               ("sydafa_default", "http"), ("ingestion_meta", "postgres"),
                               ("spark_default", "spark"), ("trino_default", "trino")):
        code, raw = http(
            "GET", AIRFLOW + "/api/v2/connections/" + conn_id,
            headers={"Authorization": "Bearer " + token},
        )
        check("Connection %s visible in Airflow metadata DB" % conn_id, code, 200)
        if code == 200:
            connection = json.loads(raw)
            check("Connection %s type" % conn_id, connection.get("conn_type"), conn_type)
            if conn_id in ("bank_source", "broker_source"):
                check("Connection %s reader" % conn_id, connection.get("login"), "sydafa")
            elif conn_id == "sydafa_default":
                check("Connection sydafa_default host", connection.get("host"), "http://sydafa")
                check("Connection sydafa_default port", connection.get("port"), 8000)
            elif conn_id == "ingestion_meta":
                check("Connection ingestion_meta host", connection.get("host"), "pg-meta")
                check("Connection ingestion_meta port", connection.get("port"), 5432)
                check("Connection ingestion_meta database", connection.get("schema"), "meta")
                check("Connection ingestion_meta user", connection.get("login"), AIRFLOW_USER)
            elif conn_id == "spark_default":
                check("Connection spark_default host", connection.get("host"), "spark://spark-master")
                check("Connection spark_default port", connection.get("port"), 7077)
                extra = json.loads(connection.get("extra") or "{}")
                check("Connection spark_default binary", extra.get("spark_binary"), "spark-submit")
            elif conn_id == "trino_default":
                check("Connection trino_default host", connection.get("host"), "trino")
                check("Connection trino_default port", connection.get("port"), 8443)
                check("Connection trino_default schema", connection.get("schema"), "raw")
                check("Connection trino_default user", connection.get("login"), TRINO_USER)
                extra = json.loads(connection.get("extra") or "{}")
                check("Connection trino_default protocol", extra.get("protocol"), "https")
                check("Connection trino_default certificate verification", extra.get("verify"), "false")

    print("Ожидание регистрации DAG update_source_bank и update_source_broker в Airflow...")
    bank_dag_ok = airflow_wait_dag_registered(token, "update_source_bank", timeout=180)
    check("DAG update_source_bank registered in Airflow", bank_dag_ok, True)

    broker_dag_ok = airflow_wait_dag_registered(token, "update_source_broker", timeout=180)
    check("DAG update_source_broker registered in Airflow", broker_dag_ok, True)

    if not (bank_dag_ok and broker_dag_ok):
        print("FATAL: DAG-и не зарегистрированы в Airflow")
        return

    print("\n==> Шаг 4: Запуск и проверка Airflow DAG update_source_bank...")
    run_id_bank = "e2e-run-bank-" + str(int(time.time()))
    code, _ = airflow_trigger_retry(token, "update_source_bank", run_id_bank)
    check("trigger update_source_bank", code, 200)
    bank_success = airflow_wait_run(token, "update_source_bank", run_id_bank, timeout=360, name="update_source_bank")
    check("update_source_bank completed successfully", bank_success, True)

    print("\n==> Шаг 5: Запуск и проверка Airflow DAG update_source_broker...")
    run_id_broker = "e2e-run-broker-" + str(int(time.time()))
    code, _ = airflow_trigger_retry(token, "update_source_broker", run_id_broker)
    check("trigger update_source_broker", code, 200)
    broker_success = airflow_wait_run(token, "update_source_broker", run_id_broker, timeout=360, name="update_source_broker")
    check("update_source_broker completed successfully", broker_success, True)

    print("\n==> Шаг 5а: Снимок банка до первой raw-загрузки (снимок сам создаёт namespace)...")
    early_snap_dag = SNAPSHOT_DAGS[0]
    registered = airflow_wait_dag_registered(token, early_snap_dag, timeout=240)
    check("DAG %s registered before first ingest" % early_snap_dag, registered, True)
    if registered:
        early_run = "e2e-snap0-%s-%d" % (early_snap_dag, time.time())
        code, _ = airflow_trigger_retry(token, early_snap_dag, early_run)
        check("trigger %s (run 0)" % early_snap_dag, code, 200)
        early_ok = airflow_wait_run(token, early_snap_dag, early_run, timeout=900, name=early_snap_dag + " snap 0")
        if early_ok:
            try:
                table = "raw.bank.retail_bank__transactions__act_snap"
                snap = trino_query(
                    "SELECT count(*), count(DISTINCT __process_id) FROM %s" % table
                )[0]
                check(table + ": first snapshot publish on a fresh namespace", snap[0] > 0, True)
                check(table + ": single portion", snap[1], 1)
            except Exception as e:
                check("snapshot table query (run 0)", str(e), "ok")

    print("\n==> Шаг 6: Регистрация и запуск DAG-ов ингестии (raw-слой), первая загрузка...")
    for dag_id in INGEST_DAGS:
        registered = airflow_wait_dag_registered(token, dag_id, timeout=240)
        check("DAG %s registered in Airflow" % dag_id, registered, True)
        if not registered:
            print("FATAL: DAG %s не зарегистрирован (проверьте импорты плагина ingestion)" % dag_id)
            return

    stamp = str(int(time.time()))
    auth_headers = {"Authorization": "Bearer " + token}
    code, raw = http("GET", AIRFLOW + "/api/v2/pools/ingest_loads", None, auth_headers)
    check("ingest_loads pool exists", code, 200)
    if code == 200:
        check("ingest_loads capacity", json.loads(raw).get("slots"), 4)
    for dag_id in INGEST_DAGS:
        code, raw = http("GET", AIRFLOW + f"/api/v2/dags/{dag_id}/tasks", None, auth_headers)
        check(f"{dag_id} tasks readable", code, 200)
        if code == 200:
            loads = [task for task in json.loads(raw).get("tasks", []) if task["task_id"].endswith(".load")]
            check(f"{dag_id} has two loads", len(loads), 2)
            for task in loads:
                check(task["task_id"] + " pool", task.get("pool"), "ingest_loads")
                check(task["task_id"] + " slots", task.get("pool_slots"), 2 if "transactions" in task["task_id"] else 1)
    for dag_id in INGEST_DAGS:
        run_id = "e2e-ingest1-%s-%s" % (dag_id, stamp)
        code, _ = airflow_trigger_retry(token, dag_id, run_id)
        check("trigger %s (run 1)" % dag_id, code, 200)
    run1_ok = True
    for dag_id in INGEST_DAGS:
        run_id = "e2e-ingest1-%s-%s" % (dag_id, stamp)
        run1_ok = airflow_wait_run(token, dag_id, run_id, timeout=900, name=dag_id + " run 1") and run1_ok
    if not run1_ok:
        return

    print("\n==> Шаг 7: Проверка raw-таблиц в Trino (первая загрузка)...")
    first_counts: dict[str, int] = {}
    try:
        max_pids = {}
        for table in RAW_SNAPSHOT_TABLES + RAW_INCREMENTAL_TABLES:
            total, distinct_pids, max_pid = raw_stats(table)
            first_counts[table] = total
            check("%s: rows loaded" % table, total > 0, True)
            check("%s: single process_id (one portion)" % table, distinct_pids, 1)
            max_pids[table] = max_pid
            portion_rows = trino_query(
                "SELECT count_if(__process_id IS NULL), count(*) FROM %s WHERE __process_id = %d"
                % (table, max_pid)
            )
            check(table + ": every row carries the run process_id",
                  (portion_rows[0][0], portion_rows[0][1]) == (0, total), True)
        for source in ("bank", "broker"):
            prefix = "retail_bank" if source == "bank" else "trading_core"
            for pipeline, tables in (("main", ("deals", "transactions")), ("refs", ("clients", "accounts"))):
                ids = {max_pids[f"raw.{source}.{prefix}__{table}"] for table in tables}
                check(f"{source}/{pipeline} shares process_id", len(ids), 1)
        check("each DAG run has its own process_id", len(set(max_pids.values())), 4)
        rows = trino_query(
            "SELECT count(*) FROM raw.bank.retail_bank__clients "
            "WHERE __load_dttm = (SELECT max(__load_dttm) FROM raw.bank.retail_bank__clients)"
        )
        check("bank clients latest snapshot equals total (run 1)", int(rows[0][0]), first_counts["raw.bank.retail_bank__clients"])
    except Exception as e:
        check("raw tables query", str(e), "ok")

    print("\n==> Шаг 8: Повторная загрузка обоих источников без изменений...")
    for dag_id in INGEST_DAGS:
        run_id = "e2e-ingest2-" + dag_id + stamp
        code, _ = airflow_trigger_retry(token, dag_id, run_id)
        check("trigger " + dag_id + " (run 2)", code, 200)
        airflow_wait_run(token, dag_id, run_id, timeout=900, name=dag_id + " run 2")
    try:
        for table in RAW_SNAPSHOT_TABLES:
            total, distinct_pids, _ = raw_stats(table)
            check("%s: full-snapshot table doubles on re-run" % table, total, 2 * first_counts[table])
            check("%s: two portions after re-run" % table, distinct_pids, 2)
        for table in RAW_INCREMENTAL_TABLES:
            total, distinct_pids, _ = raw_stats(table)
            check(table + ": no delta", total, first_counts[table])
            check(table + ": still one portion", distinct_pids, 1)
        rows = trino_query(
            "SELECT count(*) FROM raw.bank.retail_bank__clients "
            "WHERE __load_dttm = (SELECT max(__load_dttm) FROM raw.bank.retail_bank__clients)"
        )
        check("bank clients latest snapshot equals first load count", int(rows[0][0]), first_counts["raw.bank.retail_bank__clients"])
    except Exception as e:
        check("raw tables query (run 2)", str(e), "ok")

    print("\n==> Шаг 9: Миграция источника + третья загрузка (подхват дельты _batch_id)...")
    run_id_mig2 = "e2e-run-bank2-" + stamp
    code, _ = airflow_trigger_retry(token, "update_source_bank", run_id_mig2)
    check("trigger update_source_bank (migration 2)", code, 200)
    airflow_wait_run(token, "update_source_bank", run_id_mig2, timeout=360, name="update_source_bank migration 2")

    run_id_mig2 = "e2e-run-broker2-" + stamp
    code, _ = airflow_trigger_retry(token, "update_source_broker", run_id_mig2)
    check("trigger update_source_broker (migration 2)", code, 200)
    airflow_wait_run(token, "update_source_broker", run_id_mig2, timeout=360, name="broker migration 2")
    for dag_id in INGEST_DAGS:
        run_id = "e2e-ingest3-" + dag_id + stamp
        code, _ = airflow_trigger_retry(token, dag_id, run_id)
        check("trigger " + dag_id + " (run 3)", code, 200)
        airflow_wait_run(token, dag_id, run_id, timeout=900, name=dag_id + " run 3")
    try:
        for table in RAW_SNAPSHOT_TABLES:
            total, distinct_pids, max_pid = raw_stats(table)
            check(table + ": three portions", distinct_pids, 3)
            newest = raw_rows_with_process(table, max_pid)
            oldest = raw_rows_with_process(table, max_pids[table])
            check(table + ": source grew", newest > oldest, True)

        for table in RAW_INCREMENTAL_TABLES:
            total, distinct_pids, max_pid = raw_stats(table)
            check(table + ": initial + delta", distinct_pids, 2)
            check(table + ": history intact", raw_rows_with_process(table, max_pids[table]), first_counts[table])
            delta = raw_rows_with_process(table, max_pid)
            check(table + ": nonempty delta", delta > 0, True)
            check(table + ": history + delta count", total, first_counts[table] + delta)
    except Exception as e:
        check("raw tables query (run 3)", str(e), "ok")

    print("\n==> Шаг 10: DAG-и снимков (таблицы-снимки __act_snap)...")
    for dag_id in SNAPSHOT_DAGS:
        registered = airflow_wait_dag_registered(token, dag_id, timeout=240)
        check("DAG %s registered in Airflow" % dag_id, registered, True)
        if not registered:
            print("FATAL: снимочный DAG %s не зарегистрирован" % dag_id)
            return
    for dag_id in SNAPSHOT_DAGS:
        run_id = "e2e-snap1-%s-%s" % (dag_id, stamp)
        code, _ = airflow_trigger_retry(token, dag_id, run_id)
        check("trigger %s (run 1)" % dag_id, code, 200)
        airflow_wait_run(token, dag_id, run_id, timeout=900, name=dag_id + " snap 1")
    snapshot_counts: dict[str, int] = {}
    try:
        unpublished = ("bank", "e2e_never_published__act_snap")
        expected_tables = [tuple(table.split(".")[1:]) for table in ACT_SNAPSHOT_TABLES]
        inventory_values = ", ".join(
            "('%s', '%s')" % (namespace, name)
            for namespace, name in [*expected_tables, unpublished])
        inventory = trino_query(
            "WITH expected(table_schema, table_name) AS (VALUES " + inventory_values + ") "
            "SELECT e.table_schema, e.table_name, EXISTS ("
            "SELECT 1 FROM raw.information_schema.tables t "
            "WHERE t.table_schema = e.table_schema AND t.table_name = e.table_name) "
            "FROM expected e")
        check("freshness inventory includes every expected table", len(inventory), len(expected_tables) + 1)
        check("freshness inventory finds all configured snapshots",
              {(row[0], row[1]) for row in inventory if row[2]}, set(expected_tables))
        check("freshness inventory distinguishes never published",
              [(row[0], row[1]) for row in inventory if not row[2]], [unpublished])
        for table, raw_table in ACT_SNAPSHOT_TABLES.items():
            namespace, name = table.split(".")[1:]
            expected = f"s3://datalake/raw/{namespace}/{name}/"
            paths = trino_query(f'SELECT DISTINCT "$path" FROM {table}')
            check(f"{table}: exact storage prefix", bool(paths) and all(row[0].startswith(expected) for row in paths), True)
            snap = trino_query(
                "SELECT count(*), count(DISTINCT __process_id), count(DISTINCT __load_dttm), "
                "max(__process_id) FROM %s" % table
            )[0]
            check(f"{table}: rows published", snap[0] > 0, True)
            check(f"{table}: single snapshot portion", snap[1], 1)
            check(f"{table}: single load timestamp (freshness marker)", snap[2], 1)
            check(f"{table}: process id recorded", snap[3] > 0, True)
            raw_total = trino_query("SELECT count(*) FROM %s" % raw_table)[0][0]
            matched = trino_query(
                "SELECT count(*) FROM %s s JOIN %s r ON s.transaction_id = r.transaction_id"
                % (table, raw_table)
            )[0][0]
            check(f"{table}: every snapshot row is known to raw history (by id)", matched, snap[0])
            check(f"{table}: snapshot never exceeds raw history (source deletions allowed)",
                  snap[0] <= raw_total, True)
            snapshot_counts[table] = snap[0]
            freshness = trino_query(
                'SELECT max(committed_at) FROM "%s"."%s"."%s$snapshots"' % ("raw", namespace, name)
            )
            check(f"{table}: Iceberg snapshot history exists", bool(freshness[0][0]), True)
            author = trino_query(
                "SELECT value FROM \"%s\".\"%s\".\"%s$properties\" WHERE key = 'snapshot.process_id'"
                % ("raw", namespace, name)
            )
            check(f"{table}: author property matches the published process id", int(author[0][0]), snap[3])
    except Exception as e:
        check("snapshot tables query (run 1)", str(e), "ok")

    print("\n==> Шаг 11: Повторный снимок — атомарная перезапись, а не накопление...")
    dag_id = SNAPSHOT_DAGS[0]
    table = "raw.bank.retail_bank__transactions__act_snap"
    run_id = "e2e-snap2-%s-%s" % (dag_id, stamp)
    code, _ = airflow_trigger_retry(token, dag_id, run_id)
    check("trigger %s (run 2)" % dag_id, code, 200)
    airflow_wait_run(token, dag_id, run_id, timeout=900, name=dag_id + " snap 2")
    try:
        snap = trino_query(
            "SELECT count(*), count(DISTINCT __process_id) FROM %s" % table
        )[0]
        check(table + ": replaced, not appended (one portion)", snap[1], 1)
        if table in snapshot_counts:
            check(table + ": row count stable across overwrite", snap[0], snapshot_counts[table])
    except Exception as e:
        check("snapshot table query (run 2)", str(e), "ok")

    print("\n==> Шаг 12: Совместный запуск инкремента и снимка...")
    concurrent = []
    for dag in (INGEST_DAGS[0], SNAPSHOT_DAGS[0]):
        run = "e2e-concurrent-" + dag + stamp
        code, _ = airflow_trigger_retry(token, dag, run)
        check("trigger concurrent " + dag, code, 200)
        concurrent.append((dag, run))
    for dag, run in concurrent:
        airflow_wait_run(token, dag, run, timeout=900, name=dag + " concurrent")

    print("\n==> Шаг 13: Полный и частичный Clear старого DagRun...")
    dag = INGEST_DAGS[0]
    run = "e2e-clear-" + stamp
    code, _ = airflow_trigger_retry(token, dag, run)
    check("trigger Clear fixture", code, 200)
    if airflow_wait_run(token, dag, run, timeout=900):
        url = f"{AIRFLOW}/api/v2/dags/{dag}/dagRuns/{run}/taskInstances?limit=100"
        code, raw = http("GET", url, headers={"Authorization": "Bearer " + token})
        if not check("Clear task inventory", code, 200):
            return
        inventory = json.loads(raw)
        tasks = [task["task_id"] for task in inventory["task_instances"]]
        if not check("Clear task inventory complete", len(tasks), inventory["total_entries"]):
            return
        for phase in ("full", "partial"):
            selected = tasks if phase == "full" else [
                task for task in tasks if task == "generate_process_id" or "transactions" in task]
            check(phase + " Clear includes generator", "generate_process_id" in selected, True)
            check(phase + " Clear includes load", any(task.endswith(".load") for task in selected), True)
            code, raw = http(
                "POST", f"{AIRFLOW}/api/v2/dags/{dag}/clearTaskInstances",
                {"dag_run_id": run, "task_ids": selected, "dry_run": False,
                 "only_failed": False, "only_running": False, "reset_dag_runs": True},
                {"Authorization": "Bearer " + token})
            if check(phase + " Clear accepted", code, 200):
                airflow_wait_run(token, dag, run, timeout=900, name=phase + " Clear rerun")

    run_source_schema_evolution(token, stamp)


if __name__ == "__main__":
    if "--validate-manifests-only" in sys.argv:
        try:
            validate_manifests_and_dags()
        except Exception as e:
            print("FATAL: %s: %s" % (type(e).__name__, e))
            FAIL += 1
        print("---")
        print("PASS=%d FAIL=%d" % (PASS, FAIL))
        sys.exit(0 if FAIL == 0 else 1)

    try:
        main()
    except Exception as e:
        print("FATAL: %s: %s" % (type(e).__name__, e))
        FAIL += 1
    print("---")
    print("PASS=%d FAIL=%d" % (PASS, FAIL))
    sys.exit(0 if FAIL == 0 else 1)
