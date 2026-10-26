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
"""
from __future__ import annotations

import ast
import base64
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


def sydafa_post(path: str, body: Any) -> tuple[int, Any]:
    """Отправляет POST-запрос в API sydafa."""
    code, raw = http("POST", SYDAFA + path, body, {"Content-Type": "application/json"})
    try:
        parsed = json.loads(raw) if raw else None
    except Exception:
        parsed = raw
    return code, parsed


def sydafa_get(path: str) -> tuple[int, Any]:
    """Отправляет GET-запрос в API sydafa."""
    code, raw = http("GET", SYDAFA + path, None, {"Content-Type": "application/json"})
    try:
        parsed = json.loads(raw) if raw else None
    except Exception:
        parsed = raw
    return code, parsed


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


def airflow_trigger(token: str, dag_id: str, run_id: str) -> tuple[int, str]:
    url = "%s/api/v2/dags/%s/dagRuns" % (AIRFLOW, dag_id)
    return http(
        "POST",
        url,
        {"dag_run_id": run_id, "logical_date": None},
        {"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )


def airflow_trigger_retry(token: str, dag_id: str, run_id: str, timeout: int = 180) -> tuple[int, str]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        airflow_unpause(token, dag_id)
        code, raw = airflow_trigger(token, dag_id, run_id)
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
    rows = trino_query(
        "SELECT count(*), count(DISTINCT __process_id), max(__process_id) FROM %s" % table
    )
    return int(rows[0][0]), int(rows[0][1]), int(rows[0][2])


def raw_rows_with_process(table: str, process_id: int) -> int:
    """Число строк, загруженных конкретным запуском."""
    rows = trino_query("SELECT count(*) FROM %s WHERE __process_id = %d" % (table, process_id))
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
    check("bank migration has 4 appends (dims 100, 100, 1000 + fact 25k)", (
        len(bank_appends) == 4
        and bank_appends[0].get("entity_fqn") == "retail_bank.clients" and bank_appends[0].get("rows") == 100
        and bank_appends[1].get("entity_fqn") == "retail_bank.accounts" and bank_appends[1].get("rows") == 100
        and bank_appends[2].get("entity_fqn") == "retail_bank.deals" and bank_appends[2].get("rows") == 1000
        and bank_appends[3].get("entity_fqn") == "retail_bank.transactions"
        and bank_appends[3].get("rows") == 25000
        and "new_key_ratio" not in bank_appends[3]
    ), True)
    check("bank migration has 1 cascade delete of 50 deals", (
        len(bank_deletes) == 1
        and bank_deletes[0].get("entity_fqn") == "retail_bank.deals"
        and bank_deletes[0].get("limit") == 50
        and bank_deletes[0].get("cascade") is True
    ), True)
    check("bank migration has 3 updates with predicates (limits 10, 10, 50)", (
        len(bank_updates) == 3
        and bank_updates[0].get("entity_fqn") == "retail_bank.clients"
        and bank_updates[0].get("limit") == 10
        and bank_updates[0].get("predicate") == [{"column": "status", "operator": "!=", "value": "VIP"}]
        and bank_updates[1].get("entity_fqn") == "retail_bank.accounts"
        and bank_updates[1].get("limit") == 10
        and bank_updates[1].get("predicate") == [{"column": "status", "operator": "!=", "value": "Frozen"}]
        and bank_updates[2].get("entity_fqn") == "retail_bank.deals"
        and bank_updates[2].get("limit") == 50
        and bank_updates[2].get("predicate") == [{"column": "status", "operator": "!=", "value": "Closed"}]
    ), True)

    with open(broker_mig, "r", encoding="utf-8") as f:
        broker_ops = json.load(f).get("operations", [])
    b_appends = [op for op in broker_ops if op.get("op") == "append"]
    b_updates = [op for op in broker_ops if op.get("op") == "update"]
    b_deletes = [op for op in broker_ops if op.get("op") == "delete"]
    check("broker migration has 4 appends (dims 100, 100, 1000 + fact 25k)", (
        len(b_appends) == 4
        and b_appends[0].get("entity_fqn") == "trading_core.clients" and b_appends[0].get("rows") == 100
        and b_appends[1].get("entity_fqn") == "trading_core.accounts" and b_appends[1].get("rows") == 100
        and b_appends[2].get("entity_fqn") == "trading_core.deals" and b_appends[2].get("rows") == 1000
        and b_appends[3].get("entity_fqn") == "trading_core.transactions"
        and b_appends[3].get("rows") == 25000
        and "new_key_ratio" not in b_appends[3]
    ), True)
    check("broker migration has 1 cascade delete of 50 deals", (
        len(b_deletes) == 1
        and b_deletes[0].get("entity_fqn") == "trading_core.deals"
        and b_deletes[0].get("limit") == 50
        and b_deletes[0].get("cascade") is True
    ), True)
    check("broker migration has 3 updates with predicates (limits 10, 10, 50)", (
        len(b_updates) == 3
        and b_updates[0].get("entity_fqn") == "trading_core.clients"
        and b_updates[0].get("limit") == 10
        and b_updates[0].get("predicate") == [{"column": "risk_profile", "operator": "!=", "value": "Aggressive"}]
        and b_updates[1].get("entity_fqn") == "trading_core.accounts"
        and b_updates[1].get("limit") == 10
        and b_updates[1].get("predicate") == [{"column": "status", "operator": "!=", "value": "Closed"}]
        and b_updates[2].get("entity_fqn") == "trading_core.deals"
        and b_updates[2].get("limit") == 50
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
    from ingestion.generate_dags import generate_all
    from ingestion.lib.manifest import load_sources

    generated = generate_all(
        load_sources(ws / "ingestion" / "sources.yaml"),
        ws / "ingestion" / "pipelines", ws / "ingestion" / "settings.yaml",
    )
    check("generated DAG set", sorted(generated), sorted(name + ".py" for name in INGEST_DAGS))
    for name, content in generated.items():
        ast.parse(content, filename=name)
        check("generated DAG %s syntax valid" % name, True, True)


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
