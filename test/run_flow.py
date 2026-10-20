#!/usr/bin/env python3
"""run_flow.py: Сквозной (e2e) тест стенда datalake_stand.

Проверяет актуальное состояние репозитория:
1. Readiness sydafa API и регистрацию источников (bank, broker).
2. Защиту API sydafa от SQL-инъекций (HTTP 422).
3. Авторизацию и доступность DAG-ов в Airflow 3.x.
4. Исполнение и успешное завершение (state=success) DAG-ов обновления источников:
   - update_source_bank (All-or-Nothing миграция MSSQL)
   - update_source_broker (All-or-Nothing миграция Oracle)
"""
from __future__ import annotations

import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

SYDAFA = os.environ.get("SYDAFA_URL", "http://sydafa:8000")
AIRFLOW = os.environ.get("AIRFLOW_URL", "http://airflow-webserver:8080")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")

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
    except Exception as e:  # noqa: BLE001
        return 0, str(e)


def wait_http(url: str, want: int, tries: int = 40, interval: int = 3, body: Any = None, headers: dict[str, str] | None = None) -> tuple[int, str]:
    for _ in range(tries):
        code, raw = http("GET", url, body, headers, timeout=20)
        if code == want:
            return code, raw
        time.sleep(interval)
    return code, raw


# ---------- sydafa API ----------

def sydafa_post(path: str, body: Any) -> tuple[int, Any]:
    code, raw = http("POST", SYDAFA + path, body, {"Content-Type": "application/json"})
    try:
        parsed = json.loads(raw) if raw else None
    except Exception:  # noqa: BLE001
        parsed = raw
    return code, parsed


def sydafa_get(path: str) -> tuple[int, Any]:
    code, raw = http("GET", SYDAFA + path, None, {"Content-Type": "application/json"})
    try:
        parsed = json.loads(raw) if raw else None
    except Exception:  # noqa: BLE001
        parsed = raw
    return code, parsed


def sydafa_migrate(source: str, operations: list[dict[str, Any]]) -> tuple[int, Any]:
    return sydafa_post("/sources/%s/migrate" % source, {"operations": operations})


# ---------- Airflow API ----------

def airflow_token() -> str:
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
        except Exception:  # noqa: BLE001
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


def main() -> None:
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
    except Exception as e:  # noqa: BLE001
        check("sydafa /sources parse", str(e), "ok")

    # Интроспекция схем источников bank и broker
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
    check("Airflow login admin/admin", "ok" if token else None, "ok")
    if not token:
        print("FATAL: Не удалось авторизоваться в Airflow")
        return

    # Ожидание обнаружения DAG-ов планировщиком Airflow
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


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print("FATAL: %s: %s" % (type(e).__name__, e))
        FAIL += 1
    print("---")
    print("PASS=%d FAIL=%d" % (PASS, FAIL))
    sys.exit(0 if FAIL == 0 else 1)
