import base64
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

SYDAFA = os.environ.get("SYDAFA_URL", "http://sydafa:8000")
TRINO = "https://trino:8443"
AIRFLOW = "http://airflow-webserver:8080"
NESSIE = "http://nessie:19120"
SPARK = "http://spark-master:8080"

TRINO_AUTH = "Basic " + base64.b64encode(b"admin:admin").decode()
AIRFLOW_USER = os.environ.get("AIRFLOW_UI_USER", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_UI_PASSWORD", "admin")

_CTX = ssl.create_default_context()
_CTX.check_hostname = False
_CTX.verify_mode = ssl.CERT_NONE

PASS = 0
FAIL = 0


def check(name, actual, expected):
    global PASS, FAIL
    ok = str(actual) == str(expected)
    if ok:
        print("PASS  " + name)
        PASS += 1
    else:
        print("FAIL  %s  (got=%r want=%r)" % (name, actual, expected))
        FAIL += 1


def http(method, url, body=None, headers=None, timeout=120):
    data = None
    if body is not None:
        if isinstance(body, bytes):
            data = body
        elif isinstance(body, str):
            data = body.encode()
        else:
            data = json.dumps(body).encode()
    h = dict(headers or {})
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_CTX) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:  # noqa: BLE001
        return 0, str(e)


def wait_http(name, url, want, tries, interval, body=None, headers=None):
    for _ in range(tries):
        code, raw = http("GET", url, body, headers, timeout=30)
        if code == want:
            return code, raw
        time.sleep(interval)
    return code, raw


def wait_until(fn, want, timeout, interval):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = fn()
        except Exception:  # noqa: BLE001
            last = None
        if str(last) == str(want):
            return True, last
        time.sleep(interval)
    return False, last


def spark_alive():
    code, raw = http("GET", SPARK + "/json/", None, None, timeout=10)
    if code != 200:
        return None
    try:
        return int(json.loads(raw).get("aliveworkers"))
    except Exception:  # noqa: BLE001
        return None


# ---------- sydafa API ----------

def sydafa_post(path, body):
    code, raw = http("POST", SYDAFA + path, body, {"Content-Type": "application/json"})
    try:
        parsed = json.loads(raw) if raw else None
    except Exception:  # noqa: BLE001
        parsed = raw
    return code, parsed


def sydafa_get(path):
    code, raw = http("GET", SYDAFA + path, None, {"Content-Type": "application/json"})
    try:
        parsed = json.loads(raw) if raw else None
    except Exception:  # noqa: BLE001
        parsed = raw
    return code, parsed


def sydafa_exec(source, operations):
    code, parsed = sydafa_post("/sources/%s/exec" % source, {"operations": operations})
    return code, parsed


# ---------- Trino ----------

def trino_query(sql):
    headers = {
        "Authorization": TRINO_AUTH,
        "X-Trino-Catalog": "iceberg",
        "X-Trino-Schema": "information_schema",
        "Content-Type": "text/plain",
    }
    code, raw = http("POST", TRINO + "/v1/statement", sql, headers)
    if code != 200:
        raise RuntimeError("Trino POST %s: %s" % (code, raw[:200]))
    state = json.loads(raw)
    rows = []
    while True:
        if state.get("error") or state.get("stats", {}).get("state") == "FAILED":
            raise RuntimeError("Trino error: %s" % (json.dumps(state)[:300]))
        if "data" in state:
            rows.extend(state["data"])
        if not state.get("nextUri"):
            break
        _, raw2 = http("GET", state["nextUri"], None, {"Authorization": TRINO_AUTH})
        state = json.loads(raw2)
    return rows


def trino_count(sql):
    rows = trino_query(sql)
    if rows and rows[0]:
        return int(rows[0][0])
    return None


def trino_count_retry(sql, timeout=240):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            c = trino_count(sql)
            if c is not None:
                return c
        except Exception:  # noqa: BLE001
            pass
        time.sleep(5)
    return None


def trino_count_positive(sql, timeout=240):
    c = trino_count_retry(sql, timeout)
    return c is not None and c > 0


# ---------- Airflow ----------

def airflow_token():
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


def airflow_token_retry(timeout=240):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            return airflow_token()
        except Exception:  # noqa: BLE001
            time.sleep(5)
    return None


def airflow_unpause(token, dag_id):
    url = "%s/api/v2/dags/%s" % (AIRFLOW, dag_id)
    return http(
        "PATCH",
        url,
        {"is_paused": False},
        {"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )


def airflow_trigger(token, dag_id, run_id):
    url = "%s/api/v2/dags/%s/dagRuns" % (AIRFLOW, dag_id)
    return http(
        "POST",
        url,
        {"dag_run_id": run_id, "logical_date": None},
        {"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )


def airflow_trigger_retry(token, dag_id, run_id, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        # Убеждаемся, что DAG активен перед триггером
        airflow_unpause(token, dag_id)
        code, raw = airflow_trigger(token, dag_id, run_id)
        if code == 200:
            return code, raw
        time.sleep(5)
    return code, raw


def airflow_wait_run(token, dag_id, run_id, timeout=600, name=None):
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
                return False
        time.sleep(5)
    check((name or dag_id) + " -> success", "timeout:" + str(last), "success")
    return False


def main():
    # 1. Readiness инфраструктурных компонентов
    code, _ = wait_http("Nessie /iceberg/v1/config", NESSIE + "/iceberg/v1/config", 200, 40, 3)
    check("Nessie /iceberg/v1/config", code, 200)

    code, _ = wait_http("sydafa /health", SYDAFA + "/health", 200, 60, 3)
    check("sydafa /health", code, 200)

    code, raw = wait_http("sydafa /sources", SYDAFA + "/sources", 200, 40, 3)
    check("sydafa /sources", code, 200)
    try:
        sources_list = json.loads(raw).get("sources", [])
        check("sydafa /sources value", ",".join(sources_list), "mssql,oracle")
    except Exception:  # noqa: BLE001
        check("sydafa /sources value", "parse-error", "mssql,oracle")

    _, alive = wait_until(spark_alive, 1, 180, 5)
    check("Spark alive workers", alive, 1)

    _, trino_one = wait_until(lambda: trino_count("SELECT 1"), 1, 180, 5)
    check("Trino SELECT 1", trino_one, 1)

    token = airflow_token_retry()
    check("Airflow login admin/admin", "ok" if token else None, "ok")
    if token is None:
        print("---")
        print("PASS=%d FAIL=%d" % (PASS, FAIL))
        return

    sources = ["mssql", "oracle"]

    # 2. Интроспекция через API GET /sources/{source}
    for src in sources:
        code, meta = sydafa_get("/sources/%s" % src)
        check("GET /sources/%s" % src, code, 200)
        tables = meta.get("tables", []) if isinstance(meta, dict) else []
        table_names = sorted(t["name"] for t in tables)
        check("GET /sources/%s tables" % src, ",".join(table_names), "customers,orders,products")

    # 3. Очистка таблиц через единый POST /sources/{source}/exec (truncate в топологическом порядке)
    for src in sources:
        trunc_ops = [
            {"op": "truncate", "schema": "demo", "table": "orders", "cascade": True},
            {"op": "truncate", "schema": "demo", "table": "products", "cascade": True},
            {"op": "truncate", "schema": "demo", "table": "customers", "cascade": True},
        ]
        code, parsed = sydafa_exec(src, trunc_ops)
        check("exec truncate all tables for %s" % src, code, 200)
        check("exec %s truncate executed 3 ops" % src, parsed.get("executed") if isinstance(parsed, dict) else None, 3)

    # 4. Наполнение тестовыми данными через единый POST /sources/{source}/exec (append)
    counts = {
        "mssql": {"customers": 3, "products": 3, "orders": 5},
        "oracle": {"customers": 3, "products": 3, "orders": 4},
    }
    for src in sources:
        append_ops = [
            {"op": "append", "schema": "demo", "table": "customers", "rows": counts[src]["customers"], "new_key_ratio": 0.0},
            {"op": "append", "schema": "demo", "table": "products", "rows": counts[src]["products"], "new_key_ratio": 0.0},
            {"op": "append", "schema": "demo", "table": "orders", "rows": counts[src]["orders"], "new_key_ratio": 0.0},
        ]
        code, parsed = sydafa_exec(src, append_ops)
        check("exec append all tables for %s" % src, code, 200)
        check("exec %s append executed 3 ops" % src, parsed.get("executed") if isinstance(parsed, dict) else None, 3)

    # 5. ODS DAGs: unpause -> trigger -> wait -> Iceberg asserts
    for src in sources:
        dag_id = "ods_load_%s_demo" % src
        run_id = "full-flow-ods-" + src
        code, _ = airflow_trigger_retry(token, dag_id, run_id)
        check("trigger %s" % dag_id, code, 200)
        if not airflow_wait_run(token, dag_id, run_id, name=dag_id):
            return

    for src in sources:
        for table, exp in counts[src].items():
            name = "iceberg.ods.%s_%s count=%d" % (src, table, exp)
            got = trino_count_retry("SELECT count(*) FROM iceberg.ods.%s_%s" % (src, table))
            check(name, got, exp)

    # 6. Mart DAG: unpause -> trigger -> wait -> asserts
    mart_dag_id = "build_marts_demo"
    code, _ = airflow_trigger_retry(token, mart_dag_id, "full-flow-marts")
    check("trigger %s" % mart_dag_id, code, 200)
    airflow_wait_run(token, mart_dag_id, "full-flow-marts", name=mart_dag_id)

    check(
        "mart_trino.customer_totals non-empty",
        trino_count_positive("SELECT count(*) FROM iceberg.mart_trino.customer_totals"),
        True,
    )
    check(
        "mart_spark.customer_totals non-empty",
        trino_count_positive("SELECT count(*) FROM iceberg.mart_spark.customer_totals"),
        True,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print("FATAL: %s: %s" % (type(e).__name__, e))
        FAIL += 1
    print("---")
    print("PASS=%d FAIL=%d" % (PASS, FAIL))
    sys.exit(0 if FAIL == 0 else 1)
