import base64
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

DATAGEN = "http://datagen-api:8000"
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


# ---------- datagen-api ----------

def datagen_post(path, body):
    code, raw = http("POST", DATAGEN + path, body, {"Content-Type": "application/json"})
    try:
        parsed = json.loads(raw) if raw else None
    except Exception:  # noqa: BLE001
        parsed = raw
    return code, parsed


def append(source, table, rows):
    code, parsed = datagen_post(
        "/tables/%s/demo/%s/append" % (source, table),
        {"rows": rows, "new_key_ratio": 0},
    )
    inserted = parsed.get("inserted") if isinstance(parsed, dict) else None
    check("append %s.%s x%d" % (source, table, rows), code, 200)
    check("append %s.%s inserted" % (source, table), inserted, rows)


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
    # 1. readiness (poll until each service is actually ready)
    code, _ = wait_http("Nessie /iceberg/v1/config", NESSIE + "/iceberg/v1/config", 200, 40, 3)
    check("Nessie /iceberg/v1/config", code, 200)

    code, raw = wait_http("datagen-api /sources", DATAGEN + "/sources", 200, 40, 3)
    check("datagen-api /sources", code, 200)
    try:
        sources = json.loads(raw).get("sources", [])
        check("datagen-api /sources value", ",".join(sources), "mssql,oracle")
    except Exception:  # noqa: BLE001
        check("datagen-api /sources value", "parse-error", "mssql,oracle")

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
    counts = {"mssql": {"customers": 3, "products": 3, "orders": 5},
              "oracle": {"customers": 3, "products": 3, "orders": 4}}

    # 2. fill all tables (parents first, then orders)
    for src in sources:
        append(src, "customers", counts[src]["customers"])
        append(src, "products", counts[src]["products"])
    for src in sources:
        append(src, "orders", counts[src]["orders"])

    # 3. update a subset (deterministic sentinel in name)
    for src in sources:
        code, _ = datagen_post(
            "/tables/%s/demo/customers/update" % src,
            {"predicate": [{"column": "customer_id", "operator": "=", "value": 1}],
             "set": {"name": src + "-updated"}},
        )
        check("update %s.customers customer_id=1" % src, code, 200)

    # 4. delete a subset of orders (mssql: order_id < 3 -> 1,2; oracle: order_id = 1)
    code, _ = datagen_post(
        "/tables/mssql/demo/orders/delete",
        {"predicate": [{"column": "order_id", "operator": "<", "value": 3}], "cascade": False},
    )
    check("delete mssql.orders order_id<3", code, 200)
    code, _ = datagen_post(
        "/tables/oracle/demo/orders/delete",
        {"predicate": [{"column": "order_id", "operator": "=", "value": 1}], "cascade": False},
    )
    check("delete oracle.orders order_id=1", code, 200)

    counts["mssql"]["orders"] = 5 - 2
    counts["oracle"]["orders"] = 4 - 1

    # 5. corner cases (non-destructive rejections)
    code, _ = datagen_post(
        "/tables/mssql/demo/customers/delete",
        {"predicate": [{"column": "customer_id", "operator": ">=", "value": 1}], "cascade": False},
    )
    check("corner delete FK-parent without cascade -> 409", code, 409)

    code, _ = datagen_post(
        "/tables/mssql/demo/customers/evolve",
        {"ddl_operation": "drop_column", "column_name": "customer_id", "apply_to": "mssql"},
    )
    check("corner drop_column PK -> 422", code, 422)

    code, _ = datagen_post(
        "/tables/mssql/demo/orders/evolve",
        {"ddl_operation": "drop_column", "column_name": "customer_id", "apply_to": "mssql"},
    )
    check("corner drop_column FK -> 422", code, 422)

    code, _ = datagen_post(
        "/tables/mssql/demo/customers/update",
        {"predicate": [{"column": "nonexistent", "operator": "=", "value": 1}],
         "set": {"segment": "VIP"}},
    )
    check("corner unknown column -> 422", code, 422)

    code, _ = datagen_post(
        "/tables/mssql/demo/customers/update",
        {"predicate": [{"column": "customer_id", "operator": "LIKE", "value": "1"}],
         "set": {"segment": "VIP"}},
    )
    check("corner bad operator -> 422", code, 422)

    code, _ = datagen_post(
        "/tables/mssql/demo/customers/update",
        {"predicate": [{"column": "customer_id; DROP TABLE demo.customers --", "operator": "=", "value": 1}],
         "set": {"segment": "VIP"}},
    )
    check("corner injected column -> 422", code, 422)

    code, _ = datagen_post(
        "/tables/mssql/demo/customers/append",
        {"rows": 0, "new_key_ratio": 0},
    )
    check("corner append rows=0 -> 422", code, 422)

    # 6. ODS DAGs + Iceberg asserts
    for src in sources:
        run_id = "full-flow-ods-" + src
        code, raw = airflow_trigger_retry(token, "ods_load_" + src, run_id)
        check("trigger ods_load_%s" % src, code, 200)
        if not airflow_wait_run(token, "ods_load_" + src, run_id, name="ods_load_" + src):
            return

    for src in sources:
        for table, exp in counts[src].items():
            name = "iceberg.ods.%s_%s count=%d" % (src, table, exp)
            got = trino_count_retry("SELECT count(*) FROM iceberg.ods.%s_%s" % (src, table))
            check(name, got, exp)

    check("mssql updated name reflected",
          trino_count_retry("SELECT count(*) FROM iceberg.ods.mssql_customers WHERE name = 'mssql-updated'"), 1)
    check("oracle updated name reflected",
          trino_count_retry("SELECT count(*) FROM iceberg.ods.oracle_customers WHERE name = 'oracle-updated'"), 1)

    # 7. mart DAG + asserts
    code, raw = airflow_trigger_retry(token, "build_marts_demo", "full-flow-marts")
    check("trigger build_marts_demo", code, 200)
    airflow_wait_run(token, "build_marts_demo", "full-flow-marts", name="build_marts_demo")

    check("mart_trino.customer_totals non-empty",
          trino_count_positive("SELECT count(*) FROM iceberg.mart_trino.customer_totals"), True)
    check("mart_spark.customer_totals non-empty",
          trino_count_positive("SELECT count(*) FROM iceberg.mart_spark.customer_totals"), True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print("FATAL: %s: %s" % (type(e).__name__, e))
        FAIL += 1
    print("---")
    print("PASS=%d FAIL=%d" % (PASS, FAIL))
    sys.exit(0 if FAIL == 0 else 1)