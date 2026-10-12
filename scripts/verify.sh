#!/usr/bin/env bash
# Проверка сборки datalake_stand.
# Запуск: bash scripts/verify.sh
# (можно из любого каталога — скрипт сам поднимется к корню репозитория).
set -u

cd "$(dirname "$0")/.." || exit 1

PASS=0
FAIL=0
check() { # check "название" "факт" "ожидание"
  if [ "$2" = "$3" ]; then
    echo "PASS  $1"
    PASS=$((PASS + 1))
  else
    echo "FAIL  $1 (ожидалось '$3', получено '$2')"
    FAIL=$((FAIL + 1))
  fi
}

# 0. Nessie Iceberg REST-каталог
code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:19120/iceberg/v1/config)
check "Nessie /iceberg/v1/config" "$code" "200"

# 1. sydafa
code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8090/health)
check "sydafa /health" "$code" "200"

code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8090/docs)
check "sydafa /docs" "$code" "200"

sources=$(curl -s http://localhost:8090/sources 2>/dev/null | python3 -c "import sys,json;print(','.join(json.load(sys.stdin).get('sources',[])))" 2>/dev/null)
check "sydafa /sources" "$sources" "mssql,oracle"

# 1b. sydafa update/delete — формализованный predicate, без SQL-инъекций
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8090/tables/mssql/demo/customers/update \
  -H 'Content-Type: application/json' \
  -d '{"predicate":[{"column":"customer_id","operator":"=","value":-999999}],"set":{"segment":"VIP"}}')
check "sydafa update formalized predicate" "$code" "200"

code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8090/tables/mssql/demo/customers/delete \
  -H 'Content-Type: application/json' \
  -d '{"predicate":[{"column":"customer_id","operator":"=","value":-999999}],"cascade":false}')
check "sydafa delete formalized predicate" "$code" "200"

code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8090/tables/oracle/demo/customers/update \
  -H 'Content-Type: application/json' \
  -d '{"predicate":[{"column":"customer_id","operator":"=","value":-999999}],"set":{"segment":"VIP"}}')
check "sydafa update oracle bind params" "$code" "200"

code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8090/tables/mssql/demo/customers/update \
  -H 'Content-Type: application/json' \
  -d '{"predicate":[{"column":"customer_id; DROP TABLE demo.customers --","operator":"=","value":1}],"set":{"segment":"VIP"}}')
check "sydafa rejects injected column" "$code" "422"

# 2. Airflow
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8089/auth/token \
  -H 'Content-Type: application/x-www-form-urlencoded' --data 'username=admin&password=admin')
check "Airflow login admin/admin" "$code" "201"

# 3. Trino (HTTPS + password-auth)
code=$(curl -sk -o /dev/null -w '%{http_code}' -u admin:admin -X POST https://localhost:8443/v1/statement \
  -H 'X-Trino-Catalog: iceberg' -H 'X-Trino-Schema: information_schema' \
  -H 'Content-Type: text/plain' -d 'SELECT 1')
check "Trino HTTPS admin/admin" "$code" "200"

# 4. MSSQL (БД sydafa + таблицы под reader/reader)
tables=$(docker compose exec -T sydafa python -c \
  "import pymssql; c=pymssql.connect(server='mssql-source',port=1433,user='reader',password='reader',database='sydafa'); cur=c.cursor(); cur.execute('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME'); print(','.join(r[0] for r in cur.fetchall()))" 2>/dev/null)
case "$tables" in
  *customers*orders*products*) check "MSSQL sydafa tables under reader" "ok" "ok" ;;
  *) check "MSSQL sydafa tables under reader" "$tables" "customers,orders,products" ;;
esac

# 5. Oracle (БД sydafa + таблицы под reader/reader)
tables=$(docker compose exec -T sydafa python -c \
  "import oracledb; c=oracledb.connect(user='reader', password='reader', dsn=oracledb.makedsn('oracle-source',1521,service_name='sydafa')); cur=c.cursor(); cur.execute(\"SELECT table_name FROM all_tables WHERE owner = 'DEMO' ORDER BY table_name\"); print(','.join(r[0] for r in cur.fetchall()))" 2>/dev/null)
check "Oracle sydafa tables under reader" "$tables" "CUSTOMERS,ORDERS,PRODUCTS"

# 6. Spark master видит worker
alive=$(curl -s http://localhost:8080/json/ | python3 -c "import sys,json;print(json.load(sys.stdin)['aliveworkers'])" 2>/dev/null)
check "Spark alive workers" "$alive" "1"

echo "---"
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
