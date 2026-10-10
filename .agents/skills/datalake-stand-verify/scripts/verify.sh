#!/usr/bin/env bash
# Проверка сборки datalake_stand.
# Запуск: bash .agents/skills/datalake-stand-verify/scripts/verify.sh
# (можно из любого каталога — скрипт сам поднимется к корню репозитория).
set -u

# scripts/ -> datalake-stand-verify/ -> skills/ -> .agents/ -> корень репозитория
cd "$(dirname "$0")/../../.." || exit 1

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

# 1. datagen-api
code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8090/docs)
check "datagen-api /docs" "$code" "200"

sources=$(curl -s http://localhost:8090/sources 2>/dev/null | python3 -c "import sys,json;print(','.join(json.load(sys.stdin).get('sources',[])))" 2>/dev/null)
check "datagen-api /sources" "$sources" "mssql,oracle"

# 1b. datagen-api update/delete — формализованный predicate, без SQL-инъекций
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8090/tables/mssql/demo/customers/update \
  -H 'Content-Type: application/json' \
  -d '{"predicate":[{"column":"customer_id","operator":"=","value":-999999}],"set":{"segment":"VIP"}}')
check "datagen-api update formalized predicate" "$code" "200"

code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8090/tables/mssql/demo/customers/delete \
  -H 'Content-Type: application/json' \
  -d '{"predicate":[{"column":"customer_id","operator":"=","value":-999999}],"cascade":false}')
check "datagen-api delete formalized predicate" "$code" "200"

code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8090/tables/oracle/demo/customers/update \
  -H 'Content-Type: application/json' \
  -d '{"predicate":[{"column":"customer_id","operator":"=","value":-999999}],"set":{"segment":"VIP"}}')
check "datagen-api update oracle bind params" "$code" "200"

code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8090/tables/mssql/demo/customers/update \
  -H 'Content-Type: application/json' \
  -d '{"predicate":[{"column":"customer_id; DROP TABLE demo.customers --","operator":"=","value":1}],"set":{"segment":"VIP"}}')
check "datagen-api rejects injected column" "$code" "422"

# 2. Airflow
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8089/auth/token \
  -H 'Content-Type: application/x-www-form-urlencoded' --data 'username=admin&password=admin')
check "Airflow login admin/admin" "$code" "201"

# 3. Trino (HTTPS + password-auth)
code=$(curl -sk -o /dev/null -w '%{http_code}' -u admin:admin -X POST https://localhost:8443/v1/statement \
  -H 'X-Trino-Catalog: iceberg' -H 'X-Trino-Schema: information_schema' \
  -H 'Content-Type: text/plain' -d 'SELECT 1')
check "Trino HTTPS admin/admin" "$code" "200"

# 4. MSSQL (БД demo + таблицы)
tables=$(docker compose exec -T mssql-source /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U admin -P admin \
  -h -1 -Q "SET NOCOUNT ON; SELECT name FROM demo.sys.tables ORDER BY name" 2>/dev/null | tr -d ' \r\n')
case "$tables" in
  *customers*orders*products*) check "MSSQL demo tables" "ok" "ok" ;;
  *) check "MSSQL demo tables" "$tables" "customers,orders,products" ;;
esac

# 5. Oracle (таблицы схемы demo)
tables=$(docker compose exec -T datagen-api python -c \
  "import oracledb; c=oracledb.connect(user='demo', password='admin', dsn=oracledb.makedsn('oracle-source',1521,service_name='demo')); cur=c.cursor(); cur.execute('select table_name from user_tables order by table_name'); print(','.join(r[0] for r in cur.fetchall()))" 2>/dev/null)
check "Oracle demo tables" "$tables" "CUSTOMERS,ORDERS,PRODUCTS"

# 6. Spark master видит worker
alive=$(curl -s http://localhost:8080/json/ | python3 -c "import sys,json;print(json.load(sys.stdin)['aliveworkers'])" 2>/dev/null)
check "Spark alive workers" "$alive" "1"

echo "---"
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
