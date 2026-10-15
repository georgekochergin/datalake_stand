#!/usr/bin/env bash
# Единый скрипт тестирования datalake_stand.
# Запуск:
#   ./test/test.sh           — полный сквозной запуск стенда в Docker, e2e тесты и гашение
#   ./test/test.sh --verify  — быстрые smoke-проверки на уже запущенном стенде (бывший verify.sh)
set -uo pipefail
cd "$(dirname "$0")/.."
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-dlh-test}"
COMPOSE=(docker compose -p "$COMPOSE_PROJECT_NAME" -f docker-compose.yaml -f test/docker-compose.override.yaml)

MODE="full"
if [ $# -gt 0 ]; then
  case "$1" in
    --verify|--quick)
      MODE="verify"
      ;;
    --full)
      MODE="full"
      ;;
    --help|-h)
      echo "Использование: $0 [--full | --verify | --quick | --help]"
      echo "  --full    (по умолчанию) Поднять стек, выполнить e2e-тесты в test-runner и погасить стек"
      echo "  --verify  Выполнить быстрые проверки компонентов на уже работающем локальном стенде"
      exit 0
      ;;
    *)
      echo "Неизвестный аргумент: $1"
      echo "Используйте --help для справки."
      exit 1
      ;;
  esac
fi

# ==================== РЕЖИМ 1: Быстрая проверка запущенного стенда ====================
if [ "$MODE" = "verify" ]; then
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

  echo "==> Запуск быстрых проверок (verify)..."

  # 0. Nessie Iceberg REST-каталог
  code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:19120/iceberg/v1/config)
  check "Nessie /iceberg/v1/config" "$code" "200"

  # 1. sydafa
  code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8090/health)
  check "sydafa /health" "$code" "200"

  code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8090/docs)
  check "sydafa /docs" "$code" "200"

  sources=$(curl -s http://localhost:8090/sources 2>/dev/null | python3 -c "import sys,json;print(','.join(sorted(json.load(sys.stdin).get('sources',[]))))" 2>/dev/null)
  if [[ "$sources" == *"mssql"* ]]; then
    check "sydafa /sources contains mssql" "ok" "ok"
  else
    check "sydafa /sources contains mssql" "$sources" "mssql"
  fi

  # 1b. sydafa exec update/delete — формализованный predicate, без SQL-инъекций
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8090/sources/mssql/exec \
    -H 'Content-Type: application/json' \
    -d '{"operations":[{"op":"update","schema":"demo","table":"customers","predicate":[{"column":"customer_id","operator":"=","value":-999999}],"set":{"segment":"VIP"}}]}')
  check "sydafa exec update formalized predicate" "$code" "200"

  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8090/sources/mssql/exec \
    -H 'Content-Type: application/json' \
    -d '{"operations":[{"op":"delete","schema":"demo","table":"customers","predicate":[{"column":"customer_id","operator":"=","value":-999999}],"cascade":false}]}')
  check "sydafa exec delete formalized predicate" "$code" "200"

  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8090/sources/oracle/exec \
    -H 'Content-Type: application/json' \
    -d '{"operations":[{"op":"update","schema":"demo","table":"customers","predicate":[{"column":"customer_id","operator":"=","value":-999999}],"set":{"segment":"VIP"}}]}')
  check "sydafa exec update oracle bind params" "$code" "200"

  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8090/sources/mssql/exec \
    -H 'Content-Type: application/json' \
    -d '{"operations":[{"op":"update","schema":"demo","table":"customers","predicate":[{"column":"customer_id; DROP TABLE demo.customers --","operator":"=","value":1}],"set":{"segment":"VIP"}}]}')
  check "sydafa exec rejects injected column" "$code" "422"

  # 2. Airflow
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8089/auth/token \
    -H 'Content-Type: application/x-www-form-urlencoded' --data 'username=admin&password=admin')
  check "Airflow login admin/admin" "$code" "201"

  # 3. Trino (HTTPS + password-auth)
  code=$(curl -sk -o /dev/null -w '%{http_code}' -u admin:admin -X POST https://localhost:8443/v1/statement \
    -H 'X-Trino-Catalog: iceberg' -H 'X-Trino-Schema: information_schema' \
    -H 'Content-Type: text/plain' -d 'SELECT 1')
  check "Trino HTTPS admin/admin" "$code" "200"

  # 4. MSSQL (БД sydafa + таблицы под sydafa/sydafa)
  tables=$(docker compose exec -T sydafa python -c \
    "import pymssql; c=pymssql.connect(server='sydafa-src-mssql-mssql',port=1433,user='sydafa',password='sydafa',database='sydafa'); cur=c.cursor(); cur.execute('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME'); print(','.join(r[0] for r in cur.fetchall()))" 2>/dev/null)
  case "$tables" in
    *customers*orders*products*) check "MSSQL sydafa tables under reader" "ok" "ok" ;;
    *) check "MSSQL sydafa tables under reader" "$tables" "customers,orders,products" ;;
  esac

  # 5. Oracle (БД sydafa + таблицы под sydafa/sydafa, если oracle сконфигурирован)
  if [[ "$sources" == *"oracle"* ]]; then
    tables=$(docker compose exec -T sydafa python -c \
      "import oracledb; c=oracledb.connect(user='sydafa', password='sydafa', dsn=oracledb.makedsn('sydafa-src-oracle-oracle',1521,service_name='sydafa')); cur=c.cursor(); cur.execute(\"SELECT table_name FROM all_tables WHERE owner = 'DEMO' ORDER BY table_name\"); print(','.join(r[0] for r in cur.fetchall()))" 2>/dev/null)
    check "Oracle sydafa tables under reader" "$tables" "CUSTOMERS,ORDERS,PRODUCTS"
  fi

  # 6. Spark master видит worker
  alive=$(curl -s http://localhost:8080/json/ | python3 -c "import sys,json;print(json.load(sys.stdin)['aliveworkers'])" 2>/dev/null)
  check "Spark alive workers" "$alive" "1"

  echo "---"
  echo "PASS=$PASS FAIL=$FAIL"
  [ "$FAIL" -eq 0 ]
  exit $?
fi

# ==================== РЕЖИМ 2: Полный сквозной e2e запуск стенда ====================
cleanup() {
  local ids
  ids=$(docker ps -aq --filter "name=test-runner-run-")
  if [ -n "$ids" ]; then
    echo "$ids" | xargs docker rm -f >/dev/null 2>&1 || true
  fi
  "${COMPOSE[@]}" down -v --remove-orphans
  docker rm -f sydafa-test-src-mssql-mssql sydafa-test-src-oracle-oracle sydafa-test-mssql-mssql-source sydafa-test-oracle-oracle-source sydafa-test-mssql-source sydafa-test-oracle-source >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cleanup
SYDAFA_IMG="${SYDAFA_IMAGE:-sydafa:latest}"
if ! docker image inspect "$SYDAFA_IMG" >/dev/null 2>&1; then
  if [ -d "../sydafa" ]; then
    echo "Сборка локального образа sydafa из ../sydafa..."
    docker build -t "$SYDAFA_IMG" ../sydafa
  else
    echo "Загрузка образа sydafa ($SYDAFA_IMG)..."
    docker pull "$SYDAFA_IMG" || docker pull "ghcr.io/georgekochergin/sydafa:latest"
  fi
fi

"${COMPOSE[@]}" up -d --build
"${COMPOSE[@]}" run --rm --build test-runner
rc=$?
exit $rc
