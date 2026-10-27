#!/usr/bin/env bash
# Единый скрипт тестирования datalake_stand.
# Запуск:
#   ./test/test.sh           — предварительная валидация, запуск изолированного стенда в Docker, e2e тесты и гашение
#   ./test/test.sh --verify  — запуск проверок на уже работающем стенде
set -euo pipefail
cd "$(dirname "$0")/.."
PROJECT_ROOT="$(pwd)"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-dlh-test}"
if [[ ! "$COMPOSE_PROJECT_NAME" =~ ^dlh-test(-[a-z0-9]+)*$ ]]; then
  echo "FAIL: COMPOSE_PROJECT_NAME должен быть dlh-test или dlh-test-<суффикс>."
  exit 1
fi
export COMPOSE_PROJECT_NAME
export SYDAFA_CONTAINER_PREFIX="sydafa-$COMPOSE_PROJECT_NAME"
COMPOSE=(docker compose -p "$COMPOSE_PROJECT_NAME" -f docker-compose.yaml -f test/docker-compose.override.yaml)
TEST_WS="$PROJECT_ROOT/test/.workspace"

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
      echo "  --full    (по умолчанию) Предварительная валидация, подъем стека, e2e-тесты в test-runner и гашение"
      echo "  --verify  Выполнить проверки компонентов на уже работающем локальном стенде"
      exit 0
      ;;
    *)
      echo "Неизвестный аргумент: $1"
      echo "Используйте --help для справки."
      exit 1
      ;;
  esac
fi

# ==================== ШАГ 1: Быстрая валидация манифестов и DAG-ов ====================
echo "==> Предварительная проверка структуры манифестов источников, миграций и DAG-ов..."
python3 test/run_flow.py --validate-manifests-only
val_rc=$?
if [ "$val_rc" -ne 0 ]; then
  echo "FAIL: Предварительная валидация завершилась с ошибкой. Исправьте конфигурации перед запуском стенда."
  exit "$val_rc"
fi

echo "==> Запуск unit-тестов фреймворка ингестии (test/unit)..."
python3 -m unittest discover -s test/unit
ut_rc=$?
if [ "$ut_rc" -ne 0 ]; then
  echo "FAIL: Unit-тесты завершились с ошибкой."
  exit "$ut_rc"
fi

# ==================== ШАГ 2 (verify): Запуск на работающем стенде ====================
run_runtime_checks() {
  if [ ! -f "$TEST_WS/spark/e2e_schema_evolution.py" ] || [ ! -f "$TEST_WS/spark/e2e_temporal_extraction.py" ]; then
    echo "FAIL: Отсутствуют workspace Spark e2e apps; сначала выполните полный запуск."
    return 1
  fi
  "${COMPOSE[@]}" run --rm --no-deps test-runner python -u /app/run_flow.py
  "${COMPOSE[@]}" exec -T -e _AIRFLOW_PROCESS_CONTEXT=server airflow-scheduler python /opt/airflow/plugins/e2e_check_extraction.py
  "${COMPOSE[@]}" exec -T -e _AIRFLOW_PROCESS_CONTEXT=server airflow-scheduler spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark-jobs/e2e_schema_evolution.py
  "${COMPOSE[@]}" exec -T -e _AIRFLOW_PROCESS_CONTEXT=server airflow-scheduler spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark-jobs/e2e_temporal_extraction.py
}

if [ "$MODE" = "verify" ]; then
  echo "==> Запуск e2e-проверок в test-runner на работающем стенде..."
  run_runtime_checks
  exit $?
fi

# ==================== ШАГ 2 (full): Полный сквозной e2e запуск стенда ====================
cleanup() {
  if [ -n "${TEST_FAILURE_LOG_DIR:-}" ]; then
    local scheduler_id
    scheduler_id=$("${COMPOSE[@]}" ps -q airflow-scheduler 2>/dev/null || true)
    if [ -n "$scheduler_id" ]; then
      mkdir -p "$TEST_FAILURE_LOG_DIR/dlh-test-airflow-logs"
      docker cp "$scheduler_id:/opt/airflow/logs/." "$TEST_FAILURE_LOG_DIR/dlh-test-airflow-logs/" 2>/dev/null || true
    fi
  fi
  if [ -n "${TEST_FAILURE_LOG_DIR:-}" ] && [ -d "$TEST_WS/airflow/logs" ]; then
    local failure_log="${TEST_FAILURE_LOG_DIR:-${TMPDIR:-/tmp}}/dlh-test-airflow-logs"
    mkdir -p "$failure_log"
    cp -R "$TEST_WS/airflow/logs/." "$failure_log/" 2>/dev/null || true
  fi
  local ids
  ids=$(docker ps -aq --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME" --filter "label=com.docker.compose.service=test-runner" 2>/dev/null || true)
  if [ -n "$ids" ]; then
    echo "$ids" | xargs docker rm -f >/dev/null 2>&1 || true
  fi
  local sydafa_srcs
  sydafa_srcs=$(docker ps -aq --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME" --filter "label=sydafa.managed=true" --filter "label=sydafa.prefix=$SYDAFA_CONTAINER_PREFIX" 2>/dev/null || true)
  if [ -n "$sydafa_srcs" ]; then
    echo "$sydafa_srcs" | xargs docker rm -f >/dev/null 2>&1 || true
  fi
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  local volume
  while IFS= read -r volume; do
    case "$volume" in
      "$SYDAFA_CONTAINER_PREFIX"-data-*) docker volume rm "$volume" >/dev/null 2>&1 || true ;;
    esac
  done < <(docker volume ls -q --filter "label=sydafa.managed=true" 2>/dev/null || true)
  rm -rf "$TEST_WS"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

cleanup

echo "==> Генерация DAG-ов ингестии (ingestion/generate_dags.py)..."
python3 ingestion/generate_dags.py
gen_rc=$?
if [ "$gen_rc" -ne 0 ]; then
  echo "FAIL: Генерация DAG-ов завершилась с ошибкой."
  exit "$gen_rc"
fi

echo "==> Подготовка изолированного дерева проекта в $TEST_WS..."
mkdir -p "$TEST_WS/conf" "$TEST_WS/airflow" "$TEST_WS/spark" "$TEST_WS/trino"
cp -R "$PROJECT_ROOT/ingestion" "$TEST_WS/ingestion"
cp -R "$PROJECT_ROOT/conf/." "$TEST_WS/conf/"
python3 - "$TEST_WS/conf/sydafa/sources.json" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
manifest = json.loads(path.read_text())
for source in manifest["sources"].values():
    source.pop("external_port", None)
path.write_text(json.dumps(manifest, indent=2))
PY
cp -R "$PROJECT_ROOT/airflow/." "$TEST_WS/airflow/"
cp "$PROJECT_ROOT/test/check_extraction.py" "$TEST_WS/airflow/plugins/e2e_check_extraction.py"
if [ -d "$PROJECT_ROOT/spark" ]; then
  cp -R "$PROJECT_ROOT/spark/." "$TEST_WS/spark/"
fi
cp "$PROJECT_ROOT/test/schema_evolution.py" "$TEST_WS/spark/e2e_schema_evolution.py"
cp "$PROJECT_ROOT/test/temporal_extraction.py" "$TEST_WS/spark/e2e_temporal_extraction.py"
if [ -d "$PROJECT_ROOT/trino" ]; then
  cp -R "$PROJECT_ROOT/trino/." "$TEST_WS/trino/"
fi

SYDAFA_IMG="${SYDAFA_IMAGE:-ghcr.io/georgekochergin/sydafa:latest}"
if ! docker image inspect "$SYDAFA_IMG" >/dev/null 2>&1; then
  echo "Загрузка образа sydafa ($SYDAFA_IMG)..."
  docker pull "$SYDAFA_IMG"
fi

echo "==> Подъем тестового Docker Compose стека ($COMPOSE_PROJECT_NAME)..."
if [ "${TEST_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> TEST_SKIP_BUILD=1: используются ранее собранные образы без загрузки и сборки."
  "${COMPOSE[@]}" up -d --no-build --pull never
else
  "${COMPOSE[@]}" up -d --build
  "${COMPOSE[@]}" build test-runner
fi
echo "==> Запуск e2e тестового сценария в test-runner..."
run_runtime_checks
echo "==> Проверка штатной очистки Compose без ручного удаления источников..."
"${COMPOSE[@]}" down --remove-orphans --volumes
if [ -n "$(docker ps -aq --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME")" ]; then
  echo "FAIL: После Compose down остались контейнеры проекта."
  exit 1
fi
for source_volume in "$SYDAFA_CONTAINER_PREFIX-data-mssql-bank" "$SYDAFA_CONTAINER_PREFIX-data-oracle-broker"; do
  if docker volume inspect "$source_volume" >/dev/null 2>&1; then
    echo "FAIL: После Compose down остался том $source_volume."
    exit 1
  fi
done
if [ -n "$(docker network ls -q --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME")" ]; then
  echo "FAIL: После Compose down осталась сеть проекта."
  exit 1
fi
echo "PASS Compose cleanup: containers, source volumes and networks; FAIL=0"
rc=$?
exit $rc
