#!/usr/bin/env bash
# Единый скрипт тестирования datalake_stand.
# Запуск:
#   ./test/test.sh           — предварительная валидация, запуск изолированного стенда в Docker, e2e тесты и гашение
#   ./test/test.sh --verify  — запуск проверок на уже работающем стенде
set -uo pipefail
cd "$(dirname "$0")/.."
PROJECT_ROOT="$(pwd)"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-dlh-test}"
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

# ==================== ШАГ 2 (verify): Запуск на работающем стенде ====================
if [ "$MODE" = "verify" ]; then
  echo "==> Запуск e2e-проверок в test-runner на работающем стенде..."
  "${COMPOSE[@]}" run --rm --no-deps test-runner python -u /app/run_flow.py
  exit $?
fi

# ==================== ШАГ 2 (full): Полный сквозной e2e запуск стенда ====================
cleanup() {
  local ids
  ids=$(docker ps -aq --filter "name=test-runner-run-" 2>/dev/null || true)
  if [ -n "$ids" ]; then
    echo "$ids" | xargs docker rm -f >/dev/null 2>&1 || true
  fi
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  local sydafa_srcs
  sydafa_srcs=$(docker ps -aq --filter "name=sydafa.*src" 2>/dev/null || true)
  if [ -n "$sydafa_srcs" ]; then
    echo "$sydafa_srcs" | xargs docker rm -f >/dev/null 2>&1 || true
  fi
  rm -rf "$TEST_WS"
}
trap cleanup EXIT INT TERM HUP

cleanup

echo "==> Подготовка изолированного дерева проекта в $TEST_WS..."
mkdir -p "$TEST_WS/conf" "$TEST_WS/airflow" "$TEST_WS/spark" "$TEST_WS/trino"
cp -R "$PROJECT_ROOT/conf/." "$TEST_WS/conf/"
cp -R "$PROJECT_ROOT/airflow/." "$TEST_WS/airflow/"
if [ -d "$PROJECT_ROOT/spark" ]; then
  cp -R "$PROJECT_ROOT/spark/." "$TEST_WS/spark/"
fi
if [ -d "$PROJECT_ROOT/trino" ]; then
  cp -R "$PROJECT_ROOT/trino/." "$TEST_WS/trino/"
fi

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

echo "==> Подъем тестового Docker Compose стека ($COMPOSE_PROJECT_NAME)..."
"${COMPOSE[@]}" up -d --build
echo "==> Запуск e2e тестового сценария в test-runner..."
"${COMPOSE[@]}" run --rm --build test-runner
rc=$?
exit $rc
