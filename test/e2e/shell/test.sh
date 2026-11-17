#!/usr/bin/env bash
# Python-проверки запускаются отдельно перед этим параллельным e2e.
set -euo pipefail
cd "$(dirname "$0")/../../.."
PROJECT_ROOT="$(pwd)"
if [[ ! "${COMPOSE_PROJECT_NAME:-dlh-test}" =~ ^dlh-test(-[a-z0-9]+)*$ ]]; then
  echo "FAIL: COMPOSE_PROJECT_NAME должен быть dlh-test или dlh-test-<суффикс>."
  exit 1
fi
MODE=full
SOURCE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --verify) MODE=verify; shift ;;
    --source)
      [ $# -ge 2 ] || { echo 'FAIL: --source требует bank или broker'; exit 1; }
      SOURCE="$2"; shift 2 ;;
    --help|-h)
      echo "Использование: $0 | $0 --verify --source bank|broker"
      echo 'Сначала: python3 -m pytest'
      exit 0 ;;
    *) echo "FAIL: Неизвестный аргумент: $1"; exit 1 ;;
  esac
done
if { [ "$MODE" = verify ] && [[ ! "$SOURCE" =~ ^(bank|broker)$ ]]; } ||
   { [ "$MODE" = full ] && [ -n "$SOURCE" ]; }; then
  echo 'FAIL: --source bank|broker используется только вместе с --verify.'
  exit 1
fi

configure_stack() {
  export E2E_SOURCE="$1"
  export COMPOSE_PROJECT_NAME="dlh-test-$1"
  export SYDAFA_CONTAINER_PREFIX="sydafa-$COMPOSE_PROJECT_NAME"
  export TEST_WS_DIR=".workspace-$1"
  TEST_WS="$PROJECT_ROOT/test/$TEST_WS_DIR"
  COMPOSE=(docker compose -p "$COMPOSE_PROJECT_NAME" -f docker-compose.yaml -f test/e2e/docker/docker-compose.override.yaml)
}

run_temporal_checks() {
  "${COMPOSE[@]}" exec -T -e _AIRFLOW_PROCESS_CONTEXT=server airflow-scheduler spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.cores.max=2 /opt/spark-jobs/e2e_temporal_extraction.py --source "$E2E_SOURCE"
}

run_post_checks() (
  temporal_pid=""
  rc=0
  run_temporal_checks &
  temporal_pid=$!
  trap 'kill -TERM "$temporal_pid" 2>/dev/null || true; wait "$temporal_pid" 2>/dev/null || true' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM HUP
  if [ "$E2E_SOURCE" = bank ]; then
    "${COMPOSE[@]}" exec -T -e _AIRFLOW_PROCESS_CONTEXT=server airflow-scheduler python /opt/airflow/plugins/e2e_check_spark_guard.py
  else
    "${COMPOSE[@]}" exec -T -e _AIRFLOW_PROCESS_CONTEXT=server airflow-scheduler bash -c 'EXTRACTION_TEST_DSN="host=pg-meta port=5432 dbname=meta user=$STAND_USER password=$STAND_PASSWORD" python /opt/airflow/plugins/e2e_check_meta.py -v'
    "${COMPOSE[@]}" exec -T -e _AIRFLOW_PROCESS_CONTEXT=server airflow-scheduler spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.cores.max=2 /opt/spark-jobs/e2e_schema_evolution.py
  fi
  wait "$temporal_pid" || rc=$?
  trap - EXIT
  return "$rc"
)

run_runtime_checks() {
  if [ ! -f "$TEST_WS/ingestion/spark/e2e_schema_evolution.py" ] || [ ! -f "$TEST_WS/ingestion/spark/e2e_temporal_extraction.py" ] || [ ! -f "$TEST_WS/airflow/plugins/e2e_check_meta.py" ] || [ ! -f "$TEST_WS/airflow/plugins/e2e_check_spark_guard.py" ] || [ ! -f "$TEST_WS/ingestion/spark/e2e_guard_idle.py" ]; then
    echo 'FAIL: Отсутствуют workspace-артефакты; сначала выполните полный запуск.'
    return 1
  fi
  "${COMPOSE[@]}" run --rm --no-deps test-runner python -u /app/run_flow.py
  "${COMPOSE[@]}" exec -T -e E2E_SOURCE="$E2E_SOURCE" -e _AIRFLOW_PROCESS_CONTEXT=server airflow-scheduler python /opt/airflow/plugins/e2e_check_extraction.py
  run_post_checks
}

if [ "$MODE" = verify ]; then
  configure_stack "$SOURCE"
  started=$SECONDS
  run_runtime_checks
  echo "E2E_VERIFY_SECONDS=$((SECONDS - started)) source=$SOURCE"
  exit 0
fi

cleanup() {
  if [ -n "${TEST_FAILURE_LOG_DIR:-}" ]; then
    local scheduler_id
    scheduler_id=$("${COMPOSE[@]}" ps -q airflow-scheduler 2>/dev/null || true)
    if [ -n "$scheduler_id" ]; then
      mkdir -p "$TEST_FAILURE_LOG_DIR/$COMPOSE_PROJECT_NAME-airflow-logs"
      docker cp "$scheduler_id:/opt/airflow/logs/." "$TEST_FAILURE_LOG_DIR/$COMPOSE_PROJECT_NAME-airflow-logs/" 2>/dev/null || true
    fi
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

run_stack() (
  configure_stack "$1"
  trap cleanup EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM HUP
  started=$SECONDS
  python3 - "$PROJECT_ROOT" "$TEST_WS" "$E2E_SOURCE" <<'PY'
import sys
sys.path.insert(0, "test/e2e/python")
from e2e_scale import prepare_workspace

prepare_workspace(*sys.argv[1:])
PY
  "${COMPOSE[@]}" up -d --no-build --pull never
  run_runtime_checks
  "${COMPOSE[@]}" down --remove-orphans --volumes
  if [ -n "$(docker ps -aq --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME")" ] ||
     [ -n "$(docker network ls -q --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME")" ]; then
    echo 'FAIL: После Compose down остались контейнеры или сеть проекта.'
    exit 1
  fi
  for volume in "$SYDAFA_CONTAINER_PREFIX-data-mssql-bank" "$SYDAFA_CONTAINER_PREFIX-data-oracle-broker"; do
    if docker volume inspect "$volume" >/dev/null 2>&1; then
      echo "FAIL: После Compose down остался том $volume."
      exit 1
    fi
  done
  cleanup
  trap - EXIT
  echo "PASS Compose cleanup; FAIL=0; E2E_STACK_SECONDS=$((SECONDS - started)) source=$E2E_SOURCE"
)

memory=$(docker info --format '{{.MemTotal}}')
if [[ ! "$memory" =~ ^[0-9]+$ ]] || [ "$memory" -lt 21474836480 ]; then
  echo 'FAIL: Для двух стеков требуется минимум 20 GiB памяти Docker VM (рекомендуется 24 GiB).'
  exit 1
fi
started=$SECONDS
LOG_DIR="${TEST_FAILURE_LOG_DIR:-$PROJECT_ROOT/test/.results}"
mkdir -p "$LOG_DIR"
export TEST_FAILURE_LOG_DIR="$LOG_DIR"
PIDS=()
OWNED=0
finish() {
  local pid source
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do kill -TERM "$pid" 2>/dev/null || true; done
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do wait "$pid" 2>/dev/null || true; done
  if [ "$OWNED" = 1 ]; then
    for source in bank broker; do
      configure_stack "$source"
      cleanup
    done
  fi
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP
for source in bank broker; do
  configure_stack "$source"
  if [ -d "$TEST_WS" ] || [ -n "$("${COMPOSE[@]}" ps -aq)" ]; then
    echo "FAIL: Контур $COMPOSE_PROJECT_NAME уже существует; не запускайте два прогона одновременно."
    trap - EXIT
    exit 1
  fi
done
OWNED=1
python3 -m ingestion.generator.generate_dags
SYDAFA_IMG="${SYDAFA_IMAGE:-ghcr.io/georgekochergin/sydafa:latest}"
if ! docker image inspect "$SYDAFA_IMG" >/dev/null 2>&1; then
  docker pull --platform linux/amd64 "$SYDAFA_IMG"
fi
configure_stack bank
if [ "${TEST_SKIP_BUILD:-0}" != 1 ]; then
  "${COMPOSE[@]}" --profile test build
fi
for source in bank broker; do
  run_stack "$source" >"$LOG_DIR/$source.log" 2>&1 &
  PIDS+=("$!")
done
rc=0
index=0
for source in bank broker; do
  stack_rc=0
  wait "${PIDS[$index]}" || stack_rc=$?
  echo "==> $source: exit=$stack_rc; log=$LOG_DIR/$source.log"
  grep -E 'PASS=[0-9]+ FAIL=[0-9]+|FAIL=0|E2E_STACK_SECONDS=' "$LOG_DIR/$source.log" || true
  if [ "$stack_rc" -ne 0 ]; then
    tail -n 60 "$LOG_DIR/$source.log"
    rc=1
  fi
  index=$((index + 1))
done
PIDS=()
finish
trap - EXIT
elapsed=$((SECONDS - started))
echo "E2E_TOTAL_SECONDS=$elapsed E2E_BUDGET_SECONDS=420"
if [ "$elapsed" -gt 420 ]; then
  echo 'FAIL: e2e превысил 7 минут. Требуется оптимизация теста, а не повышение лимита.'
  rc=1
fi
exit "$rc"
