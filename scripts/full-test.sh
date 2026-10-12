#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."

cleanup() {
  local ids
  ids=$(docker ps -aq --filter "name=dlh-poc-test-runner-run-")
  if [ -n "$ids" ]; then
    echo "$ids" | xargs docker rm -f >/dev/null 2>&1 || true
  fi
  docker compose down -v --remove-orphans
  docker rm -f mssql-source oracle-source >/dev/null 2>&1 || true
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

docker compose up -d --build
docker compose run --rm --build test-runner
rc=$?
exit $rc