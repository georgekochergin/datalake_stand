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
docker compose up -d --build
docker compose run --rm --build test-runner
rc=$?
exit $rc