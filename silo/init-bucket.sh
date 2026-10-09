#!/bin/sh
# Создаёт bucket "warehouse" в silo (MinIO-совместимое хранилище).
# Запускается отдельным одноразовым контейнером (см. docker-compose.yaml,
# сервис silo-init, образ amazon/aws-cli).
set -eu

aws --endpoint-url "http://silo:9000" s3 mb "s3://warehouse" || true
aws --endpoint-url "http://silo:9000" s3 ls
