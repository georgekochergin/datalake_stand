# datalake_stand — Инструкции проекта

## Контекст и архитектура
- Стенд КХД/DLH: Trino, Spark, Nessie, Airflow, SILO, Postgres. Источники — `sydafa`.
- Каркас: `airflow/` `spark/` `trino/` (пользовательский код, не в git), `conf/` (как запущен стенд), `test/` (демо + e2e).
- Живой sydafa-конфиг: `conf/sydafa/mssql.yaml`. Демо-init и oracle: `test/sydafa/`.
- Инспектор источников: `reader`/`reader` (только SELECT). Админ `sydafa` снаружи недоступен.
- Демо-DAG-и: `test/dags/demo/`, подключаются `test/docker-compose.override.yaml`.
- Инспекция БД: `POST /sources/{source}/activate` → `host_port` в ответе.

## Git
- Ветки с префиксом `kodik/`. Не коммитить в `main`. Обязателен Pull Request.

## Testing
- Полный прогон: `./test/test.sh` (override + cleanup своих контейнеров стенда).
- Verify на уже поднятом стеке: `./test/test.sh --verify`.
- Не использовать `test.sh` против чужого живого стенда как «добавить источник».
- Критерий: `FAIL=0`, exit 0.
