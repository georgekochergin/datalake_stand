# datalake_stand — Инструкции проекта

## Контекст и архитектура
- Стенд КХД/DLH: Trino, Spark, Nessie, Airflow, SILO, Postgres. Источники — `sydafa`.
- Каркас: `airflow/` `spark/` `trino/` (пользовательский код, не в git), `conf/` (как запущен стенд), `test/` (демо + e2e).
- Конфигурация источников sydafa: через `init.json` (`conf/sydafa/init.json` на живом стенде, `test/sydafa/init.json` в тестах).
- Читатель источников: `sydafa`/`sydafa` (только SELECT). Админ снаружи недоступен.
- Внутренние DNS-имена СУБД: `sydafa-mssql-source:1433`, `sydafa-oracle-source:1521`.
- Демо-DAG-и: `test/dags/demo/`, подключаются `test/docker-compose.override.yaml`.
- Инспекция БД: `POST /sources/{source}/activate` → объект `connection` в ответе; внешний порт хоста задается переменной `SYDAFA_EXT_PORT_MSSQL`.

## Git
- Ветки с префиксом `kodik/`. Не коммитить в `main`. Обязателен Pull Request.

## Testing
- Полный e2e-прогон: `./test/test.sh` (полностью изолированный тестовый проект `dlh-test`, безопасен для параллельного запуска с живым стендом).
- Verify на уже поднятом стеке: `./test/test.sh --verify`.
- Критерий: `FAIL=0`, exit 0.
