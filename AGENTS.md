# datalake_stand — Инструкции проекта

## Контекст и архитектура
- Проект стенда КХД/DLH: Trino, Spark, Nessie (Iceberg REST), Airflow, SILO (S3 storage), Postgres каталоги.
- Эмуляция реляционных источников (MSSQL, Oracle) выполняется внешним сервисом `sydafa` (`ghcr.io/georgekochergin/sydafa:latest` или локальный `sydafa:latest`).
- Конфигурация источников стенда: `sydafa/sources/` (`mssql.yaml`, `oracle.yaml`) и `sydafa/init.json`.
- Учетные данные инспектора источников: `reader`/`reader` (только SELECT).
- Даги витрин (datamart DAGs) должны опираться на таблицы, имеющиеся в источниках согласно `sydafa/init.json`.
- Все демонстрационные DAG-и изолируются в `airflow/dags/demo/` и создаются в выключенном состоянии (`is_paused_upon_creation=True`).

## Git
- Все новые изменения оформляются в отдельных ветках с префиксом `kodik/` (например, `kodik/feature-name`).
- Запрещено коммитить напрямую в `main`.

## Testing
- Для проверки этого стенда используется строго команда: `./scripts/full-test.sh`
- Скрипт поднимает весь стек, при необходимости собирает `sydafa` из `../sydafa`, прогоняет сквозные проверки стенда в контейнере `test-runner` и корректно гасит окружение с очисткой динамических контейнеров (`mssql-source`, `oracle-source`).
- Критерий успешности: вывод `FAIL=0` с кодом возврата `0`.
