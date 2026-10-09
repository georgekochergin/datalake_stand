# datalake_stand — PoC стенда КХД → DLH

Локальный docker-compose стенд: загрузка данных из реальных реляционных
источников (MSSQL, Oracle) в ODS-слой Iceberg **AS IS**, и демонстрация
двух способов трансформации данных внутри DataLake (Trino SQL и Spark).

Полный план/архитектура/риски/открытые решения — см. **[docs/PLAN.md](docs/PLAN.md)**.

## Состав стенда

**Контур A (релевантно production DLH-архитектуре):** `mssql-source`,
`oracle-source`, `pg-catalog`+`nessie` (каталог Iceberg), `silo` (S3-хранилище,
форк MinIO), `spark-master`/`spark-worker` (AS IS загрузка в ODS),
`trino` (трансформации + ad-hoc), `airflow-*` (оркестрация).

**Контур B (только тестовый стенд, нет в production):** `datagen-api` —
REST API для наполнения источников синтетическими данными и ограниченной
эволюции их схемы (`rename_column`/`drop_column`). Схема источников
зафиксирована в `schema/sources/mssql.yaml` / `schema/sources/oracle.yaml`.

## Быстрый старт

```bash
cp .env.example .env   # заполнить пароли перед первым запуском
docker compose up -d --build
```

Порты:

| Сервис | Порт на хосте | Назначение |
|---|---|---|
| mssql-source | 1433 | T-SQL |
| oracle-source | 1521 | SQL*Net |
| datagen-api | 8090 | Swagger UI на `http://localhost:8090/docs` |
| nessie | 19120 | Iceberg REST catalog |
| silo (S3 API / консоль) | 9000 / 9001 | S3-совместимое хранилище |
| spark-master UI | 8080 | |
| spark-worker UI | 8081 | |
| trino | 8082 | `http://localhost:8082` |
| airflow webserver/api-server | 8089 | admin/password (см. `airflow-init`) |

## Учётные данные

Тестовый стенд — везде, где логин/пароль настраиваемы, используется
`admin` / `password` (см. `.env.example`). Исключение: MSSQL `sa` —
имя пользователя фиксировано образом, а пароль не может быть буквально
`password` (MSSQL требует длину ≥8 и минимум 3 из 4 категорий символов) —
используется `AdminPassword1!`. SILO (консоль `:9001`), pg-catalog,
airflow-postgres, Airflow UI (`:8089`), Oracle (`ORACLE_PASSWORD`,
пользователь-схема `demo`) — везде `admin`/`password` (кроме Oracle, где
имя пользователя-схемы фиксировано как `demo`, см. `docs/PLAN.md`).

## ⚠️ Важно: потеря данных при изменении схемы

Если изменить `schema/sources/mssql.yaml` или `schema/sources/oracle.yaml`
и перезапустить контейнер `datagen-api` — **все данные этого источника
будут удалены**, а таблицы пересозданы пустыми по новой схеме. Это
осознанное упрощение для тестового стенда (без архивации прежнего
состояния) — см. `docs/PLAN.md`. Пока контейнер работает без перезапуска,
изменения файла на хосте не подхватываются.

## Backlog (вне объёма текущего этапа)

- Перенос хранимых процедур КХД и сверка результата с их эталоном.
- Конкретное бизнес-содержание будущих detail/mart-слоёв
  (`build_mart_trino_demo.sql` / `build_mart_spark_demo.py` — заглушки).
- dbt-trino как альтернатива прямым SQL/PySpark-скриптам.
- `add_column` / `alter_column_type` в `datagen-api` (`evolve` поддерживает
  только `rename_column` и `drop_column`).
- Метод `validate_load` в DAG-ах `ods_load_*` — открытый вопрос
  (см. `docs/PLAN.md`).

## Проверено на этапе подготовки (см. docs/PLAN.md для полного списка рисков)

- `docker compose config` — синтаксис `docker-compose.yaml` валиден.
- `schema/sources/*.yaml` — парсятся, PK обязателен, граф FK ацикличен
  (проверено модулем `datagen-api/app/schema_loader.py`).
- `render_ddl.py` — корректно генерирует `CREATE TABLE` под оба диалекта
  в правильном топологическом порядке (проверено без подключения к БД).
- Полный `docker compose up` с реальными образами **не запускался** в рамках
  подготовки репозитория (тяжёлые образы, требует отдельной проверки).
