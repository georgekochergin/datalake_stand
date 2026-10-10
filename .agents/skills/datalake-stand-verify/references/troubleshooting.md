# Диагностика отказов datalake_stand

## DAG-и не подхватываются в Airflow (dags list пуст)

Airflow 3.3.2 убрал/переименовал часть API из Airflow 2.x:
- `from airflow.datasets import Dataset` → ModuleNotFoundError: используйте
  `from airflow.sdk import Asset` (или вообще не нужен при `schedule=None`);
- `airflow.providers.trino.operators.trino.TrinoOperator` → удалён: заменено на
  `PythonOperator` + `airflow.providers.trino.hooks.trino.TrinoHook`;
- `outlets=[...]` нет в `PythonOperator` — убрано.
Проверка парсинга: `docker compose exec airflow-scheduler airflow dags list`
(должен вернуть список, а не «No data found»).

## DAG `ods_load_oracle` падает с ошибкой подключения

Сервис Oracle в стенде — `demo` (`ORACLE_DATABASE: demo`), а не `FREEPDB1`.
Драйвер Spark читает `ORACLE_SERVICE` из окружения воркера; если не задан —
дефолт в `spark/jobs/load_ods_oracle.py` теперь `demo`.

## Триггер DAG через REST не создаёт run / state ≠ success

- Auth: `POST /auth/token` (form `username/password`) → `access_token`;
  далее `Authorization: Bearer <token>`.
- REST в Airflow 3 — `/api/v2/...` (не `/api/v1`).
- Триггер: `POST /api/v2/dags/{dag_id}/dagRuns` body `{"dag_run_id": "..."}`;
  статус: `GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}` → поле `state`
  (`queued`/`running`/`success`/`failed`).

## datagen-api `update`/`delete` возвращают 422 или 409

- `422` — ошибка валидации формализованного `predicate`/`set`: либо имя колонки
  не входит в схему таблицы (allow-list в `datagen-api/app/generators.py`),
  либо `operator` вне набора `= != < <= > >=`, либо `predicate`/`set` пусты.
  Это защита от SQL-инъекции — произвольные SQL-фрагменты больше не принимаются.
- `409` — каскадная блокировка `delete`: на удаляемые строки ссылаются дочерние
  таблицы, а `cascade` не `true`.

## datagen-api падает при старте (exit 3)

- `CREATE DATABASE statement not allowed within multi-statement transaction` —
  pymssql оборачивает DDL в транзакцию; нужен `conn.autocommit(True)`
  (см. `datagen-api/app/provisioning.py`).
- `Foreign key ... references invalid table ...` — FK-ссылки в
  `datagen-api/app/render_ddl.py` были без квалификатора схемы; должно быть
  `REFERENCES {qualify(fk.ref_table)}`.

## Airflow :8089 не пускает admin/admin

- `airflow users create` сломан в Airflow 3.3.2 (баг
  `apache-airflow-providers-fab 3.9.0`). Используется SimpleAuthManager:
  список пользователей через `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS`,
  пароли — пре-заполненный файл
  `/opt/airflow/simple_auth_manager_passwords.json.generated` (кладётся в образ).
- Гонка `db migrate`: `airflow-webserver`/`airflow-scheduler` должны ждать
  `airflow-init` через `depends_on: condition: service_completed_successfully`.

## Trino

- Password-auth работает только по HTTPS. Нужны: `internal-communication.shared-secret`,
  `http-server.https.*` (keystore) и `http-server.authentication.allow-insecure-over-http=true`
  (чтобы внутренний HTTP :8080 для Airflow остался).
- bcrypt в `password.db` с cost < 8 → 500 `Minimum cost of BCrypt password must be 8`.
  Генерировать: `htpasswd -nbB -C 10 admin admin`.
- `Password not allowed for insecure authentication` — запрос без HTTPS или
  нет `allow-insecure-over-http`.

## SILO не стартует

- MinIO требует `MINIO_ROOT_PASSWORD` ≥ 8 символов → `admin123`, не `admin`.

## MSSQL `Cannot open database "demo"`

- БД `demo` и логин `admin`/`admin` создаёт `datagen-api` при первом старте.
  Если datagen-api падал — БД/логина нет.

## MSSQL: закреплённый образ

Образ закреплён на `2022-CU27-ubuntu-22.04` (совпадает с `2022-latest` на 2026-10-09). Версию можно поднять, взяв следующий `2022-CUxx-ubuntu-22.04` из каталога MCR (`https://mcr.microsoft.com/v2/mssql/server/tags/list`) и сверив, что конкретный тег реально резолвится.

## Nessie Iceberg REST

Каталог работает на `ghcr.io/projectnessie/nessie:0.99.0`. Для запуска нужны:
- образ с встроенным каталогом (в `projectnessie/nessie` на Docker Hub каталога нет — `/iceberg/v1/config` там 404);
- `nessie.catalog.default-warehouse` + `nessie.catalog.warehouses.<name>.location`;
- `nessie.catalog.service.s3.default-options.*` (endpoint, path-style-access, auth-type=STATIC, region) + секрет `nessie.catalog.secrets.*` с креденшелами SILO.

Клиенты (Trino/Spark) пишут данные в SILO напрямую, поэтому у них тоже должны быть S3-креды: `s3.aws-access-key`/`s3.aws-secret-key` (Trino) и `s3.access-key-id`/`s3.secret-access-key` (Spark), а `warehouse` = имени склада (`warehouse`), не `s3a://...`.
