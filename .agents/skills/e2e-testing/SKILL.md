---
name: e2e-testing
description: Use when developing, extending, or debugging e2e and smoke tests in datalake_stand, adding new data sources, DAGs, ODS/Mart pipelines, or troubleshooting test failures.
---

# Сквозное (e2e) тестирование стенда datalake_stand

## 1. Фундаментальный принцип тестирования кодовой базы
**Любой новый или измененный функционал пайплайнов, источников и оркестрации обязан немедленно сопровождаться соответствующими проверками в сценарии e2e-тестирования (`test/run_flow.py` и `test/test.sh`).**

Тесты стенда тестируют **актуальное состояние репозитория**:
- Если в репозитории настроены источники и DAG-и их наполнения — e2e проверяет запуск и успешность исполнения этих DAG-ов.
- Когда добавляются пайплайны выгрузки в ODS (Spark $\to$ Iceberg) или построение витрин Mart (Trino / Spark) — соответствующие шаги расширяют `test/run_flow.py`.

### Разграничение ответственности (Separation of Concerns)
- **Зона ответственности `sydafa`**: синтетическая генерация данных, случайные распределения, инварианты `cumulative_balance`, транзакционный коммит/откат (All-or-Nothing). Все это покрыто собственной тестовой базой sydafa.
- **Зона ответственности `datalake_stand`**: оркестрация и интеграция платформы. Если вызов `POST /sources/{source}/migrate` завершается со статусом `200` и DAG переходит в `state = success`, значит СУБД запущена, схема создана, данные сгенерированы и транзакция закоммичена. Прямой SQL-доступ к СУБД источников из `test-runner` избыточен и не используется.
- Валидация данных через SQL (`SELECT count(*)`) начинается там, где начинается зона ответственности Data Lake — на слоях Iceberg ODS и Mart через Trino/Spark.

---

## 2. Архитектура и изоляция тестового контура (`dlh-test`)

Запуск тестов полностью изолирован от живого стенда (`dlh-poc`):
- **Имя проекта**: `COMPOSE_PROJECT_NAME=dlh-test`.
- **Изоляция томов**: все Docker volumes (`dlh-test_sydafa_state`, `dlh-test_airflow_pg_data` и др.) изолированы от живого стенда.
- **Изоляция портов**: публикация внешних портов на хост отключена (`ports: !override []`), предотвращая конфликты портов (8090, 8089, 14330, 15211).
- **Изоляция СУБД в sydafa**: переменная `SYDAFA_CONTAINER_PREFIX=sydafa-test-` создает изолированные контейнеры (`sydafa-test-src-mssql-bank`, `sydafa-test-src-oracle-broker`).

### Механика синхронизации `test/.workspace/`
Для чистоты репозитория и предотвращения дублирования файлов:
1. В каталоге `test/` не хранится пользовательский код или демонстрационные схемы (каталог содержит только исполняемые файлы тестов: `test.sh`, `docker-compose.override.yaml`, `Dockerfile`, `run_flow.py`, `test_bank_broker.py`).
2. `test/test.sh` перед запуском изолированно копирует актуальные рабочие каталоги репозитория во временную директорию `test/.workspace/`:
   - `conf/` $\to$ `test/.workspace/conf/`
   - `airflow/` $\to$ `test/.workspace/airflow/`
   - `spark/` $\to$ `test/.workspace/spark/`
   - `trino/` $\to$ `test/.workspace/trino/`
3. `test/.workspace/` внесена в `.gitignore` и не попадает в систему контроля версий.
4. `test/docker-compose.override.yaml` монтирует тома для всех сервисов (`sydafa`, `airflow`, `spark`, `trino`) исключительно из `./test/.workspace/`.
5. При завершении (успешном, аварийном или прерывании по `trap EXIT INT TERM HUP`) скрипт `test.sh` автоматически удаляет директорию `test/.workspace/`.

---

## 3. Режимы запуска тестов

Скрипт `./test/test.sh`:

```bash
# 1. Полный сквозной прогон e2e (по умолчанию)
./test/test.sh

# 2. Быстрая smoke-проверка компонентов на уже поднятом стенде
./test/test.sh --verify
```

**Критерий успешности**: `FAIL=0`, exit-code `0`.

---

## 4. Сценарий сквозного тестирования (`test/run_flow.py`)

Контейнер `test-runner` (`test/Dockerfile` на базе `python:3.12-slim`) исполняет скрипт `run_flow.py`:

1. **Readiness & Healthcheck**:
   - `GET http://sydafa:8000/health` (код 200);
   - `GET http://sydafa:8000/docs` (код 200);
   - `GET http://sydafa:8000/sources` (список сконфигурированных источников содержит `bank` и `broker`);
   - Интроспекция схем: `GET /sources/{source}` возвращает таблицы `clients`, `accounts`, `deals`, `transactions`.
2. **Проверка безопасности sydafa**:
   - Попытка выполнить `update` с внедренным SQL в имени колонки (`client_id; DROP TABLE ...`);
   - Проверка возврата **HTTP 422** (защита от SQL-инъекций).
3. **Готовность Airflow**:
   - Аутентификация `POST http://airflow-webserver:8080/auth/token` с учетными данными `admin:admin` (SimpleAuthManager);
   - Опрос `/api/v2/dags/{dag_id}` до подтверждения регистрации DAG-ов `update_source_bank` и `update_source_broker`.
4. **Оркестрация и исполнение DAG-ов**:
   - Разблокировка (`unpause`) и триггер `POST /api/v2/dags/{dag_id}/dagRuns`;
   - Опрос состояния раннинга: `GET /api/v2/dags/{dag_id}/dagRuns/{run_id}` до получения статуса `state == "success"`.

---

## 5. Справочник траблшутинга и дебага

### DAG-и не подхватываются в Airflow (`airflow dags list` пуст)
- В Airflow 3.x используется `dag-processor`. Проверьте логи: `docker compose logs airflow-dag-processor`.
- Обратите внимание на отличия Airflow 3.x от 2.x:
  - `from airflow.datasets import Dataset` $\to$ удален: используйте `from airflow.sdk import Asset` (или опустите при `schedule=None`);
  - `TrinoOperator` $\to$ удален: используйте `PythonOperator` + `TrinoHook`;
  - REST API в Airflow 3 доступен по префиксу `/api/v2/...` (не `/api/v1`).
- Ручная проверка парсинга:
  ```bash
  docker compose exec airflow-scheduler airflow dags list
  ```

### Ошибки sydafa: HTTP 422 или 409
- **422 Unprocessable Entity**:
  - Нарушение формата DDL/DML операций;
  - Имя колонки в `predicate`/`set` не входит в схему таблицы (защита allow-list);
  - Недопустимый оператор (разрешены только `=`, `!=`, `<`, `<=`, `>`, `>=`);
  - Нарушение инварианта `cumulative_balance` при строгом режиме (`validate_existing: true`).
- **409 Conflict**:
  - Каскадная блокировка при `delete`/`truncate`: на строки ссылаются дочерние таблицы, а `cascade` не установлен в `true`.
- При любой ошибке 422/409/500 транзакция в СУБД откатывается целиком (All-or-Nothing).

### Airflow не принимает авторизацию admin/admin
- В образе используется SimpleAuthManager с пре-сгенерированным файлом `/opt/airflow/simple_auth_manager_passwords.json.generated`.
- Убедитесь, что `airflow-init` завершился успешно (`condition: service_completed_successfully`).

### MSSQL / Oracle lazy start
- sydafa создает СУБД и служебную БД reader при **первом обращении** к источнику (`/activate` или `/migrate`).
- `/health` 200 означает готовность API sydafa, а не запущенную СУБД. Вызов `SydafaActivateOperator` или `SydafaMigrateOperator` автоматически будит СУБД.

### Trino TLS и Nessie Iceberg REST
- Trino работает с HTTPS на порту 8443 (внутренний plain HTTP для Airflow включен через `allow-insecure-over-http=true`).
- Пароли bcrypt в `password.db` требуют cost $\ge 8$ (`htpasswd -nbB -C 10 admin admin`).
- Nessie REST Catalog работает на порту 19120 (`/iceberg/v1/config`). Хранилище S3 — MinIO (SILO) с бакетом `warehouse`.
