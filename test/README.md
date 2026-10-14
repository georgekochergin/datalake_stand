# Тестирование datalake_stand

В данном каталоге собраны сценарии сквозного (e2e) тестирования и быстрой верификации стенда КХД/DLH (Trino, Spark, Nessie, Airflow, SILO, Postgres, sydafa).

---

## 1. Режимы запуска

Все проверки запускаются через единый bash-скрипт `./test/test.sh`:

```bash
# 1. Полный сквозной прогон e2e (по умолчанию)
./test/test.sh

# 2. Быстрая проверка компонентов на уже запущенном стенде
./test/test.sh --verify
```

Критерий успешности: **`FAIL=0`**, код возврата (exit-code) **`0`**.

---

## 2. Архитектура тестового контура (`dlh-test`)

Полный e2e-прогон (`./test/test.sh`) полностью изолирован от продуктового стенда:
- Используется отдельный проект Compose: `COMPOSE_PROJECT_NAME=dlh-test`;
- Применяется оверрайд `test/docker-compose.override.yaml`, который:
  - Отключает публикацию внешних портов на хост (`ports: !override []`), предотвращая конфликты портов;
  - Монтирует тестовые DAG-и из `./test/dags` в `/opt/airflow/dags`;
  - Монтирует переиспользуемые плагины из `./airflow/plugins` в `/opt/airflow/plugins:ro`;
  - Монтирует тестовые Spark-джобы из `./test/spark-jobs` в `/opt/spark-jobs:ro`;
  - Монтирует SQL-скрипты Trino из `./test/trino-sql` в `/opt/airflow/sql:ro`;
  - Подключает тестовый манифест источников `./test/sydafa/init.json:ro`;
  - Запускает контейнер `test-runner` (`Dockerfile`), исполняющий сквозной сценарий `run_flow.py`.
- По завершении скрипт выполняет автоматическую очистку: останавливает контейнеры и удаляет тома (`docker compose -p dlh-test down -v --remove-orphans`).

---

## 3. Сквозной сценарий проверки (`run_flow.py`)

Скрипт `test/run_flow.py` выполняет пошаговую проверку всего жизненного цикла данных в озере:

1. **Readiness & Healthcheck**:
   - Ожидание доступности Nessie REST Catalog (`:19120`);
   - Ожидание готовности sydafa (`:8000/health`);
   - Ожидание готовности Trino HTTPS (`:8443`) с авторизацией `admin:admin`;
   - Ожидание готовности Airflow API (`:8080`) и получение JWT-токена;
   - Проверка регистрации активных воркеров Spark Master (`aliveworkers >= 1`).
2. **Интроспекция метаданных (Safe GET)**:
   - Запрос `GET /sources/{source}` для источников `mssql` и `oracle`;
   - Проверка структуры ответа (имя БД `sydafa`, учетная запись читателя `sydafa`, список таблиц `customers`, `products`, `orders`).
3. **Проверка безопасности и валидации инъекций**:
   - Попытка выполнить `update` с внедренным SQL в имени колонки (`customer_id; DROP TABLE demo.customers --`);
   - Проверка, что сервис отклоняет запрос с ошибкой **HTTP 422**.
4. **Транзакционная подготовка данных в источниках**:
   - Каскадная очистка (`truncate` с `cascade: true`) таблиц `orders`, `products`, `customers`;
   - Вставка фиксированного числа строк (`append`) через единый транзакционный метод `POST /sources/{source}/exec`.
5. **Исполнение пайплайнов Airflow (ODS слой)**:
   - Разблокировка (`unpause`) и триггер DAG-ов `ods_load_mssql` и `ods_load_oracle`;
   - Шаг 1 DAG: оператор `SydafaActivateOperator` отправляет `POST /sources/{source}/activate`, гарантируя запуск контейнера СУБД;
   - Шаг 2 DAG: оператор `SparkSubmitOperator` запускает джобу (`load_ods_mssql.py`, `load_ods_oracle.py`), которая читает данные по JDBC под учеткой `sydafa`/`sydafa` и перезаписывает таблицы Iceberg `iceberg.ods.mssql_*` и `iceberg.ods.oracle_*`.
6. **Верификация выгрузки ODS через Trino**:
   - Trino выполняет `SELECT count(*)` по всем выгруженным таблицам Iceberg в Nessie;
   - Сверка точного количества строк с объемом, сгенерированным на шаге 4.
7. **Исполнение витринного пайплайна (Mart слой)**:
   - Разблокировка и запуск DAG `build_marts_demo`;
   - Задача A: `TrinoHook` рассчитывает витрину через Trino SQL (`build_mart_trino_demo.sql`) в `iceberg.mart_trino.customer_totals`;
   - Задача B: `SparkSubmitOperator` рассчитывает витрину через Spark (`build_mart_spark_demo.py`) в `iceberg.mart_spark.customer_totals`.
8. **Финальная сверка витрин**:
   - Trino запрашивает агрегаты из обеих витрин и валидирует успешность расчетов.

---

## 4. Быстрая проверка (`test.sh --verify`)

Режим `--verify` предназначен для быстрой (5–10 секунд) диагностики уже работающего локального стенда без пересоздания контейнеров:
- Проверка портов и кодов ответа Nessie, sydafa `/health`, `/docs`, `/sources`;
- Проверка отклонения SQL-инъекций в sydafa (HTTP 422);
- Проверка аутентификации Airflow (`admin:admin`);
- Проверка Trino TLS и каталога Iceberg;
- Прямая проверка таблиц в MSSQL и Oracle изнутри контейнера под учетной записью читателя `sydafa`/`sydafa`;
- Проверка статуса воркеров Spark.
