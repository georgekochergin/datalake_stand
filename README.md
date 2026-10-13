# datalake_stand — PoC стенда КХД → DLH

Локальный docker-compose стенд: загрузка данных из реальных реляционных
источников (MSSQL, Oracle) в ODS-слой Iceberg **AS IS**, и демонстрация
двух способов трансформации данных внутри DataLake (Trino SQL и Spark).

## Структура директорий стенда

| Путь | Назначение |
|---|---|
| `airflow/` | Пользовательские DAG-и (сейчас пусто). Не коммитятся. |
| `spark/` | Пользовательские Spark-job-ы (пусто). Не коммитятся. |
| `trino/` | Пользовательский SQL (пусто). Не коммитятся. |
| `conf/airflow` | Dockerfile, requirements, пароли UI |
| `conf/spark` | Dockerfile, spark-defaults.conf |
| `conf/trino` | runtime-конфиг Trino (catalog, TLS, password.db) |
| `conf/silo` | `init-bucket.sh` |
| `conf/sydafa` | дефолтный источник `mssql.yaml` (внутренний `port:`, без `init.json`) |
| `test/` | демо-DAG-и, spark-jobs, trino-sql, `sydafa/init.json` + sources, `test.sh`, override |
| `docker-compose.yaml` | каркас стенда без демо-наполнения |

Пользовательский код кладётся в `airflow/`, `spark/`, `trino/` на хосте — compose монтирует эти каталоги в контейнеры. Демо поднимается только через `./test/test.sh` (override).

## Архитектура

```mermaid
flowchart TB
    ANALYST["Аналитик\n(Trino CLI / BI-инструмент)"]

    subgraph Sources["Источники данных (оркестрируются sydafa)"]
        MSSQL[("sydafa-mssql-source\nmcr.microsoft.com/mssql/server:2022-CU27-ubuntu-22.04\nDB: sydafa\nuser: sydafa (только чтение)")]
        ORA[("sydafa-oracle-source\ngvenzl/oracle-free:23-slim-faststart\nPDB: sydafa\nuser: sydafa (только чтение)")]
    end

    subgraph SydafaService["Эмулятор источников (подключаемый модуль sydafa)"]
        direction TB
        CFG1[("conf/sydafa/init.json")]
        CFG2[("test/sydafa/init.json (демо)")]
        API["sydafa:latest (FastAPI, Swagger /docs :8090)\nТранзакционная эволюция схем (API-first)\nСинтетическая генерация данных (POST /exec)"]
        STATE[("sydafa_state volume:\nlive-схема и состояние")]
        CFG1 -. "bind-mount ro" .-> API
        CFG2 -. "bind-mount ro" .-> API
        API --- STATE
    end

    subgraph Catalog["Каталог метаданных"]
        PGC[("pg-catalog\npostgres:16.15-bookworm")]
        NESSIE["nessie\nghcr.io/projectnessie/nessie:0.99.0\nIceberg REST API"]
        PGC --- NESSIE
    end

    SILO["silo\npgsty/silo:RELEASE.2026-09-16T00-00-00Z\nS3 API :9000, bucket warehouse"]

    subgraph SparkCluster["Spark standalone-кластер"]
        SM["spark-master :7077"]
        SW["spark-worker"]
        SM --- SW
    end

    TRINO["trino\ntrinodb/trino:479"]

    subgraph Orchestration["Airflow 3.3.2-python3.11 (LocalExecutor)"]
        D1["DAG demo/ods_load_mssql\ntask: extract_load (SparkSubmitOperator)\ntask: validate_load (заглушка, метод валидации — TBD)"]
        D2["DAG demo/ods_load_oracle\ntask: extract_load (SparkSubmitOperator)\ntask: validate_load (заглушка, метод валидации — TBD)"]
        D3["DAG demo/build_marts_demo\nschedule=None (вручную)\nзадача A: PythonOperator+TrinoHook / задача B: SparkSubmitOperator"]
    end

    API -- "Docker SDK: запуск и bootstrap" --> MSSQL
    API -- "Docker SDK: запуск и bootstrap" --> ORA
    API -- "Транзакционный DDL (schema/evolve)\nDML (append/update/delete)" --> MSSQL
    API -- "Транзакционный DDL (schema/evolve)\nDML (append/update/delete)" --> ORA

    D1 --> SM
    D2 --> SM
    MSSQL -- "JDBC read (user: reader)" --> SW
    ORA -- "JDBC read (user: reader)" --> SW
    SW -- "write ods.mssql_*, ods.oracle_* AS IS" --> SILO
    SM -. "catalog ops" .-> NESSIE

    D3 -- "task mart_via_trino" --> TRINO
    D3 -- "task mart_via_spark" --> SM
    TRINO -. "catalog ops" .-> NESSIE
    TRINO -- "read ods.* / write mart_trino.*" --> SILO
    SW -- "write mart_spark.*" --> SILO

    ANALYST --> TRINO

    style SydafaService stroke-dasharray: 6 4,stroke:#888,fill:#f5f5f5,color:#555
```

## Состав стенда

**Контур A (релевантно production DLH-архитектуре):**

| Компонент           | Образ/версия                                                                                     |
| ------------------- | ------------------------------------------------------------------------------------------------ |
| sydafa-mssql-source | `mcr.microsoft.com/mssql/server:2022-CU27-ubuntu-22.04`                                          |
| sydafa-oracle-source| `gvenzl/oracle-free:23-slim-faststart`                                                           |
| pg-catalog          | `postgres:16.15-bookworm`                                                                        |
| nessie              | `ghcr.io/projectnessie/nessie:0.99.0`                                                           |
| silo                | `pgsty/silo:RELEASE.2026-09-16T00-00-00Z`                                                        |
| spark-master/worker | база `tabulario/spark-iceberg:3.5.5_1.8.1` + `mssql-jdbc:12.10.0.jre11` + `ojdbc11:23.8.0.25.04` |
| trino               | `trinodb/trino:479`                                                                              |
| airflow             | `apache/airflow:3.3.2-python3.11`, LocalExecutor                                                 |

<br />

**Контур B (эмуляция источников данных, подключаемый компонент):** `sydafa` —
автономный сервис (Docker-образ `sydafa:latest`), оркестрирующий контейнеры СУБД,
предоставляющий REST API для транзакционной эволюции схем и FK-aware генерации синтетических данных.
Конфигурация живого стенда: `conf/sydafa/init.json`. Демо (оба источника + тестовые таблицы) — `test/sydafa/init.json`, подключается override-ом теста.

## Быстрый старт

```bash
docker compose up -d --build
```

Порты:

| Сервис                       | Порт на хосте |
| ---------------------------- | ------------- |
| sydafa (Swagger UI :8090)    | 8090          |
| nessie                       | 19120         |
| silo (S3 API / консоль)      | 9000 / 9001   |
| spark-master UI              | 8080          |
| spark-worker UI              | 8081          |
| trino (HTTPS)                | 8443          |
| airflow webserver/api-server | 8089          |

## Доступ к сервисам

### Web-интерфейсы

| Сервис | URL | Логин/пароль |
|---|---|---|
| Spark Master UI | http://localhost:8080 | без авторизации |
| Spark Worker UI | http://localhost:8081 | без авторизации |
| SILO (консоль, MinIO-совместимая) | http://localhost:9001 | `admin` / `admin123` (`SILO_ROOT_USER`/`SILO_ROOT_PASSWORD`) |
| sydafa (Swagger UI) | http://localhost:8090/docs | без авторизации (эмулятор источников СУБД) |
| Airflow | http://localhost:8089 | `admin` / `admin` (`AIRFLOW_UI_USER`/`AIRFLOW_UI_PASSWORD`) |

### Подключение из DBeaver (на машине, где запущен docker)

**Trino** — драйвер «Trino» (встроен в DBeaver):
- Host: `localhost`, Port: `8443`
- Use SSL: да (Trino работает по HTTPS с самоподписанным сертификатом)
- Catalog/Database: `iceberg`
- Username: `admin`, Password: `admin`
  (password-authenticator — `conf/trino/`)
- Driver properties: `SSL=true;SSLVerification=NONE`
- JDBC URL: `jdbc:trino://localhost:8443/iceberg?SSL=true&SSLVerification=NONE`

**Источники MSSQL/Oracle** не имеют постоянного host-порта. Инспекция только на чтение:

1. `POST http://localhost:8090/sources/{mssql|oracle}/activate?ttl_minutes=15`
2. Из ответа взять `host_port`.
3. DBeaver: Host `localhost`, Port = `host_port`, user `reader` / password `reader`.
   - MSSQL: Database `sydafa`, `encrypt=false;trustServerCertificate=true`
   - Oracle: Service Name `sydafa`
4. По истечении TTL контейнер останавливается. Админ `sydafa` снаружи недоступен (случайный пароль).

### Добавление источника на живом стенде

1. Положить yaml в `conf/sydafa/` (например `oracle.yaml`). Поле `port:` — внутренний порт СУБД.
2. При необходимости дописать блок в init (если используете init) — для демо он в `test/sydafa/init.json`.
3. `docker compose up -d sydafa` — не `test.sh` и не `down -v`.
4. Первый `/exec` или `/activate` поднимет контейнер; `init.json` применится, только если `live_schema` пуста.

## Работа со стендом

### Управление источниками и данными — sydafa

Сервис **sydafa** (Synthetic Data Factory & DB Orchestrator) вынесен в отдельный независимый репозиторий [georgekochergin/sydafa](https://github.com/georgekochergin/sydafa) и поставляется в виде Docker-образа (`ghcr.io/georgekochergin/sydafa:latest` или локальный `sydafa:latest`).

На данном стенде `sydafa` эмулирует реляционные источники данных:
- Лениво оркестрирует контейнеры СУБД `sydafa-mssql-source` / `sydafa-oracle-source` через Docker API (данные живут в named volume);
- Создаёт рабочую базу/PDB `sydafa` и читателя `sydafa` / `sydafa` (только чтение); пароль внутреннего администратора случайный и доступен только сервису;
- Внешний доступ для DBeaver: `SYDAFA_EXT_PORT_MSSQL=14330` в `docker-compose.yaml`;
- REST API: транзакционный DDL/DML через `POST /sources/{source}/exec` и lifecycle (`activate` / `deactivate` / `status`).

Конфиг живого стенда: `conf/sydafa/init.json`. Демо-init: `test/sydafa/init.json`.

REST-интерфейс: `http://localhost:8090`, интерактивная документация: `http://localhost:8090/docs`.

#### Основные эндпоинты

| Метод | Путь | Назначение |
| --- | --- | --- |
| GET | `/health` | Готовность API sydafa (не прогрев БД) |
| GET | `/sources` | Список зарегистрированных источников |
| GET | `/sources/{source}` | Метаданные источника, статус reader и список таблиц |
| GET | `/sources/{source}/schema` | Текущая live-схема источника (JSON) |
| GET | `/sources/{source}/status` | Состояние контейнера, `host_port` |
| POST | `/sources/{source}/activate` | Разбудить и опубликовать host-порт (`?ttl_minutes=15`) |
| POST | `/sources/{source}/deactivate` | Остановить контейнер |
| GET | `/tables/{source}/{schema}/{table}` | Схема конкретной таблицы |
| POST | `/sources/{source}/schema/evolve` | Транзакционная эволюция схем (All-or-Nothing rollback) |
| POST | `/tables/{source}/{schema}/{table}/append` | Генерация и вставка строк с соблюдением связей (FK) |
| POST | `/tables/{source}/{schema}/{table}/update` | Параметризованное обновление строк по предикату |
| POST | `/tables/{source}/{schema}/{table}/delete` | Удаление строк по предикату (с контролем связей FK) |

#### Документация и спецификация API sydafa

Вся детальная документация, правила интеграции и спецификации вынесены в репозиторий проекта:
- 📖 [Руководство по интеграции sydafa в другие проекты](https://github.com/georgekochergin/sydafa#интеграция-в-сторонние-проекты) (docker-compose, сеть, volumes, healthcheck);
- 🔄 [Спецификация API эволюции схемы (10 операций миграций)](https://github.com/georgekochergin/sydafa#транзакционная-эволюция-схем-post-sourcessourceschemaevolve);
- 🧬 [Семантика колонок и генераторов данных (`sequence`, `faker`, `choice`, `random_decimal`, `random_date`)](https://github.com/georgekochergin/sydafa#семантика-колонок-и-генераторов-данных-generator);
- 💾 [Спецификация DML API (`append`, `update`, `delete`)](https://github.com/georgekochergin/sydafa#манипуляция-данными-dml-api).

---

### Обновление ODS-слоя Iceberg

Отдельные DAG-и в Airflow (расписание `schedule=None` — запуск вручную):

- `ods_load_mssql` → `test/spark-jobs/load_ods_mssql.py`: читает `demo.*` из sydafa-mssql-source и перезаписывает (AS IS) `iceberg.ods.mssql_customers`, `mssql_orders`, `mssql_products`;
- `ods_load_oracle` → `test/spark-jobs/load_ods_oracle.py`: читает `demo.*` из sydafa-oracle-source и перезаписывает (AS IS) `iceberg.ods.oracle_customers`, `oracle_orders`, `oracle_products`.

Демо-DAG-и изолированы в `test/dags/demo/` и подключаются override-ом теста. Запуск — Airflow UI → DAGs → unpause & ▶, либо CLI:

```bash
docker compose exec -T airflow-scheduler airflow dags unpause ods_load_mssql
docker compose exec -T airflow-scheduler airflow dags trigger ods_load_mssql
docker compose exec -T airflow-scheduler airflow dags unpause ods_load_oracle
docker compose exec -T airflow-scheduler airflow dags trigger ods_load_oracle
```

Проверка результата (Trino): `SELECT count(*) FROM iceberg.ods.mssql_orders;`

### Обновление детального (mart) слоя

DAG `build_marts_demo` (вручную, `schedule=None`) — две независимые задачи:

- `mart_via_trino` → `trino/sql/build_mart_trino_demo.sql`: создаёт `iceberg.mart_trino.customer_totals` (`customer_id`, `orders_count`, `total_amount`) из `iceberg.ods.mssql_orders`;
- `mart_via_spark` → `test/spark-jobs/build_mart_spark_demo.py`: создаёт `iceberg.mart_spark.customer_totals` (`customer_id`, `orders_count`, `total_amount`) из `iceberg.ods.mssql_orders`.

Запуск:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger build_marts_demo
```

Результат — таблицы `iceberg.mart_trino.customer_totals` и `iceberg.mart_spark.customer_totals` (читаются из Trino).

## Полное тестирование стенда

Полный флоу работы стенда проверяется автономным тестом: он поднимает стек,
прогоняет все этапы в контейнере и в конце полностью гасит сборку.

Запуск (из корня репозитория):

```bash
./test/test.sh
```

Скрипт делает:
1. `docker compose down -v --remove-orphans` — чистит прежние контейнеры и volume-ы (прежние данные не влияют на запуск);
2. `docker compose up -d --build` — собирает и поднимает весь стек;
3. `docker compose run --rm test-runner` — в отдельном контейнере прогоняет тест:
   - проверяет готовность Nessie / sydafa / Trino / Airflow / Spark;
   - получает метаданные таблиц через API sydafa (`GET /sources/{source}`);
   - очищает таблицы источников (`truncate`) через единый `POST /sources/{source}/exec`;
   - наполняет таблицы `customers`, `products`, `orders` через единый `POST /sources/{source}/exec` (`append`);
   - активирует (unpause) и запускает демо-DAG-и `ods_load_mssql`, `ods_load_oracle`, сверяя выгрузку в `iceberg.ods.*` через Trino;
   - активирует и запускает витринный DAG `build_marts_demo`, проверяя витрины `iceberg.mart_trino.customer_totals` и `iceberg.mart_spark.customer_totals`;
   - печатает сводку `PASS=n FAIL=m`;
4. `docker compose down -v --remove-orphans` — в любом исходе гасит сборку: удаляет контейнеры, сети и volume-ы (`pg_catalog_data`, `airflow_pg_data`, `silo_data`, `sydafa_state`).

Результат: финальная строка `PASS=… FAIL=…`; exit-code 0 — всё прошло, при
FAIL — смотреть `test/troubleshooting.md`.
После прогона стенд погашен, данные удалены; образы остаются в локальном кеше.

Предусловия: Docker + Docker Compose, склонированный репозиторий, `.env`
(уже закоммичен). Интернет нужен только для первого `build` образов — повторные
запуски работают офлайн.

Для разработки (стенд остаётся поднятым) — обычный `docker compose up -d --build`.

## Учётные данные

Тестовый стенд — везде, где логин/пароль настраиваемы, используется
`admin` / `admin` (см. `.env`). Исключение по имени — системный
bootstrap-логин MSSQL `sa`: имя фиксировано образом, а пароль не может быть
буквально `admin` (MSSQL требует длину ≥8 и минимум 3 из 4 категорий символов),
поэтому у `sa` остаётся сильный пароль `AdminPassword1!`, а для доступа извне
`sydafa` автоматически создаёт логин `sydafa`/`sydafa` с ролью sysadmin (а для чтения — `reader`/`reader`).
SILO (консоль `:9001`) — `admin`/`admin123` (MinIO требует пароль ≥8 символов,
поэтому буквально `admin` невозможен), pg-catalog, airflow-postgres,
Airflow UI (`:8089`), Trino (`:8443`) — везде `admin`/`admin`. У Oracle системные SYS/SYSTEM и
технический суперпользователь `sydafa` — пароль `sydafa`, читатель — `reader` / `reader`.
