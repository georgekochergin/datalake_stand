# datalake_stand — PoC стенда КХД → DLH

Локальный docker-compose стенд: загрузка данных из реальных реляционных
источников (MSSQL, Oracle) в ODS-слой Iceberg **AS IS**, и демонстрация
двух способов трансформации данных внутри DataLake (Trino SQL и Spark).

## Структура директорий стенда

| Путь | Назначение |
|---|---|
| `airflow/` | Пользовательские DAG-и (каркас удерживается `.gitkeep`). Содержит плагин `airflow/plugins/sydafa_plugin.py` (`SydafaActivateOperator`). |
| `spark/`              | Пользовательские Spark-job-ы (каркас удерживается `.gitkeep`).                                                                          |
| `trino/`              | Пользовательский SQL (каркас удерживается `.gitkeep`).                                                                                  |
| `conf/airflow` | Dockerfile, requirements, пароли UI |
| `conf/spark` | Dockerfile, spark-defaults.conf |
| `conf/trino` | runtime-конфиг Trino (catalog, TLS, password.db) |
| `conf/silo` | `init-bucket.sh` |
| `conf/sydafa` | декларативный манифест `init.json` источников СУБД |
| `test/` | демо-DAG-и (`test/dags/`), spark-jobs, trino-sql, `test/sydafa/init.json`, `test.sh`, `run_flow.py`, `README.md`, override |
| `docker-compose.yaml` | каркас стенда без демо-наполнения |

Пользовательский код кладётся в `airflow/`, `spark/`, `trino/` на хосте — compose монтирует эти каталоги в контейнеры (а каталог `airflow/plugins` монтируется в `/opt/airflow/plugins:ro`). Демо поднимается только через `./test/test.sh` (override).

## Архитектура

```mermaid
flowchart TB
    ANALYST["Аналитик\n(Trino CLI / BI-инструмент)"]

    subgraph Sources["Источники данных (оркестрируются sydafa)"]
        MSSQL[("sydafa-mssql-mssql-source:1433\nmcr.microsoft.com/mssql/server:2022-CU27-ubuntu-22.04\nDB: sydafa\nuser: sydafa (только чтение)")]
        ORA[("sydafa-oracle-oracle-source:1521\ngvenzl/oracle-free:23-slim-faststart\nPDB: sydafa\nuser: sydafa (только чтение)")]
    end

    subgraph SydafaService["Эмулятор источников (подключаемый модуль sydafa)"]
        direction TB
        CFG1[("conf/sydafa/init.json")]
        CFG2[("test/sydafa/init.json (демо)")]
        API["sydafa:latest (FastAPI, Swagger /docs :8090)\nТранзакционная эволюция схем (API-first)\nСинтетическая генерация данных (POST /exec)\nЧестный TTL и Safe GET (RFC 7231)"]
        STATE[("sydafa_state volume:\nlive-схема и статус")]
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
        D1["DAG ods_load_mssql\ntask: activate_source (SydafaActivateOperator)\ntask: extract_load (SparkSubmitOperator)\ntask: validate_load (заглушка)"]
        D2["DAG ods_load_oracle\ntask: activate_source (SydafaActivateOperator)\ntask: extract_load (SparkSubmitOperator)\ntask: validate_load (заглушка)"]
        D3["DAG build_marts_demo\nschedule=None (вручную)\nзадача A: PythonOperator+TrinoHook / задача B: SparkSubmitOperator"]
    end

    API -- "Docker SDK: запуск и bootstrap" --> MSSQL
    API -- "Docker SDK: запуск и bootstrap" --> ORA
    API -- "Транзакционный DDL (schema/evolve)\nDML (append/update/delete)" --> MSSQL
    API -- "Транзакционный DDL (schema/evolve)\nDML (append/update/delete)" --> ORA

    D1 -- "task activate_source\n(POST /activate)" --> API
    D2 -- "task activate_source\n(POST /activate)" --> API

    D1 --> SM
    D2 --> SM
    MSSQL -- "JDBC read (user: sydafa)" --> SW
    ORA -- "JDBC read (user: sydafa)" --> SW
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

**Источники, управляемые sydafa**:

1. Откройте в браузере Swagger UI: `http://localhost:8090/docs`.
2. Выполните метод `POST /sources/{mssql|oracle}/activate?ttl_minutes=15` (нажмите "Try it out" -> "Execute").
3. Из ответа возьмите параметры подключения `connection`.
4. По истечении `ttl_minutes` контейнер аккуратно останавливается демоном sydafa. 

### Добавление источника на живом стенде

Манифест `init.json` является **стартовым семенем (Seed)** и применяется sydafa **строго один раз** при чистом томе состояния (`/state`). На уже запущенном стенде добавление и настройка источников выполняется **динамически через REST API**:

1. **Регистрация источника** (Orchestration API):
   ```bash
   # Регистрация нового инстанса СУБД (например, crm типа oracle)
   curl -X POST http://localhost:8090/sources \
     -H 'Content-Type: application/json' \
     -d '{"name": "crm", "type": "oracle"}'
   ```
2. **Создание схем, таблиц и наполнение данными** (Data & Schema API):
   Отправьте транзакционный батч операций через `POST /sources/{name}/exec` (или через Swagger UI: `http://localhost:8090/docs`). База автоматически запустится, создаст таблицы и сгенерирует данные.
3. **Внешний порт для DBeaver**:
   Если для нового источника требуется постоянный проброс порта на хост, укажите переменную `SYDAFA_EXT_PORT_<NAME>` в `docker-compose.yaml` (например, `SYDAFA_EXT_PORT_CRM=15211`) и перезапустите контейнер `sydafa`: `docker compose up -d sydafa`.

> **Сброс к init.json**: если требуется пересоздать базы с нуля строго по файлу `conf/sydafa/init.json`, выполните сброс томов Compose: `docker compose down -v`.

## Работа со стендом

### Управление источниками и данными — sydafa

Сервис **sydafa** (Synthetic Data Factory & DB Orchestrator) вынесен в отдельный независимый репозиторий [georgekochergin/sydafa](https://github.com/georgekochergin/sydafa) и поставляется в виде Docker-образа (`ghcr.io/georgekochergin/sydafa:latest` или локальный `sydafa:latest`).

На данном стенде `sydafa` эмулирует реляционные источники данных:
- Лениво оркестрирует контейнеры СУБД `sydafa-mssql-mssql-source` / `sydafa-oracle-oracle-source` через Docker API (данные живут в named volume);
- Создаёт рабочую базу/PDB `sydafa` и читателя `sydafa` / `sydafa` (только чтение); админский доступ извне заблокирован;
- Внешний доступ для DBeaver: `SYDAFA_EXT_PORT_MSSQL=14330` в `docker-compose.yaml`;
- REST API: транзакционный DDL/DML через `POST /sources/{source}/exec` и lifecycle (`activate` / `deactivate` / `status`).

Конфиг начального семени (Seed) стенда: `conf/sydafa/init.json`. Демо-init: `test/sydafa/init.json`.

REST-интерфейс: `http://localhost:8090`, интерактивная документация: `http://localhost:8090/docs`.

#### Основные эндпоинты

| Метод | Путь | Назначение |
| --- | --- | --- |
| GET | `/health` | Готовность API sydafa |
| GET | `/sources` | Список зарегистрированных источников |
| POST | `/sources` | Динамическая регистрация нового инстанса `{"name": "...", "type": "..."}` |
| DELETE | `/sources/{source}` | Удаление инстанса СУБД и контейнера |
| GET | `/sources/{source}` | Метаданные источника, статус reader, объект connection и плоский список таблиц (Safe GET, не будит базу) |
| GET | `/sources/{source}/schema` | Детальная live-схема источника: таблицы, колонки, типы, генераторы, PK/FK (Safe GET) |
| GET | `/sources/{source}/status` | Состояние контейнера (idle/running), TTL, реквизиты `connection` (Safe GET) |
| POST | `/sources/{source}/activate` | Физический запуск СУБД в Docker и установка TTL (`?ttl_minutes=15`) |
| POST | `/sources/{source}/deactivate` | Принудительная остановка контейнера для освобождения RAM |
| POST | `/sources/{source}/exec` | Единый транзакционный метод DDL + DML (All-or-Nothing). Будит базу при необходимости |

#### Документация и спецификация API sydafa

Вся детальная документация, правила интеграции и спецификации вынесены в репозиторий проекта:
- 📖 [Руководство по sydafa](https://github.com/georgekochergin/sydafa);
- 🔄 Спецификация транзакционного метода `POST /sources/{source}/exec` (DDL + DML);
- 🧬 Семантика генераторов синтетических данных (`sequence`, `faker`, `choice`, `random_decimal`, `random_date`).

---

### Сценарий подключения к внешней sydafa (раздельный контур)

Если сервис `sydafa` развернут на отдельном сервере или в отдельном независимом Compose-проекте, стенд КХД настраивается следующим образом:
1. В `docker-compose.yaml` стенда сервис `sydafa` отключается (комментируется блок `sydafa:`).
2. В файле `.env` (или переменных сервисов Spark/Airflow) задаются внешние адреса:
   ```bash
   SYDAFA_URL=http://<IP_ИЛИ_HOST_SYDAFA>:8090
   MSSQL_HOST=<IP_ИЛИ_HOST_SYDAFA>
   MSSQL_PORT=14330
   ORACLE_HOST=<IP_ИЛИ_HOST_SYDAFA>
   ORACLE_PORT=15210
   ```
3. Если оба Compose работают на одном локальном компьютере, в качестве хоста используется `host.docker.internal` (с директивой `extra_hosts: ["host.docker.internal:host-gateway"]`). Spark и Airflow подключаются под пользователем `sydafa` / `sydafa`.

---

### Демонстрационные пайплайны (в тестовом контуре)

На живом стенде каталоги `airflow/`, `spark/`, `trino/` предназначены для прикладного кода пользователя. 

Для проверки архитектуры и сквозного тестирования в каталоге `test/` подготовлены эталонные демонстрационные пайплайны (подключаются оверрайдом `test/docker-compose.override.yaml` или при запуске `./test/test.sh`):

#### 1. Демо-загрузка ODS-слоя Iceberg
Тестовые DAG-и в Airflow (`schedule=None` — ручной запуск или через `run_flow.py`):
- `ods_load_mssql`:
  1. Таск `activate_source` (`SydafaActivateOperator` из `airflow/plugins/sydafa_plugin.py`) отправляет `POST /sources/mssql/activate?ttl_minutes=15`, гарантируя запуск СУБД;
  2. Таск `extract_load` (`SparkSubmitOperator`) запускает `test/spark-jobs/load_ods_mssql.py`: выгружает `demo.*` по JDBC под учеткой `sydafa`/`sydafa` через внутреннюю Docker-сеть в таблицы Iceberg `iceberg.ods.mssql_customers`, `mssql_orders`, `mssql_products`;
  3. Таск `validate_load` (`PythonOperator`): валидация выгрузки.
- `ods_load_oracle`:
  1. Таск `activate_source` (`SydafaActivateOperator`) будит `oracle` через `POST /sources/oracle/activate?ttl_minutes=15`;
  2. Таск `extract_load` (`SparkSubmitOperator`) запускает `test/spark-jobs/load_ods_oracle.py`: выгружает `demo.*` по JDBC в таблицы Iceberg `iceberg.ods.oracle_customers`, `oracle_orders`, `oracle_products`;
  3. Таск `validate_load` (`PythonOperator`): валидация выгрузки.

Запуск вручную из UI Airflow или CLI (в тестовом контуре):
```bash
docker compose -f docker-compose.yaml -f test/docker-compose.override.yaml exec -T airflow-scheduler airflow dags unpause ods_load_mssql
docker compose -f docker-compose.yaml -f test/docker-compose.override.yaml exec -T airflow-scheduler airflow dags trigger ods_load_mssql
```

Проверка результата (Trino): `SELECT count(*) FROM iceberg.ods.mssql_orders;`

#### 2. Демо-построение детального (mart) слоя
DAG `build_marts_demo` демонстрирует расчет витрин двумя независимыми движками:
- `mart_via_trino` → `test/trino-sql/build_mart_trino_demo.sql`: создает витрину `iceberg.mart_trino.customer_totals` средствами Trino SQL;
- `mart_via_spark` → `test/spark-jobs/build_mart_spark_demo.py`: создает витрину `iceberg.mart_spark.customer_totals` средствами Spark.

---

## Тестирование стенда

Вся подробная информация по архитектуре тестового контура `dlh-test`, сценариям и проверкам краевых случаев вынесена в документацию **[test/README.md](test/README.md)**.

Для проверки стенда используются два сценария:
- **Полный сквозной e2e-прогон** (`./test/test.sh`):
  Автономно разворачивает изолированный тестовый проект `dlh-test` (не затрагивая рабочий стенд), выполняет сквозной флоу `run_flow.py` (readiness, генерация синтетики в sydafa, запуск Airflow DAG-ов, выгрузка Spark в Iceberg, расчет витрин в Trino и сверка контрольных сумм) и полностью гасит тестовое окружение с удалением томов.
- **Быстрая проверка запущенного стенда** (`./test/test.sh --verify`):
  За 5 секунд опрашивает ключевые сервисы (Nessie, sydafa, Trino HTTPS, Airflow API, Spark воркеры, таблицы СУБД) на уже работающем локальном стенде.

Критерий успешности: `FAIL=0`, exit-code `0`. При ошибках — см. [test/troubleshooting.md](test/troubleshooting.md).

Предусловия: Docker + Docker Compose, склонированный репозиторий, `.env`
(уже закоммичен). Интернет нужен только для первого `build` образов — повторные
запуски работают офлайн.

Для разработки (стенд остаётся поднятым) — обычный `docker compose up -d --build`.

## Учётные данные

Реквизиты доступа к сервисам стенда для пользователей и инструментов разработки:

- **Источники СУБД (MSSQL, Oracle)**:
  - Пользователь: `sydafa`
  - Пароль: `sydafa`
  - Назначение: чтение данных (`SELECT`) для Spark, Airflow, Trino и подключений из DBeaver.
- **Trino** (`https://localhost:8443`):
  - Пользователь: `admin`
  - Пароль: `admin`
  - Назначение: выполнение SQL-запросов через CLI или DBeaver.
- **Airflow UI** (`http://localhost:8089`):
  - Пользователь: `admin`
  - Пароль: `admin`
  - Назначение: веб-интерфейс оркестрации пайплайнов.
- **SILO (S3-хранилище)**:
  - Веб-консоль (`http://localhost:9001`): `admin` / `admin123`
  - S3 API (`http://localhost:9000`): `admin` / `admin123`
- **Базы метаданных (pg-catalog, airflow-postgres)**:
  - Пользователь: `admin`
  - Пароль: `admin` (настраиваются в файле `.env`).
- **Сервисы без авторизации**:
  - Spark Master UI: `http://localhost:8080`
  - Spark Worker UI: `http://localhost:8081`
  - sydafa Swagger UI: `http://localhost:8090/docs`
