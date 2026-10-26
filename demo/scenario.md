# Сценарий демонстрации стенда

Пошаговая программа демонстрации: источники → загрузка в raw-слой → проверка в
Trino/pg-meta → партиции в silo → устойчивость к эволюции схемы источника.
Все запросы и JSON-запросы подготовлены — их можно копировать как есть.
Продолжительность — 30–40 минут. Теория к каждому шагу — в `README.md`.

## Подготовка (до демонстрации)

Стенд запущен (`docker compose up -d`, память Docker ≥ 16 ГБ), открыты
вкладки:

| Что | Где |
|---|---|
| Airflow UI | http://localhost:8089 (`admin123` / `admin123`) |
| Swagger UI sydafa | http://localhost:8090/docs |
| Консоль silo | http://localhost:9001 (`admin123` / `admin123`) |
| Spark Master UI | http://localhost:8080 |
| DBeaver | два подключения к источникам (создаются на шаге 2) + Trino (см. README 2.4) |
| Терминал | для генерации DAG-ов и psql к pg-meta |

Реквизиты источников (одинаковые для обоих): пользователь `sydafa`, пароль
`sydafa`. Порт bank (MSSQL) — `14330`, broker (Oracle) — `15211`.

---

## Шаг 1. Обновление источников (Airflow UI)

Источники «оживают» через DAG-и обновления — синтетические вставки,
обновления и удаления выполняются транзакционно через API sydafa.

1. Airflow UI → DAG-и `update_source_bank` и `update_source_broker`
   (расписания нет — запуск только ручной, кнопкой ▶ справа).
2. Запустить оба (можно параллельно), открыть Grid View.
3. Пока идут: таск `migrate` применяет батч из
   `airflow/plugins/migrations/{bank,broker}.json`: +100 клиентов, +100 счетов,
   +1000 сделок, +25000 транзакций; каскадное удаление 50 случайных сделок;
   точечные обновления статусов справочников.
4. Дождаться зелёного статуса обоих DAG-ов. В логе таска `migrate` виден список
   применённых операций.

**Что рассказать:** это имитация «живого» production-источника — между раундами
загрузки данные в нём меняются; именно эту дельту потом увидит инкрементальная
загрузка.

## Шаг 2. Источники через DBeaver (активация + просмотр)

СУБД-источники ленивые: контейнер поднимается по запросу и живёт ограниченное
время (TTL).

1. Swagger UI → `POST /sources/bank/activate` → «Try it out» → параметр
   `ttl_minutes=20` → «Execute». В ответе — объект `connection` с готовыми
   параметрами подключения (host `localhost`, port `14330`, database `sydafa`).
2. DBeaver → новое подключение → SQL Server: host `localhost`, port `14330`,
   database `sydafa`, пользователь `sydafa` / `sydafa`. Схема — `retail_bank`.
3. То же для Oracle: `POST /sources/broker/activate?ttl_minutes=20` → DBeaver →
   Oracle: host `localhost`, port `15211`, service name `sydafa`,
   `sydafa` / `sydafa`. Схема — `trading_core`.

Запросы к **bank (MSSQL)**:

```sql
SELECT count(*) FROM retail_bank.clients;
SELECT count(*) FROM retail_bank.accounts;
SELECT count(*) FROM retail_bank.deals;
SELECT count(*) AS rows_total, max(_batch_id) AS last_batch
FROM retail_bank.transactions;

SELECT TOP 10 * FROM retail_bank.clients ORDER BY client_id DESC;
```

Запросы к **broker (Oracle)** (колонка `_batch_id` в Oracle хранится в нижнем
регистре и требует кавычек):

```sql
SELECT count(*) FROM trading_core.clients;
SELECT count(*) AS rows_total, max("_batch_id") AS last_batch
FROM trading_core.transactions;

SELECT * FROM trading_core.clients ORDER BY client_id DESC FETCH FIRST 10 ROWS ONLY;
```

**Что рассказать:** в каждой таблице видна служебная колонка `_batch_id` — номер
партии наполнения; по ней работает инкрементальная загрузка transactions.
Читатель `sydafa` имеет права только на чтение.

## Шаг 3. Генерация DAG-ов загрузки (имитация CI/CD) и запуск

DAG-и загрузки не пишутся руками — генерируются из YAML-манифестов
(`ingestion/pipelines/`). В production это делает CI; здесь — одна команда.
Чтобы показать генерацию «вживую», начните с чистого состояния.

1. Терминал (из корня репозитория) — удалить сгенерированные DAG-и:

```bash
ls airflow/dags/generated/
rm -rf airflow/dags/generated
```

2. Airflow UI → подождать цикла парсинга (~полминуты) → четыре ingest-DAG-а
   **исчезли** из списка: их определяют только файлы в `airflow/dags/generated/`,
   никаких следов в базе Airflow они не оставляют (остались только
   `update_source_*`).
3. Сгенерировать DAG-и заново:

```bash
python3 ingestion/generate_dags.py
ls -la airflow/dags/generated/
python3 ingestion/generate_dags.py --check
```

4. Показать сгенерированные файлы: `ingest__rdbms__bank__main.py`,
   `ingest__rdbms__bank__refs.py` и два для broker; `--check` печатает
   `check: 0 difference(s)` — артефакты соответствуют манифестам.
5. Airflow UI → после цикла парсинга четыре DAG-а
   `ingest__rdbms__{bank,broker}__{main,refs}` **появились снова**. Открыть
   `ingest__rdbms__bank__main`: видна структура
   `generate_process_id → activate_source → TaskGroup на каждую сущность
   (log_start → load → log_end)`.
6. Запустить все четыре DAG-а (сначала банка, затем брокера — или все сразу).
7. Пока идут: Spark Master UI (localhost:8080) — работающие приложения
   (Running Applications), по одному на каждую загрузку.

**Что рассказать:** манифест = декларация «что грузить и как»; одна строка YAML
добавляет таблицу в озеро. Пул `ingest_loads` (Admin → Pools, 4 слота)
ограничивает одновременные Spark-приложения: transactions занимает 2 слота,
остальные — по 1.

## Шаг 4. Результат в Trino (DBeaver) и артефакты в pg-meta

**Trino** (подключение — README 2.4):

```sql
SHOW TABLES FROM raw.bank;
SHOW TABLES FROM raw.broker;

DESCRIBE raw.bank.retail_bank__clients;

SELECT count(*) FROM raw.bank.retail_bank__transactions;

SELECT __process_id, __batch_id, count(*) AS rows
FROM raw.bank.retail_bank__transactions
GROUP BY 1, 2 ORDER BY 1;

SELECT * FROM raw.bank.retail_bank__clients
WHERE __load_dttm = (SELECT max(__load_dttm) FROM raw.bank.retail_bank__clients);
```

**Что рассказать:** схема raw-таблицы = зеркало источника + три технические
колонки (`__load_dttm`, `__process_id`, `__batch_id`); каждый запуск DAG-а
оставил ровно одну порцию с одним `__process_id`; последний запрос — текущее
состояние источника (актуальный снапшот).

**pg-meta** (терминал; наружу база не опубликована — смотрим в контейнере):

```bash
docker compose exec pg-meta psql -U admin123 -d meta -c "
SELECT process_id, dag_id, run_id, status, ts
FROM ingestion.airflow_dagruns_log ORDER BY id DESC LIMIT 10;"

docker compose exec pg-meta psql -U admin123 -d meta -c "
SELECT process_id, entity_fqn, status, ts
FROM ingestion.airflow_tasks_log ORDER BY id DESC LIMIT 20;"

docker compose exec pg-meta psql -U admin123 -d meta -c "
SELECT entity_fqn, configured_mode, incremental_column, incremental_type,
       watermark, revision, last_batch_id
FROM ingestion.extraction_state ORDER BY source_id, entity_fqn;"

docker compose exec pg-meta psql -U admin123 -d meta -c "
SELECT batch_id, entity_fqn, mode, reason, status, attempt_count, row_count,
       watermark_before, watermark_after, lower_bound, upper_bound
FROM ingestion.extraction_batches ORDER BY batch_id DESC LIMIT 16;"
```

**Что рассказать:** у `transactions` первая партия — `bootstrap` (первичное
наполнение, без нижней границы) с зафиксированным `watermark_after`; у
снапшотных сущностей — партии `reason=bootstrap` в режиме `full_snapshot` без
водяного знака. Границы окна заморозились **до** чтения источника.

## Шаг 5. Повторный раунд: полный снапшот против инкремента

1. Снова запустить `update_source_bank` и `update_source_broker` (шаг 1) —
   источники выросли.
2. Снова запустить четыре ingest-DAG-а (шаг 3).
3. По завершении — запросы в Trino:

```sql
SELECT __process_id, count(*) AS rows
FROM raw.bank.retail_bank__clients GROUP BY 1 ORDER BY 1;

SELECT * FROM raw.bank.retail_bank__clients
WHERE __load_dttm = (SELECT max(__load_dttm) FROM raw.bank.retail_bank__clients);

SELECT __process_id, __batch_id, count(*) AS rows
FROM raw.bank.retail_bank__transactions GROUP BY 1, 2 ORDER BY 1;
```

**Что показать:** снапшотные таблицы получили **вторую полную порцию** (строк
в порции стало больше — источник вырос); у `transactions` вторая порция —
**только дельта** (~25 тыс. строк), история первой порции не тронута.

4. pg-meta — артефакты второго раунда:

```bash
docker compose exec pg-meta psql -U admin123 -d meta -c "
SELECT batch_id, reason, row_count, watermark_before, watermark_after, status
FROM ingestion.extraction_batches
WHERE entity_fqn = 'retail_bank.transactions' ORDER BY batch_id;"
```

**Что рассказать:** у второй партии `reason=regular`: окно
`lower_bound < _batch_id <= upper_bound` заморожено до чтения, `row_count` —
дельта, `watermark_after` продвинулся только после успешной записи.
Дополнительно можно показать no-op: запустить ingest-`main` банка ещё раз без
обновления источника — у `transactions` не появится новой порции (пустое окно
закрывается без чтения, `attempt_count=0`).

## Шаг 6. Партиционирование в консоли silo

1. http://localhost:9001 (`admin123` / `admin123`) → bucket `datalake`.
2. Путь `datalake / raw / bank / retail_bank__transactions / data` — каталоги
   вида `__load_dttm_day=2026-10-26` — дневные партиции загрузки; внутри —
   Parquet-файлы данных.
3. Рядом `datalake / raw / bank / retail_bank__transactions / metadata` —
   файлы метаданных Iceberg (манифесты и снапшоты — история коммитов таблицы).

**Что рассказать:** физическая раскладка повторяет логическую
(`каталог/схема/таблица`); партиции режутся по UTC-дню `__load_dttm`, поэтому
аналитика по дням загрузки читает минимум файлов; каждый append пишет только
новые файлы — прежние не меняются.

## Шаг 7. Стресс-тест: эволюция схемы источника

Показываем, от чего система **не падает**: удаление, переименование и
добавление колонки на источнике. Таблицы: `retail_bank.clients` (bank) и
`trading_core.clients` (broker) — снапшотные, изменения схемы пройдут в raw
автоматически.

Все запросы — Swagger UI, по одному «Try it out» на операцию (migrate сам
будит базу; активация из шага 2 ещё действует). Каждая операция — отдельная
транзакция; при желании три операции одного источника можно отправить и одним
батчем.

**7.1. Удалить колонку** — `POST /sources/bank/migrate`:

```json
{"operations": [{"op": "drop_column", "entity_fqn": "retail_bank.clients", "name": "client_type"}]}
```

`POST /sources/broker/migrate`:

```json
{"operations": [{"op": "drop_column", "entity_fqn": "trading_core.clients", "name": "is_qualified"}]}
```

**7.2. Переименовать колонку** — `POST /sources/bank/migrate`:

```json
{"operations": [{"op": "rename_column", "entity_fqn": "retail_bank.clients", "from_name": "full_name", "to_name": "client_name"}]}
```

`POST /sources/broker/migrate`:

```json
{"operations": [{"op": "rename_column", "entity_fqn": "trading_core.clients", "from_name": "full_name", "to_name": "client_name"}]}
```

**7.3. Добавить колонку** — `POST /sources/bank/migrate`:

```json
{"operations": [{"op": "add_column", "entity_fqn": "retail_bank.clients", "column": {"name": "email", "type": "string"}}]}
```

`POST /sources/broker/migrate`:

```json
{"operations": [{"op": "add_column", "entity_fqn": "trading_core.clients", "column": {"name": "email", "type": "string"}}]}
```

**7.4. Назначить генератор новой колонке** — `PUT /sources/bank/rules/retail_bank.clients`.
PUT перезаписывает профиль генераторов таблицы целиком, поэтому в теле — все
актуальные колонки (включая переименованную `client_name` и новую `email`):

```json
{
  "columns": {
    "client_id": {"kind": "sequence"},
    "client_name": {"kind": "faker", "provider": "name", "locale": "ru_RU", "null_ratio": 0.05},
    "status": {"kind": "choice", "values": ["Active", "Blocked", "VIP", "Archived"], "null_ratio": 0.05},
    "email": {"kind": "faker", "provider": "email", "null_ratio": 0.05}
  },
  "invariants": []
}
```

`PUT /sources/broker/rules/trading_core.clients`:

```json
{
  "columns": {
    "client_id": {"kind": "sequence"},
    "client_name": {"kind": "faker", "provider": "name", "locale": "ru_RU", "null_ratio": 0.05},
    "risk_profile": {"kind": "choice", "values": ["Conservative", "Moderate", "Aggressive"], "null_ratio": 0.05},
    "status": {"kind": "choice", "values": ["Active", "Blocked"], "null_ratio": 0.05},
    "email": {"kind": "faker", "provider": "email", "null_ratio": 0.05}
  },
  "invariants": []
}
```

**7.5.** Показать изменения на источнике в DBeaver:

```sql
SELECT TOP 10 * FROM retail_bank.clients ORDER BY client_id DESC;
SELECT * FROM trading_core.clients ORDER BY client_id DESC FETCH FIRST 10 ROWS ONLY;
```

Видно: `client_type`/`is_qualified` исчезли, `client_name` хранит прежние
значения (переименование сохранило данные), `email` у существующих строк NULL.

**7.6.** Запустить DAG-и обновления источников (новые строки получат значения
`email` по генератору), затем — четыре ingest-DAG-а.

## Шаг 8. Итоги эволюции схемы в raw и артефакты

**Trino:**

```sql
DESCRIBE raw.bank.retail_bank__clients;
DESCRIBE raw.broker.trading_core__clients;

SELECT __process_id, count(*) AS rows,
       count(client_type) AS client_type_filled,
       count(client_name) AS client_name_filled,
       count(email) AS email_filled
FROM raw.bank.retail_bank__clients
GROUP BY 1 ORDER BY 1;
```

**Что рассказать (главный финальный тезис):** загрузка после эволюции схемы
не упала и ничего не потеряла молча:

- `client_type` **сохранён** в raw (история читается по-старому), у новых
  порций NULL — удаление на источнике не удалило историю в озере;
- `client_name` **добавлен** (переименование на источнике выглядит как
  удалить+добавить): старые порции NULL, новые — со значениями;
- `email` добавлен автоматически: старые порции NULL, новая порция — со
  значениями (после назначения генератора и обновления источника);
- порядок колонок в таблице не важен: сопоставление по имени
  (`mergeSchema` + `accept-any-schema` + `check-ordering=false`, README 4.4).

**pg-meta** — последние партии:

```bash
docker compose exec pg-meta psql -U admin123 -d meta -c "
SELECT batch_id, entity_fqn, reason, status, row_count, finished_at
FROM ingestion.extraction_batches ORDER BY batch_id DESC LIMIT 8;"
```

Все партии загрузок после эволюции — `SUCCEEDED`: смена схемы источника не
требует правок манифестов и не трогает контракты инкремента.

---

## Резервные сюжетные линии (если останется время)

- **Ручной полный снимок:** запуск `ingest__rdbms__bank__main` с конфигурацией
  (кнопка ▶ → «Configure»):

```json
{"force_full_snapshot_fqns": ["retail_bank.transactions"], "reason": "Демо: ручная сверка полноты"}
```

  В pg-meta появится партия `reason=manual_full` без границ и с сохранённым
  водяным знаком; в Trino — полная порция transactions поверх истории.

- **No-op инкремента:** повторный запуск `ingest__rdbms__bank__main` без
  обновления источника — у `transactions` нет новой порции, в pg-meta партия с
  `row_count=0`, `attempt_count=0` (пустое окно закрывается без чтения).
