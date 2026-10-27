# Сценарий демонстрации стенда

Пошаговая программа демонстрации: источники → загрузка в raw-слой → проверка в
Trino/pg-meta → партиции в silo → устойчивость к эволюции схемы источника.
Все запросы и JSON-запросы подготовлены — их можно копировать как есть.
Продолжительность — 30–40 минут. Параметры подключений и пояснения к шагам
приведены ниже.

## Подготовка (до демонстрации)

Стенд запущен (`docker compose up -d`, память Docker ≥ 16 ГБ), открыты
вкладки:

| Что | Где |
|---|---|
| Airflow UI | http://localhost:8089 (`admin123` / `admin123`) |
| Swagger UI sydafa | http://localhost:8090/docs |
| Консоль silo | http://localhost:9001 (`admin123` / `admin123`) |
| Spark Master UI | http://localhost:8080 |
| DBeaver | два подключения к источникам (создаются на шаге 2), Trino и pg-meta (параметры ниже) |
| Терминал | для генерации DAG-ов и psql к pg-meta |

Реквизиты источников (одинаковые для обоих): пользователь `sydafa`, пароль
`sydafa`. Порт bank (MSSQL) — `14330`, broker (Oracle) — `15211`.

Подключения DBeaver, которые нужно подготовить заранее:

- **Trino**: драйвер Trino, host `localhost`, port `8443`, catalog/database
  `raw`, SSL включён; свойства драйвера `SSL=true`, `SSLVerification=NONE`
  (сертификат стенда самоподписанный). JDBC URL:
  `jdbc:trino://localhost:8443/raw?SSL=true&SSLVerification=NONE`.
- **pg-meta**: драйвер PostgreSQL, host `localhost`, port `55432`, database
  `meta`, схема `ingestion`. JDBC URL: `jdbc:postgresql://localhost:55432/meta`.

Для обоих подключений пользователь и пароль по умолчанию — `admin123` /
`admin123`. Если в `.env` переопределены `STAND_USER` и `STAND_PASSWORD`,
используйте их также для Airflow UI и консоли silo.

До демонстрации дождитесь готовности sydafa: `GET /health` на
http://localhost:8090/health должен вернуть HTTP 200. При первом старте сервис
инициализирует СУБД и схемы; последующие обращения активируют источники по TTL.
Сценарий рассчитан на исходную схему источников и отсутствие предыдущих
загрузок в raw/pg-meta. На уже использованном стенде номера партий, количество
порций и наличие колонок могут отличаться; удаление файлов DAG-ов данные не
сбрасывает. Не выполняйте общий сброс томов ради демонстрации.

Красный Triggerer на странице Health — норма: триггерер на стенде не развёрнут,
отложенных (deferrable) операторов нет, он и не нужен.

---

## Шаг 1. Обновление источников (Airflow UI)

Источники «оживают» через DAG-и обновления — синтетические вставки,
обновления и удаления выполняются транзакционно через API sydafa.

1. Airflow UI → DAG-и `update_source_bank` и `update_source_broker`
   (расписания нет — запуск только ручной, кнопкой ▶ справа).
2. Запустить оба (можно параллельно), открыть Grid View. Для демонстрации
   параллельного JDBC-чтения (шаг 3) запустите каждый update-DAG **дважды
   подряд**: в таблицах появятся строки с двумя разными `_batch_id`, и окно
   чтения даст ненулевой диапазон для параллельной нарезки.
3. Пока идут: таски `migrate_bank` и `migrate_broker` применяют батчи из
   `airflow/plugins/migrations/{bank,broker}.json`. На каждый запуск каждого
   источника: +20 клиентов, +20 счетов, +200 сделок, +5000 транзакций;
   каскадное удаление 10 сделок и связанных с ними строк. Поэтому итоговый
   прирост сделок — 190, а транзакций — не обязательно ровно 5000.
   В bank обновляются до 2 статусов клиентов на `VIP`, до 2 статусов счетов
   на `Frozen` и до 10 статусов сделок на `Closed`; в broker — до 2 профилей
   риска клиентов на `Aggressive`, до 2 статусов счетов на `Closed` и до 10
   значений рынка сделок на `OTC`.
4. Дождаться зелёного статуса обоих DAG-ов. В логе соответствующего таска
   виден результат применения операций.

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
   `sydafa` / `sydafa`, SID оставить пустым. Схема — `trading_core`.

Активацию можно повторить, если TTL истёк во время демонстрации (максимум —
30 минут). Для Oracle имена схем, таблиц и колонок созданы в нижнем регистре:
в SQL заключайте их в двойные кавычки.

Запросы к **bank (MSSQL)**:

```sql
SELECT count(*) FROM retail_bank.clients;
SELECT count(*) FROM retail_bank.accounts;
SELECT count(*) FROM retail_bank.deals;
SELECT count(*) AS rows_total, max(_batch_id) AS last_batch
FROM retail_bank.transactions;

SELECT TOP 10 * FROM retail_bank.clients ORDER BY client_id DESC;
```

Запросы к **broker (Oracle)**:

```sql
SELECT count(*) FROM "trading_core"."clients";
SELECT count(*) AS rows_total, max("_batch_id") AS last_batch
FROM "trading_core"."transactions";

SELECT * FROM "trading_core"."clients" ORDER BY "client_id" DESC FETCH FIRST 10 ROWS ONLY;
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
rm airflow/dags/generated/ingest__*.py
```

Если каталог или файлы ещё не созданы, пропустить удаление и перейти к генерации.

2. Airflow UI → подождать цикла парсинга (каталог пересканируется за 10 секунд,
   обновление списка может занять больше одного цикла) → четыре ingest-DAG-а
   становятся неактивными или скрываются фильтром активных DAG-ов: их
   определяют файлы в `airflow/dags/generated/`. История запусков в базе
   Airflow сохраняется; `update_source_*` остаются активными.
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
   (log_start → load → log_end → publish_asset)`.
   `publish_asset` публикует событие готовности таблицы после успешной загрузки.
6. Запустить все четыре DAG-а (сначала банка, затем брокера — или все сразу).
7. Пока идут: Spark Master UI (localhost:8080) — работающие приложения
   (Running Applications), по одному на каждую загрузку.

**Параллелизм JDBC и пулы Airflow.** Где показать настройку четырёх
JDBC-партиций для transactions и отключение параллельного чтения для остальных:

1. **Конфигурация** (до запуска): `ingestion/settings.yaml` —
   `spark.jdbc.num_partitions: 4`; `ingestion/pipelines/bank/main.yaml` — у
   `transactions` нет `spark.parallelism: false` (параллельное чтение
   разрешено, колонка нарезки — `_batch_id`), у `deals` и всех таблиц `refs`
   — `spark.parallelism: false` (один поток).
2. **Живьём на источнике (MSSQL)**: пока идёт `ingest__rdbms__bank__main`, в
   DBeaver (bank) выполнить — можно увидеть соединения чтения transactions
   и deals (Login `sydafa`, HostName — контейнер spark-worker):

```sql
EXEC sp_who2;
```

   **Живьём на источнике (Oracle)**: пока идёт `ingest__rdbms__broker__main`,
   в DBeaver (broker) выполнить — сессии чтения
   `trading_core.transactions` (program `JDBC Thin Client`).
   Для видимости `v$session` читателю нужен `SELECT ANY DICTIONARY` — его
   выдаёт provisioning sydafa:

```sql
SELECT sid, serial#, username, program, status, osuser
FROM v$session
WHERE username = 'SYDAFA' AND program LIKE 'JDBC%';
```

   Четыре JDBC-партиции — это настройка разбиения, а не гарантия четырёх
   одновременно видимых сессий: Spark может уменьшить число партиций при
   узком диапазоне, а выполнение зависит от доступных ядер и длительности
   чтения. На небольшом объёме сессии можно не успеть увидеть.

3. **В логе load-таска**: открыть лог
   `ingest_group.load_retail_bank__transactions.load` и показать команду
   Spark с `--num-partitions 4` и `--incremental-column _batch_id`;
   у deals/clients передаётся `--no-parallelism`. Это параметры запуска,
   а не подтверждение фактического числа соединений. Загрузчик не печатает
   физический план чтения отдельным вызовом `explain`.

Параллельная нарезка включается, только когда в окне чтения минимум ≠ максимум
колонки нарезки — поэтому шаг 1 запускает update-DAG-и дважды: у дельты из
одной партии `_batch_id` диапазон нулевой, и чтение честно идёт одним
потоком.

**Пул Airflow**: Admin → Pools → `ingest_loads` (4 слота; transactions
занимает 2, остальные загрузки по 1). Во время загрузок колонки Running/Queued
Slots оживают; на главной странице есть виджет Pool Slots.

**Что рассказать:** манифест = декларация «что грузить и как»; одна строка YAML
добавляет таблицу в озеро. Пул `ingest_loads` (Admin → Pools, 4 слота)
ограничивает одновременные Spark-приложения: transactions занимает 2 слота,
остальные — по 1.

## Шаг 4. Результат в Trino (DBeaver) и артефакты в pg-meta

**Trino** (DBeaver, подключение из раздела подготовки):

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

**pg-meta** (DBeaver, подключение из раздела подготовки; запросы поочерёдно):

```sql
SELECT process_id, dag_id, run_id, status, ts
FROM ingestion.airflow_dagruns_log ORDER BY id DESC LIMIT 10;

SELECT process_id, entity_fqn, status, ts
FROM ingestion.airflow_tasks_log ORDER BY id DESC LIMIT 20;

SELECT entity_fqn, configured_mode, incremental_column, incremental_type,
       watermark, revision, last_batch_id
FROM ingestion.extraction_state ORDER BY source_id, entity_fqn;

SELECT batch_id, entity_fqn, mode, reason, status, attempt_count, row_count,
       watermark_before, watermark_after, lower_bound, upper_bound
FROM ingestion.extraction_batches ORDER BY batch_id DESC LIMIT 16;
```

**Что рассказать:** у `transactions` первая партия — `bootstrap` (первичное
наполнение, без нижней границы) с зафиксированным `watermark_after`; у
снапшотных сущностей — партии `reason=regular` в режиме `full_snapshot` без
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
**только дельта** (не более 5000 новых транзакций после одного update-батча,
с учётом каскадного удаления), история первой порции не тронута. Изменения и
удаления старых транзакций не переносятся этим append-инкрементом по `_batch_id`:
это не CDC.

4. pg-meta (DBeaver) — артефакты второго раунда:

```sql
SELECT batch_id, reason, row_count, watermark_before, watermark_after, status
FROM ingestion.extraction_batches
WHERE entity_fqn = 'retail_bank.transactions' ORDER BY batch_id;
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
будит базу, даже если TTL активации из шага 2 истёк). Каждая операция — отдельная
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

**7.4. Назначить генераторы точечно** — вложенные ручки rules-API меняют одну
колонку за вызов, не пересылая весь профиль (полный `PUT` перезаписывает
профиль целиком, и забытый ключ молча терял бы генератор). Для bank в
Swagger UI, по одному вызову:

`PUT /sources/bank/rules/retail_bank.clients/columns/email` — генератор новой
колонки:

```json
{"kind": "faker", "provider": "email", "null_ratio": 0.05}
```

`PUT /sources/bank/rules/retail_bank.clients/columns/client_name` — перенос
генератора на переименованную колонку:

```json
{"kind": "faker", "provider": "name", "locale": "ru_RU", "null_ratio": 0.05}
```

`DELETE /sources/bank/rules/retail_bank.clients/columns/full_name` — правило
старого имени больше не нужно.

Для broker — отдельные вызовы:

`PUT /sources/broker/rules/trading_core.clients/columns/email`:

```json
{"kind": "faker", "provider": "email", "null_ratio": 0.05}
```

`PUT /sources/broker/rules/trading_core.clients/columns/client_name`:

```json
{"kind": "faker", "provider": "name", "locale": "ru_RU", "null_ratio": 0.05}
```

`DELETE /sources/broker/rules/trading_core.clients/columns/full_name`.

В каждом ответе возвращается весь профиль таблицы — удобно показать, что
остальные генераторы не тронуты. (Инварианты связности добавляются на таблицу
по одному — `POST /sources/{source}/rules/{fqn}/invariants`; справочникам
клиентов они не нужны, поэтому на демо не выполняются.)

**7.5.** Показать изменения на источнике в DBeaver:

Для bank:

```sql
SELECT TOP 10 * FROM retail_bank.clients ORDER BY client_id DESC;
```

Для broker:

```sql
SELECT * FROM "trading_core"."clients" ORDER BY "client_id" DESC FETCH FIRST 10 ROWS ONLY;
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
  (`mergeSchema=true`, свойство таблицы `write.spark.accept-any-schema=true`
  и опция записи `check-ordering=false`). Новые колонки добавляются при записи,
  отсутствующие в источнике сохраняются в raw с NULL в новых порциях.

**pg-meta** (DBeaver) — последние партии:

```sql
SELECT batch_id, entity_fqn, reason, status, row_count, finished_at
FROM ingestion.extraction_batches ORDER BY batch_id DESC LIMIT 8;
```

Все партии загрузок после эволюции — `SUCCEEDED`: смена схемы источника не
требует правок манифестов и не трогает контракты инкремента.
