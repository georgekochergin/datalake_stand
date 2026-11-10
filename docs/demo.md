# Сценарий демонстрации стенда

Пошаговая программа демонстрации: источники → загрузка в [raw-слой](glossary.md#term-raw) → проверка в
Trino/pg-meta → партиции в silo → устойчивость к эволюции схемы источника.
Все запросы и JSON-запросы подготовлены — их можно копировать как есть.
Продолжительность — 30–40 минут. Параметры подключений и пояснения к шагам
приведены ниже.

## Содержание

- [Глоссарий](glossary.md)
- [Подготовка](#подготовка)
- [Шаг 1. Обновление источников](#шаг-1-обновление-источников-airflow-ui)
- [Шаг 2. Подключение к источникам](#шаг-2-источники-через-dbeaver-активация--просмотр)
- [Шаг 3. Генерация и запуск загрузок](#шаг-3-генерация-dag-ов-загрузки-имитация-cicd-и-запуск)
- [Шаг 4. Результаты в Trino и pg-meta](#шаг-4-результат-в-trino-dbeaver-и-артефакты-в-pg-meta)
- [Шаг 5. Повторная загрузка](#шаг-5-повторный-раунд-полный-срез-против-инкремента)
  - [Подготовка показа pg-meta](#51-подготовка-показа-pg-meta)
  - [Новый запуск без изменения источника](#52-новый-запуск-без-изменения-источника)
  - [Clear только успешной load-задачи](#53-clear-только-успешной-load-задачи)
  - [Полный Clear существующего запуска](#54-полный-clear-существующего-запуска)
  - [Новая загрузка после обновления источника](#55-новая-загрузка-после-обновления-источника)
  - [Что объяснить про повтор после ошибки](#56-что-объяснить-про-повтор-после-ошибки)
- [Шаг 6. Файлы в silo](#шаг-6-партиционирование-в-консоли-silo)
- [Шаг 7. Изменение схемы источника](#шаг-7-стресс-тест-эволюция-схемы-источника)
- [Шаг 8. Проверка изменённой схемы в raw](#шаг-8-итоги-эволюции-схемы-в-raw-и-артефакты)
- [Шаг 9. Таблица-снимок](#шаг-9-таблица-снимок-dag-__act_snap)

## Подготовка

### Ресурсы и окружение

Перед запуском стенда:

1. **Выделите Docker Desktop не менее 16 ГБ оперативной памяти.** Откройте
   Docker Desktop → Settings → Resources → Advanced, установите Memory limit
   в `16 GB` и нажмите Apply & Restart. Если используется Windows с WSL 2,
   лимит задаётся в файле `%UserProfile%\.wslconfig`: в секции `[wsl2]`
   укажите `memory=16GB`, затем выполните `wsl --shutdown` в PowerShell и
   перезапустите Docker Desktop.
2. **Остановите ненужные контейнеры и Compose-стеки.** В Docker Desktop →
   Containers выберите работающие контейнеры или группы Compose других
   проектов и нажмите Stop. Это освободит
   оперативную память и порты для стенда. Не удаляйте тома (volumes) с нужными
   данными и не выполняйте общий сброс Docker.
3. **Подготовьте Python-окружение для генератора DAG-ов.** Нужен Python 3.11
   или новее. Из корня репозитория выполните (bash/zsh):

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   python -m pip install -r ingestion/generator/requirements.txt
   ```

   Оставьте `.venv` активным для команд шага 3.
   В новом терминале активируйте его снова: `source .venv/bin/activate`.
   Файл `ingestion/generator/requirements.txt` содержит внешние зависимости
   генератора и используемых им модулей. При изменении
   зависимостей генератора обновляйте этот файл. Airflow и Spark для
   генерации DAG-ов на хосте не требуются.

### Запуск и подключения

Запустите стенд из корня репозитория (`docker compose up -d`) и откройте
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

### Исходное состояние

До демонстрации дождитесь готовности sydafa: `GET /health` на
http://localhost:8090/health должен вернуть HTTP 200. При первом старте сервис
инициализирует СУБД и схемы. Последующие обращения активируют источники по [TTL](glossary.md#term-ttl).
Сценарий рассчитан на исходную схему источников и отсутствие предыдущих
загрузок в [raw](glossary.md#term-raw)/pg-meta. На уже использованном стенде номера партий, количество
[порций](glossary.md#term-portion) и наличие колонок могут отличаться. Удаление файлов DAG-ов данные не
сбрасывает. Не выполняйте общий сброс томов ради демонстрации.

Красный Triggerer на странице Health — норма: триггерер на стенде не развёрнут,
отложенных (deferrable) операторов нет, он и не нужен.

---

## Шаг 1. Обновление источников (Airflow UI)

Источники «оживают» через DAG-и обновления — синтетические вставки,
обновления и удаления выполняются транзакционно через API sydafa.

1. Airflow UI → DAG-и `update_source_bank` и `update_source_broker`
   (расписания нет — запуск только ручной, кнопкой ▶ справа).
2. Запустить оба (можно параллельно), открыть Grid View.
3. Пока идут: таски `migrate_bank` и `migrate_broker` применяют наборы операций из
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

**Что рассказать:** между раундами загрузки данные источника меняются.
Следующая инкрементальная загрузка прочитает новые значения `_batch_id`.

## Шаг 2. Источники через DBeaver (активация + просмотр)

СУБД-источники ленивые: контейнер поднимается по запросу и живёт ограниченное
время ([TTL](glossary.md#term-ttl)).

1. Swagger UI → `POST /sources/bank/activate` → «Try it out» → параметр
   `ttl_minutes=20` → «Execute». В ответе — объект `connection` с готовыми
   параметрами подключения (host `localhost`, port `14330`, database `sydafa`).
2. DBeaver → новое подключение → SQL Server: host `localhost`, port `14330`,
   database `sydafa`, пользователь `sydafa` / `sydafa`. Схема — `retail_bank`.
3. То же для Oracle: `POST /sources/broker/activate?ttl_minutes=20` → DBeaver →
   Oracle: host `localhost`, port `15211`, service name `sydafa`,
   `sydafa` / `sydafa`, SID оставить пустым. Схема — `trading_core`.

Активацию можно повторить, если [TTL](glossary.md#term-ttl) истёк во время демонстрации (максимум —
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
наполнения источника. По ней работает инкрементальная загрузка transactions.
Читатель `sydafa` имеет права только на чтение.

## Шаг 3. Генерация DAG-ов загрузки (имитация CI/CD) и запуск

DAG-и загрузки не пишутся руками — генерируются из YAML-[манифестов](glossary.md#term-manifest)
(`ingestion/conf/pipelines/`). В этом сценарии генерация запускается вручную.
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
python3 -m ingestion.generator.generate_dags
ls -la airflow/dags/generated/
python3 -m ingestion.generator.generate_dags --check
```

4. Показать сгенерированные файлы: `ingest__rdbms__bank__main.py`,
   `ingest__rdbms__bank__refs.py` и два для broker; `--check` печатает
   `check: 0 difference(s)` — артефакты соответствуют [манифестам](glossary.md#term-manifest).
5. Airflow UI → после цикла парсинга четыре DAG-а
   `ingest__rdbms__{bank,broker}__{main,refs}` **появились снова**. Открыть
   `ingest__rdbms__bank__main`: видна структура
   `generate_process_id → activate_source → TaskGroup на каждую [сущность](glossary.md#term-entity)
   (log_start → load → log_end → publish_asset)`.
   `publish_asset` публикует событие готовности таблицы после успешной загрузки.
6. Запустить все четыре DAG-а (сначала банка, затем брокера — или все сразу).
7. Пока идут: Spark Master UI (localhost:8080) — работающие приложения
   (Running Applications), по одному на каждую загрузку.

### Проверка параллельного чтения

Где показать настройку четырёх
[JDBC-партиций](glossary.md#term-jdbc-partition) для transactions и отключение параллельного чтения для остальных:

1. **Конфигурация** (до запуска): `ingestion/conf/settings.yaml` —
   `spark.jdbc.num_partitions: 4`; `ingestion/conf/pipelines/bank/main.yaml` — у
   `transactions` нет `spark.jdbc_partitioning: false` (параллельное чтение
   разрешено), у `deals` и всех таблиц `refs` — `spark.jdbc_partitioning:
   false` (один поток). Колонка нарезки выбирается автоматически единой
   эвристикой: у всех демо-таблиц это целочисленный первичный ключ
   (`transaction_id`).
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

   Четыре [JDBC-партиции](glossary.md#term-jdbc-partition) — это настройка разбиения, а не гарантия четырёх
   одновременно видимых сессий: Spark может уменьшить число партиций при
   узком диапазоне, а выполнение зависит от доступных ядер и длительности
   чтения. На небольшом объёме сессии можно не успеть увидеть.

3. **В логе load-таска**: открыть лог
   `ingest_group.load_retail_bank__transactions.load` и показать команду
   Spark с `--num-jdbc-partitions 4` и JSON `--incremental-config`, содержащим
   contract (колонку и [канонический тип позиции](glossary.md#term-canonical)) и конкретный [lookback](glossary.md#term-lookback);
   у deals и таблиц refs передаётся `--no-jdbc-partitioning`. Ниже в том же
   логе — строка выбора колонки нарезки вида

   ```text
   jdbc partitioning: column=transaction_id step=primary_key window=[1..5000] partitions=4 of 4 requested
   ```

   Колонка, ступень эвристики, MIN/MAX окна чтения и фактическое число
   партиций. Это параметры запуска и журналируемое решение, а не
   подтверждение фактического числа соединений. Загрузчик не печатает
   физический план чтения отдельным вызовом `explain`.

Параллельная нарезка включается, только когда в окне чтения минимум ≠ максимум
колонки нарезки. Дельта одного запуска update-DAG (порядка 5000 строк с
разбросом `transaction_id`) уже даёт ненулевой диапазон — специально
готовить данные не нужно.

### Ограничение числа загрузок

**[Пул](glossary.md#term-pool) Airflow**: Admin → [Pools](glossary.md#term-pool) → `ingest_loads` (4 [слота](glossary.md#term-slot); transactions
занимает 2, остальные загрузки по 1). Во время загрузок колонки Running/Queued
[Slots](glossary.md#term-slot) оживают; на главной странице есть виджет [Pool](glossary.md#term-pool) [Slots](glossary.md#term-slot).

**Что рассказать:**

- [Манифест](glossary.md#term-manifest) описывает, какие таблицы загружать и с какими настройками.
- Чтобы добавить [полный срез](glossary.md#term-full) таблицы, достаточно записи с её `fqn`.
- [Пул](glossary.md#term-pool) `ingest_loads` ограничивает число одновременно допущенных загрузок.
  В нём 4 [слота](glossary.md#term-slot): transactions занимает 2, остальные загрузки — по 1.

## Шаг 4. Результат в Trino (DBeaver) и артефакты в pg-meta

**Trino** (DBeaver, подключение из раздела подготовки):

```sql
SHOW TABLES FROM raw.bank;
SHOW TABLES FROM raw.broker;

DESCRIBE raw.bank.retail_bank__clients;

SELECT count(*) FROM raw.bank.retail_bank__transactions;

SELECT __process_id, count(*) AS rows
FROM raw.bank.retail_bank__transactions
GROUP BY 1 ORDER BY 1;

SELECT * FROM raw.bank.retail_bank__clients
WHERE __load_dttm = (SELECT max(__load_dttm) FROM raw.bank.retail_bank__clients);
```

**Что рассказать:**

- В [raw](glossary.md#term-raw) сохранены колонки источника и две технические: `__load_dttm`, `__process_id`.
- `__process_id` связывает строки с исполнением загрузки.
- Без сбоев и повторов каждая загрузка оставляет одну [порцию](glossary.md#term-portion).
- Последний запрос показывает последний **непустой** [полный срез](glossary.md#term-full).
  Если источник опустел, новой [порции](glossary.md#term-portion) нет, и запрос вернёт прежний срез.
- Для [инкремента](glossary.md#term-increment) в pg-meta есть отдельная запись [партии](glossary.md#term-batch).
  [Полные срезы](glossary.md#term-full) в реестре `incremental_batches` не учитываются.

**pg-meta** (DBeaver, подключение из раздела подготовки; запросы поочерёдно):

```sql
SELECT process_id, dag_id, run_id, status, ts
FROM ingestion.airflow_dagruns_log ORDER BY id DESC LIMIT 10;

SELECT process_id, entity_fqn, status, ts
FROM ingestion.airflow_tasks_log ORDER BY id DESC LIMIT 20;

SELECT entity_fqn, incremental_column, incremental_type,
       watermark, revision, last_process_id
FROM ingestion.incremental_state ORDER BY source_id, entity_fqn;

SELECT process_id, entity_fqn, reason, status, attempt_count, row_count,
       watermark_before, watermark_after, lower_bound, lower_bound_op, upper_bound
FROM ingestion.incremental_batches ORDER BY process_id DESC LIMIT 16;
```

**Что рассказать:**

- В реестре есть только инкрементальные загрузки — `transactions` двух источников.
- Первая [партия](glossary.md#term-batch) имеет `reason=bootstrap`: нижней границы ещё нет.
- `watermark_after` хранит максимум после успешной загрузки.
- Границы окна сохранены **до** чтения строк источника.
- У [полных срезов](glossary.md#term-full) — справочников и сделок — записей в реестре нет.
  Их загрузки видны по `__process_id` и `__load_dttm` в [raw](glossary.md#term-raw).

## Шаг 5. Повторный раунд: полный срез против инкремента

1. Снова запустить `update_source_bank` и `update_source_broker` (шаг 1) —
   источники выросли.
2. Снова запустить четыре ingest-DAG-а (шаг 3).
3. По завершении — запросы в Trino:

```sql
SELECT __process_id, count(*) AS rows
FROM raw.bank.retail_bank__clients GROUP BY 1 ORDER BY 1;

SELECT * FROM raw.bank.retail_bank__clients
WHERE __load_dttm = (SELECT max(__load_dttm) FROM raw.bank.retail_bank__clients);

SELECT __process_id, count(*) AS rows
FROM raw.bank.retail_bank__transactions GROUP BY 1 ORDER BY 1;
```

**Что показать:**

- [Полные срезы](glossary.md#term-full) получили вторую полную [порцию](glossary.md#term-portion). В ней больше строк, поскольку источник вырос.
- У `transactions` добавлена только новая часть данных: не более 5000
  транзакций после одного обновления, с учётом каскадного удаления.
- Первая [порция](glossary.md#term-portion) [raw](glossary.md#term-raw) не изменилась.
- Изменения и удаления старых транзакций не переносятся [инкрементом](glossary.md#term-increment) по `_batch_id`.
  Это не CDC — механизм чтения всех событий изменения источника.

4. pg-meta (DBeaver) — артефакты второго раунда:

```sql
SELECT process_id, reason, row_count, watermark_before, watermark_after, status
FROM ingestion.incremental_batches
WHERE entity_fqn = 'retail_bank.transactions' ORDER BY process_id;
```

**Что рассказать:** у второй [партии](glossary.md#term-batch) `reason=regular`: окно
`lower_bound < _batch_id <= upper_bound` заморожено до чтения, `row_count` —
дельта, `watermark_after` продвинулся только после успешной записи.
Следующие подразделы показывают разные варианты повторного запуска.
Для них достаточно банка: `ingest__rdbms__bank__main`.
Дополнительное время демонстрации — ориентировочно 10–15 минут, в зависимости от ресурсов стенда.

### 5.1. Подготовка показа pg-meta

Дождитесь завершения всех загрузок. Не меняйте источник между действиями,
пока сценарий явно этого не потребует. Не запускайте одновременно другие
загрузки `retail_bank.transactions`.

Откройте рядом Airflow, pg-meta и Trino в DBeaver.
В pg-meta выполните три запроса и сохраните результаты для сравнения:

```sql
SELECT process_id, run_id, reason, status, attempt_count, row_count,
       watermark_before, lower_bound, lower_bound_op, upper_bound,
       observed_max, watermark_after, expected_revision, created_at, finished_at
FROM ingestion.incremental_batches
WHERE source_id = 'bank' AND entity_fqn = 'retail_bank.transactions'
ORDER BY process_id DESC LIMIT 10;

SELECT incremental_column, incremental_type, watermark, revision,
       last_process_id, updated_at
FROM ingestion.incremental_state
WHERE source_id = 'bank' AND entity_fqn = 'retail_bank.transactions';

SELECT process_id, run_id, status, ts
FROM ingestion.airflow_dagruns_log
WHERE dag_id = 'ingest__rdbms__bank__main'
ORDER BY id DESC LIMIT 20;
```

Для проверки строк в Trino:

```sql
SELECT __process_id, count(*) AS rows, count(DISTINCT __load_dttm) AS write_attempts
FROM raw.bank.retail_bank__transactions
GROUP BY __process_id ORDER BY __process_id DESC;
```

**Что показывают поля:**

- `run_id` — какой [DagRun](glossary.md#term-dagrun) смотрим в Airflow.
- `process_id` — номер [исполнения](glossary.md#term-execution), а не идентификатор процесса ОС.
- Одна строка `incremental_batches` — одна [партия загрузки](glossary.md#term-batch).
- `attempt_count` — сколько раз началось чтение [партии](glossary.md#term-batch).
  Это не номер попытки задачи в Airflow и не число вызовов `spark-submit`.
- `row_count` — число строк успешной [попытки](glossary.md#term-attempt), не вся история записей в озере.
- `revision` — версия состояния: растёт при успешной фиксации новой [партии](glossary.md#term-batch), даже без новых строк.

Короткая памятка ожидаемых результатов:

| Действие | `run_id` | `process_id` | Реестр и данные |
|---|---|---|---|
| Trigger без новых данных | Новый | Новый | Новая успешная [партия](glossary.md#term-batch), `row_count=0`, без новых строк |
| Clear только успешной `load` | Прежний | Прежний | Та же успешная [партия](glossary.md#term-batch), без изменений состояния и данных |
| Clear генератора и всех следующих задач | Прежний | Новый | Новая [партия](glossary.md#term-batch), старые записи сохраняются |
| Trigger после обновления источника | Новый | Новый | Новая [партия](glossary.md#term-batch), новые строки и продвижение [watermark](glossary.md#term-watermark) |

### 5.2. Новый запуск без изменения источника

1. **Не запускайте** `update_source_bank`.
2. В Airflow нажмите Trigger для `ingest__rdbms__bank__main`.
   Это новый [DagRun](glossary.md#term-dagrun), не перезапуск старого.
3. Дождитесь успеха. Повторите запросы подраздела 5.1.
4. Запишите новые `run_id` и `process_id`: они понадобятся для сравнения.

**Ожидаемый результат для `transactions`:**

- Добавлена [партия](glossary.md#term-batch) с `reason=regular`, `status=SUCCEEDED`.
- `lower_bound = upper_bound`, `lower_bound_op='>'`.
  На демо-наборе [lookback](glossary.md#term-lookback) по `_batch_id` равен нулю.
- `attempt_count=0`, `row_count=0`, `observed_max=NULL`.
- [Watermark](glossary.md#term-watermark) не изменился. `revision` выросла на 1,
  `last_process_id` указывает на новый номер.
- В Trino нет строк с новым `__process_id` для `transactions`.

**Что рассказать:** система зафиксировала успешную проверку отсутствия новых данных.
Spark-приложение запускается, но чтение строк пустого [окна](glossary.md#term-window)
и запись Iceberg пропускаются. Нулевой `attempt_count` не означает, что задача не выполнялась.
В том же DAG таблица `deals` загружается [полным срезом](glossary.md#term-full)
и дописывается заново — её не используем как пример отсутствия повторной записи.

### 5.3. Clear только успешной load-задачи

1. Откройте успешный запуск **второго раунда с данными** из начала шага 5.
   Выберите его по `run_id`, а не просто последний запуск.
2. Выберите задачу `ingest_group.load_retail_bank__transactions.load`.
3. Выполните Clear только этой задачи. Не включайте upstream, downstream,
   другие запуски или задачу `generate_process_id`.
4. Дождитесь повторного успеха задачи и откройте её новый лог.
5. Повторите запросы подраздела 5.1. Сравните запись выбранной [партии](glossary.md#term-batch).

**Ожидаемый результат:**

- `run_id` и `process_id` прежние. Новой [партии](glossary.md#term-batch) нет.
- Загрузчик находит `SUCCEEDED` и выводит сохранённый результат в лог.
  Строки источника не перечитываются, Iceberg не дополняется.
- `attempt_count`, `row_count`, `finished_at` выбранной [партии](glossary.md#term-batch) не изменяются.
- Состояние `incremental_state`, включая `revision`, не меняется.
  Если после выбранного запуска уже была другая загрузка, `last_process_id`
  остаётся номером той более поздней загрузки.
- Число строк и отметок записи в Trino не изменилось.

Журнал событий DAG-а может получить новое событие завершения при повторном переходе в успех.
Это не новая [партия](glossary.md#term-batch): журналы событий и реестр загрузок показывают разные факты.

### 5.4. Полный Clear существующего запуска

1. Откройте последний успешный запуск без новых данных из подраздела 5.2.
2. Очистите **все его задачи**, включая `generate_process_id`, `activate_source`
   и обе группы загрузки. В подтверждении проверьте набор задач и единственный выбранный запуск.
3. Дождитесь успеха и повторите запросы подраздела 5.1.

**Ожидаемый результат:**

- `run_id` остался прежним, но `generate_process_id` выделил новый `process_id`.
- Добавлена новая [партия](glossary.md#term-batch). Старая запись с тем же `run_id` сохранена.
- Источник не менялся: `row_count=0`, `attempt_count=0`, [watermark](glossary.md#term-watermark) прежний.
- `revision` выросла на 1, `last_process_id` обновился.
- В журнале DAG-а видны события с разными `process_id` для одного `run_id`.
- У `transactions` новых строк нет. У `deals` добавлен ещё один [полный срез](glossary.md#term-full).

**Что рассказать:** [Clear](glossary.md#term-clear) не удаляет историю pg-meta и данные озера.
Повтор генератора создаёт новое [исполнение](glossary.md#term-execution)
внутри прежнего [DagRun](glossary.md#term-dagrun).

### 5.5. Новая загрузка после обновления источника

1. Запустите `update_source_bank` и дождитесь успеха.
2. Через Trigger создайте новый запуск `ingest__rdbms__bank__main`.
3. После успеха повторите запросы подраздела 5.1.

**Ожидаемый результат для `transactions` без сбоев:**

- Новые `run_id`, `process_id` и [партия](glossary.md#term-batch) с `reason=regular`.
- Нижняя граница равна предыдущему [watermark](glossary.md#term-watermark),
  верхняя — максимуму `_batch_id` при планировании.
- `attempt_count=1`, `row_count>0`, `status=SUCCEEDED`.
- `watermark_after` продвинулся до фактически прочитанного максимума `observed_max`.
- `revision` выросла на 1, `last_process_id` указывает на новую загрузку.
- В Trino появилась новая [порция](glossary.md#term-portion) с этим `__process_id`.

Это обычный новый запуск: ручной Trigger здесь заменяет ожидание расписания.
Настройки манифеста менять не требуется. Все числовые номера берите из своих результатов,
а не из заранее заданного примера.

### 5.6. Что объяснить про повтор после ошибки

Предыдущие действия безопасно показывают повтор уже **успешной** загрузки.
Поведение незавершённой [партии](glossary.md#term-batch) отличается:

- [Retry](glossary.md#term-retry) или [Clear](glossary.md#term-clear) `load` без повтора генератора
  использует тот же `process_id` и сохранённые границы [окна](glossary.md#term-window).
- При начале чтения `attempt_count` увеличивается. Источник между [попытками](glossary.md#term-attempt) может измениться,
  даже если границы прежние.
- Если строки уже записались в Iceberg, а фиксация успеха в pg-meta не состоялась,
  повтор может записать их ещё раз с тем же `__process_id` и новым `__load_dttm`.
  Это гарантия [at-least-once](glossary.md#term-atleast), не отсутствие дублей.
- До повтора нужно убедиться, что Spark-приложение прежней [попытки](glossary.md#term-attempt) остановлено.
  Изменение статуса задачи в UI само по себе этого не подтверждает.

Не имитируйте сбой удалением данных, ручной сменой статусов реестра или остановкой
общих сервисов во время обычного демо. Сценарий выше не проверяет восстановление
после ошибки: этот случай здесь объясняется, а не воспроизводится.

## Шаг 6. Партиционирование в консоли silo

1. http://localhost:9001 (`admin123` / `admin123`) → bucket `datalake`.
2. Путь `datalake / raw / bank / retail_bank__transactions / data` — каталоги
   вида `__load_dttm_day=2026-10-26` — дневные партиции загрузки; внутри —
   Parquet-файлы данных.
3. Рядом `datalake / raw / bank / retail_bank__transactions / metadata` —
   файлы метаданных Iceberg (manifest-файлы и [снапшоты Iceberg](glossary.md#term-iceberg-snapshot) — история коммитов таблицы).

**Что рассказать:**

- Путь объектов повторяет логическое имя: `каталог/схема/таблица`.
- Дневные партиции определяются по `__load_dttm` в UTC.
- Фильтр по времени загрузки позволяет исключать ненужные партиции.
- Каждая загрузка добавляет новые файлы, прежние не меняет.

## Шаг 7. Стресс-тест: эволюция схемы источника

Показываем, от чего система **не падает**: удаление, переименование и
добавление колонки на источнике. Таблицы: `retail_bank.clients` (bank) и
`trading_core.clients` (broker) — [полные срезы](glossary.md#term-full), изменения схемы пройдут в [raw](glossary.md#term-raw)
автоматически.

Все запросы — Swagger UI, по одному «Try it out» на операцию (migrate сам
будит базу, даже если [TTL](glossary.md#term-ttl) активации из шага 2 истёк). Каждая операция — отдельная
транзакция. При желании отправьте три операции одного источника вместе:
они выполнятся в одной транзакции.

### 7.1. Удаление колонки

`POST /sources/bank/migrate`:

```json
{"operations": [{"op": "drop_column", "entity_fqn": "retail_bank.clients", "name": "client_type"}]}
```

`POST /sources/broker/migrate`:

```json
{"operations": [{"op": "drop_column", "entity_fqn": "trading_core.clients", "name": "is_qualified"}]}
```

### 7.2. Переименование колонки

`POST /sources/bank/migrate`:

```json
{"operations": [{"op": "rename_column", "entity_fqn": "retail_bank.clients", "from_name": "full_name", "to_name": "client_name"}]}
```

`POST /sources/broker/migrate`:

```json
{"operations": [{"op": "rename_column", "entity_fqn": "trading_core.clients", "from_name": "full_name", "to_name": "client_name"}]}
```

### 7.3. Добавление колонки

`POST /sources/bank/migrate`:

```json
{"operations": [{"op": "add_column", "entity_fqn": "retail_bank.clients", "column": {"name": "email", "type": "string"}}]}
```

`POST /sources/broker/migrate`:

```json
{"operations": [{"op": "add_column", "entity_fqn": "trading_core.clients", "column": {"name": "email", "type": "string"}}]}
```

### 7.4. Настройка генераторов данных

Вложенные методы rules-API меняют одну колонку за вызов.
Полный `PUT` заменяет весь профиль: пропущенное правило будет потеряно.
Используйте точечные вызовы. Для bank в Swagger UI:

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

### 7.5. Проверка схемы источника

Покажите изменения в DBeaver.

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

### 7.6. Загрузка после изменения схемы

Запустите DAG-и обновления источников: новые строки получат `email`.
Затем запустите четыре основных ingest-DAG-а.

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

- `client_type` **сохранён** в [raw](glossary.md#term-raw) (история читается по-старому), у новых
  [порций](glossary.md#term-portion) NULL — удаление на источнике не удалило историю в озере;
- `client_name` **добавлен** (переименование на источнике выглядит как
  удалить+добавить): старые [порции](glossary.md#term-portion) NULL, новые — со значениями;
- `email` добавлен автоматически: старые [порции](glossary.md#term-portion) NULL, новая [порция](glossary.md#term-portion) — со
  значениями (после назначения генератора и обновления источника);
- порядок колонок в таблице не важен: сопоставление по имени
  (`mergeSchema=true`, свойство таблицы `write.spark.accept-any-schema=true`
  и опция записи `check-ordering=false`). Новые колонки добавляются при записи,
  отсутствующие в источнике сохраняются в [raw](glossary.md#term-raw) с NULL в новых [порциях](glossary.md#term-portion).

**pg-meta** (DBeaver) — последние [партии](glossary.md#term-batch):

```sql
SELECT process_id, entity_fqn, reason, status, row_count, finished_at
FROM ingestion.incremental_batches ORDER BY process_id DESC LIMIT 8;
```

Все [партии](glossary.md#term-batch) загрузок после эволюции — `SUCCEEDED`: смена схемы источника не
требует правок [манифестов](glossary.md#term-manifest) и не трогает контракты [инкремента](glossary.md#term-increment).

## Шаг 9. Таблица-снимок: DAG `__act_snap`

1. В Airflow (DAGs) найти снимочный DAG банка: фильтр по тегу `act_snap` или
   по префиксу имени `ingest__rdbms__bank__main__act_snap_` (суффикс-алиас
   вычисляется из [FQN](glossary.md#term-fqn) [сущности](glossary.md#term-entity); у каждого инкрементального источника свой
   DAG-снимок — их ровно два: банк и брокер). Расписание не задано —
   запустить вручную (Trigger), без параметров.
2. Структура запуска та же, что у пайплайна: `generate_process_id`
   (свой номер процесса) → `activate_source` → группа `snap_...`:
   `log_start → load → log_end → publish_asset`. В [манифестах](glossary.md#term-manifest) явно задан
   `incremental.act_snap.airflow.pool`: `ingest_loads`, вес 2, чтобы снимки
   разделяли ограничение числа Spark-приложений с обычными загрузками.
   Это самостоятельная настройка, а не наследование [пула](glossary.md#term-pool) основной загрузки.
   Исполнитель — `IngestActSnapOperator`, Spark-приложение — `load_act_snap.py`.
3. По завершении — запросы в Trino (DBeaver):

```sql
SELECT count(*) AS raw_rows FROM raw.bank.retail_bank__transactions;
SELECT count(*) AS snap_rows, count(DISTINCT __process_id) AS portions,
       count(DISTINCT __load_dttm) AS load_marks
FROM raw.bank.retail_bank__transactions__act_snap;
```

**Что показать:**

- Снимок содержит весь прочитанный источник, а не окно [инкремента](glossary.md#term-increment).
- Для непустого снимка `portions = 1` и `load_marks = 1`:
  один процесс публикации и одна отметка времени.
- `raw_rows` и `snap_rows` не обязаны совпадать. [Raw](glossary.md#term-raw) хранит историю загрузок,
  включая строки, уже удалённые из источника, и возможные повторы.
  Сравните `snap_rows` с `count(*)` источника, пока данные не меняются.

4. Запустить DAG-снимок ещё раз, затем сравнить:

```sql
SELECT __process_id, count(*) AS rows
FROM raw.bank.retail_bank__transactions__act_snap GROUP BY 1 ORDER BY 1;
```

**Что рассказать:** после повтора [порция](glossary.md#term-portion) по-прежнему **одна** с новым
`__process_id` — [таблица-снимок](glossary.md#term-snapshot) **перезаписывается целиком одной атомарной
операцией Iceberg** (RTAS), а не дополняется, как [raw](glossary.md#term-raw). Читатель в любой
момент видит целиком предыдущий или целиком новый снимок.

5. Свежесть и автор публикации — из метаданных Iceberg:

```sql
SELECT max(committed_at) AS last_published_at
FROM "raw"."bank"."retail_bank__transactions__act_snap$snapshots";

SELECT value AS published_by_process_id
FROM "raw"."bank"."retail_bank__transactions__act_snap$properties"
WHERE key = 'snapshot.process_id';
```

**Что рассказать:** время последней публикации — момент коммита Iceberg,
автор — табличное свойство `snapshot.process_id`; оба читаются без pg-meta.
Реестр [инкремента](glossary.md#term-increment) (`incremental_batches`) снимком не пополняется — у
[инкремента](glossary.md#term-increment) и снимка раздельные потоки, а пустое чтение источника не
перезаписывает существующий непустой снимок (защита от затирания данных).

### Событие готовности снимка

Постоянная задача `publish_asset` — PythonOperator.
Для снимка она читает одну строку `__process_id` через Trino и публикует
`raw://bank/retail_bank.transactions/act_snap`, только если строка принадлежит
текущему процессу. Пустая таблица или старый процесс означают пропуск задачи
без события; ошибка Trino — ошибку задачи. Локальный файл результата не нужен.
У обычных загрузок тот же PythonOperator публикует [Asset](glossary.md#term-asset) без проверки.

### Независимые настройки снимка

Пример внутри инкрементальной [сущности](glossary.md#term-entity):

```yaml
incremental:
  contract:
    source_column: _batch_id
    position_type: bigint
  act_snap:
    schedule: "0 2 * * *"
    tags: [nightly]
    spark:
      jdbc_partitioning: true
    airflow:
      pool:
        name: ingest_loads
        slots: 2
```

`catchup`, `incremental`, `fqn`, `entities`, `max_active_runs` внутри `act_snap`
запрещены. Старый ключ `full_snapshot` не поддерживается. Снимок всегда имеет
`catchup=False`, все генерируемые DAG-и — `max_active_runs=1`.
