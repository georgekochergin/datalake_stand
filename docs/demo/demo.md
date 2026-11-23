# Сценарий демонстрации стенда

Пошаговый сценарий демонстрации. Всё показанное можно повторить
самостоятельно: запросы и команды подготовлены — копируются как есть.

## Содержание

- [Глоссарий](../glossary.md)
- [Подготовка](#подготовка)
- [Шаг 1. Обновление источников](#шаг-1-обновление-источников)
- [Шаг 2. Генерация DAG-ов и запуск загрузок](#шаг-2-генерация-dag-ов-и-запуск-загрузок)
  - [Многопоточное чтение](#многопоточное-чтение)
  - [Пул загрузок](#пул-загрузок)
- [Шаг 3. Результат в Trino и pg-meta](#шаг-3-результат-в-trino-и-pg-meta)
- [Шаг 4. Повторный раунд: полный срез против инкремента](#шаг-4-повторный-раунд-полный-срез-против-инкремента)
- [Шаг 5. Повторы запусков и реестр pg-meta](#шаг-5-повторы-запусков-и-реестр-pg-meta)
- [Шаг 6. Файлы в silo](#шаг-6-файлы-в-silo)
- [Шаг 7. Эволюция схемы источника](#шаг-7-эволюция-схемы-источника)
- [Шаг 8. Итоги эволюции схемы в raw](#шаг-8-итоги-эволюции-схемы-в-raw)
- [Шаг 9. Таблица-снимок act_snap](#шаг-9-таблица-снимок-act_snap)

## Подготовка

### Ресурсы и окружение

1. Выделите Docker-хосту не менее 16 ГБ оперативной памяти и остановите
   посторонние контейнеры и Compose-стеки. Как выделить память —
   [требования к среде](../../README.md#требования-к-среде).
2. Подготовьте Python-окружение генератора DAG-ов (нужен Python 3.11 или
   новее). Из корня репозитория:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   python -m pip install -r ingestion/generator/requirements.txt
   ```

### Запуск и подключения

Запустите стенд из корня репозитория (`docker compose up -d`) и откройте
вкладки:

| Что | Где |
|---|---|
| Airflow UI | http://localhost:8089 (`admin123` / `admin123`) |
| Консоль silo | http://localhost:9001 (`admin123` / `admin123`) |
| Spark Master UI | http://localhost:8080 |
| DBeaver | Trino и pg-meta (параметры ниже) |
| Терминал | для генерации DAG-ов и скрипта миграции |

Подключения DBeaver, которые готовятся заранее:

- **Trino**:
  - драйвер: Trino
  - host: `localhost`
  - port: `8443`
  - catalog/database: `raw`
  - логин: `admin123`
  - пароль: `admin123`
  - SSL: включён, свойства драйвера `SSL=true`, `SSLVerification=NONE`
    (сертификат стенда самоподписанный)
  - JDBC URL: `jdbc:trino://localhost:8443/raw?SSL=true&SSLVerification=NONE`
- **pg-meta**:
  - драйвер: PostgreSQL
  - host: `localhost`
  - port: `55432`
  - database: `meta`, схема `ingestion`
  - логин: `admin123`
  - пароль: `admin123`
  - JDBC URL: `jdbc:postgresql://localhost:55432/meta`

### Исходное состояние

- Сценарий рассчитан на исходную схему источников и отсутствие предыдущих
  загрузок в [raw](../glossary.md#term-raw)/pg-meta. На уже использованном
  стенде номера [партий](../glossary.md#term-batch), число [порций](../glossary.md#term-portion)
  и состав колонок будут отличаться.
- Красный Triggerer на странице Health — норма: отложенные операторы на стенде
  не используются.
- Общий сброс томов ради демонстрации не выполняйте.

---

## Шаг 1. Обновление источников

Источники «оживают» через DAG-и обновления: синтетические вставки, обновления
и удаления выполняются транзакционно через API sydafa.

1. Airflow UI → DAG-и `update_source_bank` и `update_source_broker`.
2. Запустите оба и откройте Grid View.
3. Пока идут: таски `migrate_bank` и `migrate_broker` применяют наборы
   операций из `airflow/plugins/migrations/{bank,broker}.json`:

| Источник | Добавляется за запуск | Обновляется |
|---|---|---|
| bank | 20 клиентов, 20 счетов, 200 сделок, 5000 транзакций | до 2 статусов клиентов в `VIP`, до 2 счетов в `Frozen`, до 10 сделок в `Closed` |
| broker | столько же строк | до 2 профилей риска в `Aggressive`, до 2 счетов в `Closed`, до 10 рынков сделок в `OTC` |

Каскадно удаляются 10 сделок и связанные с ними строки. Поэтому итоговый
прирост сделок — 190, а транзакций — не обязательно ровно 5000.

4. Дождитесь зелёного статуса обоих DAG-ов. Результат применения операций
   виден в логе соответствующего таска.

**На что обратить внимание**

- В таблицах источников есть служебная колонка `_batch_id` — номер наполнения
  источника. По ней работает [инкрементальная](../glossary.md#term-increment)
  загрузка transactions. Между раундами данные меняются, и следующая загрузка
  прочитает новые значения `_batch_id`.
- Тип этой колонки в источниках разный: в bank — BIGINT, в broker —
  NUMBER(19,0). Поэтому в [манифесте](../glossary.md#term-manifest) bank
  заявлен `position_type: bigint`, а broker — `decimal(19,0)`. Правило выбора —
  [раздел «Выбор типа позиции» архитектуры](../architecture.md#выбор-типа-позиции).

При необходимости к источникам можно подключиться в DBeaver и выбирать из них
данные. Активация — `POST /sources/{source}/activate` с параметром
`ttl_minutes`: метод запускает контейнер СУБД на заданное время
([TTL](../glossary.md#term-ttl)), а реквизиты подключения возвращает объект
`connection` в ответе. Читатель источников — `sydafa` / `sydafa`, права только
на чтение (SELECT).

## Шаг 2. Генерация DAG-ов и запуск загрузок

DAG-и загрузки не пишутся руками — генерируются из
YAML-[манифестов](../glossary.md#term-manifest) (`ingestion/conf/pipelines/`).
В сценарии генерация запускается вручную — это имитация шага CI/CD. Чтобы
показать генерацию «вживую», начните с чистого состояния.

1. Терминал (из корня репозитория) — удалить сгенерированные DAG-и:

   ```bash
   ls airflow/dags/generated/
   rm airflow/dags/generated/ingest__*.py
   ```

   Если каталог или файлы ещё не созданы, пропустите удаление. Удаление
   файлов DAG-ов данные не сбрасывает: история запусков в базе Airflow
   сохраняется.

2. Airflow UI → подождать цикла парсинга (каталог пересканируется за 10
   секунд, обновление списка может занять больше одного цикла) → четыре
   ingest-DAG-а стали неактивными или скрылись фильтром активных. DAG-и
   `update_source_*` остаются активными.
3. Сгенерировать DAG-и заново:

   ```bash
   python3 -m ingestion.generator.generate_dags
   ls -la airflow/dags/generated/
   python3 -m ingestion.generator.generate_dags --check
   ```

4. Показать сгенерированные файлы: `ingest__rdbms__bank__main.py`,
   `ingest__rdbms__bank__refs.py` и два для broker.
   `--check` печатает `check: 0 difference(s)` — артефакты соответствуют
   [манифестам](../glossary.md#term-manifest).
5. После цикла парсинга четыре DAG-а `ingest__rdbms__{bank,broker}__{main,refs}`
   появились снова. Открыть `ingest__rdbms__bank__main`: структура
   `generate_process_id → activate_source → TaskGroup на каждую
   [сущность](../glossary.md#term-entity) (load → publish_asset)`.
   `publish_asset` публикует событие готовности таблицы
   ([Asset](../glossary.md#term-asset)) после успешной загрузки.
6. Запустить все четыре DAG-а (сначала банка, затем брокера — или все сразу).
7. Пока идут: Spark Master UI (http://localhost:8080) — работающие приложения
   (Running Applications), по одному на каждую загрузку.

### Многопоточное чтение

Многопоточное чтение таблицы — это разбиение выборки на несколько
[JDBC-партиций](../glossary.md#term-jdbc-partition): Spark читает каждую
отдельным SQL-запросом. Где показать настройку:

1. **Конфигурация** (до запуска):
   - `ingestion/conf/settings.yaml` — `spark.jdbc.num_partitions: 4`: четыре
     [JDBC-партиции](../glossary.md#term-jdbc-partition) для таблиц с включённым
     многопоточным чтением.
   - В `ingestion/conf/pipelines/bank/main.yaml` у `transactions` ключа
     `spark.jdbc_partitioning` нет — многопоточное чтение включено.
   - У `deals` и всех таблиц `refs` — `spark.jdbc_partitioning: false`: чтение
     в один поток.
   - Колонка нарезки выбирается автоматически единой эвристикой: у всех
     демо-таблиц это целочисленный первичный ключ (`transaction_id`).
2. **В логе load-таска**: открыть лог
   `ingest_group.load_retail_bank__transactions.load` и показать команду Spark
   с `--num-jdbc-partitions 4` и JSON `--incremental-config`, содержащим
   контракт (колонку и [канонический тип позиции](../glossary.md#term-canonical))
   и конкретный [lookback](../glossary.md#term-lookback). У `deals` и таблиц
   refs передаётся `--no-jdbc-partitioning`. Ниже в том же логе — строка
   выбора колонки нарезки:

   ```text
   jdbc partitioning: column=transaction_id step=primary_key window=[1..5000] partitions=4 of 4 requested
   ```

   Колонка, ступень эвристики, MIN/MAX [окна](../glossary.md#term-window) чтения
   и фактическое число [JDBC-партиций](../glossary.md#term-jdbc-partition).
   Это журналируемое решение о разбиении, а не подтверждение
   фактического числа соединений: Spark может уменьшить число
   [JDBC-партиций](../glossary.md#term-jdbc-partition) при
   узком диапазоне.

**На что обратить внимание**

- Нарезка включается, только когда в [окне](../glossary.md#term-window) чтения
  минимум ≠ максимум колонки
  нарезки. Дельта одного запуска update-DAG (порядка 5000 строк с разбросом
  `transaction_id`) уже даёт ненулевой диапазон — специально готовить данные
  не нужно.

### Пул загрузок

[Пул](../glossary.md#term-pool) Airflow ограничивает число одновременно
допущенных загрузок: Admin → [Pools](../glossary.md#term-pool) → `ingest_loads`.

- Ёмкость — 6 [слотов](../glossary.md#term-slot).
- `transactions` занимает 2 [слота](../glossary.md#term-slot), остальные
  загрузки — по 1.
- Во время загрузок оживают колонки Running/Queued
  [Slots](../glossary.md#term-slot). На главной странице Airflow есть виджет
  [пула](../glossary.md#term-pool).

**На что обратить внимание**

- [Манифест](../glossary.md#term-manifest) описывает, какие таблицы загружать
  и с какими настройками. Чтобы добавить таблицу [полным срезом](../glossary.md#term-full),
  достаточно записи с её `fqn`.

## Шаг 3. Результат в Trino и pg-meta

**Trino** (DBeaver, подключение из подготовки):

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

**На что обратить внимание**

- В [raw](../glossary.md#term-raw) сохранены колонки источника и две
  технические: `__load_dttm`, `__process_id`. `__process_id` связывает строки
  с [исполнением](../glossary.md#term-execution) загрузки.
- Без сбоев и повторов каждая загрузка оставляет одну [порцию](../glossary.md#term-portion).
- Последний запрос показывает последний **непустой**
  [полный срез](../glossary.md#term-full): если источник опустел, новой
  [порции](../glossary.md#term-portion) нет и запрос вернёт прежний срез.
- Для [инкремента](../glossary.md#term-increment) в pg-meta есть отдельная
  запись [партии](../glossary.md#term-batch). [Полные срезы](../glossary.md#term-full)
  в реестре `incremental_batches` не учитываются.

**pg-meta** (DBeaver, запросы поочерёдно):

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

**На что обратить внимание**

- В реестре только [инкрементальные](../glossary.md#term-increment) загрузки —
  `transactions` двух источников.
- Первая [партия](../glossary.md#term-batch) имеет `reason=bootstrap`: нижней
  границы ещё нет.
- `watermark_after` хранит максимум после успешной загрузки. Границы
  [окна](../glossary.md#term-window) сохранены **до** чтения строк источника.
- У [полных срезов](../glossary.md#term-full) — справочников и сделок — записей
  в реестре нет. Их загрузки видны по `__process_id` и `__load_dttm` в
  [raw](../glossary.md#term-raw).

## Шаг 4. Повторный раунд: полный срез против инкремента

1. Снова запустите `update_source_bank` и `update_source_broker` (шаг 1) —
   источники выросли.
2. Снова запустите четыре ingest-DAG-а (шаг 2).
3. По завершении — запросы в Trino:

```sql
SELECT __process_id, count(*) AS rows
FROM raw.bank.retail_bank__clients GROUP BY 1 ORDER BY 1;

SELECT * FROM raw.bank.retail_bank__clients
WHERE __load_dttm = (SELECT max(__load_dttm) FROM raw.bank.retail_bank__clients);

SELECT __process_id, count(*) AS rows
FROM raw.bank.retail_bank__transactions GROUP BY 1 ORDER BY 1;
```

4. pg-meta — артефакты второго раунда:

```sql
SELECT process_id, reason, row_count, watermark_before, watermark_after, status
FROM ingestion.incremental_batches
WHERE entity_fqn = 'retail_bank.transactions' ORDER BY process_id;
```

**На что обратить внимание**

- [Полные срезы](../glossary.md#term-full) получили вторую полную
  [порцию](../glossary.md#term-portion), в ней больше строк — источник вырос.
- У `transactions` добавлена только новая часть данных: не более 5000
  транзакций после одного обновления, с учётом каскадного удаления. Первая
  [порция](../glossary.md#term-portion) не изменилась.
- Изменения и удаления старых транзакций не переносятся
  [инкрементом](../glossary.md#term-increment) по `_batch_id`. Это не CDC —
  механизм чтения всех событий изменения источника.
- У второй [партии](../glossary.md#term-batch) `reason=regular`: окно
  `lower_bound < _batch_id <= upper_bound` заморожено до чтения, `row_count` —
  дельта, [watermark](../glossary.md#term-watermark) продвинулся только после
  успешной записи.
- Все числовые номера берите из своих результатов, а не из заранее заданных
  примеров.

## Шаг 5. Повторы запусков и реестр pg-meta

Блок показывает, как реестр переживает повторные и «нулевые» запуски.
Достаточно банка: `ingest__rdbms__bank__main`. Дополнительное время — 10–15
минут.

Подготовка:

1. Дождитесь завершения всех загрузок.
2. Не меняйте источник между действиями, пока подраздел явно этого не
   потребует. Не запускайте одновременно другие загрузки
   `retail_bank.transactions`.
3. Откройте рядом Airflow, pg-meta и Trino в DBeaver.
4. Выполните запросы и сохраните результаты для сравнения:

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

**Поля реестра:**

- `run_id` — какой [DagRun](../glossary.md#term-dagrun) смотрим в Airflow.
- `process_id` — номер [исполнения](../glossary.md#term-execution), а не
  идентификатор процесса ОС.
- Одна строка `incremental_batches` — одна [партия загрузки](../glossary.md#term-batch).
- `attempt_count` — сколько раз началось чтение [партии](../glossary.md#term-batch).
  Это не номер попытки задачи в Airflow и не число вызовов `spark-submit`.
- `row_count` — число строк успешной [попытки](../glossary.md#term-attempt),
  не вся история записей в озере.
- `revision` — версия состояния: растёт при успешной фиксации новой
  [партии](../glossary.md#term-batch), даже без новых строк.

Памятка ожидаемых результатов:

| Действие | `run_id` | `process_id` | Реестр и данные |
|---|---|---|---|
| Trigger без новых данных | Новый | Новый | Новая успешная [партия](../glossary.md#term-batch), `row_count=0`, без новых строк |
| Clear только успешной `load` | Прежний | Прежний | Та же успешная [партия](../glossary.md#term-batch), без изменений состояния и данных |
| Clear генератора и всех задач | Прежний | Новый | Новая [партия](../glossary.md#term-batch), старые записи сохраняются |
| Trigger после обновления источника | Новый | Новый | Новая [партия](../glossary.md#term-batch), новые строки и продвижение [watermark](../glossary.md#term-watermark) |

### Trigger без новых данных

1. **Не запускайте** `update_source_bank`.
2. В Airflow нажмите Trigger для `ingest__rdbms__bank__main`. Это новый
   [DagRun](../glossary.md#term-dagrun), не перезапуск старого.
3. Дождитесь успеха и повторите запросы выше. Запишите новые `run_id` и
   `process_id` для сравнения.

Ожидаемый результат для `transactions`: [партия](../glossary.md#term-batch) с
`reason=regular`, `status=SUCCEEDED`:

- `lower_bound = upper_bound`, `lower_bound_op='>'` (на демо-наборе
  [lookback](../glossary.md#term-lookback) по `_batch_id` равен нулю).
- `attempt_count=0`, `row_count=0`, `observed_max=NULL`.
- [Watermark](../glossary.md#term-watermark) не изменился, `revision` выросла
  на 1, `last_process_id` указывает на новый номер.
- В Trino нет строк с новым `__process_id`.

**На что обратить внимание**

- Система зафиксировала успешную проверку отсутствия новых данных.
  Spark-приложение запускается, но чтение строк пустого [окна](../glossary.md#term-window)
  и запись Iceberg пропускаются. Нулевой `attempt_count` не означает, что
  задача не выполнялась.
- В том же DAG таблица `deals` загружается [полным срезом](../glossary.md#term-full)
  и дописывается заново — её не используйте как пример отсутствия повторной
  записи.

### Clear только успешной load-задачи

1. Откройте успешный запуск **второго раунда с данными** из шага 4. Выберите
   его по `run_id`, а не просто последний запуск.
2. Выберите задачу `ingest_group.load_retail_bank__transactions.load`.
3. Выполните [Clear](../glossary.md#term-clear) только этой задачи: без
   upstream, downstream, других запусков и задачи `generate_process_id`.
4. Дождитесь повторного успеха задачи и повторите запросы.

Ожидаемый результат:

- `run_id` и `process_id` прежние. Новая [batch](../glossary.md#term-batch) is absent.
- The loader finds `SUCCEEDED` and outputs the saved result to the log.
  Source rows are not re-read, Iceberg is not appended to.
- `attempt_count`, `row_count`, `finished_at` of the selected [batch](../glossary.md#term-batch) are unchanged.
- The state of `incremental_state`, including `revision`, does not change.
  If another load already occurred after the selected run, `last_process_id`
  remains the number of that later load.
- The number of rows and write marks in Trino has not changed.

**На что обратить внимание**

- Журнал событий DAG-а может получить новое событие завершения при повторном
  переходе в успех. Это не новая [партия](../glossary.md#term-batch): журналы
  событий и реестр загрузок показывают разные факты.

### Полный Clear существующего запуска

1. Откройте последний успешный запуск без новых данных из предыдущего
   подраздела.
2. Очистите **все его задачи**, включая `generate_process_id`,
   `activate_source` и обе группы загрузки. В подтверждении проверьте набор
   задач и единственный выбранный запуск.
3. Дождитесь успеха и повторите запросы.

Ожидаемый результат:

- `run_id` прежний, но `generate_process_id` выделил новый `process_id`.
- Добавлена новая [партия](../glossary.md#term-batch). Старая запись с тем же
  `run_id` сохранена.
- Источник не менялся: `row_count=0`, `attempt_count=0`,
  [watermark](../glossary.md#term-watermark) прежний, `revision` выросла на 1.
- В журнале DAG-а видны события с разными `process_id` для одного `run_id`.
- У `transactions` новых строк нет. У `deals` добавлен ещё один
  [полный срез](../glossary.md#term-full).

**На что обратить внимание**

- [Clear](../glossary.md#term-clear) не удаляет историю pg-meta и данные
  озера. Повтор генератора создаёт новое
  [исполнение](../glossary.md#term-execution) внутри прежнего
  [DagRun](../glossary.md#term-dagrun).

### Повтор после ошибки: рассказать, не показывать

- [Retry](../glossary.md#term-retry) или [Clear](../glossary.md#term-clear)
  `load` без повтора генератора использует тот же `process_id` и сохранённые
  границы [окна](../glossary.md#term-window). При начале чтения
  `attempt_count` увеличивается — источник между
  [попытками](../glossary.md#term-attempt) может измениться, даже если границы
  прежние.
- Если строки уже записались в Iceberg, а фиксация успеха в pg-meta не
  состоялась, повтор может записать их ещё раз с тем же `__process_id` и новым
  `__load_dttm`. Это гарантия [at-least-once](../glossary.md#term-atleast),
  а не отсутствие дублей.
- До повтора нужно убедиться, что Spark-приложение прежней
  [попытки](../glossary.md#term-attempt) остановлено: изменение статуса задачи
  в UI само по себе этого не подтверждает.
- Не имитируйте сбой на обычном демо: этот случай объясняется, а не
  воспроизводится.

## Шаг 6. Файлы в silo

1. http://localhost:9001 (`admin123` / `admin123`) → bucket `datalake`.
2. Путь `datalake / raw / bank / retail_bank__transactions / data` — каталоги
   вида `__load_dttm_day=2026-10-26`, дневные партиции загрузки. Внутри —
   Parquet-файлы данных.
3. Рядом `datalake / raw / bank / retail_bank__transactions / metadata` —
   файлы метаданных Iceberg: manifest-файлы и
   [снапшоты Iceberg](../glossary.md#term-iceberg-snapshot) (история коммитов
   таблицы).

**На что обратить внимание**

- Путь объектов повторяет логическое имя: `каталог/схема/таблица`.
- Дневные партиции определяются по `__load_dttm` в UTC. Фильтр по времени
  загрузки позволяет исключать ненужные партиции.
- Каждая загрузка добавляет новые файлы, прежние не меняет.

## Шаг 7. Эволюция схемы источника

Стресс-тест: удаление, переименование и добавление колонки на источнике.
Показываем, от чего система **не падает**. Таблицы: `retail_bank.clients`
(bank) и `trading_core.clients` (broker) — [полные срезы](../glossary.md#term-full),
изменения схемы пройдут в [raw](../glossary.md#term-raw) автоматически.

Полную миграцию выполняет скрипт `docs/demo/schema_evolution.py`. Из корня
репозитория:

```bash
python3 docs/demo/schema_evolution.py            оба источника
python3 docs/demo/schema_evolution.py bank       только bank
python3 docs/demo/schema_evolution.py broker     только broker
```

Адрес sydafa задаётся переменной окружения `SYDAFA_URL` (по умолчанию
`http://localhost:8090`). Скрипт ждёт готовности `/health` и печатает
результат каждого шага. Миграция сама будит базу — отдельная активация не
нужна. Повторный запуск на уже мигрированной схеме завершится ошибкой
соответствующей операции.

Что делает скрипт для каждого источника:

| Действие | bank (`retail_bank.clients`) | broker (`trading_core.clients`) |
|---|---|---|
| Удаляет колонку | `client_type` | `is_qualified` |
| Переименовывает колонку | `full_name` → `client_name` | `full_name` → `client_name` |
| Добавляет колонку | `email` (string) | `email` (string) |

Три операции схемы отправляются одной транзакцией `POST /sources/{source}/migrate`.
Затем скрипт настраивает генераторы данных через rules-API:

- назначает генератор добавленной колонке `email`: faker `email`,
  `null_ratio 0.05`.
- переносит генератор на переименованную колонку `client_name`: faker `name`
  с локалью `ru_RU`.
- удаляет правило прежнего имени `full_name`.

Rules-API меняет одну колонку за вызов, поэтому скрипт использует точечные
обращения: полный `PUT` профиля таблицы заменил бы его целиком. В ответе на
каждый вызов rules-API возвращается весь профиль таблицы — удобно показать,
что остальные генераторы не тронуты.

Дальше по сценарию:

1. Запустите DAG-и обновления источников — новые строки получат `email`.
2. Запустите четыре основных ingest-DAG-а.

## Шаг 8. Итоги эволюции схемы в raw

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

**pg-meta** (DBeaver) — последние [партии](../glossary.md#term-batch):

```sql
SELECT process_id, entity_fqn, reason, status, row_count, finished_at
FROM ingestion.incremental_batches ORDER BY process_id DESC LIMIT 8;
```

**На что обратить внимание** (главный финальный тезис): загрузка после
эволюции схемы не упала и ничего не потеряла молча.

- `client_type` **сохранён** в [raw](../glossary.md#term-raw): история
  читается по-старому, у новых [порций](../glossary.md#term-portion) NULL —
  удаление на источнике не удалило историю в озере.
- `client_name` **добавлен**: переименование на источнике выглядит как
  «удалить + добавить», поэтому старые [порции](../glossary.md#term-portion)
  NULL, новые — со значениями.
- `email` добавлен автоматически: старые [порции](../glossary.md#term-portion)
  NULL, новая [порция](../glossary.md#term-portion) — со значениями (после
  назначения генератора и обновления источника).
- Сопоставление колонок идёт по имени, а не по порядку: `mergeSchema=true`,
  свойство таблицы `write.spark.accept-any-schema=true` и опция записи
  `check-ordering=false`. Новые колонки добавляются при записи, отсутствующие
  в источнике сохраняются в [raw](../glossary.md#term-raw) с NULL в новых
  [порциях](../glossary.md#term-portion).
- Все [партии](../glossary.md#term-batch) после эволюции — `SUCCEEDED`: смена
  схемы источника не требует правок [манифестов](../glossary.md#term-manifest)
  и не трогает контракты [инкремента](../glossary.md#term-increment).
- Расширения типов колонок и несовместимые изменения прогоняет автоматический
  сценарий `./test/e2e/shell/test.sh` на обоих источниках. Подтверждённая
  матрица расширений и отказов —
  [раздел 5 архитектуры](../architecture.md#5-устойчивость-к-эволюции-схемы-источника).

## Шаг 9. Таблица-снимок act_snap

[Таблица-снимок](../glossary.md#term-snapshot) — вспомогательная таблица
полного состояния [сущности](../glossary.md#term-entity) с
[инкрементальной](../glossary.md#term-increment) загрузкой. Хранится в
[raw](../glossary.md#term-raw) рядом с историей загрузок.

1. В Airflow найдите снимочный DAG банка: фильтр по тегу `act_snap` или по
   префиксу имени `ingest__rdbms__bank__main__act_snap_` (суффикс-алиас
   вычисляется из [FQN](../glossary.md#term-fqn) [сущности](../glossary.md#term-entity)).
   У каждого [инкрементального](../glossary.md#term-increment) источника свой
   DAG-снимок — их ровно два.
   Расписание не задано: запуск вручную (Trigger), без параметров.
2. Структура запуска та же, что у пайплайна: `generate_process_id` (свой номер
   процесса) → `activate_source` → группа `snap_...`: `load → publish_asset`.
   Исполнитель — `IngestActSnapOperator`, Spark-приложение — `load_act_snap.py`.
3. По завершении — запросы в Trino:

```sql
SELECT count(*) AS raw_rows FROM raw.bank.retail_bank__transactions;
SELECT count(*) AS snap_rows, count(DISTINCT __process_id) AS portions,
       count(DISTINCT __load_dttm) AS load_marks
FROM raw.bank.retail_bank__transactions__act_snap;
```

4. Запустите DAG-снимок ещё раз, затем сравните:

```sql
SELECT __process_id, count(*) AS rows
FROM raw.bank.retail_bank__transactions__act_snap GROUP BY 1 ORDER BY 1;
```

5. Свежесть и автор публикации — из метаданных Iceberg:

```sql
SELECT max(committed_at) AS last_published_at
FROM "raw"."bank"."retail_bank__transactions__act_snap$snapshots";

SELECT value AS published_by_process_id
FROM "raw"."bank"."retail_bank__transactions__act_snap$properties"
WHERE key = 'snapshot.process_id';
```

**На что обратить внимание**

- [Снимок](../glossary.md#term-snapshot) содержит весь прочитанный источник,
  а не [окно](../glossary.md#term-window)
  [инкремента](../glossary.md#term-increment). Для непустого снимка
  `portions = 1` и `load_marks = 1`: один процесс публикации и одна отметка
  времени.
- `raw_rows` и `snap_rows` не обязаны совпадать: [raw](../glossary.md#term-raw)
  хранит историю загрузок, включая строки, уже удалённые из источника.
- После повтора [порция](../glossary.md#term-portion) по-прежнему **одна** с
  новым `__process_id`: [снимок](../glossary.md#term-snapshot)
  **перезаписывается целиком одной атомарной операцией Iceberg**
  ([RTAS](../glossary.md#term-rtas)), а не дополняется,
  как [raw](../glossary.md#term-raw). Читатель в любой момент видит целиком
  предыдущий или целиком новый [снимок](../glossary.md#term-snapshot). Пустое
  чтение источника не перезаписывает существующий непустой
  [снимок](../glossary.md#term-snapshot) — защита от затирания данных.
- Время последней публикации — момент коммита Iceberg (`committed_at`), автор
  — табличное свойство `snapshot.process_id`. Оба читаются без pg-meta. Реестр
  [инкремента](../glossary.md#term-increment) снимком не пополняется: у
  [инкремента](../glossary.md#term-increment) и
  [снимка](../glossary.md#term-snapshot) раздельные потоки.
- `publish_asset` [снимка](../glossary.md#term-snapshot) читает через Trino одну строку `__process_id` и
  публикует `raw://bank/retail_bank.transactions/act_snap`, только если строка
  принадлежит текущему процессу. Пустая таблица или старый процесс — пропуск
  задачи без события. У обычных загрузок тот же PythonOperator публикует
  [Asset](../glossary.md#term-asset) без проверки.
- START/END в журнал pg-meta пишут callbacks задачи `load`.
- Ошибка записи журнала не блокирует загрузку и публикацию.

Настройки [снимка](../glossary.md#term-snapshot) задаются внутри
[инкрементальной](../glossary.md#term-increment)
[сущности](../glossary.md#term-entity) и независимы от основной загрузки —
расписание, теги, своё многопоточное чтение и свой [пул](../glossary.md#term-pool):

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

Внутри `act_snap` запрещены `catchup`, `incremental`, `fqn`, `entities`,
`max_active_runs`. Снимок всегда имеет `catchup=False`, все генерируемые
DAG-и — `max_active_runs=1`.
