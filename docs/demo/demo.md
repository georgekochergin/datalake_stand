# Сценарий демонстрации стенда

Пошаговый сценарий демонстрации. Всё показанное можно повторить
самостоятельно: запросы и команды подготовлены — копируются как есть.

## Содержание

- [Глоссарий](../glossary.md)
- [Подготовка](#подготовка)
- [Шаг 1. Обновление источников](#шаг-1-обновление-источников)
- [Шаг 2. Генерация DAG-ов и запуск загрузок](#шаг-2-генерация-dag-ов-и-запуск-загрузок)
  - [Структура манифеста](#структура-манифеста)
  - [Алгоритмы загрузки](#алгоритмы-загрузки)
  - [Многопоточное чтение](#многопоточное-чтение)
- [Шаг 3. Результат в Trino и pg-meta](#шаг-3-результат-в-trino-и-pg-meta)
- [Шаг 4. Повторный раунд: полный срез против инкремента](#шаг-4-повторный-раунд-полный-срез-против-инкремента)
- [Шаг 5. Файлы в silo](#шаг-5-файлы-в-silo)
- [Шаг 6. Эволюция схемы источника](#шаг-6-эволюция-схемы-источника)
- [Шаг 7. Итоги эволюции схемы в raw](#шаг-7-итоги-эволюции-схемы-в-raw)
- [Шаг 8. Таблица-снимок act_snap](#шаг-8-таблица-снимок-act_snap)

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
3. В процессе работы DAG-ов на источниках выполняются операции, перечисленные
   в файлах:

   - `airflow/plugins/migrations/bank.json`
   - `airflow/plugins/migrations/broker.json`

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
В этом сценарии генерация запускается вручную — имитация шага CI/CD.

1. Терминал (из корня репозитория) — пересоздать сгенерированные DAG-и:

   ```bash
   rm -rf airflow/dags/generated
   python3 -m ingestion.generator.generate_dags
   ```

2. Запустить все четыре DAG-а.
3. Во время загрузок в Spark Master UI видны работающие приложения
   (Running Applications), по одному на каждую загрузку. Имя приложения —
   полное имя целевой таблицы в озере, например
   `raw.bank.retail_bank__transactions`.

### Структура манифеста

[Манифест](../glossary.md#term-manifest) пайплайна — YAML-файл
`ingestion/conf/pipelines/<source_id>/<pipeline>.yaml`. На стенде:
`bank/main.yaml`, `bank/refs.yaml` и два таких же для broker.

Ключи верхнего уровня:

| Параметр | Тип | Обязателен | По умолчанию | Описание |
|---|---|---|---|---|
| `source_id` | string | да | — | Должен существовать в `sources.yaml`.<br>Задаёт схему-неймспейс целевых таблиц |
| `schedule` | string \| null | нет | `null` (ручной запуск) | Расписание Airflow: только стандартный пятиполевой cron с числовыми полями.<br>Макросы (`@daily`) и иные формы отклоняются валидацией |
| `catchup` | bool | нет | `false` | Догоняющие запуски Airflow |
| `tags` | list[string] | нет | `[]` | Теги DAG-а |
| `entities` | list | да | — | Непустой список [сущностей](../glossary.md#term-entity) |

Настройки [сущности](../glossary.md#term-entity):

| Параметр | Тип | Обязателен | По умолчанию | Описание |
|---|---|---|---|---|
| `fqn` | string | да | — | `<схема>.<таблица>` источника, ровно два фрагмента.<br>Регистр исходных имён сохраняется |
| `spark.jdbc_partitioning` | bool | нет | `true` | `false` — полностью отключить нарезку JDBC-чтения основной загрузки: без разведки ключей и запроса MIN/MAX.<br>Не влияет на снимок |
| `airflow.pool.name` | string | нет | `default_pool` | [Пул](../glossary.md#term-pool) load-таска.<br>Пользовательский [пул](../glossary.md#term-pool) должен заранее существовать в Airflow |
| `airflow.pool.slots` | int | нет | `1` | Вес load-таска в [пуле](../glossary.md#term-pool) |
| `incremental` | mapping | нет | [полный срез](../glossary.md#term-full) | [Инкрементальная](../glossary.md#term-increment) загрузка таблицы: обязательный блок `contract` (строки ниже), `lookback` — отступ [окна](../glossary.md#term-window) чтения, `act_snap` — конфигурация снимочного DAG-а (шаг 8) |
| `incremental.contract.source_column` | string | да | — | Колонка [позиции инкремента](../glossary.md#term-position), идентификатор в нижнем регистре |
| `incremental.contract.position_type` | string | да | — | [Канонический тип позиции](../glossary.md#term-canonical). Правило выбора — [раздел «Выбор типа позиции» архитектуры](../architecture.md#выбор-типа-позиции) |
| `incremental.contract.check_not_null` | bool | нет | `true` | `false` — отключить проверку NOT NULL колонки позиции при планировании. Строки с NULL не загружаются даже при [bootstrap](../glossary.md#term-bootstrap) |
| `incremental.act_snap.schedule` | string \| null | нет | `null` | Строгий пятиполевой cron.<br>`null` — ручной запуск |
| `incremental.act_snap.tags` | list[string] | нет | `[]` | Дополнительные теги снимочного DAG-а.<br>Теги основного пайплайна не наследуются |
| `incremental.act_snap.spark.jdbc_partitioning` | bool | нет | `true` | Независимое от основной [сущности](../glossary.md#term-entity) многопоточное JDBC-чтение |
| `incremental.act_snap.airflow.pool.name` | string | нет | `default_pool` | [Пул](../glossary.md#term-pool) load-таска снимка |
| `incremental.act_snap.airflow.pool.slots` | int | нет | `1` | Положительный вес load-таска снимка |

Тип источника задаётся в `ingestion/conf/sources.yaml`:

| Параметр | Тип | Обязателен | Описание |
|---|---|---|---|
| `type` | `rdbms` | да | Категория источника, участвующая в имени DAG.<br>Поддерживается только `rdbms` |
| `airflow_conn_id` | string | да | Ссылка на Airflow Connection с реквизитами источника.<br>Конкретная СУБД определяется по его `conn_type` во время [исполнения](../glossary.md#term-execution) |

Имя DAG собирается из типа источника, `source_id` и имени файла
[манифеста](../glossary.md#term-manifest):

`ingest__<type>__<source_id>__<pipeline>` — например, `ingest__rdbms__bank__main`
([манифест](../glossary.md#term-manifest) `bank/main.yaml`). Если имя файла совпадает
с `source_id`, последняя
часть опускается. DAG-снимок получает суффикс `__act_snap_<alias>`, где
`alias` — первые 8 символов `base32(sha256(FQN))`:
`ingest__rdbms__bank__main__act_snap_bbphs7ay`.

**На что обратить внимание**

- Чтобы добавить таблицу [полным срезом](../glossary.md#term-full), достаточно
  записи с её `fqn`.
- Полные описания атрибутов и правила адресации имён —
  [раздел 2 архитектуры](../architecture.md#2-манифесты-и-генерация-dag-ов).

### Алгоритмы загрузки

Таблица загружается одним из двух алгоритмов. Выбор определяется наличием
блока `incremental` у [сущности](../glossary.md#term-entity) — таблица выше.

**Полный срез** — [full](../glossary.md#term-full):

- Таблица читается целиком и дописывается в историю
  [raw](../glossary.md#term-raw) новой [порцией](../glossary.md#term-portion).
- Запись — [append-only](../glossary.md#term-append): строки только
  дописываются, мержа с ранее загруженными нет. История накапливается.
- Режим по умолчанию: сущности без блока `incremental` достаточно записи
  с `fqn`.
- На стенде так загружаются справочники и `deals`.

**Инкремент** — [incremental](../glossary.md#term-increment):

- Читаются только строки из очередного [окна](../glossary.md#term-window)
  значений колонки [позиции](../glossary.md#term-position):
  `lower_bound < позиция <= upper_bound`.
- Первый запуск — [bootstrap](../glossary.md#term-bootstrap): сохранённого
  [watermark](../glossary.md#term-watermark) ещё нет.
- Границы [окна](../glossary.md#term-window) фиксируются в pg-meta до чтения
  строк. После успешной записи [watermark](../glossary.md#term-watermark)
  продвигается до фактически прочитанного максимума.
- Каждая загрузка фиксируется в реестре pg-meta отдельной
  [партией](../glossary.md#term-batch). Повторный запуск без новых данных
  завершается пустой успешной [партией](../glossary.md#term-batch).
- Запись — [append-only](../glossary.md#term-append): в историю
  [raw](../glossary.md#term-raw) дописываются только строки
  [окна](../glossary.md#term-window), мержа с ранее загруженными нет.
- Изменения и удаления старых строк не переносятся: это не CDC.
- На стенде так загружаются `transactions` обоих источников.
- Вместе с [инкрементальной](../glossary.md#term-increment)
  [сущностью](../glossary.md#term-entity) генератор создаёт вспомогательный
  снимочный DAG `act_snap`: таблицу полного состояния источника в
  [raw](../glossary.md#term-raw), перезаписываемую целиком одной атомарной
  операцией (шаг 8). Снимок независим: не меняет
  [watermark](../glossary.md#term-watermark) основной загрузки и не пополняет
  реестр [инкремента](../glossary.md#term-increment).

### Многопоточное чтение

Многопоточное чтение таблицы — это разбиение выборки на несколько
[JDBC-партиций](../glossary.md#term-jdbc-partition): Spark читает каждую
отдельным SQL-запросом. Настройка хранится в конфигурации и отражается
в логе load-таска:

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
2. **В логе load-таска**: в логе задачи
   `ingest_group.load_retail_bank__transactions.load` видны команда Spark
   с `--num-jdbc-partitions 4` и JSON `--incremental-config`, содержащий
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
- В реестре pg-meta видны обе [партии](../glossary.md#term-batch)
  `transactions`: первая — `reason=bootstrap`, вторая — `reason=regular`
  с [окном](../glossary.md#term-window) `lower_bound < _batch_id <= upper_bound`.
  Границы заморожены до чтения, `row_count` — дельта,
  [watermark](../glossary.md#term-watermark) продвинулся только после
  успешной записи.
- Изменения и удаления старых транзакций не переносятся
  [инкрементом](../glossary.md#term-increment) по `_batch_id`. Это не CDC —
  механизм чтения всех событий изменения источника.
- Все числовые номера берите из своих результатов, а не из заранее заданных
  примеров.
- Нюансы повторных запусков, [Clear](../glossary.md#term-clear) и поведения
  после ошибок — [раздел «Как различать партии и попытки»
  архитектуры](../architecture.md#как-различать-партии-и-попытки).

## Шаг 5. Файлы в silo

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

## Шаг 6. Эволюция схемы источника

Стресс-тест: удаление, переименование и добавление колонки на источнике.
Таблицы: `retail_bank.clients`
(bank) и `trading_core.clients` (broker) — [полные срезы](../glossary.md#term-full),
изменения схемы пройдут в [raw](../glossary.md#term-raw) автоматически.

Полную миграцию выполняет скрипт `docs/demo/schema_evolution.py`. Из корня
репозитория:

```bash
python3 docs/demo/schema_evolution.py            оба источника
python3 docs/demo/schema_evolution.py bank       только bank
python3 docs/demo/schema_evolution.py broker     только broker
```

Что делает скрипт для каждого источника:

| Действие | bank (`retail_bank.clients`) | broker (`trading_core.clients`) |
|---|---|---|
| Удаляет колонку | `client_type` | `is_qualified` |
| Переименовывает колонку | `full_name` → `client_name` | `full_name` → `client_name` |
| Добавляет колонку | `email` (string) | `email` (string) |

Дальше по сценарию:

1. Запустите DAG-и обновления источников — новые строки получат `email`.
2. Запустите четыре основных ingest-DAG-а.

## Шаг 7. Итоги эволюции схемы в raw

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

**На что обратить внимание**: загрузка после эволюции схемы не упала
и ничего не потеряла молча.

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

## Шаг 8. Таблица-снимок act_snap

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
