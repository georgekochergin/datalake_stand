# Архитектура datalake_stand

Документ описывает фреймворк загрузки: конфигурацию, DAG-и, хранение данных,
повторные [попытки](glossary.md#term-attempt) и ограничения. Запуск стенда и подключения — в
[README](../README.md). Правила разработки — в [AGENTS.md](../AGENTS.md).

## Содержание

- [Глоссарий](glossary.md)
- [1. Слои данных](#1-слои-данных)
- [2. Манифесты и генерация DAG-ов](#2-манифесты-и-генерация-dag-ов)
- [3. Структура сгенерированного DAG-а](#3-структура-сгенерированного-dag-а)
- [4. Таблицы raw-слоя: структура и наполнение](#4-таблицы-raw-слоя-структура-и-наполнение)
- [5. Устойчивость к эволюции схемы источника](#5-устойчивость-к-эволюции-схемы-источника)
- [6. Параллелизм Spark](#6-параллелизм-spark)
  - [Порядок выполнения load-задачи](#61-порядок-выполнения-load-задачи)
- [7. Инкрементальная загрузка](#7-инкрементальная-загрузка)
  - [Контракт](#71-контракт)
  - [Жизненный цикл](#72-жизненный-цикл-загрузки)
  - [Состояние сущности](#73-incremental_state--состояние-сущности)
  - [Реестр партий](#74-incremental_batches--реестр-партий)
  - [Пример инкремента и повтора](#75-пример-одного-инкремента-и-повтора)
  - [Смена контракта](#76-смена-контракта-и-ручные-миграции)
  - [Таблицы-снимки](#77-dag-и-снимков-и-таблицы-снимки)
- [8. Остальные объекты pg-meta](#8-база-метаданных-pg-meta-остальные-объекты)
- [Руководство по расширению стенда](extending.md)
- [9. Типы и гарантии](#9-типы-данных-и-гарантии)
- [10. Ограничения](#10-известные-ограничения)

Пример: [DagRun](glossary.md#term-dagrun) получил `process_id=42` и загрузил две таблицы — это две [партии](glossary.md#term-batch).
Повтор `load` одной таблицы остаётся в её [партии](glossary.md#term-batch) 42, но может добавить новую [порцию](glossary.md#term-portion).
После [Clear](glossary.md#term-clear) генератора выделится `process_id=43`: это уже новые [партии](glossary.md#term-batch),
хотя `run_id` Airflow не изменился.

## 1. Слои данных

Стенд реализует классическую послойную схему КХД → DLH: каждый следующий слой
строится поверх предыдущего трансформациями, нижние слои при этом не меняются.

### raw — данные источника без бизнес-преобразований

[raw](glossary.md#term-raw) — первый слой озера, точное зеркало таблиц источника «AS IS»: все колонки
источника на момент чтения плюс технические колонки загрузки. Никакой
бизнес-логики, преобразований и отбора колонок.

**Общий принцип — [append-only](glossary.md#term-append):** загрузчик только дописывает строки в [raw](glossary.md#term-raw),
не обновляя и не удаляя ранее загруженные данные. Это не запрещает отдельные
административные операции обслуживания Iceberg.

Есть два метода загрузки:

1. **[Полный срез](glossary.md#term-full)** — каждая итерация читает всю таблицу источника и
   дописывает её [полный срез](glossary.md#term-full) с новыми техническими метаданными. Удалённой строки
   не будет в новом срезе, но её ранее загруженные версии останутся в [raw](glossary.md#term-raw).
2. **[Инкремент](glossary.md#term-increment)** — изменения отслеживаются по выбранной колонке таблицы
   источника; загрузчик дописывает строки из очередного [окна](glossary.md#term-window) её значений.
   Изменения видны только тогда, когда значение этой колонки попадает в [окно](glossary.md#term-window)
   чтения; удаления таким методом не обнаруживаются (раздел 7).

### ods (планируется)

[ODS](glossary.md#term-ods) — слой выше [raw](glossary.md#term-raw), в текущей версии стенда не реализован. Его задачи:

1. Дедупликация строк по ключам [сущности](glossary.md#term-entity).
2. Выделение актуального состояния и, при необходимости, ведение истории
   версий (например, SCD2 с признаком актуальности).

[ODS](glossary.md#term-ods) должен строиться [идемпотентным](glossary.md#term-idempotency) merge по ключу/версии источника,
а не агрегированием [raw](glossary.md#term-raw)-строк. Это требование к будущей реализации.

### dds / dma (планируется)

[DDS](glossary.md#term-dds) (например, Data Vault) и витрины ([DMA](glossary.md#term-dma)) — слои обработки и выдачи,
строятся поверх [ODS](glossary.md#term-ods) в будущих этапах.


## 2. Манифесты и генерация DAG-ов

### Подготовка DAG-ов

Генератор `ingestion/generator/generate_dags.py` превращает YAML-[манифесты](glossary.md#term-manifest)
в Python-файлы в `airflow/dags/generated/`. Этот каталог не отслеживается в git.

1. Генератор читает YAML и проверяет конфигурацию.
2. В Python-файл записываются задачи, группы и зависимости.
3. Airflow разбирает готовый файл. YAML и реквизиты подключений на этом этапе не читает.
4. Во время выполнения оператор получает реквизиты из Airflow Connections.

Подготовьте окружение генератора из корня репозитория:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r ingestion/generator/requirements.txt
python -m ingestion.generator.generate_dags
python -m ingestion.generator.generate_dags --check
```

Используйте Python 3.11+. Airflow и Spark на хост устанавливать не нужно.

`ingestion/conf/sources.yaml` ссылается на Connections через `airflow_conn_id`,
но не создаёт их. Настройка подключений и запуск стенда описаны в `README.md`.

Режим генератора `--check` строит ожидаемые Python-файлы в памяти и сравнивает
их с содержимым выходного каталога, **ничего не записывая**. Отсутствующий,
отличающийся или лишний `.py` означает код возврата 1; совпадение — 0.
Это проверка актуальности артефактов перед их передачей Airflow, а не проверка
доступности источников или [исполнения](glossary.md#term-execution) DAG-а.

Генератор обходит `ingestion/conf/pipelines/` рекурсивно; один YAML-файл = один DAG
загрузки. Идентификатор DAG и имя файла: `ingest__<type>__<source_id>[__<имя_файла_без_расширения>]`
(постфикс опускается, если имя файла совпадает с `source_id`): так четыре
[манифеста](glossary.md#term-manifest) `{bank,broker}/{main,refs}.yaml` дают четыре DAG-а загрузки.
Кроме них генератор создаёт **DAG-и снимков**: для каждой инкрементальной
[сущности](glossary.md#term-entity) — отдельный DAG `ingest__<type>__<source_id>[__<файл>]__act_snap_<alias>`,
где `alias` — первые 8 символов base32(sha256([FQN](glossary.md#term-fqn))) (раздел 7.7). [Сущности](glossary.md#term-entity) без
[инкремента](glossary.md#term-increment) снимочных DAG-ов не получают.

До передачи DAG-ов Airflow генератор выявляет:

- неизвестные ключи и неверные типы значений конфигурации;
- повторяющиеся [FQN](glossary.md#term-fqn) внутри [манифеста](glossary.md#term-manifest);
- одинаковые DAG ID у разных [манифестов](glossary.md#term-manifest) (включая снимочные);
- совпадения целевых таблиц, в том числе после приведения имён к нижнему
  регистру; снимочные таблицы `__act_snap` проверяются в общем списке целей.

При межфайловой коллизии сообщение указывает оба файла. Обычная генерация
удаляет больше не нужные `ingest__*.py` только с собственной меткой
`Generated DAG for pipeline` или `Generated snapshot DAG for pipeline`.
Если ожидаемое имя занято файлом без такой метки,
генерация завершается ошибкой, не перезаписывая его. Посторонние файлы
не удаляются; при этом `--check` считает лишним любой неожиданный `.py`.

### Реестр источников

`ingestion/conf/sources.yaml` содержит источники. Ключ записи — `source_id`.

| Параметр | Тип | Обязателен | Описание |
|---|---|---|---|
| `type` | `rdbms` | да | Категория источника; участвует в имени DAG. Сейчас поддерживается только `rdbms`; `kafka` зарезервирован на будущее и пока отвергается |
| `airflow_conn_id` | string | да | Ссылка на Airflow Connection с реквизитами источника; конкретная СУБД определяется по его `conn_type` во время [исполнения](glossary.md#term-execution) |

**Разница между `type` источника и `conn_type` подключения Airflow:**

- `type` — категория источника, доступная генератору в CI/CD и входящая в DAG ID
  (`ingest__rdbms__bank__main`). Сейчас поддерживается только `rdbms`;
  `kafka` пока отклоняется валидатором. Способ чтения задаётся реализацией
  оператора, а не этим атрибутом: текущая реализация использует JDBC.
- `conn_type` — конкретная технология (`mssql` / `oracle`), определяемая
  в рантайме из Airflow Connection: оператор строит JDBC URL, а Spark
  по его префиксу выбирает драйвер и SQL-диалект.

Генератор не обращается к Airflow Connections: в DAG записываются `source_type`
и `airflow_conn_id`. В рантайме выполняются две независимые проверки:
оператор принимает только `source_type="rdbms"`, а построение JDBC URL
поддерживает только `conn_type` из списка `mssql`, `oracle`.

### Манифест пайплайна

Файлы: `ingestion/conf/pipelines/**/*.yaml`. Ключи верхнего уровня:

| Параметр | Тип | Обязателен | По умолчанию | Описание |
|---|---|---|---|---|
| `source_id` | string | да | — | Должен существовать в `sources.yaml`; задаёт схему-неймспейс целевых таблиц |
| `schedule` | string \| null | нет | `null` (ручной запуск) | Расписание Airflow: только стандартный пятиполевой cron с числовыми полями; макросы (`@daily`) и иные формы отклоняются валидацией (правило и связь с [lookback](glossary.md#term-lookback) — раздел 7.1) |
| `catchup` | bool | нет | `false` | Догоняющие запуски Airflow |
| `tags` | list[string] | нет | `[]` | Теги DAG-а |
| `entities` | list | да | — | Непустой список [сущностей](glossary.md#term-entity) (см. ниже) |

### Настройки таблицы источника

Регистр исходных имён сохраняется: для объектов `SCHEMA.TABLE` укажите
`fqn: "SCHEMA.TABLE"`. Это пример латинских идентификаторов: кириллица текущим
парсером [FQN](glossary.md#term-fqn) не поддерживается. Кавычки YAML не являются SQL-кавычками.
Загрузчик сам оформляет каждый фрагмент как `[SCHEMA].[TABLE]` в MSSQL или
`"SCHEMA"."TABLE"` в Oracle. Для Oracle нужно писать фактический регистр
объекта: некавыченные при создании имена обычно хранятся в верхнем регистре,
кавыченные — как были созданы. Только **целевое** имя приводится к нижнему
регистру: `raw.bank.schema__table`.

| Параметр | Тип | Обязателен | По умолчанию | Описание |
|---|---|---|---|---|
| `fqn` | string | да | — | `<схема>.<таблица>` источника, ровно два фрагмента; преобразуется в имя целевой таблицы по правилам именования (раздел 4) |
| `incremental` | mapping | нет | [полный срез](glossary.md#term-full) | Обязательный `contract: {source_column, position_type}`; `lookback` — отступ [окна](glossary.md#term-window) (раздел 7.1); `act_snap` — независимая конфигурация вспомогательного снимочного DAG-а (раздел 7.7) |
| `incremental.act_snap.schedule` | string \| null | нет | `null` | Строгий пятиполевой cron; null — ручной запуск |
| `incremental.act_snap.tags` | list[string] | нет | `[]` | Дополнительные теги снимочного DAG-а; теги основного [пайплайна](glossary.md#term-pipeline) не наследуются |
| `incremental.act_snap.spark.jdbc_partitioning` | bool | нет | `true` | Независимое от основной [сущности](glossary.md#term-entity) параллельное JDBC-чтение |
| `incremental.act_snap.airflow.pool.name` | string | нет | `default_pool` | [Пул](glossary.md#term-pool) load-таска снимка |
| `incremental.act_snap.airflow.pool.slots` | int | нет | `1` | Положительный вес load-таска снимка |
| `spark.jdbc_partitioning` | bool | нет | `true` | `false` — полностью отключить нарезку JDBC-чтения основной загрузки: без разведки ключей и запроса MIN/MAX (раздел 6); не влияет на снимок |
| `airflow.pool.name` | string | нет | `default_pool` | [Пул](glossary.md#term-pool) load-таска; пользовательский [пул](glossary.md#term-pool) должен заранее существовать в Airflow |
| `airflow.pool.slots` | int | нет | `1` | Вес load-таска в [пуле](glossary.md#term-pool) |

Блок `act_snap` допустим только внутри `incremental`: он настраивает
гарантированно существующий DAG-снимок (расписание и [пул](glossary.md#term-pool)), а не включает его
(раздел 7.7). Внутри `act_snap`
запрещены `fqn`, `entities`, `incremental`, `catchup`, `max_active_runs` и
плоский `pool`: настройки используют те же вложенные пути `spark` и
`airflow.pool`, что у основной [сущности](glossary.md#term-entity). Defaults независимы от родителя.

### Общие настройки и демо-набор

**`ingestion/conf/settings.yaml`** — общие технические настройки: сейчас один ключ
`spark.jdbc.num_partitions` (положительное целое, по умолчанию на стенде `4`) —
запрашиваемое число [JDBC-партиций](glossary.md#term-jdbc-partition) чтения, которое Spark может уменьшить
(раздел 6). Значение записывается непосредственно в
сгенерированный DAG. При включённом [JDBC-партиционировании](glossary.md#term-jdbc-partition) оператор передаёт
Spark-приложению `--num-jdbc-partitions N`; при отключённом — только
`--no-jdbc-partitioning`, без неиспользуемого числа партиций. Правило одинаково
для полной загрузки, [инкремента](glossary.md#term-increment) и `act_snap`.

Текущие [манифесты](glossary.md#term-manifest) стенда (демо-набор): в `main.yaml` каждого источника —
`transactions` ([инкремент](glossary.md#term-increment) по `_batch_id`) и `deals` ([полный срез](glossary.md#term-full)); в `refs.yaml` —
`clients` и `accounts` ([полные срезы](glossary.md#term-full), у всех демо-таблиц кроме transactions
JDBC-параллелизм отключён). Обе [сущности](glossary.md#term-entity) `transactions` получают по
DAG-у снимка без расписания — ручной запуск, раздел 7.7. В обоих [манифестах](glossary.md#term-manifest)
`act_snap.airflow.pool` явно задаёт `ingest_loads` с весом 2: обычные загрузки
и снимки разделяют ограничение числа Spark-приложений. Это явная настройка
стенда; default фреймворка остаётся независимым `default_pool` с весом 1.

## 3. Структура сгенерированного DAG-а

### Идентификаторы и метаданные

`pg-meta` — отдельная PostgreSQL-база фреймворка,
не metadata DB Airflow и не каталог Iceberg. В её схеме `ingestion` хранятся:

- журналы DAG-запусков и задач (`airflow_dagruns_log`, `airflow_tasks_log`);
- состояние инкрементальных [сущностей](glossary.md#term-entity) (`incremental_state`);
- инкрементальный реестр — [окна](glossary.md#term-window) и [watermark](glossary.md#term-watermark) инкрементальных [сущностей](glossary.md#term-entity)
  (`incremental_state`, `incremental_batches`); [полные срезы](glossary.md#term-full) и снимки в
  него не пишутся.

Каждый запуск задачи `generate_process_id` получает новый `process_id`.
Все таблицы этого [исполнения](glossary.md#term-execution) используют один номер.
[Партия](glossary.md#term-batch) таблицы имеет ключ `(process_id, source_id, entity_fqn)`.
Для инкрементальной загрузки pg-meta также хранит [watermark](glossary.md#term-watermark).

### Зависимости задач

```text
DAG ingest__rdbms__bank__main
│  schedule=None, catchup=False, max_active_runs=1, tags из YAML,
│  колбэки успеха/провала → события END/FAILED в pg-meta
│
├── generate_process_id   валидация conf запуска; выделение process_id (XCom)
└── activate_source       SydafaActivateOperator: поднять СУБД, TTL 15 минут
    └── TaskGroup "ingest_group"                       (сущности параллельно)
        ├── TaskGroup "load_retail_bank__transactions" (одна сущность)
        │   ├── log_start   событие START в pg-meta
        │   ├── load        IngestAppendOperator → spark/raw/rdbms/<поток>/load_entity.py
        │   ├── log_end     событие END в pg-meta
        │   └── publish_asset  публикация Asset после успешного log_end
        └── TaskGroup "load_retail_bank__deals" ...
```

Зависимости: `generate_process_id >> activate_source >> ingest_group` и
прямая `generate_process_id >> ingest_group`; внутри
группы [сущности](glossary.md#term-entity) независимы, внутри [сущности](glossary.md#term-entity) —
`log_start >> load >> log_end >> publish_asset`.
Прямая зависимость от генератора применяется и к корням снимочных групп.
Она нужна при частичном [Clear](glossary.md#term-clear) генератора и группы без `activate_source`:
оставшаяся успешной активация не должна разрешать START до завершения
новой генерации и публикации process_id в XCom.
`max_active_runs=1` сериализует запуски одного DAG-а — последний [полный срез](glossary.md#term-full) и
[watermark](glossary.md#term-watermark) каждой [сущности](glossary.md#term-entity) однозначен.

### Назначение задач

- **`generate_process_id`** (`GenerateProcessIdOperator`) — выделяет
  `process_id` исполнителя: `meta.allocate_process_id()` вставляет START в
  `airflow_dagruns_log` с `nextval('ingestion.process_id_seq')` и возвращает
  новый номер. **Каждое [исполнение](glossary.md#term-execution) задачи — новый номер**: [retry](glossary.md#term-retry) самой задачи
  `generate_process_id` и полный [Clear](glossary.md#term-clear) (генератор и все задачи после него)
  создают новое [исполнение](glossary.md#term-execution) с новыми [партиями](glossary.md#term-batch). [Retry](glossary.md#term-retry) downstream-задач
  использует неизменный `process_id` из XCom. У сгенерированных DAG-ов нет
  объявленных параметров запуска и обрабатываемой конфигурации запуска:
  обычный запуск не требует ввода, диалог Trigger открывается с сразу
  активной кнопкой.
- **`activate_source`** (`SydafaActivateOperator`, пакет `sydafa-airflow`) —
  будит СУБД источника через REST sydafa с TTL 15 минут, чтобы к моменту JDBC-чтения
  СУБД источника была запущена.
- **`log_start` / `log_end`** (`TableLogOperator`) — события жизненного цикла
  [сущности](glossary.md#term-entity) в `airflow_tasks_log`, без публикации [Asset](glossary.md#term-asset) и управления состоянием
  извлечения. Таблицы `ingestion.incremental_batches` и
`ingestion.incremental_state`
  обслуживает Spark-функция `managed_load()` через `ingestion.meta`;
  `incremental_column` передаётся только задаче `load`.
- **`publish_asset`** (`PythonOperator` с `outlets`, callable `publish_asset_event`) — отдельная задача публикации
  события обновления [Asset](glossary.md#term-asset) `raw://<source_id>/<fqn>`, на который смогут
  подписываться DAG-и следующих слоёв. Выполняется только после успешного
  `log_end`: загрузка завершилась и её успех записан в pg-meta. Ошибка или
  пропуск `log_end` не допускает публикацию.
- **`load`** (`IngestAppendOperator`, наследник `SparkSubmitOperator`,
  client-mode) — исполнитель загрузки. Приложение выбирается по типу
  [сущности](glossary.md#term-entity): инкрементальная — `ingestion/spark/raw/rdbms/incremental/load_entity.py`
  ([окна](glossary.md#term-window) с реестром pg-meta), [полный срез](glossary.md#term-full) — `ingestion/spark/raw/rdbms/full/load_entity.py`
  (append без реестра); оба читают источник и пишут [raw](glossary.md#term-raw) через общие модули
  `reader.py` и `raw_append.py`. Во время
  [исполнения](glossary.md#term-execution) проверяет `source_type="rdbms"`, выбирает JDBC-диалект по
  `conn_type`, получает Connection источника и `process_id` из XCom.
  Параметры чтения и реквизиты источника, **включая пароль**, передаются
  аргументами `spark-submit`; реквизиты pg-meta — переменными `PGHOST`,
  `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`. [Пул](glossary.md#term-pool) применяется Airflow и
  Spark-job не передаётся. Текущий способ передачи пароля источника не даёт
  гарантии отсутствия секрета в командной строке процесса.

### Ограничение числа загрузок

Все load-таски всех ingest-DAG-ов делят [пул](glossary.md#term-pool) `ingest_loads` на
4 [слота](glossary.md#term-slot) (создаётся сервисом `airflow-init`); `transactions` занимает 2 [слота](glossary.md#term-slot),
остальные загрузки — по 1. [Пул](glossary.md#term-pool) ограничивает количество одновременно работающих
Spark-приложений (каждое — отдельный [driver](glossary.md#term-driver) + [executors](glossary.md#term-executor), что дорого для
Docker-машины), но не выделяет им CPU/память.

### Структура DAG-а снимка

Для каждой таблицы с инкрементальной загрузкой генератор создаёт
отдельный DAG `<dag_id_пайплайна>__act_snap_<alias>` (`catchup=False`,
`max_active_runs=1`, теги `act_snap`, `source_id`, имя целевой таблицы плюс
теги [манифеста](glossary.md#term-manifest)). Структура повторяет [пайплайн](glossary.md#term-pipeline), но с одной [сущностью](glossary.md#term-entity) и другим
исполнителем:

```text
DAG ingest__rdbms__bank__main__act_snap_<alias>
│  schedule из incremental.act_snap (нет — ручной запуск)
│
├── generate_process_id   собственный process_id снимочного запуска
└── activate_source       SydafaActivateOperator: поднять СУБД, TTL 15 минут
    └── TaskGroup "snap_retail_bank__transactions"
        ├── log_start      событие START в pg-meta
        ├── load           IngestActSnapOperator → ingestion/spark/raw/rdbms/incremental/load_act_snap.py
        ├── log_end        событие END в pg-meta
        └── publish_asset  PythonOperator: в таблице есть строки текущего
                           процесса — публикация, иначе пропуск
```

Снимочный поток не пишет в инкрементальный реестр pg-meta; контракт снимка —
раздел 7.7.

## 4. Таблицы raw-слоя: структура и наполнение

### Имена и колонки

Полное имя таблицы источника кодируется: каждая точка → `__`.
`retail_bank.clients` → таблица `retail_bank__clients` в схеме `bank`
(источник) каталога `raw`: полное имя `raw.bank.retail_bank__clients` (каталог —
слой, схема — источник).

**Структура таблицы.** Колонки — точное зеркало схемы источника на момент
чтения — плюс две технические колонки:

| Колонка | Тип | Смысл |
|---|---|---|
| `__load_dttm` | `timestamp_ltz` | Момент загрузки (одно значение на [порцию](glossary.md#term-portion)); аналитическое измерение времени; колонка партиционирования |
| `__process_id` | `bigint` | Идентификатор [исполнения](glossary.md#term-execution) DAG-а из pg-meta; он же — ключ [партии](glossary.md#term-batch) загрузки этой таблицы (подробное объяснение — ниже) |

### Как различать партии и попытки

[Исполнение](glossary.md#term-execution) DAG-а получает один
`process_id` на все свои таблицы (раздел 3). Внутри [исполнения](glossary.md#term-execution) каждая
таблица читается отдельной **[партией](glossary.md#term-batch)**: все строки, дописанные [партией](glossary.md#term-batch) в [raw](glossary.md#term-raw),
получают её `process_id` в `__process_id`; [raw](glossary.md#term-raw)-таблица принадлежит одной
[сущности](glossary.md#term-entity), поэтому пары (таблица, `__process_id`) достаточно, чтобы отличить
[порции](glossary.md#term-portion) одной [партии](glossary.md#term-batch) от [порций](glossary.md#term-portion) другой в данных.

Учёт [партии](glossary.md#term-batch) зависит от режима:

- **[Полный срез](glossary.md#term-full).** В реестре партий записи нет. Повтор `load` снова читает
  всю таблицу и дописывает её с прежним `process_id`, но новым `__load_dttm`.
- **[Инкремент](glossary.md#term-increment).** До чтения строк создаётся запись `incremental_batches`
  с зафиксированными границами [окна](glossary.md#term-window). Повтор использует ту же запись,
  `process_id` и [окно](glossary.md#term-window). Повторно записанные строки сохраняют номер [партии](glossary.md#term-batch).

Пример просмотра инкрементальной [партии](glossary.md#term-batch) в pg-meta:

```sql
SELECT * FROM ingestion.incremental_batches
WHERE process_id = 42 AND source_id = 'bank'
  AND entity_fqn = 'retail_bank.transactions';
```

### Создание таблицы и размещение файлов

Партиционирование фиксировано фреймворком: `days(__load_dttm)` — дневные
партиции по границам UTC-дня. Первая загрузка создаёт таблицу DDL-ом по
фактически прочитанной схеме источника (колонки источника никогда не
перечисляются в YAML):

```sql
CREATE NAMESPACE IF NOT EXISTS raw.bank;

CREATE TABLE raw.bank.retail_bank__clients (
  `client_id` bigint, `full_name` string, ...,
  `__load_dttm` timestamp_ltz, `__process_id` bigint
) USING iceberg
PARTITIONED BY (days(`__load_dttm`))
LOCATION 's3://datalake/raw/bank/retail_bank__clients'
TBLPROPERTIES ('write.spark.accept-any-schema'='true')
```

Namespace проверяется при каждой исполняемой загрузке, таблица создаётся
только при отсутствии. Location задаётся явно по шаблону
`s3://datalake/raw/<source_id>/<имя_таблицы>` (`ingestion/iceberg.py`).

LOCATION задаётся явно, без UUID в имени каталога. Одного выбора
`NessieCatalog` для такого пути недостаточно. Создание через REST в используемой
версии Nessie также не гарантирует этот путь.

- Существующие таблицы автоматически не переносятся.
- Не создавайте одноимённые таблицы в разных ветках Nessie с одним LOCATION.
- Не удаляйте и не переименовывайте файлы таблицы вручную.
- При повторном создании удалённой таблицы учитывайте файлы и ссылки,
  оставшиеся в истории каталога. Штатная загрузка таблицу не пересоздаёт.

### Полный срез и инкремент

- **[Полный срез](glossary.md#term-full)** (по умолчанию) — каждый запуск читает всю таблицу
  источника и дописывает её полное состояние новой [порцией](glossary.md#term-portion). Удаления на
  источнике отражаются (удалённые строки отсутствуют в новой [порции](glossary.md#term-portion), но
  остаются в истории). Текущее состояние источника — последняя непустая
  [порция](glossary.md#term-portion) в данных: запрос по `MAX(__load_dttm)`; реестровой записи у полного
  среза нет, опустевший источник виден по отсутствию новой непустой [порции](glossary.md#term-portion)
  после запуска.
- **Инкрементальный** (`incremental.contract.source_column`) — каждый запуск читает [окно](glossary.md#term-window)
  новых строк по колонке [инкремента](glossary.md#term-increment). Первая загрузка без
  сохранённого [watermark](glossary.md#term-watermark) — первичное наполнение (без нижней границы,
  до максимума, зафиксированного при планировании). Подробно —
  раздел 7.

Каждая загрузка одной таблицы = одна [партия](glossary.md#term-batch): в данных её отмечает пара
(`__process_id`, `__load_dttm`); у инкрементальной [сущности](glossary.md#term-entity) ей соответствует
также строка `incremental_batches` с ключом `(process_id, source_id,
entity_fqn)`, у [полного среза](glossary.md#term-full) реестровой записи нет.

### Запросы к raw в Trino

```sql
SHOW TABLES FROM raw.bank;

DESCRIBE raw.bank.retail_bank__clients;

SELECT count(*) FROM raw.bank.retail_bank__clients;

SELECT * FROM raw.bank.retail_bank__clients
WHERE __load_dttm = (SELECT max(__load_dttm) FROM raw.bank.retail_bank__clients);

SELECT __process_id, count(*) AS rows
FROM raw.bank.retail_bank__clients
GROUP BY 1 ORDER BY 1;

SELECT count(*) FROM raw.bank.retail_bank__transactions WHERE __process_id = 42;
```

Запрос с `max(__load_dttm)` возвращает последнюю **непустую** [порцию](glossary.md#term-portion); про
пустой [полный срез](glossary.md#term-full) смотрите пояснение в подразделе
[«Полный срез и инкремент»](#полный-срез-и-инкремент).

`__load_dttm` — `timestamp_ltz` (абсолютный момент); партиции следуют UTC-дням,
а «сегодня» — календарное понятие. Сравнивайте с зоной:

```sql
SET TIME ZONE 'Europe/Moscow';
SELECT * FROM raw.bank.retail_bank__clients
WHERE __load_dttm >= current_date
  AND __load_dttm < current_date + INTERVAL '1' DAY;
```

Литералы без зоны (`TIMESTAMP '2026-10-26 00:00:00'`) интерпретируются в зоне
сессии и молча сдвигают [окно](glossary.md#term-window) на часы — избегайте; пишите
`TIMESTAMP '...' AT TIME ZONE 'UTC'`.

### Как исключить повторные попытки из выборки

Одна [партия](glossary.md#term-batch)
(`__process_id`) может быть физически дописана больше одного раза: сбой
после Iceberg-коммита, но до финализации pg-meta приводит к повторному
append того же замороженного [окна](glossary.md#term-window) при [retry](glossary.md#term-retry) ([at-least-once](glossary.md#term-atleast)). Все строки
одной [попытки](glossary.md#term-attempt) несут одинаковое `__load_dttm` (`current_timestamp` фиксируется
на старте записи), поэтому канонический способ снять повторы [попыток](glossary.md#term-attempt) —
оконная функция по `__process_id`: она оставляет **последнюю непустую
[попытку](glossary.md#term-attempt)** каждой [партии](glossary.md#term-batch) ([окно](glossary.md#term-window) `max(__load_dttm)` нельзя писать прямо
в `WHERE`, поэтому подзапрос):

```sql
SELECT * FROM (
  SELECT t.*, max(__load_dttm) OVER (PARTITION BY __process_id) AS __last_attempt
  FROM raw.bank.retail_bank__transactions t
) WHERE __load_dttm = __last_attempt;
```

Запрос оставляет последнюю непустую [попытку](glossary.md#term-attempt) каждой [партии](glossary.md#term-batch).
Он не объединяет версии строк по бизнес-ключу и не устраняет пересечения
между разными [партиями](glossary.md#term-batch), например из-за [lookback](glossary.md#term-lookback).
Источник между [попытками](glossary.md#term-attempt) может измениться, даже если границы [окна](glossary.md#term-window) прежние.
Без фильтра в [raw](glossary.md#term-raw) остаётся вся история записей.

### Ограничение: последняя попытка прочитала пустое окно

Пустая [попытка](glossary.md#term-attempt) не оставляет строк в [raw](glossary.md#term-raw). Поэтому SQL выше вернёт предыдущую
непустую [попытку](glossary.md#term-attempt), даже если последняя успешная [попытка](glossary.md#term-attempt) была пустой.

Пример:

1. Первая [попытка](glossary.md#term-attempt) записала строки в Iceberg, но не завершила фиксацию в pg-meta.
2. В источнике строки [окна](glossary.md#term-window) удалили или изменили.
3. [Retry](glossary.md#term-retry) прочитал пустое [окно](glossary.md#term-window) и завершился с `row_count=0`.
4. В [raw](glossary.md#term-raw) остались только строки первой [попытки](glossary.md#term-attempt).

У [инкремента](glossary.md#term-increment) такой случай можно искать в реестре по `attempt_count > 1`
и `row_count=0`. У [полного среза](glossary.md#term-full) реестра нет: новая [порция](glossary.md#term-portion) просто не появилась.
Оконный запрос не восстанавливает пустой результат последней [попытки](glossary.md#term-attempt).

Оптимизация для больших таблиц: федеративный запрос — подключить pg-meta
в Trino JDBC-каталогом, найти в `ingestion.incremental_batches` [партии](glossary.md#term-batch) с
`attempt_count > 1` и применять оконный фильтр **только к их** `__process_id`,
остальные [порции](glossary.md#term-portion) читая как есть. На стенде JDBC-каталог pg-meta не настроен
(отдельная настройка Trino); оптимизация касается только инкрементальных
[сущностей](glossary.md#term-entity) — у [полных срезов](glossary.md#term-full) нет строк реестра, их повторы снимаются
каноническим оконным запросом. Прежде чем применять оптимизацию, сверьте
масштаб: фильтр по готовому списку `process_id` выгоден, когда повторных
партий единицы.

### Гарантии записи

Данные озера и метаданные pg-meta фиксируются в разных
системах: сбой после коммита Iceberg, но до финализации [watermark](glossary.md#term-watermark) приведёт
к повторной записи той же [порции](glossary.md#term-portion) при повторе запуска ([at-least-once](glossary.md#term-atleast), не
exactly-once). Повтор диапазона допустим и не считается ошибкой — следующему
слою требуется [идемпотентный](glossary.md#term-idempotency) merge по ключу/версии источника, а не `sum` по
[raw](glossary.md#term-raw)-строкам. Пустой источник тоже создаёт [raw](glossary.md#term-raw)-таблицу (проверка схемы без
строк).

## 5. Устойчивость к эволюции схемы источника

Список колонок и их типы не описываются в [манифесте](glossary.md#term-manifest): загрузчик получает схему
через JDBC. Исключение — имя колонки [инкремента](glossary.md#term-increment): оно задано явно и вместе с
обнаруженным типом образует сохраняемый контракт (раздел 7.6).
Три настройки интеграции Iceberg со Spark обеспечивают автоматическую
обработку добавленных и отсутствующих колонок, но не произвольную смену типов:

### Отсутствующие колонки

**`write.spark.accept-any-schema=true`** — свойство таблицы, задаётся один раз
в DDL при создании и действует на всех будущих писателей. По умолчанию Iceberg
строго сверяет схему записываемого DataFrame со схемой таблицы: не хватает
колонки или лишняя — запись падает. Это свойство делает проверку сопоставлением
по имени и разрешает DataFrame'у **не иметь** части колонок таблицы: в
дописываемых строках они сохраняются как NULL. Новые колонки на этом уровне
всё ещё ошибка — их добавляет вторая настройка.

### Новые колонки

**`mergeSchema=true`** — опция Iceberg Spark writer, передаваемая через API
Spark `DataFrameWriterV2` при каждом append; это не универсальная гарантия
Spark для любого формата. Непосредственно
перед записью Iceberg сравнивает схему DataFrame со схемой таблицы и добавляет
каждую колонку DataFrame, которой таблице не хватает (эквивалент `ALTER TABLE
ADD COLUMN` внутри коммита записи). Существующие строки читают новую колонку
как NULL.

### Порядок колонок

**`check-ordering=false`** — также опция Iceberg Spark writer,
отключающая сверку **позиций** колонок.
По умолчанию Iceberg проверяет, что порядок полей записываемого DataFrame
совпадает с порядком полей таблицы. Здесь сопоставление всегда по имени:
источник свободно меняет порядок колонок, а новые колонки после слияния схем
могут оказаться в схеме таблицы после технических колонок — и это корректно
(в Iceberg колонки идентифицируются field id и именем, а не позицией). Проверка
типов при этом сохраняется.

### Результат изменения схемы

Совместно настройки дают следующие результаты:

| Изменение на источнике | Эффект в [raw](glossary.md#term-raw)-таблице |
|---|---|
| добавлена колонка | автоматически добавлена (`mergeSchema`); исторические строки — NULL, новые — со значениями |
| колонка удалена | колонка в таблице сохраняется; новые строки — NULL |
| колонка переименована | распознаётся как удалить+добавить: старая колонка сохраняется (новые строки NULL), новая добавляется (история NULL) |
| изменён тип колонки | фреймворк не выполняет отдельную миграцию типов; Iceberg может принять поддерживаемое безопасное расширение при merge схемы, несовместимое изменение приводит к ошибке; смена типа колонки [инкремента](glossary.md#term-increment) отдельно блокируется контрактом pg-meta |
| изменён порядок колонок | никакого (сопоставление по имени, `check-ordering=false`) |

Нельзя обещать, что любое изменение схемы пройдёт без ошибки. Фреймворк
явно разрешает слияние схем при append, а допустимость изменения типов
определяет Iceberg. Удаления/переименования не удаляют старые колонки из [raw](glossary.md#term-raw);
несовместимые типы требуют отдельного решения и миграции.

Физически каждый append пишет только новые Parquet-файлы плюс один коммит
метаданных (новый снапшот таблицы): файлы предыдущих загрузок не меняются,
более поздняя загрузка не может испортить более раннюю историю.

## 6. Параллелизм Spark

Параллелизм загрузки реализован на трёх уровнях: чтение источника по JDBC,
одновременная загрузка [сущностей](glossary.md#term-entity) и распределённая обработка в Spark.

### JDBC-партиции: несколько запросов к источнику

Чтение таблицы по JDBC —
один запрос на партицию: Spark может одновременно выполнять несколько SELECT к
СУБД, каждый читает свой диапазон строк. Фактическая одновременность зависит
от свободных [слотов](glossary.md#term-slot) [executors](glossary.md#term-executor). Диапазоны задаёт четвёрка параметров чтения:
`partitionColumn` (колонка, по которой режем), `lowerBound`/`upperBound`
(минимум/максимум этой колонки) и `numPartitions` (на сколько диапазонов
разбить). При диапазоне id 1..1000 Spark формирует до четырёх непересекающихся
предикатов вида `id < граница`, `id >= граница AND id < следующая`,
`id >= последняя_граница`; первая партиция также принимает NULL.
`lowerBound`/`upperBound` задают шаг нарезки, **не фильтруют** всю выборку:
инкрементальное [окно](glossary.md#term-window) ограничено отдельным WHERE внутри подзапроса.

### Как читать лог параллельной загрузки

Пример — задача
`ingest__rdbms__bank__main`, запуск
`manual__2026-11-09T15:54:58.294859+00:00`,
`ingest_group.load_retail_bank__transactions.load`, [попытка](glossary.md#term-attempt) 1.
В логе этой задачи видна следующая последовательность
(самодостаточные фрагменты ниже; время — UTC из Spark):

1. В `Spark-Submit cmd` следует проверить `--num-jdbc-partitions`,
   `--incremental-config` и `--process-id`. Значение `--num-jdbc-partitions 4` —
   запрошенное число [JDBC-партиций](glossary.md#term-jdbc-partition), не число [executors](glossary.md#term-executor) и не общее число
   запросов к источнику за весь запуск.
   Контракт колонки и конкретный [lookback](glossary.md#term-lookback) передаются одним
   `--incremental-config` (раздел 7.1). Генератор разрешает дефолт заранее;
   для целочисленного `_batch_id` дефолт [lookback](glossary.md#term-lookback) равен нулю. Тип и конкретный [lookback](glossary.md#term-lookback)
   записываются в `incremental config:` при планировании новой [партии](glossary.md#term-batch);
   [retry](glossary.md#term-retry) использует уже сохранённую [партию](glossary.md#term-batch), не вычисляя [окно](glossary.md#term-window) заново.
2. До основного чтения выполняется отдельная разведка MIN/MAX:
   `collect at .../reader.py:88`, `1 output partitions`.
   Это не одна из четырёх партиций данных.
3. В 15:55:16 `JDBCRelation: Number of partitions: 4` показывает
   фактически построенные предикаты по `transaction_id`:

   | Партиция | Предикат Spark |
   |---|---|
   | 0 | `"transaction_id" < 27502 or "transaction_id" is null` |
   | 1 | `"transaction_id" >= 27502 AND "transaction_id" < 40001` |
   | 2 | `"transaction_id" >= 40001 AND "transaction_id" < 52500` |
   | 3 | `"transaction_id" >= 52500` |

   Это **четыре запроса чтения данных**, а не два запроса внутри диапазона
   плюс два дополнительных. Крайние предикаты не ограничены снизу и сверху
   соответственно: MIN/MAX нужны Spark для расчёта шага, а не для отсечения
   строк. Первая партиция также принимает NULL; требование NOT NULL не
   является необходимым условием полноты такого разбиения. При большом
   числе NULL первая партиция может оказаться существенно тяжелее остальных.
   В текущей эвристике NOT NULL пока остаётся условием отбора кандидатов
   ступеней 2–4, а не ограничением самого Spark JDBC.
4. Строка `Scan JDBCRelation` показывает общий подзапрос:

   ```sql
   (SELECT extract_source.*, [_batch_id] AS "__extract_position"
    FROM (SELECT * FROM [retail_bank].[transactions]) extract_source
    WHERE [_batch_id] > 4 AND [_batch_id] <= 5) extract_batch
   ```

   Каждый из четырёх предикатов применяется к этому подзапросу. Поэтому
   отсутствие внешних границ по `transaction_id` **не отменяет [окно](glossary.md#term-window) батча**:
   строки с `_batch_id` вне `(4, 5]` не входят ни в одну партицию.
   `PushedFilters: []` в плане не означает отсутствия фильтра источника:
   оконный WHERE уже находится внутри SQL-подзапроса.
5. `Submitting 4 missing tasks` и `Starting task 0.0`–`3.0 in stage 1.0`
   показывают [исполнение](glossary.md#term-execution) партиций. В этом запуске все четыре задачи
   стартовали на [executor](glossary.md#term-executor) 0 практически вместе, в 15:55:16.520–16.521,
   и завершились за 732–733 мс. Ранее [executor](glossary.md#term-executor) получил 10 ядер; четыре
   партиции могут исполняться на одном [executor](glossary.md#term-executor), четыре [executor](glossary.md#term-executor) не нужны.
   Эти строки подтверждают перекрытие Spark-задач, но сами по себе не
   доказывают одновременный старт SQL на сервере или чтение данных на единый момент времени.
6. Последующие стадии `first`, shuffle и `append` относятся к дальнейшей
   обработке и записи. Их `4 output partitions` нельзя считать ещё четырьмя
   независимыми JDBC-запросами: чтение кешируется, повторное обращение к
   источнику возможно при повторном вычислении или повторе задач.

Для диагностики сопоставляйте команду запуска, оконный SQL, предикаты
`JDBCRelation`, старт/завершение задач **стадии JDBC Scan** и сообщения
`jdbc partitioning:` загрузчика, если они есть. Число партиций не доказывает
равенства объёмов: Spark делит диапазон значений, а не строки поровну.

### Как выбирается колонка разбиения

Колонка всегда одна и выбирается автоматически. В [манифесте](glossary.md#term-manifest) её задать нельзя.
Эвристика едина для всех режимов загрузки (регулярный [инкремент](glossary.md#term-increment),
первичное наполнение, [полный срез](glossary.md#term-full)) и живёт в отдельном модуле
`ingestion/sources/rdbms/jdbc_partitioning.py`. Порядок ступеней:

1. первичный ключ из одной колонки целочисленного типа;
2. целочисленная NOT NULL-колонка с одноколоночным уникальным ограничением
   (NOT NULL обязателен: UNIQUE сам по себе допускает NULL-значения — один
   в MSSQL, много в Oracle);
3. колонка типа дата с NOT NULL;
4. колонка типа timestamp с NOT NULL — нарезка по номеру
   календарного дня (см. ниже).

Правила выбора:

- В ступенях 2–4 кандидаты сортируются по нормализованному имени.
  Берётся первый: порядок JDBC-метаданных может меняться между запусками.
- Первичный ключ (PK) — единственный кандидат первой ступени.
- Допустимые целочисленные типы: целые JDBC-типы, `NUMBER(p,0)` и `decimal(p,0)`.
  Дробные decimal, float, строки и GUID не подходят.
- Колонка [инкремента](glossary.md#term-increment) не имеет приоритета. Она задаёт [окно](glossary.md#term-window), а колонка
  разбиения — способ разделить это [окно](glossary.md#term-window) между запросами.
- В лог попадают выбранная колонка, ступень, MIN/MAX, запрошенное
  и фактическое число партиций. Большее число партиций само по себе не доказывает ускорение.
- Если кандидат не найден, предупреждение показывает PK, уникальные колонки,
  типы и допустимость NULL. Ошибка получения метаданных логируется отдельно.

Например, чтение останется однопоточным при составном PK и отсутствии
подходящих UNIQUE, date или timestamp NOT NULL.

Ничего не нашли или метаданные недоступны — однопоточное чтение. Кандидаты
берутся из стандартного JDBC API `DatabaseMetaData` (`getPrimaryKeys`,
`getIndexInfo`, `getColumns`) — он одинаково работает в MSSQL и Oracle, в
отличие от `INFORMATION_SCHEMA`, которого в Oracle нет. Разведка ключей —
best-effort: недоступность метаданных не валит загрузку. Составные,
функциональные и фильтрованные индексы исключаются. Произвольные колонки
таблицы перебираются только в ступенях 3–4 (дата/timestamp NOT NULL): сумма
транзакции или строковый код кандидатами не станут. UNIQUE гарантирует
высокую кардинальность, но не равномерность (значения 1, 2, 3, 10^12
уникальны при жёстком перекосе) и не использование индекса СУБД —
равномерность распределения и план выполнения не обещаются.

### Разбиение timestamp по календарным дням

Нарезать timestamp-колонку
напрямую нельзя. Когда Spark читает таблицу по JDBC, он назначает каждой
колонке внутренний тип Spark по сведениям, которые сообщает JDBC-драйвер;
фреймворк включает у чтения опцию `preferTimestampNTZ`, поэтому
timestamp-колонки источника становятся типом `TimestampNTZType` — «момент
без часового пояса». Диапазоны же для нарезки Spark строит только по числам
и датам, `TimestampNTZType` он делить не умеет. Под то же ограничение
попадает Oracle DATE: несмотря на имя, в нём есть время суток, и драйвер
сообщает его как timestamp. Поэтому на timestamp-ступени загрузчик
вычисляет в подзапросе чтения номер календарного дня каждой строки — целое
число — и нарезает по нему:

- Oracle — номер юлианского дня
  `CAST(TO_NUMBER(TO_CHAR(колонка, 'J')) AS NUMBER(10,0))`; явная точность
  нужна, чтобы колонка осталась целочисленной (несдержанный `NUMBER` Spark
  отображает как десятичную с масштабом);
- MSSQL — `DATEDIFF(day, 0, колонка)`: число дней от 1900-01-01.

Целое число Spark делит всегда. Колонка номера дня публикуется в подзапросе
под служебным именем `__jdbc_day`, и `partitionColumn` указывает на неё;
после чтения загрузчик удаляет её из DataFrame, и в целевую таблицу она не
попадает (колонка источника с таким именем заняла бы алиас — в этом случае
нарезка отключается с предупреждением). Для timestamp-с-поясом день
считается по значению, уже переведённому в UTC (`SYS_EXTRACT_UTC` в
Oracle), для остальных типов — по самой колонке.

Свойства этой нарезки:

- **[окно](glossary.md#term-window) батча не грубеет**: предикаты [окна](glossary.md#term-window) остаются на исходной колонке с
  полной точностью источника, номер дня участвует только в разбиении на
  потоки;
- **календарь не влияет на правильность**: номер дня — фиксированная функция
  значения строки, поэтому каждая строка попадает ровно в один диапазон — не
  теряется и не дублируется, по какому бы календарю день ни считался;
- **гранулярность — сутки**: [окно](glossary.md#term-window) короче суток даёт один номер дня
  (MIN = MAX) и один поток; перекос строк по дням оставляет часть
  диапазонов пустыми — осознанное ограничение timestamp-ступени;
- **стоимость**: предикат по номеру дня — функция над колонкой; предикаты по
  числам и датам потенциально используют индексы СУБД, но фактический план
  зависит от СУБД, статистики и размера [окна](glossary.md#term-window) — ни одна ступень не
  гарантирует план [исполнения](glossary.md#term-execution).

### Когда разбиение работает

Для выбранной колонки на источнике выполняется `SELECT MIN(col), MAX(col)`
**по [окну](glossary.md#term-window) чтения** (вся таблица или инкрементный подзапрос), и нарезка на
потоки включается **только при строгом неравенстве MIN < MAX**:

- MIN и MAX равны NULL — нарезка не включается, выполняется чтение одним
  потоком. Это не обязательно пустое [окно](glossary.md#term-window): выбранная колонка может
  содержать только NULL;
- MIN = MAX — в [окне](glossary.md#term-window) ровно одно значение колонки нарезки, диапазон для
  разбиения отсутствует: чтение идёт **одной** [JDBC-партицией](glossary.md#term-jdbc-partition) при любом
  `num_partitions` (узкое [окно](glossary.md#term-window) — ожидаемое поведение, а не сбой настройки);
- MIN < MAX — Spark получает запрошенное `numPartitions`
  (`spark.jdbc.num_partitions` из `settings.yaml`, на стенде 4).
  На узком целочисленном диапазоне Spark 3.5 уменьшает число партиций:
  MIN=11, MAX=12 даст **одну**, MIN=11, MAX=13 — две, а не четыре.
  Решает ширина диапазона, не число различных значений: два значения
  11 и 100 могут дать четыре партиции, часть из них пустые.
- числовые границы за пределами знакового 64-битного диапазона — Spark
  разбирает опции границ как Long независимо от точности колонки, поэтому
  такие границы отключают нарезку с предупреждением (значения `NUMBER(38,0)`
  вне Long допустимы для самой колонки, но не для механизма границ), чтение
  идёт одним потоком.

Фактическое число партиций в логе выбора берётся по числу партиций
построенного DataFrame после чтения, а не из запроса: Spark может уменьшить
его на узком диапазоне.

**Когда параллельное чтение отключается:**

- `spark.jdbc_partitioning: false` у [сущности](glossary.md#term-entity) — намеренно (так настроены все
  демо-таблицы кроме `transactions`): полностью отключаются разведка ключей и
  запрос MIN/MAX, чтение идёт в один поток;
- ни одна ступень эвристики не нашла кандидата или метаданные недоступны —
  чтение одним потоком.

### Ограничение: данные читаются в разные моменты

[JDBC-партиции](glossary.md#term-jdbc-partition) используют отдельные соединения и SELECT.
Единой транзакции, фиксирующей состояние источника на момент времени, нет.
Это ограничение относится и к [инкременту](glossary.md#term-increment), и к [полному срезу](glossary.md#term-full).

Пример: строка во время чтения изменила значение колонки разбиения
и перешла из первого диапазона во второй. Она может попасть в обе выборки
или не попасть ни в одну.

Запросы не обязаны стартовать одновременно:

- Spark запускает задачи по мере освобождения ядер.
- Ожидающая задача ещё не держит JDBC-соединение.
- При нехватке ядер запросы выполняются волнами, и интервал чтения растёт.
- Для одновременного чтения N партиций приложению нужно не менее N ядер.
  Нескольким приложениям должно хватать суммарных ресурсов worker.

На стенде один worker, а лимиты приложений пока не заданы. Первое приложение
может занять все ядра. Настройка ресурсов — задача
[«Управление ресурсами Spark на уровне отдельной джобы»](backlog.md#управление-ресурсами-spark-на-уровне-отдельной-джобы).

### Несколько загрузок и обработка внутри Spark

Таски `load` разных [сущностей](glossary.md#term-entity) внутри
DAG-а и разных DAG-ов исполняются параллельно; их количество сдерживает [пул](glossary.md#term-pool)
Airflow `ingest_loads` (4 [слота](glossary.md#term-slot); см. раздел 3). Это допуск к [исполнению](glossary.md#term-execution), а не
выделение ресурсов.

Отключение JDBC-параллелизма (одна партиция
чтения) не запрещает распределённую обработку и запись, но само по себе
не создаёт несколько потоков обработки данных. Параллельная запись зависит
от дальнейшего плана и shuffle; явного `repartition()` после JDBC-чтения
в загрузчике нет.

Каждая загрузка — отдельное Spark-приложение: [driver](glossary.md#term-driver) работает в контейнере
`airflow-scheduler` (client-mode), [executors](glossary.md#term-executor) — на `spark-worker`. В образе
Airflow используется Python 3.11, в образе Spark-кластера — Python 3.10:
JDBC/DataFrame-загрузки не запускают Python-worker'ов, но для Python UDF/RDD
потребуется выравнивание minor-версий. Изоляция [driver](glossary.md#term-driver) описана в
[отдельной задаче](backlog.md#изолировать-spark-drivers-через-dockeroperator).

### 6.1. Порядок выполнения load-задачи

Речь именно о Spark-приложении `load`, после генерации process_id и активации
источника; START/END и публикация [Asset](glossary.md#term-asset) — отдельные задачи Airflow.

**[Полный срез](glossary.md#term-full) ([full](glossary.md#term-full), append):**

1. Проверить аргументы, запустить SparkSession. Инкрементального контракта,
   запросов MAX/COUNT для [watermark](glossary.md#term-watermark) и партий pg-meta нет.
2. Если JDBC-нарезка включена, прочитать метаданные PK/UNIQUE/колонок для
   выбора колонки нарезки. Получить метаданные совместимой проекции
   (например, timezone-колонки Oracle). При выключенной нарезке разведка
   ключей пропускается, но построение совместимой схемы остаётся.
3. При найденной колонке выполнить MIN/MAX этой колонки по всей таблице,
   чтобы задать шаг JDBC-диапазонов. Это не фильтр полноты среза.
4. Прочитать всю таблицу одним или несколькими JDBC-запросами, кешировать
   DataFrame; посчитать строки, добавить технические колонки и выполнить
   append в Iceberg с согласованием схемы; освободить кеш.
5. [Retry](glossary.md#term-retry) повторяет полное чтение и append: возможны дубли, реестра [партии](glossary.md#term-batch) нет.

**[Инкремент](glossary.md#term-increment) (increment, включая [bootstrap](glossary.md#term-bootstrap)):**

1. Проверить JSON `--incremental-config`, найти [партию](glossary.md#term-batch) в pg-meta по
   process_id/источнику/[сущности](glossary.md#term-entity). Существующую [партию](glossary.md#term-batch) сверить с колонкой,
   целевой таблицей и типом контракта. SUCCEEDED — выход без JDBC/записи.
2. Прочитать JDBC-метаданные инкрементальной колонки запросом
   `SELECT col FROM table WHERE 1=0`; сверить [канонический тип](glossary.md#term-canonical). Результат
   пустой, агрегатов и сканирования строк для статистики здесь нет.
   Стоимость — подключение, сетевой обмен, разбор и разрешение схемы;
   она обычно ниже COUNT/MAX, но задержки/блокировки схемы возможны.
3. Только для новой [партии](glossary.md#term-batch) выполнить по всей таблице
   `MAX(col), COUNT(*), COUNT(col)`: максимум фиксирует upper, COUNT
   различают пустой источник и NULL в инкрементальной колонке. MIN для
   планирования [инкремента](glossary.md#term-increment) не нужен. Взять [watermark](glossary.md#term-watermark) из pg-meta,
   применить уже разрешённый [lookback](glossary.md#term-lookback) и сохранить замороженное [окно](glossary.md#term-window).
   [Retry](glossary.md#term-retry) не повторяет эти агрегаты и использует сохранённое [окно](glossary.md#term-window).
4. Если сохранённое [окно](glossary.md#term-window) доказуемо пустое при нулевом [lookback](glossary.md#term-lookback) — закрыть
   [партию](glossary.md#term-batch) без чтения DataFrame. Иначе отметить RUNNING.
5. Разведать колонку JDBC-нарезки по метаданным и совместимую проекцию.
   MIN/MAX колонки нарезки вычислить **по [окну](glossary.md#term-window) [партии](glossary.md#term-batch)**, не по всей таблице.
   Это отдельный запрос для параллелизма, он возможен и при [retry](glossary.md#term-retry); колонка
   нарезки может совпасть с инкрементальной, но [окно](glossary.md#term-window) не перепланируется.
6. Прочитать [окно](glossary.md#term-window) через JDBC, кешировать DataFrame, вычислить число строк и
   точный MAX служебной позиции `__extract_position`, выполнить Iceberg
   append и финализировать [партию](glossary.md#term-batch)/[водяной знак](glossary.md#term-watermark) в pg-meta. [Retry](glossary.md#term-retry) после
   неподтверждённого append может дописать те же строки повторно.

**Вспомогательный снимок инкрементальной таблицы (act_snap, [RTAS](glossary.md#term-rtas)):**

1. Проверить аргументы и автора текущей таблицы Iceberg
   (`snapshot.process_id`). Если этот process_id уже опубликован — выход
   без чтения источника. Это восстановление после успешного [RTAS](glossary.md#term-rtas) и сбоя задачи.
2. Иначе выполнить метаданные выбора JDBC-колонки/совместимой проекции и
   MIN/MAX колонки нарезки **по всей таблице**, как для [full](glossary.md#term-full). Контракт
   [инкремента](glossary.md#term-increment) не проверяется; MAX/COUNT планирования и реестр партий не нужны.
3. Прочитать весь источник через JDBC, кешировать результат и проверить
   пустоту. Пустой результат сохраняет предыдущую таблицу; при первом пустом
   чтении создаётся пустая таблица с авторством, без события [Asset](glossary.md#term-asset).
4. Непустой результат дополнить техническими колонками и атомарно заменить
   таблицу через [RTAS](glossary.md#term-rtas) с новым авторством; освободить кеш. [Retry](glossary.md#term-retry) при отсутствии
   подтверждения авторства повторяет полное чтение, не старое [окно](glossary.md#term-window) [инкремента](glossary.md#term-increment).

JDBC-метаданные типа и метаданные выбора нарезки — разные проверки.
Подтверждение типа не фиксирует схему на время всех последующих запросов;
общей транзакции, фиксирующей состояние источника для всех этих этапов, нет.

## 7. Инкрементальная загрузка

### 7.1. Контракт

#### Что задаётся в YAML

Для инкрементальной загрузки укажите:

- колонку источника — `incremental.contract.source_column`;
- тип позиции — `incremental.contract.position_type`;
- при необходимости отступ назад — `lookback` с полями `size` и `unit`.

**Пример:** [watermark](glossary.md#term-watermark) равен `120`, [lookback](glossary.md#term-lookback) отключён. Следующий запуск ищет
значения больше 120. Это не доказывает, что все меньшие значения загружены:
транзакция со значением 119 могла завершиться позже предыдущего чтения.

**[Канонический тип позиции](glossary.md#term-canonical) [инкремента](glossary.md#term-increment)** — независимое от СУБД описание
допустимых значений инкрементальной колонки: диапазона, точности и временной
семантики. Он используется для проверки контракта, точного хранения [watermark](glossary.md#term-watermark),
сравнения позиций и формирования SQL-границ. Это не JDBC-тип, не JVM-класс
и не тип Spark DataFrame; он не задаёт приведение исходной колонки в Spark.
Пользователь выбирает его после анализа пригодности колонки для [инкремента](glossary.md#term-increment).

```yaml
incremental:
  contract:
    source_column: updated_at
    position_type: timestamp_local(7)
  lookback:
    size: 3
    unit: h
```

#### Выбор типа позиции

Таблица выбора `position_type` по типу источника:

| Тип источника | Значение `position_type` | Ограничения |
|---|---|---|
| MSSQL SMALLINT / INT / BIGINT | `smallint` / `integer` / `bigint` | Знаковые 16 / 32 / 64 бита; писать `integer`, не `int` |
| MSSQL DECIMAL(p,0), NUMERIC(p,0); Oracle NUMBER(p,0) | `decimal(p,0)` | p = 1..38; scale строго 0; точность берётся из JDBC, не из текущих значений |
| MSSQL DATE | `date` | Дата без времени |
| MSSQL SMALLDATETIME / DATETIME | `timestamp_local(0)` / `timestamp_local(3)` | Семантика без часового пояса |
| MSSQL DATETIME2(p) | `timestamp_local(p)` | p = 0..7 |
| MSSQL DATETIMEOFFSET(p) | `timestamp_instant(p)` | p = 0..7; позиция нормализуется в UTC |
| Oracle DATE | `timestamp_local(0)` | Содержит время, поэтому не `date` |
| Oracle TIMESTAMP(p) | `timestamp_local(p)` | p = 0..9 |
| Oracle TIMESTAMP(p) WITH TIME ZONE / WITH LOCAL TIME ZONE | `timestamp_instant(p)` | p = 0..9; UTC |

Строки, GUID, float, дробные decimal и NUMBER без определённого допустимого
p/scale не подходят. Метаданные JDBC преобразуются диалектом в фактический
[канонический тип](glossary.md#term-canonical); он должен точно совпасть с заявленным, включая точность.
Несовпадение — ошибка до агрегатов новой [партии](glossary.md#term-batch) и до записи; в сообщении
есть ожидаемый/фактический тип, JDBC type_name, precision и scale. Никакого
автоматического cast, расширения типа или сброса [watermark](glossary.md#term-watermark) нет.

#### Передача конфигурации загрузчику

Генератор определяет [lookback](glossary.md#term-lookback) до запуска приложения: явный YAML имеет
приоритет; иначе целые получают 0, дата — 3 дня, timestamp — 3 часа при
двух и более запусках в сутки и 3 дня при одном запуске или без расписания.
Нулевой размер передаётся с `unit: null`. Только инкрементальный загрузчик
получает JSON `--incremental-config`, например:

```json
{"contract":{"source_column":"_batch_id","position_type":"bigint"},"lookback":{"size":0,"unit":null}}
```

`act_snap` в JSON не входит. Колонка и тип задаются внутри `contract`,
а конкретный отступ — внутри `lookback`. Расписание и контекст
дефолтов в Spark не передаются: JSON содержит конкретные значения.

#### Формат хранения и точность

Фактический тип колонки проверяется через JDBC. Значение сохраняется текстом, но
сравнивается и участвует в вычитании **как число, дата или время**, а не как
произвольная строка: число `100` больше `99`, хотя текстовое сравнение дало бы
обратное. Дробные секунды позиции сохраняются до точности источника:

| Семейство | Типы источника | [Канонический вид позиции](glossary.md#term-canonical) |
|---|---|---|
| целочисленные (знаковые) | `smallint`, `int`, `bigint`, `decimal(p,0)` | целое число: `-10`, `0`, `12345` |
| дата | `date` (MSSQL) | `2026-10-26` |
| дата-время без часового пояса (`timestamp_local`) | MSSQL `smalldatetime`/`datetime`/`datetime2(p)`, Oracle `DATE`, Oracle `TIMESTAMP(p)` | `2026-10-26T12:00:00.123456` |
| абсолютный момент (`timestamp_instant`, UTC) | MSSQL `datetimeoffset(p)`, Oracle `TIMESTAMP(p) WITH [LOCAL] TIME ZONE` | `2026-10-26T12:00:00.123456Z` |

Примеры различий:

- `timestamp_local`: `2026-10-26T12:00:00` — показание часов без информации
  о часовом поясе; нельзя однозначно перевести его в UTC. Загрузчик не
  приписывает ему пояс сервера или Spark.
- `timestamp_instant`: `2026-10-26T15:00:00+03:00` и
  `2026-10-26T12:00:00Z` — один момент; в pg-meta сохраняется второй вид.
- Oracle `DATE '2026-10-26'` может иметь также часы, минуты и секунды:
  позиция хранится как `timestamp_local(0)`, например `2026-10-26T12:34:56`.
- MSSQL `datetime2(7)` со значением `2026-10-26T12:00:00.1234567`
  сохраняет все семь цифр в [watermark](glossary.md#term-watermark), хотя обычная timestamp-колонка
  DataFrame ограничена шестью. Для этого источник дополнительно возвращает
  позицию строкой (`CONVERT` + `DATEPART(nanosecond)`). В Oracle применяется
  `TO_CHAR` с маской вплоть до `FF9`.
- Oracle `TIMESTAMP WITH [LOCAL] TIME ZONE` проецируется через
  `SYS_EXTRACT_UTC` ещё в SELECT: Spark JDBC не отображает исходные JDBC-типы
  -101/-102. Это касается и обычных колонок [raw](glossary.md#term-raw), не только позиции.

Слово `timestamp` неоднозначно: **Oracle TIMESTAMP поддерживается**,
а **MSSQL `timestamp`/`rowversion` не поддерживается** как позиция. В SQL Server
это бинарный счётчик версий, не дата-время. [Lookback](glossary.md#term-lookback) в днях не делает его
поддерживаемым: нужны отдельные правила бинарной позиции и SQL-предикатов.

#### Настройка lookback

- `size` — целое число; дробные и нечисловые значения отклоняются
  валидацией во всех точках приёма ([манифест](glossary.md#term-manifest), meta-клиент, арифметика
  позиций).
- Для целочисленных колонок `unit` запрещён при ненулевом отступе (отступ —
  просто число, без выравнивания); для date допустим только `d`; для
  timestamp — `h` или `d`.
- Явный `size: 0` отключает отступ целиком; `unit` при этом игнорируется без
  проверки допустимости. Каноническая запись отключения — `size: 0` без
  `unit`.
- Блок `lookback` в YAML необязателен и заменяет default целиком, а не
  сливается с ним по полям. Default определяется типом колонки [инкремента](glossary.md#term-increment):
  целочисленная — всегда 0; date — всегда `3d`; timestamp — по расписанию
  DAG-а (см. ниже). Генератор запекает конкретные size/unit в сгенерированный
  DAG; фактический тип колонки узнаётся только при [исполнении](glossary.md#term-execution), поэтому
  запечённый временной default при целочисленной колонке превращается в 0,
  а при date — в `3d`; в реестр партий попадают фактически применённые
  значения.

#### Отступ по умолчанию и расписание

Расписание — стандартный пятиполевой cron с числовыми полями.
Допустимы `*`, числа, диапазоны, шаги и списки.
Макросы вроде `@daily`, имена месяцев и дней недели, шестиполевые выражения запрещены.

Для timestamp генератор считает количество значений minute × количество
значений hour:

- Два и более срабатывания — отступ `3h`.
- Одно срабатывание или отсутствие расписания — `3d`.

Примеры: `0 */6 * * *` даёт `3h`, `0 2 * * *` — `3d`.
Результат не зависит от даты генерации.
Это правило выбирает объём повторного чтения, но не гарантирует покрытие простоя.
Например, три дня не покрывают интервал между еженедельными запусками.

#### Выравнивание временной границы

При ненулевом временном отступе нижняя граница равна началу
часа (`h`) или календарного дня (`d`), достигнутого вычитанием:
`lower = floor_hour(watermark - N·h)` или `floor_day(watermark - N·d)`.
Выравнивание выполняется в календаре самого значения: для
`timestamp_instant` позиция хранится в UTC-виде и сетка часов/дней одна —
общая; для `timestamp_local` — по настенным цифрам значения без назначения
часового пояса (приписывание пояса ради выравнивания — произвольное
допущение). Примеры при [lookback](glossary.md#term-lookback) `3h` и [watermark](glossary.md#term-watermark) `12:34:56`: instant
`2026-10-26T12:34:56Z` → `2026-10-26T09:00:00Z` (в том числе для значения
источника `15:34:56+03:00` — это тот же инстант); local
`2026-10-26T12:34:56` → `2026-10-26T09:00:00` без зоны. Для `date` значение
уже кратно дню: `3d` при [watermark](glossary.md#term-watermark) `2026-10-26` даёт `2026-10-23`.

#### Границы окна

[Окно](glossary.md#term-window) чтения регулярной [партии](glossary.md#term-batch):
`lower OP col AND col <= upper`, где `upper` — максимум колонки источника,
зафиксированный **до** чтения. Оператор `OP` зависит от типа и отступа:

| Отступ | Тип позиции | Нижняя граница | OP |
|---|---|---|---|
| 0 | любой | [watermark](glossary.md#term-watermark) | `>` |
| N > 0 | целочисленная | `watermark − N` (без выравнивания) | `>` |
| N > 0 | date / timestamp | выровненное начало часа/дня | `>=` |

Строгий `>` у числового отступа перечитывает диапазон
`(watermark − N, watermark]` и без включительной границы. Фактически
применённый оператор сохраняется в колонке [партии](glossary.md#term-batch) `lower_bound_op`, и читатель
[окна](glossary.md#term-window) берёт его оттуда, а не выводит из типа или отступа.

Если вычитание выходит за минимум типа, нижняя граница не отбрасывается
частично, а снимается: lower = NULL, [окно](glossary.md#term-window) читается от начала (безопаснее
потерять производительность, чем строки); `lower_bound_op` при этом
сохраняет «потенциальный» оператор и на предикат не влияет.

#### Первичное наполнение и проверка источника

Первичное наполнение (**[bootstrap](glossary.md#term-bootstrap)**, [watermark](glossary.md#term-watermark) ещё нет) читает
строки до зафиксированного `upper`, без нижней границы и [lookback](glossary.md#term-lookback); это фаза
инкрементального потока (`reason=bootstrap`), а не отдельный
режим. Если источник меняется во время загрузки, строки выше этого максимума
относятся уже к следующему запуску. NULL в колонке [инкремента](glossary.md#term-increment) блокирует
планирование новой [партии](glossary.md#term-batch) регулярной загрузки, включая [bootstrap](glossary.md#term-bootstrap).
Перед созданием новой [партии](glossary.md#term-batch) функция `source_position()` выполняет по всей
таблице `SELECT MAX(col), COUNT(*), COUNT(col)` (с преобразованием MAX для
временных типов). Разные COUNT означают наличие NULL: до создания [партии](glossary.md#term-batch)
и до записи [raw](glossary.md#term-raw) выбрасывается `NULL incremental value in source`.
Повторное [исполнение](glossary.md#term-execution) существующей незавершённой [партии](glossary.md#term-batch) проверяет только
JDBC-метаданные типа: границы и `empty_at_plan` уже зафиксированы,
повторные COUNT/MAX для планирования и проверка NULL не выполняются.
Ожидаемый тип сравнивается с типом [партии](glossary.md#term-batch) и фактическим типом источника.
[Партия](glossary.md#term-batch) со статусом `SUCCEEDED` завершается без
подключения к источнику и без записи Iceberg: в лог задачи выводится
зафиксированный ранее успех (`process_id`, ключ [партии](glossary.md#term-batch), `row_count`,
`finished_at`, пометка «повторное чтение не выполняется»).

Если текущий максимум стал меньше сохранённого [watermark](glossary.md#term-watermark) либо ранее непустой
источник опустел, планирование новой [партии](glossary.md#term-batch) отклоняется: возможен сброс
источника; требуется явная миграция контракта (раздел 7.3).

#### Что фиксируется при планировании

Верхняя граница `upper` ограничивает [партию](glossary.md#term-batch) и сохраняет те же
предикаты при Airflow [retry](glossary.md#term-retry)/[clear](glossary.md#term-clear). Максимум фактически прочитанных строк
(`observed_max`) всё равно вычисляется после чтения — именно он, а не
запланированный upper, продвигает [watermark](glossary.md#term-watermark). MIN/MAX для JDBC-нарезки —
другая операция, выполняемая над уже ограниченным [окном](glossary.md#term-window).
Даже фиксированные границы **не фиксируют содержимое** источника: общей
транзакции, фиксирующей состояние источника для всех JDBC-запросов и повторов, у загрузчика нет.

#### Требования к колонке инкремента

- Колонка должна иметь поддерживаемый тип и не содержать NULL.
- Для insert-only подходят растущий ID или номер итерации источника;
  обновления старых строк они не обнаруживают, если значение не меняется.
- Для insert/update нужна техническая метка изменения, обновляемая при
  каждом изменении строки; бизнес-дата сделки обычно этого не гарантирует.
- Разработчик должен знать задержки коммитов, возможность заднего числа,
  сброса значений и смены часового пояса. [Lookback](glossary.md#term-lookback) выбирается с учётом этих
  свойств, индекса на колонке и стоимости повторного чтения.

[Lookback](glossary.md#term-lookback) снижает риск пропуска поздних коммитов **только внутри своего [окна](glossary.md#term-window)**.
Даже `3d` не обнаруживает более поздний коммит со старой меткой, изменение
без обновления метки или физическое удаление. Для периодических корректировок
фреймворк предоставляет отдельные [таблицы-снимки](glossary.md#term-snapshot) (раздел 7.7); сопоставление
с накопленным [инкрементом](glossary.md#term-increment) выполняет следующий слой. Один [lookback](glossary.md#term-lookback), верхняя
граница и высокая точность не дают гарантии полноты и не восстанавливают
уже удалённую историю источника.

### 7.2. Жизненный цикл загрузки

Загрузкой инкрементального [окна](glossary.md#term-window) управляет `managed_load()` в
`ingestion/spark/raw/rdbms/incremental/load_entity.py`. Она вызывает
`ingestion/meta.py` для коротких транзакций pg-meta и общие модули
`reader.py`/`raw_append.py` для JDBC-чтения и записи [raw](glossary.md#term-raw). [Полный срез](glossary.md#term-full)
исполняет `full/load_entity.py` без обращений к реестру, снимочный поток —
`incremental/load_act_snap.py` (раздел 7.7).
**[Партия](glossary.md#term-batch)** — одна загрузка одной [сущности](glossary.md#term-entity) в одном [исполнении](glossary.md#term-execution) DAG-а; ключ —
`(process_id, source_id, entity_fqn)`, в [raw](glossary.md#term-raw) [партию](glossary.md#term-batch) отмечает `__process_id`.
**[Попытка](glossary.md#term-attempt)** — [исполнение](glossary.md#term-execution) этой [партии](glossary.md#term-batch): [retry](glossary.md#term-retry) задач Airflow не создаёт новую
[партию](glossary.md#term-batch); новый `process_id` (полный [Clear](glossary.md#term-clear) генератора и downstream или [retry](glossary.md#term-retry)
самого генератора) создаёт новое [исполнение](glossary.md#term-execution) с новыми [партиями](glossary.md#term-batch). В начале
каждой [попытки](glossary.md#term-attempt) load-задача пишет в лог `process_id` и составной ключ [партии](glossary.md#term-batch).

`revision` — счётчик изменений строки `incremental_state`, начиная с 1.
[Партия](glossary.md#term-batch) сохраняет его как `expected_revision`. При успехе оба изменения —
[watermark](glossary.md#term-watermark) состояния и статус [партии](glossary.md#term-batch) — фиксируются одной транзакцией pg-meta.
Iceberg-коммит в эту транзакцию не входит.

Реестр обслуживает **только инкрементальные [сущности](glossary.md#term-entity)**:

| Поток | reason | target | Границы | [Watermark](glossary.md#term-watermark) |
|---|---|---|---|---|
| Инкрементальный [bootstrap](glossary.md#term-bootstrap) ([watermark](glossary.md#term-watermark) NULL) | `bootstrap` | [raw](glossary.md#term-raw)-таблица | только upper | устанавливается (NULL → максимум) |
| Инкрементальный регулярный | `regular` | [raw](glossary.md#term-raw)-таблица | lower и upper | max([watermark](glossary.md#term-watermark), observed) |

[Полный срез](glossary.md#term-full) ([сущность](glossary.md#term-entity) без инкрементальной колонки) и снимочный поток
(DAG-и снимков, отдельная доработка) в реестр **не пишутся**: их загрузки
не имеют окон и [watermark](glossary.md#term-watermark), факт и свежесть загрузки читаются по данным [raw](glossary.md#term-raw)
и метаданным Iceberg, а не по pg-meta. Наличие строки `incremental_state`
само по себе означает, что [сущность](glossary.md#term-entity) инкрементальная; отдельной колонки
режима нет.

1. **План.** [Driver](glossary.md#term-driver) ищет [партию](glossary.md#term-batch) через `meta.get_batch()`.
   - [Партия](glossary.md#term-batch) уже `SUCCEEDED`: вывести сохранённый результат и завершиться без чтения источника.
   - Незавершённая [партия](glossary.md#term-batch) существует: проверить JDBC-тип, использовать сохранённое [окно](glossary.md#term-window).
     MAX и COUNT для планирования повторно не выполнять.
   - [Партии](glossary.md#term-batch) нет: проверить тип, максимум и NULL в источнике. При необходимости
     создать `incremental_state` с ревизией 1. Сохранить [партию](glossary.md#term-batch) `PLANNED`,
     границы, [lookback](glossary.md#term-lookback) и оператор `lower_bound_op`.
2. **Пустое [окно](glossary.md#term-window) без чтения строк.** Если замороженные границы доказывают, что
   новых строк нет (регулярная [партия](glossary.md#term-batch) с нулевым [lookback](glossary.md#term-lookback), у которой нижняя
   граница равна верхней), [партия](glossary.md#term-batch) сразу закрывается с `row_count=0` и
   `attempt_count=0`. При ненулевом [lookback](glossary.md#term-lookback) [окно](glossary.md#term-window) читается всегда, даже при
   равных границах: включительная выровненная нижняя граница покрывает
   диапазон между ними. Это ветка внутри `managed_load()`:
   оператор не отменяется, `spark-submit` и SparkSession уже запущены,
   диагностические JDBC-запросы выполнены; не выполняется чтение DataFrame
   [окна](glossary.md#term-window) и запись Iceberg. Для первичного наполнения пустого источника эта
   оптимизация не действует: нужен пустой DataFrame для создания [raw](glossary.md#term-raw)-таблицы.
3. **[Исполнение](glossary.md#term-execution).** Для инкрементальной [партии](glossary.md#term-batch) `meta.start_batch()` ставит
   `RUNNING`, увеличивает `incremental_batches.attempt_count` на 1 и
   очищает `finished_at` ([полный срез](glossary.md#term-full) стартует без этого шага). Затем
   загрузчик строит SELECT с сохранёнными границами (оператор нижней границы —
   из `lower_bound_op`), читает его через Spark JDBC и кэширует DataFrame.

   **Служебная колонка `__extract_position`.** В подзапрос чтения
   инкрементальной [сущности](glossary.md#term-entity) добавляется ещё одна колонка — точное значение
   [позиции инкремента](glossary.md#term-position) строки, отрендеренное диалектом в канонический текст.
   Зачем она: Spark хранит timestamp с точностью до микросекунд, а позиция
   источника бывает точнее (до наносекунд, раздел 7.1); [watermark](glossary.md#term-watermark) обязан
   сохранять точность источника, поэтому позиция передаётся строкой —
   `CONVERT`/`DATEPART(nanosecond)` в MSSQL, `TO_CHAR` с маской до `FF9`
   в Oracle. Агрегация [попытки](glossary.md#term-attempt) считает число строк и
   `MAX(__extract_position)` — это `observed_max` и новый [watermark](glossary.md#term-watermark).
   Колонка существует только в кадре чтения: перед append в [raw](glossary.md#term-raw) она
   удаляется, в целевой таблице её нет (колонка источника с таким именем
   отвергается при создании таблицы как конфликт со служебными именами).
4. **Финализация.** После Iceberg append функция `meta.finalize_batch()`
   в одной транзакции закрывает [партию](glossary.md#term-batch) и обновляет `incremental_state`:
   [watermark](glossary.md#term-watermark) становится максимумом прежней
   позиции и фактически прочитанной, `revision` увеличивается на 1,
   `last_process_id` указывает на это [исполнение](glossary.md#term-execution), `updated_at` получает
   текущее время. [Партия](glossary.md#term-batch) получает `SUCCEEDED`, `row_count`, `observed_max`,
   `watermark_after`, `finished_at`. Проверка `WHERE revision =
   expected_revision` называется [compare-and-set](glossary.md#term-cas) ([CAS](glossary.md#term-cas)): записать результат
   можно лишь поверх версии состояния, с которой планировалась [партия](glossary.md#term-batch).
   Чужая финализация или миграция меняет ревизию — старая [партия](glossary.md#term-batch)
   отклоняется без частичного изменения pg-meta. [Полный срез](glossary.md#term-full) этого шага
   не имеет вовсе.
5. **Сбой.** Ошибка может случиться на любом шаге [попытки](glossary.md#term-attempt) — и до, и после
   Iceberg-коммита; обработчик единый, и если он смог обратиться к pg-meta,
   отдельной короткой транзакцией [партия](glossary.md#term-batch) помечается `FAILED`;
   состояние ([watermark](glossary.md#term-watermark)) не меняется. Запись данных при этом могла уже
   состояться — `FAILED` фиксирует незафиксированный успех, а не отсутствие
   записи ([at-least-once](glossary.md#term-atleast), см. ниже). Причина сбоя в реестре не хранится:
   сырой текст исключения рисковал бы утечкой реквизитов источника;
   подробности — в task logs Airflow (сопоставление — по `dag_id`/`run_id`
   и load-задаче [сущности](glossary.md#term-entity)). Повтор начинается снова с шага 2/3 той же
   [партии](glossary.md#term-batch).

Каждая операция pg-meta берёт `SELECT ... FOR UPDATE` сначала на строку
`incremental_state`, затем на строку [партии](glossary.md#term-batch). Единый порядок уменьшает риск
взаимных блокировок; блокировки отпускаются при завершении короткой
транзакции и не удерживаются всё время JDBC-чтения.

Если [партия](glossary.md#term-batch) уже `SUCCEEDED`, `managed_load()` возвращается **без повторного
чтения источника и append**. Если Iceberg-коммит состоялся, а финализация
pg-meta не состоялась, [retry](glossary.md#term-retry) повторит append с тем же `process_id` — возможны
дубли, причём `__load_dttm` новой [попытки](glossary.md#term-attempt) может отличаться. [CAS](glossary.md#term-cas) защищает
метаданные, но не запрещает двум живым исполнителям одной [партии](glossary.md#term-batch) одновременно
дописать данные. Перед [retry](glossary.md#term-retry) прежний исполнитель должен быть остановлен.

Ошибки планирования происходят до `start_batch()` и вне блока обработки
ошибок [исполнения](glossary.md#term-execution): они не обязаны оставлять [партию](glossary.md#term-batch) со статусом `FAILED`.
Новая [партия](glossary.md#term-batch) при таком отказе может вообще не появиться.

### 7.3. `incremental_state` — состояние сущности

Одна строка на пару (источник, [сущность](glossary.md#term-entity)) — только у инкрементальных
[сущностей](glossary.md#term-entity); первичный ключ `(source_id, entity_fqn)`. [Полные срезы](glossary.md#term-full) строк
не имеют вовсе: факт их загрузок читается по данным [raw](glossary.md#term-raw) и метаданным
Iceberg.

| Колонка | Тип | Смысл |
|---|---|---|
| `source_id` | text, PK | Идентификатор источника (`bank`, `broker`) |
| `entity_fqn` | text, PK | Полное имя [сущности](glossary.md#term-entity) (`retail_bank.transactions`) |
| `incremental_column` | text | Имя колонки [инкремента](glossary.md#term-increment), обязательно. Смена значения требует миграции контракта |
| `incremental_type` | text | Внутреннее, независимое от СУБД имя типа позиции: `bigint`, `decimal(18,0)`, `date`, `timestamp_local(6)`, `timestamp_instant(7)`; например Oracle `DATE` отображается в `timestamp_local(0)`, MSSQL `datetime2(7)` — в `timestamp_local(7)` |
| `watermark` | text | Максимальная успешно загруженная позиция в едином текстовом формате. NULL до непустого первичного наполнения. Точность источника сохраняется |
| `revision` | bigint | Версия строки состояния, начиная с 1; увеличивается при успешной финализации и ручной миграции; сравнивается с `expected_revision` [партии](glossary.md#term-batch) (раздел 7.2) |
| `last_process_id` | bigint | [Исполнение](glossary.md#term-execution) последней успешной инкрементальной [партии](glossary.md#term-batch); составной FK `(last_process_id, source_id, entity_fqn)` на реестр партий с отложенной проверкой (один process_id обслуживает несколько [сущностей](glossary.md#term-entity) — одиночная ссылка была бы неоднозначной) |
| `updated_at` | timestamptz | Момент последнего изменения строки |

Колонки контракта `NOT NULL`: строка существует — [сущность](glossary.md#term-entity) инкрементальная.

### 7.4. `incremental_batches` — реестр партий

Одна строка описывает [партию](glossary.md#term-batch) одной таблицы. Ключ —
`(process_id, source_id, entity_fqn)`.

- [Retry](glossary.md#term-retry) задачи `load` сохраняет ключ [партии](glossary.md#term-batch): `process_id` берётся из XCom.
- Повтор `generate_process_id` выделяет новый номер и создаёт новые [партии](glossary.md#term-batch).
- `dag_id` и `run_id` нужны для поиска запуска и логов, но не определяют [партию](glossary.md#term-batch).
- [Полные срезы](glossary.md#term-full) и [таблицы-снимки](glossary.md#term-snapshot) в этом реестре не учитываются.

**Пример:** запуск с `run_id=R` загрузил [партию](glossary.md#term-batch) с `process_id=42`.
После полного [Clear](glossary.md#term-clear) тот же запуск получил номер 43.
В реестре сохранятся [партии](glossary.md#term-batch) 42 и 43 с одним `run_id=R`.

| Колонка | Тип | Смысл |
|---|---|---|
| `process_id` | bigint, PK | Идентификатор [исполнения](glossary.md#term-execution) DAG-а; то же значение, что и `__process_id` в [raw](glossary.md#term-raw) |
| `dag_id`, `run_id` | text | Принадлежность [DagRun](glossary.md#term-dagrun) (корреляция и логи, не ключ идентичности) |
| `source_id`, `entity_fqn` | text, PK | [Сущность](glossary.md#term-entity); FK на `incremental_state` |
| `target_table` | text | Целевая [raw](glossary.md#term-raw)-таблица |
| `reason` | text | Фаза инкрементального потока: `bootstrap` (первичное наполнение) или `regular` (очередное [окно](glossary.md#term-window)) |
| `incremental_column`, `incremental_type` | text | Снимок контракта [инкремента](glossary.md#term-increment) на момент [партии](glossary.md#term-batch) |
| `expected_revision` | bigint | Ревизия состояния, которую [партия](glossary.md#term-batch) вправе финализировать (для [CAS](glossary.md#term-cas)) |
| `watermark_before` | text | Значение [watermark](glossary.md#term-watermark) до [партии](glossary.md#term-batch) |
| `lookback_size`, `lookback_unit` | numeric / text | Фактически применённый отступ (в [партию](glossary.md#term-batch) попадают значения после приведения default к фактическому типу колонки) |
| `lower_bound`, `lower_bound_op` | text | Зафиксированная нижняя граница и оператор (`>` / `>=`). NULL границы означает её отсутствие: [bootstrap](glossary.md#term-bootstrap) или выход за минимум типа |
| `upper_bound` | text | Замороженная верхняя граница [окна](glossary.md#term-window) чтения |
| `empty_at_plan` | boolean | Вся таблица источника была пуста при предварительной проверке перед созданием [партии](glossary.md#term-batch) (подробнее ниже); для пустого [bootstrap](glossary.md#term-bootstrap) фиксирует пустую выборку через `WHERE 1=0` |
| `status` | text | `PLANNED` → `RUNNING` → `SUCCEEDED` / `FAILED` |
| `attempt_count` | int | Число запусков [исполнения](glossary.md#term-execution) (0 у [партии](glossary.md#term-batch), закрытой без чтения) |
| `row_count` | bigint | Дописано строк (заполняется при успехе) |
| `observed_max` | text | Точный максимум колонки [инкремента](glossary.md#term-increment) в прочитанном [окне](glossary.md#term-window) |
| `watermark_after` | text | Значение [watermark](glossary.md#term-watermark) после [партии](glossary.md#term-batch) |
| `created_at`, `finished_at` | timestamptz | `created_at` задаётся DEFAULT `clock_timestamp()` при INSERT [партии](glossary.md#term-batch); `finished_at` — при SUCCEEDED или FAILED; повторный `start_batch()` очищает его |

**Что фиксирует `empty_at_plan`.** До создания [партии](glossary.md#term-batch) [сущности](glossary.md#term-entity) с настроенным
[инкрементом](glossary.md#term-increment) `source_position()` выполняет предварительный SELECT по всей
таблице источника: максимум выбранной колонки, общее число строк `COUNT(*)`
и число ненулевых значений `COUNT(col)`. Для временных типов максимум
дополнительно преобразуется в точную строку. Этот запрос получает границу
и проверяет пустоту/NULL; он не загружает строки таблицы в [raw](glossary.md#term-raw).

Если `COUNT(*)=0`, при первичном наполнении сохраняется
`empty_at_plan=true`. Одного `upper_bound=NULL` недостаточно: NULL также
означает отсутствие верхнего ограничения. Поэтому при чтении такой [партии](glossary.md#term-batch)
добавляется `WHERE 1=0`: даже если после проверки в источнике появились
строки, эта [партия](glossary.md#term-batch) остаётся пустой. Загрузчик получает схему пустого DataFrame
и создаёт [raw](glossary.md#term-raw)-таблицу, если её ещё нет. [Watermark](glossary.md#term-watermark) остаётся NULL; следующий
новый [DagRun](glossary.md#term-dagrun) снова выполняет первичное наполнение. Это признак пустоты
**источника при планировании**, не признак отсутствия новых строк после
[watermark](glossary.md#term-watermark). При [retry](glossary.md#term-retry) сохранённый флаг не пересчитывается.

**`observed_max` и `watermark_after`.** Первое поле описывает текущую
успешную выборку, второе — позицию [сущности](glossary.md#term-entity) после её обработки:

- Для инкрементальных партий ([bootstrap](glossary.md#term-bootstrap) и regular) `watermark_after` равен
  максимуму прежнего [watermark](glossary.md#term-watermark) и `observed_max`, с учётом типа позиции.
- При пустой выборке `observed_max=NULL`, но `watermark_after` сохраняет
  прежний [watermark](glossary.md#term-watermark). Если [watermark](glossary.md#term-watermark) ещё не было, он остаётся NULL.
- Если из-за [lookback](glossary.md#term-lookback) прочитаны только значения ниже прежнего [watermark](glossary.md#term-watermark),
  `observed_max` будет меньше него, но `watermark_after` назад не сдвинется.
- [Полные срезы](glossary.md#term-full) здесь не учитываются: у них нет строки [партии](glossary.md#term-batch) и этих полей результата.

Следующая **новая** [партия](glossary.md#term-batch) отсчитывает нижнюю границу от
`incremental_state.watermark`, куда при финализации записан `watermark_after`,
с применением [lookback](glossary.md#term-lookback). [Retry](glossary.md#term-retry) не вычисляет [окно](glossary.md#term-window) заново, а использует
`lower_bound`, `lower_bound_op` и `upper_bound` уже созданной [партии](glossary.md#term-batch).

Основные CHECK-ограничения жизненного цикла (полный DDL —
`conf/pg-meta/init/01_ingestion.sql`):

- Колонка и тип [инкремента](glossary.md#term-increment) обязательны (NOT NULL).
- [Bootstrap](glossary.md#term-bootstrap): прежнего [watermark](glossary.md#term-watermark) и нижней границы нет; верхняя граница может
  быть задана.
- Regular: прежний [watermark](glossary.md#term-watermark) известен.
- Успех: число строк и время завершения заполнены.
- До успеха, включая `FAILED`: поля результата пусты.

```sql
CHECK (reason IN ('bootstrap', 'regular'));
CHECK (reason <> 'bootstrap' OR
  (watermark_before IS NULL AND lower_bound IS NULL));
CHECK (reason <> 'regular' OR watermark_before IS NOT NULL);
CHECK (status <> 'SUCCEEDED' OR
  (row_count IS NOT NULL AND finished_at IS NOT NULL));
CHECK (status = 'SUCCEEDED' OR
  (row_count IS NULL AND observed_max IS NULL AND watermark_after IS NULL));
```

Это выражения из определения таблицы, не самостоятельный SQL-скрипт миграции.

### 7.5. Пример одного инкремента и повтора

Условный пример: `bank`, `retail_bank.transactions`, колонка `_batch_id`
типа `bigint`, без [lookback](glossary.md#term-lookback). В источнике уже загружены значения до 10,
затем добавились две строки со значением 11. Номера и времена ниже
иллюстративные, не результаты запуска стенда.

1. До запуска строка состояния содержит:

   | Поле | Значение |
   |---|---|
   | `source_id`, `entity_fqn` | `bank`, `retail_bank.transactions` |
   | `incremental_column`, `incremental_type` | `_batch_id`, `bigint` |
   | `watermark`, `revision`, `last_process_id` | `10`, `5`, `40` |
   | `updated_at` | время предыдущего успеха |

2. Новый [DagRun](glossary.md#term-dagrun) исполняет `generate_process_id` и получает `process_id=42`.
   В `airflow_dagruns_log` появляется START с новым `id`, этим `process_id`,
   `dag_id`, `run_id`, логической датой запуска и `ts`. `log_start` добавляет
   в `airflow_tasks_log` новый `id`, `process_id=42`, источник, [FQN](glossary.md#term-fqn), целевую
   таблицу, `status=START`, `ts`.
3. [Партии](glossary.md#term-batch) с ключом `(42, bank, retail_bank.transactions)` ещё нет —
   загрузчик проверяет источник: MAX=11, COUNT(*)=COUNT(_batch_id): NULL нет.
   Планирование создаёт строку [партии](glossary.md#term-batch); состояние пока не меняется:

   | Поля [партии](glossary.md#term-batch) | Значения при INSERT |
   |---|---|
   | `process_id` | `42` |
   | `dag_id`, `run_id` | `ingest__rdbms__bank__main`, идентификатор нового [DagRun](glossary.md#term-dagrun) |
   | `source_id`, `entity_fqn` | `bank`, `retail_bank.transactions` |
   | `target_table` | `raw.bank.retail_bank__transactions` |
   | `reason` | `regular` |
   | `incremental_column`, `incremental_type` | `_batch_id`, `bigint` |
   | `expected_revision`, `watermark_before` | `5`, `10` |
   | `lookback_size`, `lookback_unit` | `0`, NULL |
   | `lower_bound`, `lower_bound_op`, `upper_bound` | `10`, `>`, `11` |
   | `empty_at_plan` | false |
   | `status`, `attempt_count` | `PLANNED`, `0` |
   | `row_count`, `observed_max`, `watermark_after` | NULL, NULL, NULL |
   | `created_at`, `finished_at` | время INSERT, NULL |

4. Для строки [партии](glossary.md#term-batch) функция `start_batch()` меняет `status` на `RUNNING`,
   увеличивает счётчик [попыток](glossary.md#term-attempt) `attempt_count` с 0 до 1 и устанавливает
   `finished_at=NULL`. Строка `ingestion.incremental_state` не меняется.
   SELECT читает `10 < _batch_id AND
   _batch_id <= 11`. В raw дописываются две строки с `__process_id=42`
   и временем этого append в `__load_dttm`.
5. Успех меняет в состоянии `watermark=11`, `revision=6`,
   `last_process_id=42`, `updated_at=время финализации`. В [партии](glossary.md#term-batch) меняются
   `status=SUCCEEDED`, `row_count=2`, `observed_max=11`,
   `watermark_after=11`, `finished_at=время завершения`. Остальные поля
   неизменны. Далее `log_end` добавляет событие END [сущности](glossary.md#term-entity), публикуется
   [Asset](glossary.md#term-asset), при успехе всего DAG-а колбэк добавляет END запуска.
6. Альтернативный исход шага 5: Iceberg append уже закоммичен, но транзакция
   финализации pg-meta не закоммичена. Две строки в [raw](glossary.md#term-raw) уже существуют,
   а метаданные ещё не подтверждают успех.

   Откуда берётся ошибка: Iceberg-коммит — не последний шаг [попытки](glossary.md#term-attempt). Сразу
   после него `managed_load()` вызывает `meta.finalize_batch()`, и отказ
   именно этого вызова — pg-meta недоступна, обрыв соединения, несовпадение
   ревизии в [CAS](glossary.md#term-cas) — попадает в общий обработчик ошибок, который ставит [попытке](glossary.md#term-attempt)
   `FAILED`. `FAILED` означает «успех [попытки](glossary.md#term-attempt) не зафиксирован в реестре»,
   а не «данные не записаны»: строки в Iceberg могут уже существовать, и
   именно поэтому [retry](glossary.md#term-retry) повторяет append того же [окна](glossary.md#term-window) ([at-least-once](glossary.md#term-atleast)).
   Сбой до Iceberg-коммита (чтение [окна](glossary.md#term-window), запись) обрабатывается так же;
   при аварийной смерти [driver](glossary.md#term-driver) обработчик не выполняется вовсе и [партия](glossary.md#term-batch)
   остаётся `RUNNING`. Состояния на этот момент:

   - В `ingestion.incremental_state` по ключу
     `(bank, retail_bank.transactions)` остаются `watermark=10`,
     `revision=5`, `last_process_id=40`, прежний `updated_at`.
   - Если обработчик ошибки смог обратиться к pg-meta, в
     `ingestion.incremental_batches` он ставит `status=FAILED`,
     `finished_at=время регистрации ошибки`. `attempt_count=1`;
     `row_count`, `observed_max`, `watermark_after` остаются NULL.
   - Повторное [исполнение](glossary.md#term-execution) load-задачи — [retry](glossary.md#term-retry) таска или [Clear](glossary.md#term-clear) только
     downstream-задач: XCom отдаёт прежний `process_id=42`, новая [партия](glossary.md#term-batch)
     и новый `process_id` не создаются. `start_batch()` увеличивает
     `attempt_count` до 2, ставит `RUNNING`, очищает `finished_at`.
     Планирование не подключается к источнику; границы остаются
     `lower_bound=10` (оператор `>`), `upper_bound=11`.
   - Если источник не изменился, повтор снова дописывает две строки
     с `__process_id=42`, но временем новой записи в `__load_dttm`.
     В [raw](glossary.md#term-raw) теперь четыре строки этой [партии](glossary.md#term-batch).
   - При успешной финализации метаданные получают значения шага 5,
     а `attempt_count` остаётся 2. Значение
     `incremental_batches.row_count=2` описывает успешную [попытку](glossary.md#term-attempt),
     **не все четыре физически дописанные строки**. Это следствие
     отсутствия общей транзакции Iceberg и pg-meta.
7. **[Clear](glossary.md#term-clear) существующего [DagRun](glossary.md#term-dagrun).** Результат зависит от выбранных задач:
   - Очищены генератор и все задачи после него: новый `process_id=43`, новые [партии](glossary.md#term-batch).
   - Очищена только успешная `load`: прежний `process_id=42`.
     Загрузчик находит `SUCCEEDED`, выводит результат и завершает задачу
     без чтения, append и изменения состояния.
   - При частичном [Clear](glossary.md#term-clear) таблицы одного [DagRun](glossary.md#term-dagrun) могут относиться к разным
     [исполнениям](glossary.md#term-execution). Общего согласованного среза всех таблиц это не гарантирует.
   - История pg-meta сохраняется. По ней нельзя определить, был [Clear](glossary.md#term-clear) полным или частичным.
   - Событие завершения [DagRun](glossary.md#term-dagrun) относится к последнему `process_id`
     для пары `(dag_id, run_id)`.
8. **Новый [DagRun](glossary.md#term-dagrun)** — отдельный запуск по расписанию или через Trigger,
   с другим `run_id`, а не [Clear](glossary.md#term-clear) существующего. Он получает новый
   `process_id`, например 44. Если после успеха шага 5 источник не менялся,
   прежний [watermark](glossary.md#term-watermark) равен 11, MAX источника также 11, [lookback](glossary.md#term-lookback)=0. Новая
   [партия](glossary.md#term-batch) получает `watermark_before=11`, `expected_revision=6`,
   `lower_bound=11` (оператор `>`), `upper_bound=11`. [Окно](glossary.md#term-window)
   `11 < col AND col <= 11` пусто, поэтому чтение [окна](glossary.md#term-window) и append
   не выполняются. При финализации:

   - В `ingestion.incremental_batches` для [партии](glossary.md#term-batch) [исполнения](glossary.md#term-execution) 44:
     `status=SUCCEEDED`, `attempt_count=0`, `row_count=0`,
     `observed_max=NULL`, `watermark_after=11`, `finished_at` заполнен.
   - В `ingestion.incremental_state`: `watermark=11`, `revision=7`,
     `last_process_id=44`, `updated_at=время новой финализации`.

   Новая [партия](glossary.md#term-batch) фиксирует отдельную успешную проверку отсутствия новых
   данных, даже если физических строк в Iceberg не добавилось. [Retry](glossary.md#term-retry) задач
   этого [DagRun](glossary.md#term-dagrun) использует тот же `process_id=44` и не создаёт новых партий.

### 7.6. Смена контракта и ручные миграции

Контракт — пара `incremental_column`, `incremental_type` строки
`incremental_state`. Смена колонки или её внутреннего типа блокируется
проверкой pg-meta. Удалённая колонка может привести к SQL-ошибке ещё до
этой проверки. Изменение только [lookback](glossary.md#term-lookback) миграции не требует; уже созданная
[партия](glossary.md#term-batch) сохраняет старые границы.

Переходы между [полным срезом](glossary.md#term-full) и [инкрементом](glossary.md#term-increment) миграцией не являются: отключение
[инкремента](glossary.md#term-increment) в [манифесте](glossary.md#term-manifest) оставляет строку состояния замороженной ([watermark](glossary.md#term-watermark)
сохраняется), повторное включение той же колонки продолжает [окна](glossary.md#term-window) от старого
[watermark](glossary.md#term-watermark), а включение [инкремента](glossary.md#term-increment) у [сущности](glossary.md#term-entity) без строки состояния начинается
с [bootstrap](glossary.md#term-bootstrap) при первой же загрузке.

Порядок ручной миграции:

- Приостановить DAG, чтобы не допускать новые плановые загрузки, и исключить
  ручные запуски на время миграции. Дождаться завершения либо остановить
  каждое Spark-приложение, загружающее эту [сущность](glossary.md#term-entity): текущую load-задачу,
  приложения прежних [попыток](glossary.md#term-attempt), если они пережили сбой Airflow, и отдельно
  запущенные загрузки той же таблицы. Проверить, что их [driver](glossary.md#term-driver)-процессы
  и выполняющие запись [executor](glossary.md#term-executor)-задачи больше не работают. Пауза DAG-а
  или изменение статуса задачи в UI сами по себе этого не обеспечивают.
- Проверить фактическую схему источника и соответствие [raw](glossary.md#term-raw). Новая позиция
  должна поддерживаться диалектом; миграция pg-meta не исправляет
  несовместимую схему Iceberg и не добавляет поддержку нового JDBC-типа.
- В одной транзакции заблокировать состояние, проверить прежний контракт
  и ревизию, вставить аудит, изменить контракт, сбросить [watermark](glossary.md#term-watermark) и
  `last_process_id`, увеличить revision. Историю [raw](glossary.md#term-raw) и партий не удалять.
- Изменить [манифест](glossary.md#term-manifest) и регенерировать DAG. Запустить **новый [DagRun](glossary.md#term-dagrun)**:
  неуспешную [партию](glossary.md#term-batch) старой ревизии повторно использовать нельзя.
- При инкрементальном новом контракте отсутствие [watermark](glossary.md#term-watermark) запускает
  [bootstrap](glossary.md#term-bootstrap); при полном режиме — обычный [полный срез](glossary.md#term-full). Проверить результат
  и аудит до возобновления расписания.

В коде **нет автоматического заполнения** `incremental_state_migrations`,
триггера или готовой команды миграции. Аудит вставляет администратор явно.
Ниже шаблон PostgreSQL для существующей строки состояния. Это пример,
не проверенная на работающем стенде процедура; старые значения и новую
тройку нужно заменить на проверенные значения своей [сущности](glossary.md#term-entity).

```sql
BEGIN;
DO $migration$
DECLARE
  old_state ingestion.incremental_state%ROWTYPE;
  next_column text := 'updated_at';
  next_type text := 'timestamp_local(7)';
BEGIN
  SELECT * INTO STRICT old_state
  FROM ingestion.incremental_state
  WHERE source_id = 'bank' AND entity_fqn = 'retail_bank.transactions'
  FOR UPDATE;

  IF old_state.revision <> 6
     OR old_state.incremental_column IS DISTINCT FROM '_batch_id'
     OR old_state.incremental_type IS DISTINCT FROM 'bigint' THEN
    RAISE EXCEPTION 'Unexpected old incremental contract or revision';
  END IF;

  INSERT INTO ingestion.incremental_state_migrations (
    source_id, entity_fqn, old_revision, new_revision,
    old_column, new_column, old_type, new_type,
    old_watermark, new_watermark, performed_by, reason_text
  ) VALUES (
    old_state.source_id, old_state.entity_fqn,
    old_state.revision, old_state.revision + 1,
    old_state.incremental_column, next_column,
    old_state.incremental_type, next_type,
    old_state.watermark, NULL, current_user,
    'Переход на техническую метку изменения; повторное первичное наполнение'
  );

  UPDATE ingestion.incremental_state
  SET incremental_column = next_column,
      incremental_type = next_type, watermark = NULL, last_process_id = NULL,
      revision = old_state.revision + 1, updated_at = clock_timestamp()
  WHERE source_id = old_state.source_id AND entity_fqn = old_state.entity_fqn;
END
$migration$;
COMMIT;
```

Один шаблон покрывает следующие случаи; меняются объявленные переменные,
проверка старого контракта и осмысленная причина аудита:

| Случай | `next_column` | `next_type` | Изменение YAML |
|---|---|---|---|
| Смена/переименование колонки | `'updated_at'` | `'timestamp_local(7)'` для MSSQL datetime2(7) | новое `source_column` |
| Смена типа той же колонки | `'_batch_id'` | `'decimal(20,0)'` для decimal(20,0) | обновить `contract.position_type` согласованно с состоянием |
| Явный сброс позиции после подтверждённого сброса источника | прежняя колонка | прежний тип | без изменений; [watermark](glossary.md#term-watermark) сбрасывается, следующий запуск — [bootstrap](glossary.md#term-bootstrap) |

Неподдерживаемый новый тип не исправляется заменой строки `incremental_type`:
нужно выбрать другую колонку, отключить [инкремент](glossary.md#term-increment) или доработать диалект.
Удаление/переименование самой [сущности](glossary.md#term-entity) и перенос её в другой `source_id` —
изменение идентичности, не миграция этой тройки.

### 7.7. DAG-и снимков и таблицы-снимки

Каждая таблица с инкрементальной загрузкой получает отдельный **DAG-снимок**.
Отключить его генерацию настройкой нельзя.
Он читает источник целиком и одной операцией публикует **[таблицу-снимок](glossary.md#term-snapshot)**.

Снимок позволяет отдельно от накопленного [инкремента](glossary.md#term-increment) увидеть полное прочитанное
состояние, включая строки за пределами инкрементального [окна](glossary.md#term-window).
Это не гарантирует состояние источника на один момент: JDBC-запросы могут
выполняться в разное время. Время публикации доступно из метаданных Iceberg.

#### Генерация и настройка

- Каждая запись `entities` с блоком `incremental` получает свой DAG-снимок.
  Например, две инкрементальные таблицы дают два DAG-а, даже при одинаковом расписании.
  У каждого — собственные [DagRun](glossary.md#term-dagrun) и `process_id`.
- Имя DAG: `<dag_id пайплайна>__act_snap_<alias>`, где `alias` — первые 8
  символов `base32(sha256(FQN))`; коллизии dag_id и целевых имён ловятся
  генератором глобально (раздел 2).
- Теги: `act_snap`, `source_id`, целевое имя таблицы (`resolve_target_table_name`)
  плюс собственные `act_snap.tags`. По ним снимочные DAG-и находятся
  в UI Airflow без чтения документации DAG-а.
- Блок `incremental.act_snap` задаёт самостоятельный ограниченный контракт
  DAG-а: `schedule`, `tags`, `spark.jdbc_partitioning`, `airflow.pool.name/slots`.
  Defaults и типы — раздел 2; настройки родительской [сущности](glossary.md#term-entity) не наследуются.
  Общий `DagConfig` содержит только schedule и tags; `PipelineManifest`
  расширяет его полями основного [пайплайна](glossary.md#term-pipeline), `ActSnapConfig` — композициями
  `SparkConfig` и `AirflowConfig`. Это не добавляет spark/airflow в корень
  YAML основного [пайплайна](glossary.md#term-pipeline). `catchup=False` для снимков фиксирован;
  `MAX_ACTIVE_RUNS=1` — проектная константа генератора для обоих видов DAG-ов,
  а не параметр YAML.
- Активация источника, колбэки успеха/провала и `TableLogOperator`
  START/END — штатные. В **инкрементальный реестр** (`incremental_state`,
  `incremental_batches`) снимок не пишет ничего; `finalize_batch()`
  выполняет только инкрементальный поток. Контракт [инкремента](glossary.md#term-increment) (колонка/тип)
  снимками не проверяется и не меняется.

#### Структура таблицы-снимка

- Имя: `raw.<namespace>.<table>__act_snap` — то же правило кодирования [FQN](glossary.md#term-fqn),
  что у [raw](glossary.md#term-raw)-таблицы, с суффиксом `__act_snap`; LOCATION
  `s3://datalake/raw/<namespace>/<table>__act_snap`.
- Мета-колонки те же: `__process_id` (автор публикации) и `__load_dttm`
  (единый литерал на весь снимок — по нему считается свежесть данных);
  схему остальной части задаёт JDBC-проекция источника на момент снимка.
- Namespace каталога снимочный поток создаёт сам (`CREATE NAMESPACE IF NOT
  EXISTS` перед первой публикацией): DAG-снимок независим от того, запускалась
  ли когда-либо обычная загрузка [сущности](glossary.md#term-entity).
- Единственное отличие DDL от [raw](glossary.md#term-raw)-таблицы — отсутствие `PARTITIONED BY`:
  все данные одного снимка несут одну дату загрузки, партиционирование
  дням не нужно.

#### Атомарная замена данных

Публикация — один атомарный коммит Iceberg:
`CREATE OR REPLACE TABLE ... USING iceberg TBLPROPERTIES (...) AS SELECT`
([RTAS](glossary.md#term-rtas)). Читатель всегда видит либо целиком предыдущий снимок, либо целиком
новый; промежуточного состояния нет. Схема опубликованной таблицы равна
JDBC-проекции источника плюс мета-колонки: исчезнувшие из источника колонки
после непустой публикации пропадают (в отличие от [append-only](glossary.md#term-append) [raw](glossary.md#term-raw) с
накоплением схемы). История версий доступна средствами Iceberg (time travel)
до очистки устаревших снапшотов; политика очистки не настраивается.

#### Автор и время публикации

`process_id` записывается в табличное
свойство `snapshot.process_id`, обновляемое каждой перезаписью: автор текущей
версии читается из метаданных (`SHOW TBLPROPERTIES` в Spark, `"таблица$properties"`
в Trino) даже когда снапшот пуст. Время публикации — `committed_at` последнего
[снапшота Iceberg](glossary.md#term-iceberg-snapshot) (метаданные `.snapshots` / `"таблица$snapshots"`). Свойства
сводки коммита (`snapshot-property.*`) на закреплённом стеке не доставляются:
в Iceberg 1.8.1 они читаются только из опций конкретной записи, а [RTAS](glossary.md#term-rtas) их не
прокидывает (проверено PoC на стенде) — потому носителем автора выбрано
табличное свойство, заменяющее их в этом контракте.

#### Повтор после сбоя

Iceberg-коммит и завершение задачи — не одна
транзакция. [Retry](glossary.md#term-retry) сначала читает свойство `snapshot.process_id`: оно равно
ключу [партии](glossary.md#term-batch) — публикация уже состоялась, источник не перечитывается, задача
завершается успешно как опубликованная. Не равно — обычный путь (свежее
полное чтение). Перезапись поверх чужой более новой публикации этим не
запрещена: пережившее сбой Spark-приложение может закоммитить старую выборку
поверх новой, автоматический [fencing](glossary.md#term-fencing) не предоставляется — **преждевременный
writer обязан быть остановлен до повтора** (эксплуатационное условие;
`max_active_runs=1` ограничивает только запуски одного DAG-а).

#### Пустое чтение

- Таблица уже существует: сохранить её без изменений, записать предупреждение.
  Нового [snapshot Iceberg](glossary.md#term-iceberg-snapshot) и события [Asset](glossary.md#term-asset) нет. Запуск считается успешной проверкой.
- Таблицы ещё нет: создать пустую таблицу с текущей схемой и автором публикации.
  Событие [Asset](glossary.md#term-asset) не публикуется.
- Повторное пустое чтение не обновляет время публикации.

Следствие: сохранённая непустая таблица может уже не соответствовать опустевшему источнику.

#### Событие готовности Asset

URI снимка — `raw://<source_id>/<fqn>/act_snap`,
обычной загрузки — `raw://<source_id>/<fqn>`. Структура DAG-а постоянна:
`log_start → load → log_end → publish_asset`. Последняя задача всегда
`PythonOperator` с заранее объявленным outlet и callable `publish_asset_event`.
Для обычной загрузки callable не выполняет проверок. Для снимка вызывает
отдельную функцию `snapshot_has_current_process`, которая через `TrinoHook`
(`trino_default`) читает `SELECT __process_id FROM <таблица> LIMIT 1` и
сравнивает значение с process_id из XCom задачи `generate_process_id`.
Все строки атомарного снимка принадлежат одному процессу, поэтому фильтр
и полный скан не нужны. Нет строк или процесс другой — AirflowSkipException:
задача пропущена, события нет, DAG-run успешен. Ошибка запроса означает
ошибку задачи. Первая пустая таблица также не порождает событие. [Retry](glossary.md#term-retry) после
успешной непустой записи проходит проверку без повторной загрузки источника.
Локальных файлов результата и XCom результата Spark-job нет. Для изменения
политики достаточно изменить функцию условия; для безусловной публикации —
вызывать callable без target_table, не меняя структуру задач.

#### Контроль свежести снимков

Сначала проверьте наличие ожидаемой таблицы. Для bank:

```sql
WITH expected(table_name) AS (
    VALUES 'retail_bank__transactions__act_snap'
)
SELECT e.table_name,
       CASE WHEN t.table_name IS NULL THEN 'никогда не публиковался'
            ELSE 'проверить метаданные публикации' END AS publication_status
FROM expected e
LEFT JOIN raw.information_schema.tables t
    ON t.table_schema = 'bank' AND t.table_name = e.table_name;
```

Для существующей таблицы получите метаданные без чтения её строк:

```sql
SELECT
    (SELECT max(committed_at)
     FROM raw.bank."retail_bank__transactions__act_snap$snapshots") AS published_at,
    (SELECT value
     FROM raw.bank."retail_bank__transactions__act_snap$properties"
     WHERE key = 'snapshot.process_id') AS process_id;
```

Для broker используйте схему `broker` и имя `trading_core__transactions__act_snap`.

- Нет таблицы — снимок ещё не публиковался.
- Нет автора — публикация не подтверждена.
- Первая пустая таблица может иметь автора, но не строки и не событие [Asset](glossary.md#term-asset).
- Пустое чтение не обновляет прежнюю публикацию.
- При расписании сравнивайте время с последним ожидаемым запуском cron
  и допустимой задержкой. Календарные интервалы могут различаться.
- Ручные снимки проверяются по отдельному регламенту, не по автоматическому SLA.
- Успешный [DagRun](glossary.md#term-dagrun) сам по себе не доказывает свежесть снимка.

Автоматическая проверка свежести и оповещения на стенде не реализованы.

## 8. База метаданных pg-meta: остальные объекты

`pg-meta` (база `meta`, схема `ingestion`) — выделенная операционная база
фреймворка: последовательность идентификаторов не может жить в озере (нет
атомарного [инкремента](glossary.md#term-increment) на объектном хранилище), а операционные метаданные
должны переживать недоступность озера. Каталог Iceberg (pg-catalog) и база
Airflow — отдельные инстансы. DDL — `conf/pg-meta/init/01_ingestion.sql`.

### Последовательность идентификаторов

**`ingestion.process_id_seq`** — последовательность идентификаторов процессов.
Каждое [исполнение](glossary.md#term-execution) задачи `generate_process_id` любого ingest-DAG-а берёт
`nextval` один раз и вставляет строку START; [retry](glossary.md#term-retry) самой задачи и полный
[Clear](glossary.md#term-clear) выделяют новое значение, [retry](glossary.md#term-retry) downstream-задач использует значение
из XCom. Это значение становится `__process_id` всех [порций](glossary.md#term-portion) [исполнения](glossary.md#term-execution).

### Журналы событий

**`ingestion.airflow_dagruns_log`** — журнал DAG-запусков ([append-only](glossary.md#term-append)):
`process_id`, `dag_id`, `run_id`, `execution_date`, `ts`, `status` (`START` —
из `generate_process_id`; `END`/`FAILED` — колбэки DAG-а, привязанные к
последнему [исполнению](glossary.md#term-execution) `max(process_id)` по `(dag_id, run_id)`). Текущий
статус запуска — последнее событие; длительность — разность `ts`.

**`ingestion.airflow_tasks_log`** — журнал тасков ([append-only](glossary.md#term-append)): `process_id`,
`source_id`, `entity_fqn`, `datalake_table`, `ts`, `status` (`START` / `END` —
`log_start`/`log_end`; `FAILED` — колбэк неудачи `load`).

### Аудит миграций

**`ingestion.incremental_state_migrations`** — аудит ручных миграций контракта
[инкремента](glossary.md#term-increment): старая/новая ревизия (обязательно +1), колонка, тип и
[watermark](glossary.md#term-watermark) до/после, `performed_by`, `reason_text`, `created_at`.

### Запросы к метаданным

Выполняйте в pg-meta через psql или DBeaver.
[Параметры подключения](../README.md#postgresql-pg-meta-в-dbeaver).

```sql
SELECT process_id, dag_id, run_id, status, ts
FROM ingestion.airflow_dagruns_log ORDER BY id DESC LIMIT 10;

SELECT process_id, entity_fqn, datalake_table, status, ts
FROM ingestion.airflow_tasks_log ORDER BY id DESC LIMIT 20;

SELECT entity_fqn, incremental_column, incremental_type,
       watermark, revision, last_process_id, updated_at
FROM ingestion.incremental_state ORDER BY source_id, entity_fqn;

SELECT process_id, entity_fqn, reason, lower_bound, lower_bound_op, upper_bound,
       status, attempt_count, row_count, watermark_after, created_at, finished_at
FROM ingestion.incremental_batches ORDER BY process_id DESC, source_id, entity_fqn LIMIT 15;
```

## 9. Типы данных и гарантии

Путь типа при загрузке: СУБД источника → JDBC → PySpark (Spark JDBC reader,
у обоих диалектов включён `preferTimestampNTZ=true`) → Iceberg DDL. Схема
[raw](glossary.md#term-raw)-таблицы создаётся автоматически по фактически прочитанной схеме источника
и никогда не описывается вручную.

**Поддерживаемые типы колонок (PySpark → Iceberg DDL):**

| PySpark | Iceberg |
|---|---|
| `StringType` | `string` |
| `BooleanType` | `boolean` |
| `ByteType` / `ShortType` / `IntegerType` / `LongType` | `tinyint` / `smallint` / `int` / `bigint` |
| `FloatType` / `DoubleType` | `float` / `double` |
| `DateType` | `date` |
| `TimestampType` / `TimestampNTZType` | `timestamp_ltz` / `timestamp_ntz` |
| `DecimalType(p,s)` | `decimal(p,s)` |
| `BinaryType` | `binary` |

Тип вне этого списка — явная ошибка загрузки (честный отказ, не молчальное
преобразование). Маппинг типов СУБД → PySpark для обычных (не инкрементных)
колонок выполняет Spark JDBC по своей стандартной таблице соответствий;
фреймворк дополнительно гарантирует whitelist DDL выше, проекцию
Oracle timezone-типов и строгую типизацию [позиции инкремента](glossary.md#term-position) (раздел 7.1).

**Гарантии:**

- **Точность [позиции инкремента](glossary.md#term-position) не теряется**: суб-микросекундная точность
  читается из источника строкой (`TO_CHAR … FF9` в Oracle,
  `CONVERT` + `DATEPART(nanosecond)` в MSSQL) и хранится в [watermark](glossary.md#term-watermark) без
  округления до микросекунд Spark (раздел 7.1). Это гарантия именно для
  [позиции инкремента](glossary.md#term-position): значения timestamp-колонок в самих [raw](glossary.md#term-raw)-строках проходят
  через DataFrame Spark и ограничены его микросекундной точностью.
- **Зеркальность «AS IS»** означает отсутствие бизнес-трансформаций, но не
  побитовое сохранение представления: например, Oracle
  `TIMESTAMP WITH [LOCAL] TIME ZONE` не отображается Spark JDBC вовсе
  (типы -101/-102) и проецируется в UTC выражением `SYS_EXTRACT_UTC` ещё на
  стороне источника.
- **Типизация колонки [инкремента](glossary.md#term-increment)** строго ограничена (таблица канонических
  видов — раздел 7.1): целые и `decimal(p,0)` при 1 ≤ p ≤ 38, `date`,
  дата-время без пояса и абсолютные моменты с точностью 0–9 (для MSSQL до 7);
  MSSQL `timestamp`/`rowversion` как позиция не поддерживается,
  Oracle `TIMESTAMP` поддерживается.
- **Смена типа колонки источника** ограничена совместимостью Iceberg при
  слиянии схем; несовместимое изменение прерывает загрузку. Тип колонки
  [инкремента](glossary.md#term-increment) дополнительно защищён контрактом pg-meta (разделы 5 и 7.6).

## 10. Известные ограничения

| Ограничение | Где подробнее |
|---|---|
| Тихие пропуски [инкремента](glossary.md#term-increment): транзакция источника, зафиксированная после запроса максимума, может быть потеряна без следа — если её позиция не превышает новый [watermark](glossary.md#term-watermark), а [lookback](glossary.md#term-lookback) её не покрывает; изменения и удаления старых строк вне [окна](glossary.md#term-window) [инкремент](glossary.md#term-increment) не видит вовсе. Для периодических корректировок доступны [таблицы-снимки](glossary.md#term-snapshot); сопоставление — на следующем слое | разделы 7, 7.7 |
| Поддерживаются только MSSQL и Oracle. `kafka` в [манифестах](glossary.md#term-manifest) отвергается | раздел 2, [планы](../README.md#7-статус-и-планы) |
| Ни одна ступень эвристики не нашла кандидата (нет подходящих PK, уникальной колонки, даты и timestamp NOT NULL) или метаданные недоступны — чтение одним потоком; строки, float и дробные числа кандидатами не бывают | раздел 6 |
| Колонка нарезки JDBC-чтения всегда одна и выбирается автоматически — это не настройка | раздел 6 |
| Нарезка timestamp-колонок гранулярна до календарного дня: [окно](glossary.md#term-window) короче суток читается одним потоком | раздел 6 |
| Дневные партиции `days(__load_dttm)`: несколько загрузок одного дня ложатся в одну партицию | раздел 4 |
| [At-least-once](glossary.md#term-atleast): сбой между коммитом Iceberg и финализацией [watermark](glossary.md#term-watermark) приводит к дублю [порции](glossary.md#term-portion) при повторе; следующему слою нужен [идемпотентный](glossary.md#term-idempotency) merge | раздел 4 |
| Снимочный поток без [fencing](glossary.md#term-fencing): пережившее сбой Spark-приложение может закоммитить старую выборку поверх новой; прежний writer обязан быть остановлен до повтора вручную | раздел 7.7 |
| Пустое чтение снимка не обновляет таблицу и её свежесть: существующие данные сохраняются, новый [снапшот Iceberg](glossary.md#term-iceberg-snapshot) не создаётся | раздел 7.7 |
| MSSQL `timestamp`/`rowversion` не поддерживается как колонка [инкремента](glossary.md#term-increment); это не ограничение Oracle TIMESTAMP | раздел 7.1 |
