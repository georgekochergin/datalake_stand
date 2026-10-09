# План: тестовый стенд КХД → DLH (PoC)

## Цель

Локальный docker-compose стенд, демонстрирующий перенос цепочки
КХД → DataLakehouse: загрузка данных из реальных реляционных источников
(MSSQL, Oracle) в ODS-слой Iceberg AS IS, и демонстрация двух способов
трансформации данных внутри лейка (Trino SQL и Spark).

Перенос хранимых процедур КХД и конкретное бизнес-содержание
детального/витринного слоя — **вне объёма этого этапа** (отдельный
будущий шаг, backlog).

## Архитектура — два контура

### Контур A — релевантно production-архитектуре DLH

| Компонент | Образ/версия | Роль |
|---|---|---|
| mssql-source | `mcr.microsoft.com/mssql/server:2022-CU23-ubuntu-22.04`¹ | источник (DB `demo`, схема `demo`) |
| oracle-source | `gvenzl/oracle-free:23-slim-faststart` | источник (PDB `demo`, пользователь/схема `demo`) |
| pg-catalog | `postgres:16.15-bookworm` | metastore для Nessie |
| nessie | `projectnessie/nessie:0.76.6` | Iceberg REST каталог, JDBC version-store |
| silo | `pgsty/silo:RELEASE.2026-09-16T00-00-00Z` | S3-совместимое хранилище (форк MinIO), бакет `warehouse` |
| spark-master/worker | база `tabulario/spark-iceberg:3.5.5_1.8.1` + `mssql-jdbc:12.10.0.jre11` + `ojdbc11:23.8.0.25.04` | AS IS загрузка источников → ODS |
| trino | `trinodb/trino:479` | трансформации внутри лейка + ad-hoc доступ аналитиков |
| airflow | `apache/airflow:3.3.2-python3.11`, LocalExecutor | оркестрация |

¹ тег не подтверждён независимо (сетевые тайм-ауты при планировании) — перепроверить по каталогу MCR перед сборкой.

DAG-и Airflow:
- `ods_load_mssql`, `ods_load_oracle` — каждый: task `extract_load`
  (SparkSubmitOperator) → task `validate_load` (финальный, метод валидации
  — открытый вопрос, см. раздел «Открытые решения»), `outlets=[Dataset(...)]`
  на `validate_load`.
- `build_marts_demo` — `schedule=None`, вручную: task `mart_via_trino`
  (TrinoOperator) и task `mart_via_spark` (SparkSubmitOperator), независимые
  заглушки-трансформации над `ods.*` — демонстрация двух способов обработки,
  без требования идентичности результата.

### Контур B — только тестовый стенд (нет в production)

`datagen-api` — FastAPI-сервис без веб-UI (только Swagger `/docs`):
генерация/изменение тестовых данных источников и ограниченная эволюция схемы.

- Схема каждого источника — независимый YAML (`schema/sources/mssql.yaml`,
  `schema/sources/oracle.yaml`), bind-mount в контейнер (read-only). Никакой
  привязки к star-схеме: только обязательный `primary_key` у каждой таблицы
  и явные `foreign_keys` там, где они есть. Граф FK должен быть ацикличным.
- `append` / `update` / `delete` — с рекурсивным FK-aware каскадом (при
  вставке факта, ссылающегося на несуществующий PK в родительской таблице,
  сначала создаётся недостающая родительская строка, вверх по графу).
  `delete` на таблице с зависимыми строками — блокируется, если не передан
  `cascade=true`.
- `evolve` — только `rename_column` и `drop_column` (эти операции
  заблокированы для PK/FK-колонок); любая другая операция → `422`.
  `add_column`/`alter_column_type` — backlog.
- При каждом (ре)старте контейнера, на источник независимо: `sha256`
  bind-mounted YAML сравнивается с сохранённым хэшем в volume
  `datagen_state`. Совпал → ничего не трогаем. Не совпал/первый раз →
  полностью пересоздаём пустые таблицы источника по новой схеме,
  **без архивации** предыдущего `live_schema`/`schema_evolution_log`
  (осознанное упрощение для тестового стенда — старое состояние теряется).
  Во время работы контейнера изменения YAML на хосте не подхватываются.

## Открытые решения / backlog (вне этого этапа)

- Перенос хранимых процедур КХД и сверка результата с их эталоном.
- Конкретное бизнес-содержание будущих detail/mart-слоёв.
- dbt-trino как альтернатива прямым SQL/PySpark-скриптам.
- `add_column`/`alter_column_type` в `evolve`.
- Метод `validate_load`: простой `COUNT(*)` неверен, если в источнике были
  `DELETE` между загрузками, а Iceberg хранит накопленную историю —
  нужно сперва решить стратегию ODS-загрузки (чистый `APPEND` vs
  `MERGE`/overwrite, отражающий текущее состояние источника).
- Совместимость `SparkSubmitOperator` (провайдер `apache-airflow-providers-apache-spark==6.3.2`,
  зависимость `pyspark-client>=4.0.0`) с классическим `spark-submit` на
  Standalone-кластере — проверить на практике.
- Точная топология контейнеров Airflow 3.x (возможен отдельный
  dag-processor) — первый шаг реализации должен это подтвердить.

## Известные риски

- Лицензия AGPLv3 у SILO (та же, что у апстрима MinIO) — для внутреннего PoC
  не создаёт практических обязательств, не покрывает прод без отдельной
  юридической оценки.
- Ограничения Oracle Database Free (~2 CPU threads, 2GB SGA, 12GB данных).
- `mssql-source` требует amd64; на Apple Silicon — через Rosetta/QEMU-эмуляцию
  (`platform: linux/amd64`).
- `predicate`/`payload` в `datagen-api` — сырой SQL-фрагмент (осознанный
  риск для доверенного внутреннего инструмента, не для внешнего доступа).

## Порядок запуска (см. README.md корня репозитория)
