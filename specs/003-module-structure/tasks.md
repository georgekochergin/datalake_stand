# Задачи

Каждая задача — отдельный коммит; после каждого коммита — ревью пользователя. Зелёность шага: unit-тесты (`python3 -m unittest discover -s test/unit`) и генерация `python3 -m ingestion.generator.generate_dags --check`.

- [x] T001 Диалект: `RdbmsDialect` и type-helpers → `ingestion/sources/rdbms/dialects/base.py`, re-export из `contracts.py`, импорты mssql/oracle и тестов, extending.md §2.
- [x] T002 Пакет `ingestion/meta/`: логирование в `__init__.py`, реестр партий в `registry.py`, явные импорты реестра у `incremental/load_entity.py`, тестов и e2e-скриптов, architecture.md §7.
- [x] T003 Перенос `ingestion/iceberg.py` → `ingestion/spark/raw/iceberg.py`, обновление импортов операторов, приложений и тестов, architecture.md §4.
- [x] T004 Конфигурация: `FullEntityConfig`/`IncrementalEntityConfig` в `ingestion/generator/manifest.py`, `incremental_column` из контракта, единственная развилка `isinstance` в генераторе, тесты манифеста и генератора.
- [x] T005 Пакет `ingestion/operators/` (`__init__`/`process`/`assets`/`raw_rdbms`), `IngestFullOperator` и `IngestIncrementalOperator` (DSN `INGESTION_META_DSN` через `make_dsn`), удаление `IngestAppendOperator`, генератор эмиссит конкретные классы, харнесс `test_operators.py` на пакет, architecture.md §3, extending.md §3.
- [x] T006 Чтение: явный контракт `read_extract_frame` (filters/extra_select), окно и `__extract_position` → `ingestion/spark/raw/rdbms/incremental/window.py`, тесты reader и загрузчиков.
- [x] T007 Запись: writer `ingestion/spark/raw/append.py` (очищенный DataFrame + число строк), агрегация count/max и канонизация в `window.py`, удаление `raw_append.py`, тесты.
- [ ] T008 Финал: полный изолированный `./test/test.sh` (`FAIL=0`, exit 0), граф (локальные обновления по ходу, `graphify-out/graph.json` финальным коммитом), PR по скиллу pull-request.

Порядок фиксированный: T005 требует T004; T006–T007 после T003; T008 последним.
