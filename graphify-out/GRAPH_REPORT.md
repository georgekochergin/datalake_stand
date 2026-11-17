# Graph Report - datalake_stand  (2026-11-17)

## Corpus Check
- 169 files · ~146,141 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 12 file(s) not represented in the graph (top: (none) 9, .properties 2, .conf 1)

## Summary
- 3001 nodes · 5365 edges · 196 communities (147 shown, 49 thin omitted)
- Extraction: 95% EXTRACTED · 4% INFERRED · 0% AMBIGUOUS · INFERRED: 241 edges (avg confidence: 0.9)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `4eafe704`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- parse_args
- keys
- Shared JDBC Reader
- pathlib
- load_pipeline
- TestGenerateSnapshotDagSource
- raw/__init__.py
- test_dialects.py
- Приоритет: можно отложить
- Batch Registry Tests
- validate_target_table
- run_flow.py
- TestActSnapManifest
- Compose Environment Vars
- extending.md
- AGENTS.md — инструкции проекта datalake_stand
- manifest.py
- Postgres Registry Tests
- TestConfiguration
- managed_load
- meta/registry.py
- Demo Walkthrough Docs
- 8. База метаданных pg-meta: остальные объекты
- temporal_extraction.py
- Spec Kit Workflow
- test.sh
- Spec Kit Cycle Instructions
- speckit-plan
- check_extraction.py
- RdbmsDialect
- canonical
- Shell Helper Scripts
- raw_rdbms.py
- guard.py
- speckit-specify
- Implementation Plan Template
- TestSparkGuard
- Airflow Env Config
- check
- read_extract_frame
- Nessie Catalog Config
- Architecture Documentation
- ParallelE2ETests
- Dev Tooling Documentation
- Target Name Resolution Tests
- key_metadata
- Ingest Operator Tests
- speckit-checklist
- Init Services Compose
- allocate_process_id
- Airflow Services Compose
- Docker Compose Layout
- Приоритет: важно
- Spec Kit Converge Skill
- Bank Pipeline Manifest
- Broker Pipeline Manifest
- Constitution Templates
- DATARCH-2243: Templatize code in generator/generate_dags.py
- TestSchemaColumns
- generate_all
- Code Style Guards
- check_partitioning
- speckit-analyze
- IncrementalEntityConfig
- speckit.converge.md
- DAG Processor Service
- by_jdbc_url
- generate_dags.py
- Stand Config Tests
- speckit-tasks
- 3. Структура сгенерированного DAG-а
- Act Snap Plan Docs
- parse_args
- Incremental Manifest Tests
- Analysis Detection Passes
- Named Volumes
- 7.1. Контракт
- 003-module-structure/spec.md
- by_conn_type
- resolve_namespace
- speckit.analyze.md
- load_operators
- Сквозное (e2e) тестирование стенда datalake_stand
- Postgres Services Compose
- TestAppendRaw
- Test Runner Service
- Спецификация: безопасный повтор Spark-загрузки
- Spec Kit Plan Skill
- speckit.tasks.md
- Project Constitution
- TestGenerateDagSource
- Silo Service Compose
- Предпочтительный путь решения
- Bank References Manifest
- Broker References Manifest
- resolve_source_connection
- Registry Transaction Tests
- Act Snap Operator Tests
- pg-meta DDL Tables
- Sydafa Service Compose
- 4. Таблицы raw-слоя: структура и наполнение
- full/load_entity.py
- .plan
- TestKeyStructure
- ScriptedResult
- Platform Component Overview
- incremental/load_entity.py
- Airflowignore Scanner Tests
- Ведение документации
- check_registry_recovery
- plan_batch
- jdbc_partitioning.py
- depends_on
- Implementation Plan: [FEATURE]
- speckit-plan
- _column_metadata
- managed_snapshot
- Speckit Checklist Phase
- Feature Spec Template
- SQL Identifier Quoting
- TestTaskLifecycleCallbacks
- 6. Параллелизм Spark
- Sources Configuration File
- Настройка graphify — графа кодовой базы
- Глоссарий
- Архитектура datalake_stand
- 7. Инкрементальная загрузка
- Управление ресурсами Spark на уровне отдельной джобы
- Шаблонизация кода в generator/generate_dags.py
- plan
- TestPgMetaInitSql
- DAG Run Callbacks
- Asset Publication Policy
- speckit.converge.md
- Process ID Generation
- Спецификация: необязательный журнал загрузок
- make_batch
- positions.py
- TestAirflowInit
- Temporal Column Rendering
- first_row
- Task Failure Logging
- 2. Манифесты и генерация DAG-ов
- Обработка подвисших процессов Spark при retry задач
- DAG generator (ingestion/generator/generate_dags.py)
- all_dialects
- README — стенд хранилища и озера данных
- TestManifestShapeRejections
- 7.7. DAG-и снимков и таблицы-снимки
- Сценарий демонстрации стенда
- read_window_frame
- Task Logging Tasks
- 5. Устойчивость к эволюции схемы источника
- test_dialects.py
- План реализации DATARCH-2245
- _apply_entity_fields
- FakeOperator
- Анализ согласованности перед реализацией
- Задачи DATARCH-2245
- Проверка требований DATARCH-2245
- run_spark_guard
- current_snapshot_author
- Bucket Init Script
- Auth Init Script
- Source Activation Operator
- Asset Event Callables
- Generator Dependencies
- check_spark_guard.py
- Publish Asset Task
- Bootstrap Process
- CAS Revision Control
- 4. Detection Passes (Token-Efficient Analysis)
- parse_args
- empty_incremental_window
- Шаг 5. Повторный раунд: полный срез против инкремента
- incremental/load_entity.py
- TestMetaConnect
- TestMetaDouble
- Шаг 7. Стресс-тест: эволюция схемы источника
- assert_unique_targets
- test_jdbc_partitioning.py
- .setUpClass
- position_select
- TestAirflowInit
- .columns
- Расширение стенда
- Проверка реализации
- План: параллельный e2e
- Спецификация: параллельный e2e за семь минут
- Задачи: параллельный e2e
- Анализ согласованности
- Snapshot table (actual snapshot, __act_snap)
- TestBuildRtasSql
- snapshot_dag_id

## God Nodes (most connected - your core abstractions)
1. `RdbmsDialect` - 42 edges
2. `load_pipeline()` - 37 edges
3. `keys()` - 35 edges
4. `generate_all()` - 33 edges
5. `write_temp_yaml()` - 31 edges
6. `resolve_target_table_name()` - 26 edges
7. `load_sources()` - 25 edges
8. `TestLoadPipeline` - 24 edges
9. `TestChoose` - 23 edges
10. `ConnectionSpec` - 22 edges

## Surprising Connections (you probably didn't know these)
- `Память executor` --references--> `EntityConfig`  [INFERRED]
  docs/backlog.md → ingestion/generator/manifest.py
- `Проблема` --references--> `append_raw()`  [INFERRED]
  docs/backlog.md → ingestion/spark/raw/append.py
- `Настройки таблицы источника` --references--> `incremental()`  [INFERRED]
  docs/architecture.md → test/temporal_extraction.py
- `4. Сценарий сквозного тестирования` --references--> `PostgresTests`  [INFERRED]
  .agents/skills/full-testing/SKILL.md → test/unit/test_meta.py
- `Контекст` --references--> `_entity_block()`  [INFERRED]
  docs/backlog.md → ingestion/generator/generate_dags.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Snapshot DAG atomic publication flow** — docs_architecture__ref__ingestion_spark_raw_rdbms_incremental_load_act_snap, ingestion_spark_raw_rdbms_incremental_load_act_snap_build_rtas_sql, docs_architecture_snapshot_table, docs_architecture_publish_asset_event, docs_architecture_snapshot_has_current_process [EXTRACTED 0.85]
- **Spec Kit command pipeline (specify -> clarify -> plan -> tasks -> implement -> converge/analyze)** — zcode_skills_speckit_specify_skill, zcode_skills_speckit_clarify_skill, zcode_skills_speckit_plan_skill, zcode_skills_speckit_tasks_skill, zcode_skills_speckit_implement_skill, zcode_skills_speckit_converge_skill, zcode_skills_speckit_analyze_skill [EXTRACTED 0.85]
- **Архитектура стенда dlh-poc: сервисы и хранилища** — readme_sydafa, readme_airflow, readme_spark, readme_trino, readme_nessie, readme_silo, readme_pg_meta, readme_pg_catalog [EXTRACTED 0.90]
- **Incremental batch lifecycle (managed_load flow)** — docs_architecture__ref__ingestion_spark_raw_rdbms_incremental_load_entity_managed_load, docs_architecture__ref__ingestion_meta_registry_get_batch, docs_architecture__ref__ingestion_meta_registry_start_batch, docs_architecture__ref__ingestion_meta_registry_finalize_batch, docs_architecture_incremental_batches, docs_architecture_incremental_state [EXTRACTED 0.90]
- **Ingest DAG per-entity task flow (generate_process_id >> activate_source >> log_start >> load >> log_end >> publish_asset)** — docs_architecture_generate_process_id_task, docs_architecture_activate_source_task, docs_architecture_log_start_task, docs_architecture_load_task, docs_architecture_log_end_task, docs_architecture_publish_asset_task [EXTRACTED 0.90]
- **pg-meta ingestion schema objects (run logs, entity state, batch registry)** — docs_architecture_pg_meta, docs_architecture_airflow_dagruns_log, docs_architecture_airflow_tasks_log, docs_architecture_incremental_state, docs_architecture_incremental_batches, docs_architecture_generate_process_id_task [EXTRACTED 0.90]
- **pg-meta metadata schema objects** — docs_architecture_pg_meta, docs_architecture_incremental_state, docs_architecture_incremental_batches, docs_architecture_incremental_state_migrations, docs_architecture_airflow_dagruns_log, docs_architecture_airflow_tasks_log, docs_architecture_process_id_seq [EXTRACTED 0.90]
- **Спек-цикл Spec Kit: constitution → specify → clarify → plan → tasks → analyze → implement → converge** — opencode_commands_speckit_constitution, opencode_commands_speckit_specify, opencode_commands_speckit_clarify, opencode_commands_speckit_plan, opencode_commands_speckit_tasks, opencode_commands_speckit_analyze, opencode_commands_speckit_implement, opencode_commands_speckit_converge [EXTRACTED 0.95]
- **Спек-цикл разработки Spec Kit + graphify** — docs_agents_speckit, docs_agents_speckit_specify_cli, docs_agents_graphify, specs_001_act_snap_contract_spec, specs_002_incremental_config_spec, specs_003_module_structure_spec [INFERRED 0.80]
- **Механизм таблиц-снимков act_snap** — specs_001_act_snap_contract_spec, specs_001_act_snap_contract_spec_act_snap_config, specs_001_act_snap_contract_spec_ingest_act_snap_operator, specs_001_act_snap_contract_spec_publish_asset, docs_demo_load_act_snap, docs_glossary_snapshot, docs_glossary_rtas [INFERRED 0.85]
- **Концепция инкрементальной загрузки (контракт инкремента)** — docs_glossary_increment, docs_glossary_position, docs_glossary_canonical, docs_architecture_watermark, docs_glossary_window, docs_architecture_lookback, docs_glossary_batch, docs_glossary_attempt, docs_glossary_portion, specs_002_incremental_config_spec [INFERRED 0.90]

## Communities (196 total, 49 thin omitted)

### Community 0 - "parse_args"
Cohesion: 0.09
Nodes (7): main(), parse_args(), build_argv(), TestMain, TestParseArgs, TestPlanBatch, TestSourcePosition

### Community 1 - "keys"
Cohesion: 0.08
Nodes (11): day_expression(), no_candidate_reason(), partition_name(), PartitionChoice, plan(), derived_day_frame(), keys(), TestChoose (+3 more)

### Community 2 - "Shared JDBC Reader"
Cohesion: 0.08
Nodes (8): build_extract_query(), jdbc_reader(), read_extract_frame(), make_args(), RecordingReader, TestJdbcReader, TestReadBounds, TestReadExtractFrame

### Community 3 - "pathlib"
Cohesion: 0.06
Nodes (4): cli_args(), TestOverwriteSnapshot, FakeSkip, TestPluginSyntax

### Community 4 - "load_pipeline"
Cohesion: 0.15
Nodes (4): load_pipeline(), TestLoadPipeline, TestLoadSources, write_temp_yaml()

### Community 5 - "TestGenerateSnapshotDagSource"
Cohesion: 0.12
Nodes (7): Генерация и настройка, generate_snapshot_dag_source(), ActSnapConfig, AirflowConfig, DagConfig, SparkConfig, TestGenerateSnapshotDagSource

### Community 6 - "raw/__init__.py"
Cohesion: 0.25
Nodes (3): append_raw(), schema_columns(), main()

### Community 7 - "test_dialects.py"
Cohesion: 0.06
Nodes (23): AGENTS.md, conf/airflow/Dockerfile, conf/spark/Dockerfile, ConnectionSpec, .agents/skills/e2e-testing/SKILL.md, ingestion/operators/ (пакет), ingestion/conf/pipelines/, RdbmsDialect (базовый контракт диалекта) (+15 more)

### Community 8 - "Приоритет: можно отложить"
Cohesion: 0.12
Nodes (17): Автоматически формировать теги DAG и исключать дубликаты, Бэклог, Выбирать JDBC-колонку по метрикам с управляемым повторным анализом, Контекст, Контекст, Оглавление, Предпочтительный путь решения, Предпочтительный путь решения (+9 more)

### Community 9 - "Batch Registry Tests"
Cohesion: 0.18
Nodes (3): finalize_batch(), start_batch(), TestBatchLifecycle

### Community 10 - "validate_target_table"
Cohesion: 0.19
Nodes (4): build_create_table_sql(), table_location(), TestBuildCreateTableSql, TestTableLocation

### Community 11 - "run_flow.py"
Cohesion: 0.10
Nodes (23): airflow_task_log(), airflow_task_states(), airflow_token(), airflow_token_retry(), airflow_trigger(), airflow_trigger_retry(), airflow_unpause(), airflow_wait_dag_registered() (+15 more)

### Community 13 - "Compose Environment Vars"
Cohesion: 0.08
Nodes (36): environment, healthcheck, image, networks, volumes, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, POSTGRES_DB (+28 more)

### Community 14 - "extending.md"
Cohesion: 0.22
Nodes (8): Канонический тип позиции, Инкремент (incremental), Позиция инкремента, CLI-флаг --incremental-config, incremental.contract (YAML: source_column, position_type), Контракт инкрементальной загрузки, Требования, Уточнения

### Community 15 - "AGENTS.md — инструкции проекта datalake_stand"
Cohesion: 0.10
Nodes (24): AGENTS.md — инструкции проекта datalake_stand, Скилл backlog — ведение docs/backlog.md, Скилл pull-request — создание Pull Request, пакет sydafa-airflow (SydafaActivateOperator), airflow/plugins/migrations/ — миграции источников bank/broker, conf/airflow/Dockerfile, conf/pg-meta/init/ — DDL pg-meta, conf/spark/Dockerfile (+16 more)

### Community 16 - "manifest.py"
Cohesion: 0.10
Nodes (14): Предпочтительный путь решения, load_sources(), _expand_cron_field(), IngestionSettings, load_settings(), _parse_act_snap(), _parse_dag_fields(), _parse_entity() (+6 more)

### Community 20 - "meta/registry.py"
Cohesion: 0.13
Nodes (7): _row(), fail_batch(), get_batch(), _lock_batch(), _lock_state(), check_registry_recovery(), TestGetBatch

### Community 21 - "Demo Walkthrough Docs"
Cohesion: 0.08
Nodes (30): Apache Airflow, Задача generate_process_id, Apache Iceberg, Таблица ingestion.incremental_batches, DAG ingest__rdbms__bank__main, ingest__rdbms__bank__main (generated DAG), load_act_snap.py (Spark-приложение снимка), pg-meta (реестр загрузок) (+22 more)

### Community 22 - "8. База метаданных pg-meta: остальные объекты"
Cohesion: 0.18
Nodes (10): 8. База метаданных pg-meta: остальные объекты, Аудит миграций, Журналы событий, Запросы к метаданным, Последовательность идентификаторов, Запуск и подключения, Исходное состояние, Подготовка (+2 more)

### Community 23 - "temporal_extraction.py"
Cohesion: 0.10
Nodes (17): Независимые настройки снимка, Событие готовности снимка, Шаг 9. Таблица-снимок: DAG `__act_snap`, check_post_append_retry(), check_snapshot_recovery(), incremental(), projection(), registry() (+9 more)

### Community 24 - "Spec Kit Workflow"
Cohesion: 0.06
Nodes (30): args, inputs, integration, spec, default, prompt, type, any (+22 more)

### Community 25 - "test.sh"
Cohesion: 0.15
Nodes (16): ACT_SNAPSHOT_TABLES, Дополнение: пункты 3–5 плана от 2026-11-08, Исправления по третьему ревью, Итоговая проверка, Проверка уточнений после ревью, 001-act-snap-contract: задачи, README.md, Завершение пунктов 3–5 внешнего плана (+8 more)

### Community 26 - "Spec Kit Cycle Instructions"
Cohesion: 0.07
Nodes (29): 10. Обновление Spec Kit, 11. Взаимодействие с graphify и Pull Request, 12. Работа в ZCode Desktop (интерактивно), 1. Назначение, 2. Установка для сопровождающего (выполнена один раз), 3. Инструкция для разработчика, 4. Состояние цикла: каталоги, машины, ветки, 5. Состав артефактов (+21 more)

### Community 27 - "speckit-plan"
Cohesion: 0.18
Nodes (11): speckit-plan, Completion Report, Done When, Key rules, Mandatory Post-Execution Hooks, Outline, Phase 0: Outline & Research, Phase 1: Design & Contracts (+3 more)

### Community 28 - "check_extraction.py"
Cohesion: 0.06
Nodes (7): check_snapshot_publication(), main(), records(), require(), FakeConnection, FakeCursor, FakePg

### Community 29 - "RdbmsDialect"
Cohesion: 0.06
Nodes (14): 2. Подключение новой JDBC-СУБД, Контракт диалекта, Проверки совместимости, Регистрация и окружение, ConnectionSpec, __getattr__(), RdbmsDialect, check_partitioning() (+6 more)

### Community 30 - "canonical"
Cohesion: 0.07
Nodes (11): canonical(), maximum(), resolve_lookback(), window_bounds(), _plan_batch(), calculate_lower(), window_summary(), literal() (+3 more)

### Community 31 - "Shell Helper Scripts"
Cohesion: 0.12
Nodes (30): check-prerequisites.sh script, check_dir(), check_file(), find_specify_root(), format_speckit_command(), get_current_branch(), get_feature_paths(), get_invoke_separator() (+22 more)

### Community 32 - "raw_rdbms.py"
Cohesion: 0.10
Nodes (10): Событие готовности Asset, publish_asset_event(), snapshot_has_current_process(), dagrun_failure_callback(), dagrun_success_callback(), log_task_failed(), _pull_process_id(), resolve_source_connection() (+2 more)

### Community 33 - "guard.py"
Cohesion: 0.17
Nodes (8): _all_applications_completed(), _applications(), _get_master_state(), guard_spark_submit(), _kill_application(), _remaining(), _request(), SparkGuardError

### Community 34 - "speckit-specify"
Cohesion: 0.07
Nodes (30): checklists/requirements.md (spec quality checklist), .specify/extensions.yml — хуки расширений Spec Kit, .specify/init-options.json, speckit-clarify, Completion Report, Done When, Mandatory Post-Execution Hooks, Outline (+22 more)

### Community 35 - "Implementation Plan Template"
Cohesion: 0.08
Nodes (26): Dependencies & Execution Order, Format: `[ID] [P?] [Story] Description`, Implementation for User Story 1, Implementation for User Story 2, Implementation for User Story 3, Implementation Strategy, Incremental Delivery, MVP First (User Story 1 Only) (+18 more)

### Community 36 - "TestSparkGuard"
Cohesion: 0.18
Nodes (4): app(), inventory(), load_guard(), TestSparkGuard

### Community 37 - "Airflow Env Config"
Cohesion: 0.26
Nodes (25): environment, environment, environment, environment, AIRFLOW__API_AUTH__JWT_SECRET, AIRFLOW__API__EXPOSE_CONFIG, AIRFLOW__API__SECRET_KEY, AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION (+17 more)

### Community 38 - "check"
Cohesion: 0.13
Nodes (13): airflow_check_asset_publication(), airflow_wait_run(), airflow_wait_run_failed(), check(), evolution_ddl(), evolution_first_load(), evolution_host(), evolution_incompatible() (+5 more)

### Community 39 - "read_extract_frame"
Cohesion: 0.24
Nodes (4): ColumnMeta, compatibility_projection(), precision_fixtures(), TestCompatibilityProjection

### Community 40 - "Nessie Catalog Config"
Cohesion: 0.09
Nodes (23): pg-catalog, nessie.catalog.default-warehouse, nessie.catalog.secrets.access-key.name, nessie.catalog.secrets.access-key.secret, nessie.catalog.service.s3.default-options.access-key, nessie.catalog.service.s3.default-options.auth-type, nessie.catalog.service.s3.default-options.endpoint, nessie.catalog.service.s3.default-options.path-style-access (+15 more)

### Community 41 - "Architecture Documentation"
Cohesion: 0.11
Nodes (21): pg-meta DDL (conf/pg-meta/init/01_ingestion.sql), Glossary (glossary.md), MSSQL dialect (ingestion/sources/rdbms/dialects/mssql.py), Oracle dialect (ingestion/sources/rdbms/dialects/oracle.py), Iceberg writer layer (ingestion/spark/raw/iceberg.py), Full-slice loader (ingestion/spark/raw/rdbms/full/load_entity.py), Incremental loader (ingestion/spark/raw/rdbms/incremental/load_entity.py), Incremental window reader (incremental/window.py) (+13 more)

### Community 42 - "ParallelE2ETests"
Cohesion: 0.12
Nodes (3): scale_migration(), validate_manifests_and_dags(), ParallelE2ETests

### Community 43 - "Dev Tooling Documentation"
Cohesion: 0.13
Nodes (17): AGENTS.md, graphify-out/graph.json, graphify (CLI-утилита), AGENTS.md, .specify/feature.json (локальный указатель фичи), opencode, .opencode/commands/, specify CLI (GitHub Spec Kit) (+9 more)

### Community 44 - "Target Name Resolution Tests"
Cohesion: 0.17
Nodes (3): resolve_target_table_name(), _split_fragments(), TestResolveTargetTableName

### Community 45 - "key_metadata"
Cohesion: 0.16
Nodes (8): _degraded_metadata(), key_metadata(), KeyMetadata, check_partitioned_load_path(), fake_jvm(), TestCatalogAddressing, TestDegradedKeyMetadata, TestKeyMetadata

### Community 47 - "speckit-checklist"
Cohesion: 0.12
Nodes (15): tasks.md (task list), $speckit-github-taskstoissues (github extension), speckit-checklist, Anti-Examples: What NOT To Do, Checklist Purpose: "Unit Tests for English", Example Checklist Types & Sample Items, Execution Steps, Post-Execution Checks (+7 more)

### Community 48 - "Init Services Compose"
Cohesion: 0.11
Nodes (19): nessie, silo-init, trino-init, condition, spark-master, trino, condition, command (+11 more)

### Community 49 - "allocate_process_id"
Cohesion: 0.08
Nodes (12): Назначение задач, allocate_process_id(), find_process_id(), log_dagrun(), log_task(), _dagrun_event(), GenerateProcessIdOperator, _meta_connect() (+4 more)

### Community 50 - "Airflow Services Compose"
Cohesion: 0.11
Nodes (18): depends_on, condition, command, depends_on, image, networks, pull_policy, volumes (+10 more)

### Community 51 - "Docker Compose Layout"
Cohesion: 0.12
Nodes (16): name, networks, dlh-poc, services, spark-worker, trino-init, command, depends_on (+8 more)

### Community 52 - "Приоритет: важно"
Cohesion: 0.22
Nodes (9): Задача Jira, Задача Jira, Контекст, Определить и проверить минимальные поддерживаемые версии компонентов, Предпочтительный путь решения, Приоритет: важно, Проблема, Проблема (+1 more)

### Community 53 - "Spec Kit Converge Skill"
Cohesion: 0.16
Nodes (11): 1. Initialize Convergence Context, 2. Load Artifacts (Progressive Disclosure), 3. Build the Intent Inventory, 4. Assess the Codebase and Classify Findings, 5. Assign Severity, 6. Present the In-Session Findings Summary, 7. Append Convergence Tasks (or report converged), 8. Provide Next Actions (Handoff) (+3 more)

### Community 54 - "Bank Pipeline Manifest"
Cohesion: 0.12
Nodes (16): airflow, pool, position_type, source_column, entities, airflow, fqn, incremental (+8 more)

### Community 55 - "Broker Pipeline Manifest"
Cohesion: 0.12
Nodes (16): airflow, pool, position_type, source_column, entities, airflow, fqn, incremental (+8 more)

### Community 56 - "Constitution Templates"
Cohesion: 0.12
Nodes (15): Outline, Post-Execution Checks, Pre-Execution Checks, Scope Guard, User Input, Core Principles, Governance, [PRINCIPLE_1_NAME] (+7 more)

### Community 57 - "DATARCH-2243: Templatize code in generator/generate_dags.py"
Cohesion: 0.40
Nodes (10): generate_dag_source(), generate_snapshot_dag_source(), build_create_table_sql(), build_rtas_sql(), build_extract_query(), read_bounds(), DATARCH-2243: Templatize code in generator/generate_dags.py, SQL-файлы и шаблоны (+2 more)

### Community 58 - "TestSchemaColumns"
Cohesion: 0.15
Nodes (3): fake_pyspark_types(), SchemaDouble, TestSchemaColumns

### Community 59 - "generate_all"
Cohesion: 0.09
Nodes (7): compute_drift(), generate_all(), main(), load_sources(), TestCheckMode, TestGenerateAll, TestGenerateAllValidation

### Community 60 - "Code Style Guards"
Cohesion: 0.17
Nodes (3): TestNoHashComments, manipulates_sys_path(), TestNoSysPathManipulation

### Community 61 - "check_partitioning"
Cohesion: 0.18
Nodes (5): apply_partition_options(), bound_literal(), long_representable(), read_bounds(), TestBoundLiteral

### Community 62 - "speckit-analyze"
Cohesion: 0.14
Nodes (14): speckit-analyze, 7. Provide Next Actions, 8. Offer Remediation, 9. Check for extension hooks, .specify/extensions.yml (extension hooks registry), Analysis Guidelines, Context, Context Efficiency (+6 more)

### Community 63 - "IncrementalEntityConfig"
Cohesion: 0.13
Nodes (19): Границы рефакторинга, Задача Jira, Контекст, Контекст, Контекст, Предпочтительный путь решения, Проблема, Проверки и критерии готовности (+11 more)

### Community 64 - "speckit.converge.md"
Cohesion: 0.12
Nodes (15): 1. Initialize Convergence Context, 2. Load Artifacts (Progressive Disclosure), 3. Build the Intent Inventory, 4. Assess the Codebase and Classify Findings, 5. Assign Severity, 6. Present the In-Session Findings Summary, 7. Append Convergence Tasks (or report converged), 8. Provide Next Actions (Handoff) (+7 more)

### Community 65 - "DAG Processor Service"
Cohesion: 0.20
Nodes (14): build, command, image, networks, pull_policy, volumes, build, build (+6 more)

### Community 66 - "by_jdbc_url"
Cohesion: 0.16
Nodes (5): by_jdbc_url(), supported_conn_types(), supported_url_prefixes(), source_kind(), TestRegistry

### Community 67 - "generate_dags.py"
Cohesion: 0.10
Nodes (10): asset_uri(), dag_id_for(), _entity_block(), file_name_for(), load_operator_name(), snapshot_asset_uri(), snapshot_dag_id(), _snapshot_entity_block() (+2 more)

### Community 69 - "speckit-tasks"
Cohesion: 0.07
Nodes (33): contracts/ (interface contracts), plan.md (implementation plan), spec.md (feature specification), Модель состояния guard, Содержание, Состояния, Исследование, Источники и ограничения (+25 more)

### Community 70 - "3. Структура сгенерированного DAG-а"
Cohesion: 0.40
Nodes (5): 3. Структура сгенерированного DAG-а, Зависимости задач, Идентификаторы и метаданные, Ограничение числа загрузок, Структура DAG-а снимка

### Community 71 - "Act Snap Plan Docs"
Cohesion: 0.10
Nodes (18): Asset (Airflow), ActSnapConfig (dataclass), DagConfig (базовый dataclass), MAX_ACTIVE_RUNS (константа генератора), Дополнительное покрытие пунктов 3–5, Проверка согласованности, PipelineManifest, Завершение проверки контракта (+10 more)

### Community 72 - "parse_args"
Cohesion: 0.20
Nodes (4): parse_args(), build_argv(), TestManagedLoad, TestParseArgs

### Community 74 - "Analysis Detection Passes"
Cohesion: 0.15
Nodes (13): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 4. Detection Passes (Token-Efficient Analysis), 5. Severity Assignment, 6. Produce Compact Analysis Report, A. Duplication Detection, B. Ambiguity Detection (+5 more)

### Community 75 - "Named Volumes"
Cohesion: 0.17
Nodes (12): name, name, volumes, airflow_logs, airflow_pg_data, pg_catalog_data, pg_meta_data, silo_data (+4 more)

### Community 76 - "7.1. Контракт"
Cohesion: 0.11
Nodes (14): 7.1. Контракт, Выбор типа позиции, Выравнивание временной границы, Границы окна, Настройка lookback, Отступ по умолчанию и расписание, Первичное наполнение и проверка источника, Передача конфигурации загрузчику (+6 more)

### Community 77 - "003-module-structure/spec.md"
Cohesion: 0.15
Nodes (12): Готовность фичи, Заметки, Полнота требований, Чек-лист качества спецификации: структура модулей загрузчика, FullEntityConfig, IncrementalEntityConfig, IngestAppendOperator (удаляется), IngestFullOperator (+4 more)

### Community 78 - "by_conn_type"
Cohesion: 0.33
Nodes (3): by_conn_type(), render_source_table(), TestRenderSourceTable

### Community 79 - "resolve_namespace"
Cohesion: 0.08
Nodes (9): resolve_fully_qualified(), resolve_namespace(), resolve_snapshot_fully_qualified(), resolve_snapshot_table_name(), snapshot_alias(), _validate_identifier(), TestResolveFullyQualified, TestResolveNamespace (+1 more)

### Community 80 - "speckit.analyze.md"
Cohesion: 0.14
Nodes (12): 7. Provide Next Actions, 8. Offer Remediation, 9. Check for extension hooks, Analysis Guidelines, Context, Context Efficiency, Goal, Operating Constraints (+4 more)

### Community 81 - "load_operators"
Cohesion: 0.15
Nodes (4): fake_make_dsn(), load_meta_api(), load_operators(), TestGuardOperatorWiring

### Community 82 - "Сквозное (e2e) тестирование стенда datalake_stand"
Cohesion: 0.13
Nodes (14): 1. Фундаментальный принцип тестирования кодовой базы, 2. Архитектура и изоляция тестового контура (`dlh-test`), 3. Режимы запуска тестов, 4. Сценарий сквозного тестирования, 5. Справочник траблшутинга и дебага, DAG-и не подхватываются в Airflow, MSSQL / Oracle lazy start, Trino TLS и Nessie Iceberg REST (+6 more)

### Community 83 - "Postgres Services Compose"
Cohesion: 0.18
Nodes (11): command, depends_on, image, networks, pull_policy, volumes, condition, airflow-postgres (+3 more)

### Community 85 - "Test Runner Service"
Cohesion: 0.18
Nodes (11): AIRFLOW_PASSWORD, AIRFLOW_USER, TRINO_PASSWORD, TRINO_USER, test-runner, build, environment, image (+3 more)

### Community 86 - "Спецификация: безопасный повтор Spark-загрузки"
Cohesion: 0.17
Nodes (11): US1: повтор после потери процесса задачи (P1), US2: запрет неподтверждённого запуска (P1), US3: предсказуемые повторы и диагностика (P2), Краевые случаи, Критерии успеха, Содержание, Спецификация: безопасный повтор Spark-загрузки, Сущности (+3 more)

### Community 87 - "Spec Kit Plan Skill"
Cohesion: 0.07
Nodes (27): Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, User Input, Completion Report, Done When (+19 more)

### Community 88 - "speckit.tasks.md"
Cohesion: 0.08
Nodes (21): Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, User Input, Checklist Format (REQUIRED), Completion Report (+13 more)

### Community 89 - "Project Constitution"
Cohesion: 0.18
Nodes (11): I. Граф кодовой базы прежде чтения файлов, II. Изменения подтверждаются тестами, III. Чистый Python без комментариев, IV. Дисциплина Git: ветка → Pull Request → мерж пользователем, V. Перестроение графа после каждого коммита, VI. Документация на русском языке, Базовые принципы, Конституция проекта datalake_stand (+3 more)

### Community 90 - "TestGenerateDagSource"
Cohesion: 0.09
Nodes (5): generate_dag_source(), PipelineManifest, validate_pipeline(), TestGenerateDagSource, TestValidatePipeline

### Community 91 - "Silo Service Compose"
Cohesion: 0.20
Nodes (10): silo, AWS_DEFAULT_REGION, silo-init, condition, depends_on, entrypoint, environment, image (+2 more)

### Community 92 - "Предпочтительный путь решения"
Cohesion: 0.20
Nodes (10): Жизненный цикл, Задача Jira, Изолировать Spark drivers через DockerOperator, Контракт оператора, Образ и зависимости, Предпочтительный путь решения, Проблема, Проверки и критерии готовности (+2 more)

### Community 93 - "Bank References Manifest"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 94 - "Broker References Manifest"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 95 - "resolve_source_connection"
Cohesion: 0.19
Nodes (6): _connect(), execute(), mssql_dba_connect(), oracle_dba_connect(), query_scalar(), read_dba_secrets()

### Community 98 - "pg-meta DDL Tables"
Cohesion: 0.33
Nodes (8): airflow_dagruns_log_process_id_idx, airflow_dagruns_log_run_idx, airflow_tasks_log_run_idx, ingestion.airflow_dagruns_log, ingestion.airflow_tasks_log, ingestion.incremental_batches, ingestion.incremental_state, ingestion.incremental_state_migrations

### Community 99 - "Sydafa Service Compose"
Cohesion: 0.22
Nodes (9): start_period, sydafa, environment, healthcheck, image, networks, platform, ports (+1 more)

### Community 100 - "4. Таблицы raw-слоя: структура и наполнение"
Cohesion: 0.22
Nodes (9): 4. Таблицы raw-слоя: структура и наполнение, Гарантии записи, Запросы к raw в Trino, Имена и колонки, Как исключить повторные попытки из выборки, Как различать партии и попытки, Ограничение: последняя попытка прочитала пустое окно, Полный срез и инкремент (+1 more)

### Community 101 - "full/load_entity.py"
Cohesion: 0.24
Nodes (13): conf/spark/spark-defaults.conf, Global settings (ingestion/conf/settings.yaml), Sources registry (ingestion/conf/sources.yaml), load_sources(), Load operators module (ingestion/operators.py), DATARCH-2242: Split sources registry into YAML files, per-source JDBC parallelism, DATARCH-2244: Isolate Spark drivers via DockerOperator, DATARCH-2245: Handle hung Spark processes on task retry (+5 more)

### Community 103 - "TestKeyStructure"
Cohesion: 0.17
Nodes (3): _primary_key(), _unique_single_columns(), TestKeyStructure

### Community 105 - "Platform Component Overview"
Cohesion: 0.33
Nodes (9): Airflow — оркестрация загрузок, JDBC-драйверы MSSQL/Oracle в образах Airflow и Spark, Nessie — каталог Iceberg, pg-catalog — база метаданных Nessie, pg-meta — база журналов и состояния загрузок, silo — MinIO-совместимое S3-хранилище, Spark — вычисления и загрузка по JDBC, sydafa — сервис управления источниками (+1 more)

### Community 108 - "Ведение документации"
Cohesion: 0.25
Nodes (8): Ведение документации, Когда применять, Ответственность документов, Порядок работы и проверки, Содержание, Термины и глоссарий, Техническая точность, Язык и порядок изложения

### Community 109 - "check_registry_recovery"
Cohesion: 0.22
Nodes (11): Batch registry (ingestion/meta/registry.py), Incremental batch, Bootstrap (initial fill), ingestion.incremental_batches, incremental contract (canonical position type), ingestion.incremental_state, ingestion.incremental_state_migrations, Lookback (+3 more)

### Community 110 - "plan_batch"
Cohesion: 0.14
Nodes (7): _batch_reason(), _check_batch(), _check_contract(), _incremental_bounds(), _IncrementalConfig, plan_batch(), TestPureHelpers

### Community 111 - "jdbc_partitioning.py"
Cohesion: 0.32
Nodes (4): column_position_kind(), choose(), _family(), _first_candidate()

### Community 112 - "depends_on"
Cohesion: 0.18
Nodes (11): condition, airflow-webserver, spark-master, spark-worker, sydafa, trino, condition, condition (+3 more)

### Community 113 - "Implementation Plan: [FEATURE]"
Cohesion: 0.22
Nodes (8): Complexity Tracking, Constitution Check, Documentation (this feature), Implementation Plan: [FEATURE], Project Structure, Source Code (repository root), Summary, Technical Context

### Community 114 - "speckit-plan"
Cohesion: 0.29
Nodes (3): main(), managed_load(), TestMain

### Community 115 - "_column_metadata"
Cohesion: 0.21
Nodes (4): _column_metadata(), _exact_pattern(), _optional_int(), TestCatalogPatternFiltering

### Community 116 - "managed_snapshot"
Cohesion: 0.26
Nodes (3): managed_snapshot(), FakeCatalog, TestManagedSnapshot

### Community 117 - "Speckit Checklist Phase"
Cohesion: 0.15
Nodes (11): Anti-Examples: What NOT To Do, Checklist Purpose: "Unit Tests for English", Example Checklist Types & Sample Items, Execution Steps, Post-Execution Checks, Pre-Execution Checks, User Input, [Category 1] (+3 more)

### Community 118 - "Feature Spec Template"
Cohesion: 0.15
Nodes (12): Assumptions, Edge Cases, Feature Specification: [FEATURE NAME], Functional Requirements, Key Entities *(include if feature involves data)*, Measurable Outcomes, Requirements *(mandatory)*, Success Criteria *(mandatory)* (+4 more)

### Community 121 - "6. Параллелизм Spark"
Cohesion: 0.22
Nodes (9): 6.1. Порядок выполнения load-задачи, 6. Параллелизм Spark, JDBC-партиции: несколько запросов к источнику, Как выбирается колонка разбиения, Как читать лог параллельной загрузки, Когда разбиение работает, Несколько загрузок и обработка внутри Spark, Ограничение: данные читаются в разные моменты (+1 more)

### Community 122 - "Sources Configuration File"
Cohesion: 0.29
Nodes (6): bank, airflow_conn_id, type, broker, airflow_conn_id, type

### Community 123 - "Настройка graphify — графа кодовой базы"
Cohesion: 0.25
Nodes (8): 1. Назначение, 3. Переменные окружения, 4. Перестроение графа, 5. Артефакты `graphify-out/` и git, 6. Запросы по графу, Настройка graphify — графа кодовой базы, Результат настройки, Содержание

### Community 124 - "Глоссарий"
Cohesion: 0.25
Nodes (8): Вычисления и ресурсы, Глоссарий, Данные и загрузки, Запуски и повторные попытки, Инкрементальная загрузка, Разработка и проверки, Содержание, Хранение и гарантии

### Community 125 - "Архитектура datalake_stand"
Cohesion: 0.25
Nodes (8): 10. Известные ограничения, 1. Слои данных, 9. Типы данных и гарантии, dds / dma (планируется), ods (планируется), raw — данные источника без бизнес-преобразований, Архитектура datalake_stand, Содержание

### Community 126 - "7. Инкрементальная загрузка"
Cohesion: 0.27
Nodes (11): 7.2. Жизненный цикл загрузки, 7.3. `incremental_state` — состояние сущности, 7.4. `incremental_batches` — реестр партий, 7.5. Пример одного инкремента и повтора, 7.6. Смена контракта и ручные миграции, 7. Инкрементальная загрузка, meta.finalize_batch(), meta.get_batch() (+3 more)

### Community 127 - "Управление ресурсами Spark на уровне отдельной джобы"
Cohesion: 0.25
Nodes (8): Выполнение и обмен данными, Задача Jira, Контекст, Память executor, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Управление ресурсами Spark на уровне отдельной джобы

### Community 128 - "Шаблонизация кода в generator/generate_dags.py"
Cohesion: 0.29
Nodes (7): Генерация DAG, Загрузка и окружение, Задача Jira, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Шаблонизация кода в generator/generate_dags.py

### Community 134 - "Process ID Generation"
Cohesion: 0.33
Nodes (6): ingestion.airflow_dagruns_log, generate_process_id (Airflow task), GenerateProcessIdOperator, generate_process_id (DAG task), pg-meta metadata database, ingestion.process_id_seq

### Community 135 - "Спецификация: необязательный журнал загрузок"
Cohesion: 0.33
Nodes (6): conf/airflow/Dockerfile, conf/airflow/requirements.txt, conf/spark/Dockerfile, ingestion/generator/requirements.txt, E2E test suite (test/test.sh), DATARCH-2241: Define and verify minimal supported component versions

### Community 137 - "positions.py"
Cohesion: 0.13
Nodes (5): validate(), _format(), kind_info(), _parse(), TestKindInfo

### Community 138 - "TestAirflowInit"
Cohesion: 0.16
Nodes (3): main(), disable_retries(), prepare_workspace()

### Community 140 - "first_row"
Cohesion: 0.29
Nodes (3): first_row(), _row_size(), TestFirstRow

### Community 142 - "2. Манифесты и генерация DAG-ов"
Cohesion: 0.33
Nodes (6): 2. Манифесты и генерация DAG-ов, Манифест пайплайна, Настройки таблицы источника, Общие настройки и демо-набор, Подготовка DAG-ов, Реестр источников

### Community 143 - "Обработка подвисших процессов Spark при retry задач"
Cohesion: 0.33
Nodes (6): Задача Jira, Обработка подвисших процессов Spark при retry задач, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Реализация и граница решения

### Community 144 - "DAG generator (ingestion/generator/generate_dags.py)"
Cohesion: 0.40
Nodes (5): Bank pipeline manifest (ingestion/conf/pipelines/bank/main.yaml), DAG generator (ingestion/generator/generate_dags.py), Manifest parser (ingestion/generator/manifest.py), Backlog: Auto-generate DAG tags and drop duplicates, Backlog: Split Airflow pools for loads vs service tasks

### Community 145 - "all_dialects"
Cohesion: 0.17
Nodes (3): all_dialects(), TestDayExpression, TestPositionKind

### Community 146 - "README — стенд хранилища и озера данных"
Cohesion: 0.50
Nodes (4): conf/airflow/requirements.txt — Airflow Image Dependencies, apache-airflow-providers-apache-spark, apache-airflow-providers-trino, sydafa-airflow — Airflow Integration Package (SydafaActivateOperator, SydafaMigrateOperator)

### Community 148 - "7.7. DAG-и снимков и таблицы-снимки"
Cohesion: 0.29
Nodes (7): 7.7. DAG-и снимков и таблицы-снимки, Автор и время публикации, Атомарная замена данных, Контроль свежести снимков, Повтор после сбоя, Пустое чтение, Структура таблицы-снимка

### Community 149 - "Сценарий демонстрации стенда"
Cohesion: 0.20
Nodes (10): Ограничение числа загрузок, Проверка параллельного чтения, Содержание, Сценарий демонстрации стенда, Шаг 1. Обновление источников (Airflow UI), Шаг 2. Источники через DBeaver (активация + просмотр), Шаг 3. Генерация DAG-ов загрузки (имитация CI/CD) и запуск, Шаг 4. Результат в Trino (DBeaver) и артефакты в pg-meta (+2 more)

### Community 150 - "read_window_frame"
Cohesion: 0.24
Nodes (4): read_window_frame(), window_filters(), make_args(), TestReadWindowFrame

### Community 151 - "Task Logging Tasks"
Cohesion: 0.67
Nodes (4): ingestion.airflow_tasks_log, log_end (DAG task), log_start (DAG task), TableLogOperator

### Community 152 - "5. Устойчивость к эволюции схемы источника"
Cohesion: 0.40
Nodes (5): 5. Устойчивость к эволюции схемы источника, Новые колонки, Отсутствующие колонки, Порядок колонок, Результат изменения схемы

### Community 153 - "test_dialects.py"
Cohesion: 0.08
Nodes (8): MetadataLocation, parse_source_table(), TestColumnProjection, TestContractsReExport, TestMetadataLocation, TestOracleIntegerKinds, TestParseSourceTable, CatalogDialect

### Community 154 - "План реализации DATARCH-2245"
Cohesion: 0.18
Nodes (10): План реализации DATARCH-2245, Подход, Проверка конституции, Содержание, Структура и проверка, Технический контекст, Команды, Приёмка (+2 more)

### Community 155 - "_apply_entity_fields"
Cohesion: 0.28
Nodes (3): _apply_entity_fields(), log_spark_task_event(), _strict_submit_options()

### Community 157 - "Анализ согласованности перед реализацией"
Cohesion: 0.29
Nodes (6): Анализ согласованности перед реализацией, Исправления ревью PR 69, Итог converge, Результат, Содержание, Состояние проверок перед публикацией

### Community 158 - "Задачи DATARCH-2245"
Cohesion: 0.40
Nodes (4): Задачи DATARCH-2245, Приёмка, Реализация, Содержание

### Community 159 - "Проверка требований DATARCH-2245"
Cohesion: 0.50
Nodes (3): Проверка требований DATARCH-2245, Результат, Содержание

### Community 160 - "run_spark_guard"
Cohesion: 0.29
Nodes (3): Контекст, _pgmeta_dsn(), run_spark_guard()

### Community 168 - "check_spark_guard.py"
Cohesion: 0.16
Nodes (4): main(), orphan(), require(), snapshot()

### Community 172 - "4. Detection Passes (Token-Efficient Analysis)"
Cohesion: 0.15
Nodes (13): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 4. Detection Passes (Token-Efficient Analysis), 5. Severity Assignment, 6. Produce Compact Analysis Report, A. Duplication Detection, B. Ambiguity Detection (+5 more)

### Community 175 - "Шаг 5. Повторный раунд: полный срез против инкремента"
Cohesion: 0.29
Nodes (7): 5.1. Подготовка показа pg-meta, 5.2. Новый запуск без изменения источника, 5.3. Clear только успешной load-задачи, 5.4. Полный Clear существующего запуска, 5.5. Новая загрузка после обновления источника, 5.6. Что объяснить про повтор после ошибки, Шаг 5. Повторный раунд: полный срез против инкремента

### Community 176 - "incremental/load_entity.py"
Cohesion: 0.07
Nodes (17): __act_snap auxiliary snapshot, append.py (raw-layer writer), dds / dma layers (planned), IngestActSnapOperator, IngestFullOperator, IngestIncrementalOperator, Airflow pool ingest_loads, load (DAG task) (+9 more)

### Community 179 - "Шаг 7. Стресс-тест: эволюция схемы источника"
Cohesion: 0.29
Nodes (7): 7.1. Удаление колонки, 7.2. Переименование колонки, 7.3. Добавление колонки, 7.4. Настройка генераторов данных, 7.5. Проверка схемы источника, 7.6. Загрузка после изменения схемы, Шаг 7. Стресс-тест: эволюция схемы источника

### Community 186 - "Расширение стенда"
Cohesion: 0.40
Nodes (5): 1. Выбор вида изменения, 4. Новый слой данных, 5. Порядок разработки, Расширение стенда, Содержание

### Community 187 - "Проверка реализации"
Cohesion: 0.40
Nodes (4): Выполнено, Ограничения, Проверка реализации, Содержание

### Community 188 - "План: параллельный e2e"
Cohesion: 0.40
Nodes (4): План: параллельный e2e, Проверки, Решение, Содержание

### Community 189 - "Спецификация: параллельный e2e за семь минут"
Cohesion: 0.40
Nodes (4): Приёмка, Содержание, Спецификация: параллельный e2e за семь минут, Требования

### Community 190 - "Задачи: параллельный e2e"
Cohesion: 0.40
Nodes (4): Задачи: параллельный e2e, Проверка, Реализация, Содержание

### Community 191 - "Анализ согласованности"
Cohesion: 0.50
Nodes (3): Анализ согласованности, Вывод, Содержание

### Community 192 - "Snapshot table (actual snapshot, __act_snap)"
Cohesion: 0.67
Nodes (3): Snapshot stream (incremental/load_act_snap.py), Increment (incremental column contract), Snapshot table (actual snapshot, __act_snap)

## Ambiguous Edges - Review These
- `tasks-template.md` → `speckit-tasks`  [AMBIGUOUS]
  .specify/templates/tasks-template.md · relation: references

## Knowledge Gaps
- **104 isolated node(s):** `TEST_FAILURE_LOG_DIR`, `conf/spark/spark-defaults.conf`, `pg-catalog — база метаданных Nessie`, `Bootstrap (initial fill)`, `ingestion.incremental_state_migrations` (+99 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 1422 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **49 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `services` connect `Docker Compose Layout` to `DAG Processor Service`, `Sydafa Service Compose`, `Nessie Catalog Config`, `Compose Environment Vars`, `Init Services Compose`, `Airflow Services Compose`, `Postgres Services Compose`, `Test Runner Service`, `Silo Service Compose`?**
  _High betweenness centrality (0.116) - this node is a cross-community bridge._
- **Are the 16 inferred relationships involving `RdbmsDialect` (e.g. with `Контекст` and `Предпочтительный путь решения`) actually correct?**
  _`RdbmsDialect` has 16 INFERRED edges - model-reasoned connections that need verification._
- **What connects `TEST_FAILURE_LOG_DIR`, `conf/spark/spark-defaults.conf`, `pg-catalog — база метаданных Nessie` to the rest of the system?**
  _104 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `parse_args` be split into smaller, more focused modules?**
  _Cohesion score 0.08865248226950355 - nodes in this community are weakly interconnected._
- **Why does `README — стенд хранилища и озера данных` connect `AGENTS.md — инструкции проекта datalake_stand` to `test_dialects.py`, `Architecture Documentation`, `Dev Tooling Documentation`, `Сквозное (e2e) тестирование стенда datalake_stand`, `Docker Compose Layout`, `README — стенд хранилища и озера данных`, `Demo Walkthrough Docs`?**
  _High betweenness centrality (0.105) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `generate_all()` (e.g. with `IncrementalEntityConfig` and `SourceConfig`) actually correct?**
  _`generate_all()` has 2 INFERRED edges - model-reasoned connections that need verification._

### Low-confidence Hints
_AMBIGUOUS edges — the extractor was unsure. Verify before acting on these._

- **What is the exact relationship between `tasks-template.md` and `speckit-tasks`?**
  _Edge tagged AMBIGUOUS (relation: references) - confidence is low._