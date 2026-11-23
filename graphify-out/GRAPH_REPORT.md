# Graph Report - datalake_stand  (2026-11-23)

## Corpus Check
- 195 files · ~158,184 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 14 file(s) not represented in the graph (top: (none) 9, .properties 3, .conf 1)

## Summary
- 3214 nodes · 5852 edges · 219 communities (153 shown, 66 thin omitted)
- Extraction: 95% EXTRACTED · 5% INFERRED · 0% AMBIGUOUS · INFERRED: 286 edges (avg confidence: 0.91)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `4282593f`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- parse_args
- keys
- read_extract_frame
- PostgresTests
- RdbmsDialect
- reader.py
- manifest.py
- entity.py
- TestBatchLifecycle
- TestSparkGuard
- run_flow.py
- ParallelE2ETests
- silo
- load_sources
- README — стенд хранилища и озера данных
- load_pipeline
- TestIngestOperator
- jdbc.py
- TestTaskLifecycleCallbacks
- all_dialects
- Попытка (attempt)
- managed_load
- FakeCursor
- Spec Kit Workflow
- temporal_extraction.py
- Spec Kit Cycle Instructions
- speckit-tasks
- check_extraction.py
- ColumnMeta
- canonical
- Shell Helper Scripts
- test_load_act_snap.py
- guard.py
- speckit-specify
- Implementation Plan Template
- TestKeyStructure
- Airflow Env Config
- generate_dags.py
- managed_snapshot
- Nessie Catalog Config
- Architecture Documentation
- TestActSnapManifest
- Dev Tooling Documentation
- resolve_target_table_name
- key_metadata
- speckit-analyze
- StandConfigTests
- Init Services Compose
- MssqlDialect
- Airflow Services Compose
- Docker Compose Layout
- Предпочтительный путь решения
- Execution Steps
- Bank Pipeline Manifest
- Broker Pipeline Manifest
- Constitution Templates
- TestQuoting
- TestAppendRaw
- generate_all
- test_no_sys_path.py
- demo/schema_evolution.py
- DATARCH-2243: Templatize code in generator/generate_dags.py
- Базовые принципы
- TestManifestShapeRejections
- DAG Processor Service
- validate_target_table
- by_conn_type
- Watermark
- meta/registry.py
- TestSchemaColumns
- Act Snap Plan Docs
- Глоссарий
- dsn_connect
- Analysis Detection Passes
- Named Volumes
- 7.1. Контракт
- TestGenerateAll
- Приоритет: важно
- build_create_table_sql
- speckit.analyze.md
- ScriptedResult
- Тестирование datalake_stand
- Postgres Services Compose
- raw_rdbms.py
- Test Runner Service
- Спецификация: безопасный повтор Spark-загрузки
- speckit-checklist
- speckit.tasks.md
- empty_incremental_window
- IncrementalEntityConfig
- Silo Service Compose
- IngestFullOperator
- Bank References Manifest
- Broker References Manifest
- first_row
- 002-incremental-config/spec.md
- IntPlanner
- pg-meta DDL Tables
- Sydafa Service Compose
- 4. Таблицы raw-слоя: структура и наполнение
- full/load_entity.py
- source_ddl.py
- TestPublishAsset
- speckit-checklist
- Platform Component Overview
- Шаблонизация кода в generator/generate_dags.py
- spark-worker
- Ведение документации
- append.py (raw-layer writer)
- TestIncrementalManifest
- TestDagrunCallbacks
- assert_unique_targets
- speckit.plan.md
- parse_args
- by_jdbc_url
- base.py
- speckit.taskstoissues.md
- Feature Spec Template
- test_operators.py
- test.sh
- Спецификация: упрощение чтения по JDBC и инкрементальной загрузки
- Sources Configuration File
- Настройка graphify — графа кодовой базы
- MetadataLocation
- DATARCH-2241: Define and verify minimal supported component versions
- 7. Инкрементальная загрузка
- Управление ресурсами Spark на уровне отдельной джобы
- 4. Detection Passes (Token-Efficient Analysis)
- JdbcOperation
- speckit.converge.md
- TestIngestActSnapOperator
- TestAirflowIgnore
- 7. Практики прохождения цикла
- Process ID Generation
- Обработка подвисших процессов Spark при retry задач
- 7.7. DAG-и снимков и таблицы-снимки
- create-new-feature.sh
- test_sql_logging.py
- Execution Steps
- План реализации DATARCH-2245
- TestTaskFailure
- speckit.implement.md
- План: callbacks журнала загрузок
- conf/airflow/requirements.txt — Airflow Image Dependencies
- 6. Параллелизм Spark
- TestPureHelpers
- Диагностика шагов загрузки
- IncrementalEntityConfig
- literal
- Архитектура datalake_stand
- Task Logging Tasks
- TestCatalogPatternFiltering
- TestResolveFullyQualified
- speckit.plan.md
- Execution Steps
- TestPositionExpression
- Анализ согласованности перед реализацией
- Задачи DATARCH-2245
- Проверка требований DATARCH-2245
- extending.md
- test_jdbc.py
- Bucket Init Script
- Auth Init Script
- Source Activation Operator
- Asset Event Callables
- Generator Dependencies
- TestWindowSummary
- Publish Asset Task
- Bootstrap Process
- CAS Revision Control
- TestPluginSyntax
- TestColumnProjection
- json
- 2. Манифесты и генерация DAG-ов
- Приоритет: можно отложить
- Specification Analysis Report
- Исследование
- TestContractsReExport
- TestAirflowInit
- 3. Структура сгенерированного DAG-а
- Convergence Findings
- 5. Устойчивость к эволюции схемы источника
- 008-raw-utc-timestamps/plan.md
- 8. База метаданных pg-meta: остальные объекты
- TestOracleIntegerKinds
- Проверка реализации
- План: параллельный e2e
- Спецификация: параллельный e2e за семь минут
- Задачи: параллельный e2e
- Анализ согласованности
- 008-raw-utc-timestamps/spec.md
- 008-raw-utc-timestamps/tasks.md
- Task Generation Rules
- TestResolveFullyQualified
- Задачи
- make_batch
- Clear
- Проверка требований
- Модель данных
- 006-ingestion-step-logs/plan.md
- 006-ingestion-step-logs/tasks.md
- DagRun
- Сущность (entity)
- Исполнение (execution)
- FQN
- Snapshot Iceberg
- Манифест
- Пул Airflow (pool)
- Порция (portion)
- Retry
- parse_source_table
- RTAS
- TTL активации источника
- TestPgMetaInitSql
- 007-simplify-jdbc-increment/plan.md
- 007-simplify-jdbc-increment/tasks.md

## God Nodes (most connected - your core abstractions)
1. `RdbmsDialect` - 58 edges
2. `load_pipeline()` - 39 edges
3. `generate_all()` - 33 edges
4. `write_temp_yaml()` - 32 edges
5. `keys()` - 29 edges
6. `load_sources()` - 26 edges
7. `resolve_target_table_name()` - 26 edges
8. `TestLoadPipeline` - 25 edges
9. `ConnectionSpec` - 24 edges
10. `read_extract_frame()` - 24 edges

## Surprising Connections (you probably didn't know these)
- `Память executor` --references--> `EntityConfig`  [INFERRED]
  docs/backlog.md → ingestion/generator/manifest.py
- `Состав pytest-проверок` --references--> `log_sql()`  [INFERRED]
  .agents/skills/full-testing/SKILL.md → ingestion/logging.py
- `Решение` --references--> `log_task()`  [INFERRED]
  specs/005-load-lifecycle-callbacks/plan.md → ingestion/meta/__init__.py
- `Выводы` --references--> `log_task()`  [INFERRED]
  specs/005-load-lifecycle-callbacks/research.md → ingestion/meta/__init__.py
- `Требования` --references--> `IngestActSnapOperator`  [INFERRED]
  specs/001-act-snap-contract/spec.md → ingestion/operators/raw_rdbms.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Spec Kit command pipeline (specify -> clarify -> plan -> tasks -> implement -> converge/analyze)** — zcode_skills_speckit_specify_skill, zcode_skills_speckit_clarify_skill, zcode_skills_speckit_plan_skill, zcode_skills_speckit_tasks_skill, zcode_skills_speckit_implement_skill, zcode_skills_speckit_converge_skill, zcode_skills_speckit_analyze_skill [EXTRACTED 0.85]
- **Архитектура стенда dlh-poc: сервисы и хранилища** — readme_sydafa, readme_airflow, readme_spark, readme_trino, readme_nessie, readme_silo, readme_pg_meta, readme_pg_catalog [EXTRACTED 0.90]
- **Incremental batch lifecycle (managed_load flow)** — docs_architecture__ref__ingestion_spark_raw_rdbms_incremental_load_entity_managed_load, docs_architecture__ref__ingestion_meta_registry_get_batch, docs_architecture__ref__ingestion_meta_registry_start_batch, docs_architecture__ref__ingestion_meta_registry_finalize_batch, docs_architecture_incremental_batches, docs_architecture_incremental_state [EXTRACTED 0.90]
- **Ingest DAG per-entity task flow (generate_process_id >> activate_source >> log_start >> load >> log_end >> publish_asset)** — docs_architecture_generate_process_id_task, docs_architecture_activate_source_task, docs_architecture_log_start_task, docs_architecture_load_task, docs_architecture_log_end_task, docs_architecture_publish_asset_task [EXTRACTED 0.90]
- **pg-meta ingestion schema objects (run logs, entity state, batch registry)** — docs_architecture_pg_meta, docs_architecture_airflow_dagruns_log, docs_architecture_airflow_tasks_log, docs_architecture_incremental_state, docs_architecture_incremental_batches, docs_architecture_generate_process_id_task [EXTRACTED 0.90]
- **pg-meta metadata schema objects** — docs_architecture_pg_meta, docs_architecture_incremental_state, docs_architecture_incremental_batches, docs_architecture_incremental_state_migrations, docs_architecture_airflow_dagruns_log, docs_architecture_airflow_tasks_log, docs_architecture_process_id_seq [EXTRACTED 0.90]
- **Спек-цикл Spec Kit: constitution → specify → clarify → plan → tasks → analyze → implement → converge** — opencode_commands_speckit_constitution, opencode_commands_speckit_specify, opencode_commands_speckit_clarify, opencode_commands_speckit_plan, opencode_commands_speckit_tasks, opencode_commands_speckit_analyze, opencode_commands_speckit_implement, opencode_commands_speckit_converge [EXTRACTED 0.95]
- **Спек-цикл разработки Spec Kit + graphify** — docs_agents_speckit, docs_agents_speckit_specify_cli, docs_agents_graphify, specs_001_act_snap_contract_spec, specs_002_incremental_config_spec, specs_003_module_structure_spec [INFERRED 0.80]
- **Концепция инкрементальной загрузки (контракт инкремента)** — docs_glossary_increment, docs_glossary_position, docs_glossary_canonical, docs_architecture_watermark, docs_glossary_window, docs_architecture_lookback, docs_glossary_batch, docs_glossary_attempt, docs_glossary_portion, specs_002_incremental_config_spec [INFERRED 0.90]

## Communities (219 total, 66 thin omitted)

### Community 0 - "parse_args"
Cohesion: 0.11
Nodes (6): main(), parse_args(), build_argv(), TestMain, TestParseArgs, TestResolveBatch

### Community 1 - "keys"
Cohesion: 0.05
Nodes (30): Clear только успешной load-задачи, Trigger без новых данных, Запуск и подключения, Исходное состояние, Многопоточное чтение, Повтор после ошибки: рассказать, не показывать, Подготовка, Полный Clear существующего запуска (+22 more)

### Community 2 - "read_extract_frame"
Cohesion: 0.05
Nodes (14): PartitionChoice, apply_partition_options(), bound_literal(), inspect_source(), jdbc_reader(), probe_bounds(), read_extract_frame(), ReadSource (+6 more)

### Community 4 - "PostgresTests"
Cohesion: 0.06
Nodes (7): transaction(), key(), IntPlanner, PostgresTests, calculate_lower(), sql(), TransactionTests

### Community 5 - "RdbmsDialect"
Cohesion: 0.07
Nodes (12): Контракт диалекта, __getattr__(), RdbmsDialect, column_position_kind(), compatibility_projection(), choose(), _family(), _first_candidate() (+4 more)

### Community 6 - "reader.py"
Cohesion: 0.06
Nodes (12): _Contract, IncrementalConfig, _Lookback, validate(), window_contract(), _format(), kind_info(), _parse() (+4 more)

### Community 7 - "manifest.py"
Cohesion: 0.11
Nodes (7): Генерация и настройка, generate_snapshot_dag_source(), ActSnapConfig, AirflowConfig, DagConfig, SparkConfig, TestGenerateSnapshotDagSource

### Community 8 - "entity.py"
Cohesion: 0.07
Nodes (15): IngestFullOperator, IngestIncrementalOperator, Airflow pool ingest_loads, load (DAG task), reader.py (shared JDBC reader), configure_driver_logging(), log_sql(), log_step() (+7 more)

### Community 9 - "TestBatchLifecycle"
Cohesion: 0.08
Nodes (5): BatchTestCase, IntPlanner, TestBatchLifecycle, TestGetBatch, TestPlanBatch

### Community 10 - "TestSparkGuard"
Cohesion: 0.18
Nodes (4): app(), inventory(), load_guard(), TestSparkGuard

### Community 11 - "run_flow.py"
Cohesion: 0.06
Nodes (41): airflow_check_asset_publication(), airflow_task_log(), airflow_task_states(), airflow_token(), airflow_token_retry(), airflow_trigger(), airflow_trigger_retry(), airflow_unpause() (+33 more)

### Community 12 - "ParallelE2ETests"
Cohesion: 0.07
Nodes (5): disable_retries(), prepare_workspace(), scale_migration(), ParallelE2ETests, token()

### Community 13 - "silo"
Cohesion: 0.08
Nodes (36): environment, healthcheck, image, networks, volumes, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, POSTGRES_DB (+28 more)

### Community 14 - "load_sources"
Cohesion: 0.11
Nodes (7): IngestionSettings, load_settings(), load_sources(), _positive_integer(), _read_yaml(), TestGenerateAllValidation, TestLoadSources

### Community 15 - "README — стенд хранилища и озера данных"
Cohesion: 0.10
Nodes (22): AGENTS.md — инструкции проекта datalake_stand, Скилл backlog — ведение docs/backlog.md, пакет sydafa-airflow (SydafaActivateOperator), airflow/plugins/migrations/ — миграции источников bank/broker, conf/airflow/Dockerfile, conf/pg-meta/init/ — DDL pg-meta, conf/spark/Dockerfile, conf/sydafa/sources.json — конфигурация источников (+14 more)

### Community 16 - "load_pipeline"
Cohesion: 0.13
Nodes (7): _expand_cron_field(), load_pipeline(), _parse_dag_fields(), parse_schedule(), temporal_lookback_default(), TestLoadPipeline, write_temp_yaml()

### Community 18 - "jdbc.py"
Cohesion: 0.12
Nodes (7): Шаг 2. Источники через DBeaver, connection(), _degraded_metadata(), _exact_pattern(), _optional_int(), _primary_key(), _unique_single_columns()

### Community 20 - "all_dialects"
Cohesion: 0.15
Nodes (3): all_dialects(), TestConnections, TestPositionKind

### Community 22 - "managed_load"
Cohesion: 0.16
Nodes (8): _execute_batch(), managed_load(), _plan_new_batch(), _resolve_batch(), resolve_incremental_column(), _ResolvedBatch, _reuse_batch(), TestManagedLoad

### Community 23 - "FakeCursor"
Cohesion: 0.07
Nodes (3): FakeConnection, FakeCursor, FakePg

### Community 24 - "Spec Kit Workflow"
Cohesion: 0.06
Nodes (30): args, inputs, integration, spec, default, prompt, type, any (+22 more)

### Community 25 - "temporal_extraction.py"
Cohesion: 0.09
Nodes (18): WindowContract, check_post_append_retry(), source_rows(), check_raw_temporal(), check_snapshot_recovery(), allocate(), incremental(), projection() (+10 more)

### Community 26 - "Spec Kit Cycle Instructions"
Cohesion: 0.07
Nodes (29): 10. Обновление Spec Kit, 11. Взаимодействие с graphify и Pull Request, 12. Работа в ZCode Desktop (интерактивно), 1. Назначение, 2. Установка для сопровождающего (выполнена один раз), 3. Инструкция для разработчика, 4. Состояние цикла: каталоги, машины, ветки, 5. Состав артефактов (+21 more)

### Community 27 - "speckit-tasks"
Cohesion: 0.06
Nodes (44): contracts/ (interface contracts), plan.md (implementation plan), spec.md (feature specification), Модель состояния guard, Содержание, Состояния, Исследование, Источники и ограничения (+36 more)

### Community 28 - "check_extraction.py"
Cohesion: 0.15
Nodes (6): resolve_source_connection(), check_logged_submit_replay(), check_snapshot_publication(), main(), records(), require()

### Community 29 - "ColumnMeta"
Cohesion: 0.18
Nodes (5): ColumnMeta, check_partitioned_load_path(), jdbc_count(), load_module(), TestCompatibilityProjection

### Community 30 - "canonical"
Cohesion: 0.11
Nodes (6): canonical(), maximum(), window_bounds(), _WindowPlanner, window_summary(), TestPositions

### Community 31 - "Shell Helper Scripts"
Cohesion: 0.16
Nodes (21): check-prerequisites.sh script, check_dir(), check_file(), find_specify_root(), format_speckit_command(), get_current_branch(), get_feature_paths(), get_invoke_separator() (+13 more)

### Community 32 - "test_load_act_snap.py"
Cohesion: 0.17
Nodes (3): cli_args(), TestBuildRtasSql, TestOverwriteSnapshot

### Community 33 - "guard.py"
Cohesion: 0.14
Nodes (8): _all_applications_completed(), _applications(), _get_master_state(), guard_spark_submit(), _kill_application(), _remaining(), _request(), SparkGuardError

### Community 34 - "speckit-specify"
Cohesion: 0.07
Nodes (30): checklists/requirements.md (spec quality checklist), .specify/extensions.yml — хуки расширений Spec Kit, .specify/init-options.json, speckit-clarify, Completion Report, Done When, Mandatory Post-Execution Hooks, Outline (+22 more)

### Community 35 - "Implementation Plan Template"
Cohesion: 0.08
Nodes (26): Dependencies & Execution Order, Format: `[ID] [P?] [Story] Description`, Implementation for User Story 1, Implementation for User Story 2, Implementation for User Story 3, Implementation Strategy, Incremental Delivery, MVP First (User Story 1 Only) (+18 more)

### Community 37 - "Airflow Env Config"
Cohesion: 0.26
Nodes (25): environment, environment, environment, environment, AIRFLOW__API_AUTH__JWT_SECRET, AIRFLOW__API__EXPOSE_CONFIG, AIRFLOW__API__SECRET_KEY, AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION (+17 more)

### Community 38 - "generate_dags.py"
Cohesion: 0.06
Nodes (16): asset_uri(), dag_id_for(), file_name_for(), snapshot_asset_uri(), snapshot_dag_id(), _snapshot_entity_block(), SourceConfig, resolve_fully_qualified() (+8 more)

### Community 39 - "managed_snapshot"
Cohesion: 0.15
Nodes (5): current_snapshot_author(), managed_snapshot(), FakeCatalog, TestCurrentSnapshotAuthor, TestManagedSnapshot

### Community 40 - "Nessie Catalog Config"
Cohesion: 0.09
Nodes (23): pg-catalog, nessie.catalog.default-warehouse, nessie.catalog.secrets.access-key.name, nessie.catalog.secrets.access-key.secret, nessie.catalog.service.s3.default-options.access-key, nessie.catalog.service.s3.default-options.auth-type, nessie.catalog.service.s3.default-options.endpoint, nessie.catalog.service.s3.default-options.path-style-access (+15 more)

### Community 41 - "Architecture Documentation"
Cohesion: 0.11
Nodes (21): pg-meta DDL (conf/pg-meta/init/01_ingestion.sql), Glossary (glossary.md), MSSQL dialect (ingestion/sources/rdbms/dialects/mssql.py), Oracle dialect (ingestion/sources/rdbms/dialects/oracle.py), Iceberg writer layer (ingestion/spark/raw/iceberg.py), Full-slice loader (ingestion/spark/raw/rdbms/full/load_entity.py), Incremental loader (ingestion/spark/raw/rdbms/incremental/load_entity.py), Incremental window reader (incremental/window.py) (+13 more)

### Community 43 - "Dev Tooling Documentation"
Cohesion: 0.12
Nodes (18): AGENTS.md, graphify-out/graph.json, graphify (CLI-утилита), AGENTS.md, .specify/feature.json (локальный указатель фичи), opencode, .opencode/commands/, specify CLI (GitHub Spec Kit) (+10 more)

### Community 44 - "resolve_target_table_name"
Cohesion: 0.17
Nodes (3): resolve_target_table_name(), _split_fragments(), TestResolveTargetTableName

### Community 45 - "key_metadata"
Cohesion: 0.19
Nodes (7): key_metadata(), KeyMetadata, keys(), fake_jvm(), TestCatalogAddressing, TestDegradedKeyMetadata, TestKeyMetadata

### Community 46 - "speckit-analyze"
Cohesion: 0.14
Nodes (14): speckit-analyze, 7. Provide Next Actions, 8. Offer Remediation, 9. Check for extension hooks, .specify/extensions.yml (extension hooks registry), Analysis Guidelines, Context, Context Efficiency (+6 more)

### Community 48 - "Init Services Compose"
Cohesion: 0.07
Nodes (30): condition, airflow-webserver, nessie, silo-init, spark-master, spark-worker, sydafa, trino (+22 more)

### Community 50 - "Airflow Services Compose"
Cohesion: 0.12
Nodes (17): command, depends_on, image, networks, pull_policy, volumes, condition, command (+9 more)

### Community 51 - "Docker Compose Layout"
Cohesion: 0.12
Nodes (16): command, image, networks, ports, pull_policy, volumes, name, networks (+8 more)

### Community 52 - "Предпочтительный путь решения"
Cohesion: 0.20
Nodes (10): Жизненный цикл, Задача Jira, Изолировать Spark drivers через DockerOperator, Контракт оператора, Образ и зависимости, Предпочтительный путь решения, Проблема, Проверки и критерии готовности (+2 more)

### Community 53 - "Execution Steps"
Cohesion: 0.29
Nodes (7): 1. Initialize Convergence Context, 2. Load Artifacts (Progressive Disclosure), 3. Build the Intent Inventory, 4. Assess the Codebase and Classify Findings, 5. Assign Severity, 6. Present the In-Session Findings Summary, Execution Steps

### Community 54 - "Bank Pipeline Manifest"
Cohesion: 0.12
Nodes (17): airflow, pool, check_not_null, position_type, source_column, entities, airflow, fqn (+9 more)

### Community 55 - "Broker Pipeline Manifest"
Cohesion: 0.12
Nodes (17): airflow, pool, check_not_null, position_type, source_column, entities, airflow, fqn (+9 more)

### Community 56 - "Constitution Templates"
Cohesion: 0.12
Nodes (15): Outline, Post-Execution Checks, Pre-Execution Checks, Scope Guard, User Input, Core Principles, Governance, [PRINCIPLE_1_NAME] (+7 more)

### Community 59 - "generate_all"
Cohesion: 0.13
Nodes (5): compute_drift(), generate_all(), main(), TestCheckMode, TestConfiguration

### Community 60 - "test_no_sys_path.py"
Cohesion: 0.17
Nodes (3): TestNoHashComments, manipulates_sys_path(), TestNoSysPathManipulation

### Community 61 - "demo/schema_evolution.py"
Cohesion: 0.23
Nodes (4): main(), migrate_source(), request(), wait_health()

### Community 62 - "DATARCH-2243: Templatize code in generator/generate_dags.py"
Cohesion: 0.21
Nodes (16): Bank pipeline manifest (ingestion/conf/pipelines/bank/main.yaml), DAG generator (ingestion/generator/generate_dags.py), generate_dag_source(), generate_snapshot_dag_source(), Manifest parser (ingestion/generator/manifest.py), build_create_table_sql(), build_rtas_sql(), build_extract_query() (+8 more)

### Community 63 - "Базовые принципы"
Cohesion: 0.18
Nodes (11): I. Граф кодовой базы прежде чтения файлов, II. Изменения подтверждаются тестами, III. Чистый Python без комментариев, IV. Дисциплина Git: ветка → Pull Request → мерж пользователем, V. Перестроение графа после каждого коммита, VI. Документация на русском языке, Базовые принципы, Конституция проекта datalake_stand (+3 more)

### Community 65 - "DAG Processor Service"
Cohesion: 0.39
Nodes (9): build, build, build, build, context, dockerfile, build, build (+1 more)

### Community 67 - "by_conn_type"
Cohesion: 0.13
Nodes (6): by_conn_type(), position_select(), render_source_table(), render_source_reference(), TestRenderSourceTable, TestPositionSelect

### Community 68 - "Watermark"
Cohesion: 0.18
Nodes (13): Batch registry (ingestion/meta/registry.py), Snapshot stream (incremental/load_act_snap.py), Incremental batch, Bootstrap (initial fill), Increment (incremental column contract), ingestion.incremental_batches, incremental contract (canonical position type), ingestion.incremental_state (+5 more)

### Community 69 - "meta/registry.py"
Cohesion: 0.09
Nodes (24): allocate_process_id(), DbConnect, DbCursor, DbRow, _execute(), find_process_id(), log_dagrun(), log_task() (+16 more)

### Community 70 - "TestSchemaColumns"
Cohesion: 0.15
Nodes (3): fake_pyspark_types(), SchemaDouble, TestSchemaColumns

### Community 71 - "Act Snap Plan Docs"
Cohesion: 0.07
Nodes (27): Asset (Airflow), ActSnapConfig (dataclass), DagConfig (базовый dataclass), MAX_ACTIVE_RUNS (константа генератора), Дополнительное покрытие пунктов 3–5, Проверка согласованности, ACT_SNAPSHOT_TABLES, Дополнение: пункты 3–5 плана от 2026-11-08 (+19 more)

### Community 72 - "Глоссарий"
Cohesion: 0.25
Nodes (8): Вычисления и ресурсы, Глоссарий, Данные и загрузки, Запуски и повторные попытки, Инкрементальная загрузка, Разработка и проверки, Содержание, Хранение и гарантии

### Community 73 - "dsn_connect"
Cohesion: 0.29
Nodes (3): DbConnection, dsn_connect(), TestDsnConnect

### Community 74 - "Analysis Detection Passes"
Cohesion: 0.15
Nodes (13): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 4. Detection Passes (Token-Efficient Analysis), 5. Severity Assignment, 6. Produce Compact Analysis Report, A. Duplication Detection, B. Ambiguity Detection (+5 more)

### Community 75 - "Named Volumes"
Cohesion: 0.17
Nodes (12): name, name, volumes, airflow_logs, airflow_pg_data, pg_catalog_data, pg_meta_data, silo_data (+4 more)

### Community 76 - "7.1. Контракт"
Cohesion: 0.17
Nodes (12): 7.1. Контракт, Выбор типа позиции, Выравнивание временной границы, Границы окна, Настройка lookback, Отступ по умолчанию и расписание, Первичное наполнение и проверка источника, Передача конфигурации загрузчику (+4 more)

### Community 78 - "Приоритет: важно"
Cohesion: 0.17
Nodes (12): Бэклог, Задача Jira, Задача Jira, Контекст, Оглавление, Определить и проверить минимальные поддерживаемые версии компонентов, Предпочтительный путь решения, Приоритет: важно (+4 more)

### Community 79 - "build_create_table_sql"
Cohesion: 0.15
Nodes (5): build_create_table_sql(), table_location(), main(), TestBuildCreateTableSql, TestTableLocation

### Community 80 - "speckit.analyze.md"
Cohesion: 0.22
Nodes (8): Analysis Guidelines, Context, Context Efficiency, Goal, Operating Constraints, Operating Principles, Pre-Execution Checks, User Input

### Community 82 - "Тестирование datalake_stand"
Cohesion: 0.22
Nodes (8): Изоляция и диагностика, Как расширять тестирование, Порядок запуска и бюджет, Содержание, Состав e2e-проверок, Состав pytest-проверок, Структура и ответственность, Тестирование datalake_stand

### Community 83 - "Postgres Services Compose"
Cohesion: 0.18
Nodes (11): command, depends_on, image, networks, pull_policy, volumes, condition, airflow-postgres (+3 more)

### Community 84 - "raw_rdbms.py"
Cohesion: 0.07
Nodes (13): Назначение задач, Событие готовности Asset, publish_asset_event(), snapshot_has_current_process(), _dagrun_event(), dagrun_failure_callback(), dagrun_success_callback(), GenerateProcessIdOperator (+5 more)

### Community 85 - "Test Runner Service"
Cohesion: 0.20
Nodes (10): AIRFLOW_PASSWORD, AIRFLOW_USER, TRINO_PASSWORD, TRINO_USER, test-runner, environment, image, networks (+2 more)

### Community 86 - "Спецификация: безопасный повтор Spark-загрузки"
Cohesion: 0.17
Nodes (11): US1: повтор после потери процесса задачи (P1), US2: запрет неподтверждённого запуска (P1), US3: предсказуемые повторы и диагностика (P2), Краевые случаи, Критерии успеха, Содержание, Спецификация: безопасный повтор Spark-загрузки, Сущности (+3 more)

### Community 87 - "speckit-checklist"
Cohesion: 0.12
Nodes (15): tasks.md (task list), $speckit-github-taskstoissues (github extension), speckit-checklist, Anti-Examples: What NOT To Do, Checklist Purpose: "Unit Tests for English", Example Checklist Types & Sample Items, Execution Steps, Post-Execution Checks (+7 more)

### Community 88 - "speckit.tasks.md"
Cohesion: 0.18
Nodes (10): Checklist Format (REQUIRED), Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Phase Structure, Pre-Execution Checks, Task Generation Rules (+2 more)

### Community 90 - "IncrementalEntityConfig"
Cohesion: 0.09
Nodes (5): generate_dag_source(), PipelineManifest, validate_pipeline(), TestGenerateDagSource, TestValidatePipeline

### Community 91 - "Silo Service Compose"
Cohesion: 0.20
Nodes (10): silo, AWS_DEFAULT_REGION, silo-init, condition, depends_on, entrypoint, environment, image (+2 more)

### Community 92 - "IngestFullOperator"
Cohesion: 0.09
Nodes (16): Границы рефакторинга, Контекст, Контекст, Контекст, Предпочтительный путь решения, 3. Новый тип источника, _apply_entity_fields(), IngestActSnapOperator (+8 more)

### Community 93 - "Bank References Manifest"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 94 - "Broker References Manifest"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 95 - "first_row"
Cohesion: 0.29
Nodes (3): first_row(), _row_size(), TestFirstRow

### Community 96 - "002-incremental-config/spec.md"
Cohesion: 0.29
Nodes (6): Инкремент (incremental), CLI-флаг --incremental-config, incremental.contract (YAML: source_column, position_type), Контракт инкрементальной загрузки, Требования, Уточнения

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
Cohesion: 0.16
Nodes (18): conf/spark/spark-defaults.conf, Global settings (ingestion/conf/settings.yaml), Sources registry (ingestion/conf/sources.yaml), load_sources(), Load operators module (ingestion/operators.py), DATARCH-2242: Split sources registry into YAML files, per-source JDBC parallelism, DATARCH-2244: Isolate Spark drivers via DockerOperator, DATARCH-2245: Handle hung Spark processes on task retry (+10 more)

### Community 102 - "source_ddl.py"
Cohesion: 0.21
Nodes (6): _connect(), execute(), mssql_dba_connect(), oracle_dba_connect(), query_scalar(), read_dba_secrets()

### Community 104 - "speckit-checklist"
Cohesion: 0.15
Nodes (11): Anti-Examples: What NOT To Do, Checklist Purpose: "Unit Tests for English", Example Checklist Types & Sample Items, Execution Steps, Post-Execution Checks, Pre-Execution Checks, User Input, [Category 1] (+3 more)

### Community 105 - "Platform Component Overview"
Cohesion: 0.33
Nodes (9): Airflow — оркестрация загрузок, JDBC-драйверы MSSQL/Oracle в образах Airflow и Spark, Nessie — каталог Iceberg, pg-catalog — база метаданных Nessie, pg-meta — база журналов и состояния загрузок, silo — MinIO-совместимое S3-хранилище, Spark — вычисления и загрузка по JDBC, sydafa — сервис управления источниками (+1 more)

### Community 106 - "Шаблонизация кода в generator/generate_dags.py"
Cohesion: 0.33
Nodes (6): Генерация DAG, Загрузка и окружение, Задача Jira, Предпочтительный путь решения, Проверки и критерии готовности, Шаблонизация кода в generator/generate_dags.py

### Community 107 - "spark-worker"
Cohesion: 0.29
Nodes (7): spark-worker, command, depends_on, entrypoint, networks, ports, volumes

### Community 108 - "Ведение документации"
Cohesion: 0.25
Nodes (8): Ведение документации, Когда применять, Ответственность документов, Порядок работы и проверки, Содержание, Термины и глоссарий, Техническая точность, Язык и порядок изложения

### Community 109 - "append.py (raw-layer writer)"
Cohesion: 0.33
Nodes (7): __act_snap auxiliary snapshot, append.py (raw-layer writer), dds / dma layers (planned), IngestActSnapOperator, ods layer (planned), raw layer, source schema evolution handling

### Community 113 - "speckit.plan.md"
Cohesion: 0.22
Nodes (8): Complexity Tracking, Constitution Check, Documentation (this feature), Implementation Plan: [FEATURE], Project Structure, Source Code (repository root), Summary, Technical Context

### Community 114 - "parse_args"
Cohesion: 0.08
Nodes (11): add_jdbc_load_args(), validate_jdbc_load_args(), main(), managed_load(), parse_args(), parse_args(), TestParseArgs, build_argv() (+3 more)

### Community 115 - "by_jdbc_url"
Cohesion: 0.12
Nodes (11): 1. Выбор вида изменения, 2. Подключение новой JDBC-СУБД, 4. Новый слой данных, 5. Порядок разработки, Проверки совместимости, Расширение стенда, Регистрация и окружение, Содержание (+3 more)

### Community 116 - "base.py"
Cohesion: 0.09
Nodes (7): ConnectionSpec, decimal_kind(), fractional_kind(), integer_kind(), normalize_type_name(), OracleDialect, TestRegistry

### Community 117 - "speckit.taskstoissues.md"
Cohesion: 0.33
Nodes (5): Deprecation Notice, Outline, Post-Execution Checks, Pre-Execution Checks, User Input

### Community 118 - "Feature Spec Template"
Cohesion: 0.15
Nodes (12): Assumptions, Edge Cases, Feature Specification: [FEATURE NAME], Functional Requirements, Key Entities *(include if feature involves data)*, Measurable Outcomes, Requirements *(mandatory)*, Success Criteria *(mandatory)* (+4 more)

### Community 119 - "test_operators.py"
Cohesion: 0.08
Nodes (10): fake_make_dsn(), FakeOperator, FakeSkip, load_meta_api(), load_operators(), test_driver_arguments_have_an_explicit_log_display_policy(), test_standard_jdbc_urls_are_visible_for_all_registered_dialects(), TestMetaConnect (+2 more)

### Community 120 - "test.sh"
Cohesion: 0.36
Nodes (8): cleanup(), configure_stack(), finish(), run_post_checks(), run_runtime_checks(), run_stack(), test.sh script, TEST_FAILURE_LOG_DIR

### Community 121 - "Спецификация: упрощение чтения по JDBC и инкрементальной загрузки"
Cohesion: 0.12
Nodes (14): Готовность, Содержание и полнота, Чек-лист качества спецификации, Граничные случаи, Допущения и границы, Ключевые понятия, Критерии успеха, Пользовательские сценарии и проверки (+6 more)

### Community 122 - "Sources Configuration File"
Cohesion: 0.29
Nodes (6): bank, airflow_conn_id, type, broker, airflow_conn_id, type

### Community 123 - "Настройка graphify — графа кодовой базы"
Cohesion: 0.22
Nodes (9): 1. Назначение, 2. Установка утилиты, 3. Переменные окружения, 4. Перестроение графа, 5. Артефакты `graphify-out/` и git, 6. Запросы по графу, Настройка graphify — графа кодовой базы, Результат настройки (+1 more)

### Community 124 - "MetadataLocation"
Cohesion: 0.17
Nodes (4): MetadataLocation, _column_metadata(), TestMetadataLocation, TestColumnMetadata

### Community 125 - "DATARCH-2241: Define and verify minimal supported component versions"
Cohesion: 0.33
Nodes (6): conf/airflow/Dockerfile, conf/airflow/requirements.txt, conf/spark/Dockerfile, ingestion/generator/requirements.txt, E2E test suite (test/test.sh), DATARCH-2241: Define and verify minimal supported component versions

### Community 126 - "7. Инкрементальная загрузка"
Cohesion: 0.27
Nodes (11): 7.2. Жизненный цикл загрузки, 7.3. `incremental_state` — состояние сущности, 7.4. `incremental_batches` — реестр партий, 7.5. Пример одного инкремента и повтора, 7.6. Смена контракта и ручные миграции, 7. Инкрементальная загрузка, meta.finalize_batch(), meta.get_batch() (+3 more)

### Community 127 - "Управление ресурсами Spark на уровне отдельной джобы"
Cohesion: 0.25
Nodes (8): Выполнение и обмен данными, Задача Jira, Контекст, Память executor, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Управление ресурсами Spark на уровне отдельной джобы

### Community 128 - "4. Detection Passes (Token-Efficient Analysis)"
Cohesion: 0.29
Nodes (7): 4. Detection Passes (Token-Efficient Analysis), A. Duplication Detection, B. Ambiguity Detection, C. Underspecification, D. Constitution Alignment, E. Coverage Gaps, F. Inconsistency

### Community 129 - "JdbcOperation"
Cohesion: 0.22
Nodes (3): JdbcOperation, JdbcReadScope, JdbcReason

### Community 130 - "speckit.converge.md"
Cohesion: 0.22
Nodes (8): 7. Append Convergence Tasks (or report converged), 8. Provide Next Actions (Handoff), 9. Check for extension hooks, Convergence Findings, Goal, Operating Constraints, Pre-Execution Checks, User Input

### Community 133 - "7. Практики прохождения цикла"
Cohesion: 0.22
Nodes (8): US1 — загрузка без обязательного журнала (P1), US2 — повторные попытки (P1), Границы, Критерии успеха, Пользовательские сценарии, Содержание, Спецификация: необязательный журнал загрузок, Требования

### Community 134 - "Process ID Generation"
Cohesion: 0.33
Nodes (6): ingestion.airflow_dagruns_log, generate_process_id (Airflow task), GenerateProcessIdOperator, generate_process_id (DAG task), pg-meta metadata database, ingestion.process_id_seq

### Community 135 - "Обработка подвисших процессов Spark при retry задач"
Cohesion: 0.33
Nodes (6): Задача Jira, Обработка подвисших процессов Spark при retry задач, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Реализация и граница решения

### Community 136 - "7.7. DAG-и снимков и таблицы-снимки"
Cohesion: 0.29
Nodes (7): 7.7. DAG-и снимков и таблицы-снимки, Автор и время публикации, Атомарная замена данных, Контроль свежести снимков, Повтор после сбоя, Пустое чтение, Структура таблицы-снимка

### Community 137 - "create-new-feature.sh"
Cohesion: 0.36
Nodes (9): clean_branch_name(), contains_non_ascii(), fit_branch_name(), generate_branch_name(), get_highest_from_specs(), is_feature_number_in_range(), create-new-feature.sh script, spec_prefix_exists() (+1 more)

### Community 138 - "test_sql_logging.py"
Cohesion: 0.12
Nodes (9): restore_sql_blocks(), _format_submit_command(), mask(), build_extract_query(), test_create_table_sql_is_multiline_with_one_column_per_line(), test_execute_logs_template_and_params(), test_extract_query_keeps_quoted_identifiers_intact(), test_pg_meta_commit_summary_keeps_only_changed_fields_at_info() (+1 more)

### Community 139 - "Execution Steps"
Cohesion: 0.29
Nodes (7): 1. Initialize Convergence Context, 2. Load Artifacts (Progressive Disclosure), 3. Build the Intent Inventory, 4. Assess the Codebase and Classify Findings, 5. Assign Severity, 6. Present the In-Session Findings Summary, Execution Steps

### Community 140 - "План реализации DATARCH-2245"
Cohesion: 0.18
Nodes (10): План реализации DATARCH-2245, Подход, Проверка конституции, Содержание, Структура и проверка, Технический контекст, Команды, Приёмка (+2 more)

### Community 142 - "speckit.implement.md"
Cohesion: 0.29
Nodes (6): Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, User Input

### Community 143 - "План: callbacks журнала загрузок"
Cohesion: 0.33
Nodes (5): Конституция, План: callbacks журнала загрузок, Проверки, Решение, Содержание

### Community 144 - "conf/airflow/requirements.txt — Airflow Image Dependencies"
Cohesion: 0.50
Nodes (4): conf/airflow/requirements.txt — Airflow Image Dependencies, apache-airflow-providers-apache-spark, apache-airflow-providers-trino, sydafa-airflow — Airflow Integration Package (SydafaActivateOperator, SydafaMigrateOperator)

### Community 145 - "6. Параллелизм Spark"
Cohesion: 0.22
Nodes (9): 6.1. Порядок выполнения load-задачи, 6. Параллелизм Spark, JDBC-партиции: несколько запросов к источнику, Как выбирается колонка разбиения, Как читать лог параллельной загрузки, Когда разбиение работает, Несколько загрузок и обработка внутри Spark, Ограничение: данные читаются в разные моменты (+1 more)

### Community 147 - "Диагностика шагов загрузки"
Cohesion: 0.40
Nodes (4): Диагностика шагов загрузки, Приёмка, Требования, Цель

### Community 148 - "IncrementalEntityConfig"
Cohesion: 0.14
Nodes (13): Контекст, _entity_block(), load_operator_name(), EntityConfig, FullEntityConfig, IncrementalEntityConfig, _parse_act_snap(), _parse_entity() (+5 more)

### Community 150 - "Архитектура datalake_stand"
Cohesion: 0.25
Nodes (8): 10. Известные ограничения, 1. Слои данных, 9. Типы данных и гарантии, dds / dma (планируется), ods (планируется), raw — данные источника без бизнес-преобразований, Архитектура datalake_stand, Содержание

### Community 151 - "Task Logging Tasks"
Cohesion: 0.67
Nodes (4): ingestion.airflow_tasks_log, log_end (DAG task), log_start (DAG task), TableLogOperator

### Community 154 - "speckit.plan.md"
Cohesion: 0.07
Nodes (29): Скилл pull-request — создание Pull Request, graphify-out/graph.json — граф кодовой базы, Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, User Input (+21 more)

### Community 155 - "Execution Steps"
Cohesion: 0.33
Nodes (6): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 5. Severity Assignment, 6. Produce Compact Analysis Report, Execution Steps

### Community 157 - "Анализ согласованности перед реализацией"
Cohesion: 0.29
Nodes (6): Анализ согласованности перед реализацией, Исправления ревью PR 69, Итог converge, Результат, Содержание, Состояние проверок перед публикацией

### Community 158 - "Задачи DATARCH-2245"
Cohesion: 0.40
Nodes (4): Задачи DATARCH-2245, Приёмка, Реализация, Содержание

### Community 159 - "Проверка требований DATARCH-2245"
Cohesion: 0.50
Nodes (3): Проверка требований DATARCH-2245, Результат, Содержание

### Community 160 - "extending.md"
Cohesion: 0.05
Nodes (37): AGENTS.md, conf/airflow/Dockerfile, conf/spark/Dockerfile, ConnectionSpec, .agents/skills/e2e-testing/SKILL.md, load_sources(), ingestion/operators/ (пакет), ingestion/conf/pipelines/ (+29 more)

### Community 174 - "json"
Cohesion: 0.10
Nodes (6): load_migration_operations(), main(), main(), orphan(), require(), snapshot()

### Community 175 - "2. Манифесты и генерация DAG-ов"
Cohesion: 0.33
Nodes (6): 2. Манифесты и генерация DAG-ов, Манифест пайплайна, Настройки таблицы источника, Общие настройки и демо-набор, Подготовка DAG-ов, Реестр источников

### Community 176 - "Приоритет: можно отложить"
Cohesion: 0.13
Nodes (15): Автоматически формировать теги DAG и исключать дубликаты, Выбирать JDBC-колонку по метрикам с управляемым повторным анализом, Контекст, Контекст, Предпочтительный путь решения, Предпочтительный путь решения, Предпочтительный путь решения, Предпочтительный путь решения (+7 more)

### Community 177 - "Specification Analysis Report"
Cohesion: 0.50
Nodes (4): 7. Provide Next Actions, 8. Offer Remediation, 9. Check for extension hooks, Specification Analysis Report

### Community 178 - "Исследование"
Cohesion: 0.50
Nodes (3): Выводы, Исследование, Содержание

### Community 181 - "3. Структура сгенерированного DAG-а"
Cohesion: 0.40
Nodes (5): 3. Структура сгенерированного DAG-а, Зависимости задач, Идентификаторы и метаданные, Ограничение числа загрузок, Структура DAG-а снимка

### Community 182 - "Convergence Findings"
Cohesion: 0.50
Nodes (4): 7. Append Convergence Tasks (or report converged), 8. Provide Next Actions (Handoff), 9. Check for extension hooks, Convergence Findings

### Community 183 - "5. Устойчивость к эволюции схемы источника"
Cohesion: 0.40
Nodes (5): 5. Устойчивость к эволюции схемы источника, Новые колонки, Отсутствующие колонки, Порядок колонок, Результат изменения схемы

### Community 185 - "8. База метаданных pg-meta: остальные объекты"
Cohesion: 0.40
Nodes (5): 8. База метаданных pg-meta: остальные объекты, Аудит миграций, Журналы событий, Запросы к метаданным, Последовательность идентификаторов

### Community 187 - "Проверка реализации"
Cohesion: 0.25
Nodes (7): Выполнено, Ограничения, Очистка хвоста бэклога, Проверка реализации, Проверка решений ревью от 17.11.2026, Раздельные бюджеты и заключительная приёмка, Содержание

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

### Community 194 - "Task Generation Rules"
Cohesion: 0.40
Nodes (4): Контракт callbacks, Развёртывание, События, Содержание

### Community 195 - "TestResolveFullyQualified"
Cohesion: 0.40
Nodes (4): Команды, Ожидания, Проверка, Содержание

### Community 196 - "Задачи"
Cohesion: 0.40
Nodes (4): Задачи, Реализация, Результаты проверки, Содержание

### Community 198 - "make_batch"
Cohesion: 0.15
Nodes (5): read_window_frame(), make_args(), make_batch(), TestReadWindowFrame, TestWindowFilters

### Community 200 - "Проверка требований"
Cohesion: 0.50
Nodes (3): Проверка требований, Результат, Содержание

### Community 201 - "Модель данных"
Cohesion: 0.50
Nodes (3): Журнал, Модель данных, Содержание

## Ambiguous Edges - Review These
- `tasks-template.md` → `speckit-tasks`  [AMBIGUOUS]
  .specify/templates/tasks-template.md · relation: references

## Knowledge Gaps
- **105 isolated node(s):** `TEST_FAILURE_LOG_DIR`, `conf/spark/spark-defaults.conf`, `pg-catalog — база метаданных Nessie`, `conf/airflow/requirements.txt`, `conf/spark/Dockerfile` (+100 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 1557 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **66 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AGENTS.md — инструкции проекта datalake_stand` connect `README — стенд хранилища и озера данных` to `extending.md`, `Architecture Documentation`, `speckit.plan.md`, `Dev Tooling Documentation`?**
  _High betweenness centrality (0.173) - this node is a cross-community bridge._
- **Are the 29 inferred relationships involving `RdbmsDialect` (e.g. with `Контекст` and `Предпочтительный путь решения`) actually correct?**
  _`RdbmsDialect` has 29 INFERRED edges - model-reasoned connections that need verification._
- **What connects `TEST_FAILURE_LOG_DIR`, `conf/spark/spark-defaults.conf`, `pg-catalog — база метаданных Nessie` to the rest of the system?**
  _105 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `parse_args` be split into smaller, more focused modules?**
  _Cohesion score 0.10960960960960961 - nodes in this community are weakly interconnected._
- **Why does `README — стенд хранилища и озера данных` connect `README — стенд хранилища и озера данных` to `extending.md`, `Architecture Documentation`, `Dev Tooling Documentation`, `conf/airflow/requirements.txt — Airflow Image Dependencies`, `Тестирование datalake_stand`, `Docker Compose Layout`?**
  _High betweenness centrality (0.111) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `generate_all()` (e.g. with `IncrementalEntityConfig` and `SourceConfig`) actually correct?**
  _`generate_all()` has 2 INFERRED edges - model-reasoned connections that need verification._

### Low-confidence Hints
_AMBIGUOUS edges — the extractor was unsure. Verify before acting on these._

- **What is the exact relationship between `tasks-template.md` and `speckit-tasks`?**
  _Edge tagged AMBIGUOUS (relation: references) - confidence is low._