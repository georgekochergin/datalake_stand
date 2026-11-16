# Graph Report - datalake_stand  (2026-11-16)

## Corpus Check
- 162 files · ~145,143 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 12 file(s) not represented in the graph (top: (none) 9, .properties 2, .conf 1)

## Summary
- 2931 nodes · 5262 edges · 179 communities (130 shown, 49 thin omitted)
- Extraction: 95% EXTRACTED · 5% INFERRED · 0% AMBIGUOUS · INFERRED: 241 edges (avg confidence: 0.9)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `f7450907`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- parse_args
- JDBC Partition Planning Tests
- Shared JDBC Reader
- pathlib
- Pipeline Manifest Loading
- manifest.py
- TestGenerateSnapshotDagSource
- jdbc.py
- positions.py
- Batch Registry Tests
- validate_target_table
- run_flow.py
- TestActSnapManifest
- Compose Environment Vars
- Project Documentation
- Project Agent Guides
- generate_dags.py
- Postgres Registry Tests
- generate_all
- process.py
- Incremental Batch Planning
- Demo Walkthrough Docs
- Сценарий демонстрации стенда
- temporal_extraction.py
- Spec Kit Workflow
- 003-module-structure/plan.md
- Spec Kit Cycle Instructions
- speckit-tasks
- FakeCursor
- RDBMS Dialect Adapter
- canonical
- Shell Helper Scripts
- incremental/load_entity.py
- guard.py
- Spec Kit Checklists
- Implementation Plan Template
- TestSparkGuard
- Airflow Env Config
- check_snapshot_recovery
- compatibility_projection
- Nessie Catalog Config
- Architecture Documentation
- managed_load
- Dev Tooling Documentation
- Target Name Resolution Tests
- key_metadata
- Ingest Operator Tests
- Post-Run Verification
- Init Services Compose
- pg-meta Logging Client
- Airflow Services Compose
- Docker Compose Layout
- Приоритет: важно
- Spec Kit Converge Skill
- Bank Pipeline Manifest
- Broker Pipeline Manifest
- Constitution Templates
- Приоритет: можно отложить
- TestAppendRaw
- Spec Kit Tasks Skill
- Code Style Guards
- operators/__init__.py
- read_window_frame
- IngestFullOperator
- Watermark
- DAG Processor Service
- test_dialects.py
- TestGenerateDagSource
- Stand Config Tests
- Spec Kit Analyze Skill
- TestPlanBatch
- Act Snap Plan Docs
- parse_args
- Incremental Manifest Tests
- Analysis Detection Passes
- Named Volumes
- source_position
- 003-module-structure/spec.md
- by_jdbc_url
- source_position
- 4. Detection Passes (Token-Efficient Analysis)
- load_operators
- main
- Postgres Services Compose
- TestGenerateAll
- Test Runner Service
- Спецификация: безопасный повтор Spark-загрузки
- Spec Kit Plan Skill
- Spec Kit Tasks Template
- Project Constitution
- DATARCH-2244: Isolate Spark drivers via DockerOperator
- Silo Service Compose
- asset_uri
- Bank References Manifest
- Broker References Manifest
- check_partitioning
- Registry Transaction Tests
- Act Snap Operator Tests
- pg-meta DDL Tables
- Sydafa Service Compose
- 4. Таблицы raw-слоя: структура и наполнение
- MetadataLocation
- ScriptedResult
- speckit.analyze.md
- Speckit Converge Phase
- Platform Component Overview
- migrations/__init__.py
- Airflowignore Scanner Tests
- Ведение документации
- run_spark_guard
- 002-incremental-config/spec.md
- Spark Job Resources
- depends_on
- Implementation Plan: [FEATURE]
- test_meta_unit.py
- DATARCH-2243: Templatize code in generator/generate_dags.py
- managed_snapshot
- Speckit Checklist Phase
- Feature Spec Template
- SQL Identifier Quoting
- Table Lifecycle Logging
- 6. Параллелизм Spark
- Sources Configuration File
- Настройка graphify — графа кодовой базы
- Глоссарий
- Архитектура datalake_stand
- TestPureHelpers
- План реализации DATARCH-2245
- FakeConnection
- Шаг 5. Повторный раунд: полный срез против инкремента
- TestPgMetaInitSql
- DAG Run Callbacks
- Asset Publication Policy
- Шаг 7. Стресс-тест: эволюция схемы источника
- Process ID Generation
- Environment Build Configuration
- _apply_entity_fields
- bound_literal
- Airflow Connection Initialization
- Temporal Column Rendering
- first_row
- Task Failure Logging
- 2. Манифесты и генерация DAG-ов
- empty_incremental_window
- DAG generator (ingestion/generator/generate_dags.py)
- parse_args
- conf/airflow/requirements.txt — Airflow Image Dependencies
- window_summary
- 7.7. DAG-и снимков и таблицы-снимки
- Обработка подвисших процессов Spark при retry задач
- Шаблонизация кода в generator/generate_dags.py
- Task Logging Tasks
- 5. Устойчивость к эволюции схемы источника
- parse_source_table
- literal
- cli_args
- FakeOperator
- Анализ согласованности перед реализацией
- Задачи DATARCH-2245
- Проверка требований DATARCH-2245
- TestPluginSyntax
- speckit-plan
- Bucket Init Script
- Auth Init Script
- Source Activation Operator
- Asset Event Callables
- Generator Dependencies
- check_spark_guard.py
- Publish Asset Task
- Bootstrap Process
- CAS Revision Control
- StubKindDialect
- 8. База метаданных pg-meta: остальные объекты
- Snapshot table (actual snapshot, __act_snap)
- FakeSkip
- TestBuildRtasSql
- TestMetaConnect
- TestMetaDouble

## God Nodes (most connected - your core abstractions)
1. `RdbmsDialect` - 42 edges
2. `load_pipeline()` - 37 edges
3. `keys()` - 35 edges
4. `generate_all()` - 31 edges
5. `write_temp_yaml()` - 31 edges
6. `resolve_target_table_name()` - 26 edges
7. `TestLoadPipeline` - 24 edges
8. `load_sources()` - 23 edges
9. `TestChoose` - 23 edges
10. `ConnectionSpec` - 22 edges

## Surprising Connections (you probably didn't know these)
- `Память executor` --references--> `EntityConfig`  [INFERRED]
  docs/backlog.md → ingestion/generator/manifest.py
- `Первичное наполнение и проверка источника` --references--> `source_position()`  [INFERRED]
  docs/architecture.md → ingestion/spark/raw/rdbms/incremental/load_entity.py
- `Настройки таблицы источника` --references--> `incremental()`  [INFERRED]
  docs/architecture.md → test/temporal_extraction.py
- `Независимые настройки снимка` --references--> `incremental()`  [INFERRED]
  docs/demo.md → test/temporal_extraction.py
- `Контекст` --references--> `_entity_block()`  [INFERRED]
  docs/backlog.md → ingestion/generator/generate_dags.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Snapshot DAG atomic publication flow** — docs_architecture__ref__ingestion_spark_raw_rdbms_incremental_load_act_snap, ingestion_spark_raw_rdbms_incremental_load_act_snap_build_rtas_sql, docs_architecture_snapshot_table, docs_architecture_publish_asset_event, docs_architecture_snapshot_has_current_process [EXTRACTED 0.85]
- **Spec Kit command pipeline (specify -> clarify -> plan -> tasks -> implement -> converge/analyze)** — zcode_skills_speckit_specify_skill, zcode_skills_speckit_clarify_skill, zcode_skills_speckit_plan_skill, zcode_skills_speckit_tasks_skill, zcode_skills_speckit_implement_skill, zcode_skills_speckit_converge_skill, zcode_skills_speckit_analyze_skill [EXTRACTED 0.85]
- **Маршрутизация процессов агента по скиллам (документация, бэклог, e2e-тесты, PR)** — agents, agents_skills_documentation_skill, agents_skills_backlog_skill, agents_skills_e2e_testing_skill, agents_skills_pull_request_skill [EXTRACTED 0.90]
- **Архитектура стенда dlh-poc: сервисы и хранилища** — readme_sydafa, readme_airflow, readme_spark, readme_trino, readme_nessie, readme_silo, readme_pg_meta, readme_pg_catalog [EXTRACTED 0.90]
- **Incremental batch lifecycle (managed_load flow)** — docs_architecture__ref__ingestion_spark_raw_rdbms_incremental_load_entity_managed_load, docs_architecture__ref__ingestion_meta_registry_get_batch, docs_architecture__ref__ingestion_meta_registry_start_batch, docs_architecture__ref__ingestion_meta_registry_finalize_batch, docs_architecture_incremental_batches, docs_architecture_incremental_state [EXTRACTED 0.90]
- **Ingest DAG per-entity task flow (generate_process_id >> activate_source >> log_start >> load >> log_end >> publish_asset)** — docs_architecture_generate_process_id_task, docs_architecture_activate_source_task, docs_architecture_log_start_task, docs_architecture_load_task, docs_architecture_log_end_task, docs_architecture_publish_asset_task [EXTRACTED 0.90]
- **pg-meta ingestion schema objects (run logs, entity state, batch registry)** — docs_architecture_pg_meta, docs_architecture_airflow_dagruns_log, docs_architecture_airflow_tasks_log, docs_architecture_incremental_state, docs_architecture_incremental_batches, docs_architecture_generate_process_id_task [EXTRACTED 0.90]
- **pg-meta metadata schema objects** — docs_architecture_pg_meta, docs_architecture_incremental_state, docs_architecture_incremental_batches, docs_architecture_incremental_state_migrations, docs_architecture_airflow_dagruns_log, docs_architecture_airflow_tasks_log, docs_architecture_process_id_seq [EXTRACTED 0.90]
- **Спек-цикл Spec Kit: constitution → specify → clarify → plan → tasks → analyze → implement → converge** — opencode_commands_speckit_constitution, opencode_commands_speckit_specify, opencode_commands_speckit_clarify, opencode_commands_speckit_plan, opencode_commands_speckit_tasks, opencode_commands_speckit_analyze, opencode_commands_speckit_implement, opencode_commands_speckit_converge [EXTRACTED 0.95]
- **Спек-цикл разработки Spec Kit + graphify** — docs_agents_speckit, docs_agents_speckit_specify_cli, docs_agents_graphify, specs_001_act_snap_contract_spec, specs_002_incremental_config_spec, specs_003_module_structure_spec [INFERRED 0.80]
- **Механизм таблиц-снимков act_snap** — specs_001_act_snap_contract_spec, specs_001_act_snap_contract_spec_act_snap_config, specs_001_act_snap_contract_spec_ingest_act_snap_operator, specs_001_act_snap_contract_spec_publish_asset, docs_demo_load_act_snap, docs_glossary_snapshot, docs_glossary_rtas [INFERRED 0.85]
- **Концепция инкрементальной загрузки (контракт инкремента)** — docs_glossary_increment, docs_glossary_position, docs_glossary_canonical, docs_architecture_watermark, docs_glossary_window, docs_architecture_lookback, docs_glossary_batch, docs_glossary_attempt, docs_glossary_portion, specs_002_incremental_config_spec [INFERRED 0.90]

## Communities (179 total, 49 thin omitted)

### Community 0 - "parse_args"
Cohesion: 0.16
Nodes (5): main(), parse_args(), build_argv(), TestMain, TestParseArgs

### Community 1 - "JDBC Partition Planning Tests"
Cohesion: 0.08
Nodes (10): day_expression(), no_candidate_reason(), partition_name(), PartitionChoice, plan(), keys(), TestChoose, TestDayExpression (+2 more)

### Community 2 - "Shared JDBC Reader"
Cohesion: 0.08
Nodes (8): build_extract_query(), jdbc_reader(), read_extract_frame(), make_args(), RecordingReader, TestJdbcReader, TestReadBounds, TestReadExtractFrame

### Community 4 - "Pipeline Manifest Loading"
Cohesion: 0.06
Nodes (12): _expand_cron_field(), load_pipeline(), load_sources(), parse_schedule(), temporal_lookback_default(), assert_unique_targets(), TestGenerateAllValidation, TestLoadPipeline (+4 more)

### Community 5 - "manifest.py"
Cohesion: 0.08
Nodes (24): Генерация и настройка, Границы рефакторинга, Контекст, Предпочтительный путь решения, ActSnapConfig, AirflowConfig, DagConfig, EntityConfig (+16 more)

### Community 7 - "jdbc.py"
Cohesion: 0.11
Nodes (8): __getattr__(), decimal_kind(), fractional_kind(), integer_kind(), normalize_type_name(), _column_metadata(), _exact_pattern(), _optional_int()

### Community 8 - "positions.py"
Cohesion: 0.12
Nodes (6): validate(), _format(), kind_info(), _parse(), resolve_lookback(), TestKindInfo

### Community 9 - "Batch Registry Tests"
Cohesion: 0.13
Nodes (4): finalize_batch(), start_batch(), TestBatchLifecycle, TestPlanBatch

### Community 10 - "validate_target_table"
Cohesion: 0.07
Nodes (17): __act_snap auxiliary snapshot, append.py (raw-layer writer), dds / dma layers (planned), IngestActSnapOperator, ods layer (planned), raw layer, source schema evolution handling, build_create_table_sql() (+9 more)

### Community 11 - "run_flow.py"
Cohesion: 0.06
Nodes (43): airflow_check_asset_publication(), airflow_task_log(), airflow_task_states(), airflow_token(), airflow_token_retry(), airflow_trigger(), airflow_trigger_retry(), airflow_unpause() (+35 more)

### Community 13 - "Compose Environment Vars"
Cohesion: 0.08
Nodes (36): environment, healthcheck, image, networks, volumes, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, POSTGRES_DB (+28 more)

### Community 14 - "Project Documentation"
Cohesion: 0.08
Nodes (25): 1. Выбор вида изменения, 4. Новый слой данных, 5. Порядок разработки, AGENTS.md, conf/airflow/Dockerfile, conf/spark/Dockerfile, ConnectionSpec, .agents/skills/e2e-testing/SKILL.md (+17 more)

### Community 15 - "Project Agent Guides"
Cohesion: 0.11
Nodes (25): AGENTS.md — инструкции проекта datalake_stand, Скилл backlog — ведение docs/backlog.md, Скилл e2e-testing — сквозное тестирование стенда, Скилл pull-request — создание Pull Request, пакет sydafa-airflow (SydafaActivateOperator), airflow/plugins/migrations/ — миграции источников bank/broker, conf/airflow/Dockerfile, conf/pg-meta/init/ — DDL pg-meta (+17 more)

### Community 16 - "generate_dags.py"
Cohesion: 0.07
Nodes (12): _entity_block(), load_operator_name(), _snapshot_entity_block(), resolve_fully_qualified(), resolve_namespace(), resolve_snapshot_fully_qualified(), resolve_snapshot_table_name(), snapshot_alias() (+4 more)

### Community 17 - "Postgres Registry Tests"
Cohesion: 0.09
Nodes (3): 2. Установка утилиты, sql(), PostgresTests

### Community 18 - "generate_all"
Cohesion: 0.11
Nodes (10): Предпочтительный путь решения, dag_id_for(), file_name_for(), generate_all(), snapshot_dag_id(), IngestionSettings, load_settings(), _read_yaml() (+2 more)

### Community 19 - "process.py"
Cohesion: 0.11
Nodes (9): _dagrun_event(), dagrun_failure_callback(), dagrun_success_callback(), log_task_failed(), _meta_connect(), _pull_process_id(), _validate_process_id(), resolve_source_connection() (+1 more)

### Community 20 - "Incremental Batch Planning"
Cohesion: 0.11
Nodes (10): _row(), _batch_reason(), _check_batch(), _check_contract(), fail_batch(), _IncrementalConfig, _lock_batch(), _lock_state() (+2 more)

### Community 21 - "Demo Walkthrough Docs"
Cohesion: 0.08
Nodes (30): Apache Airflow, Задача generate_process_id, Apache Iceberg, Таблица ingestion.incremental_batches, DAG ingest__rdbms__bank__main, ingest__rdbms__bank__main (generated DAG), load_act_snap.py (Spark-приложение снимка), pg-meta (реестр загрузок) (+22 more)

### Community 22 - "Сценарий демонстрации стенда"
Cohesion: 0.15
Nodes (13): Независимые настройки снимка, Ограничение числа загрузок, Проверка параллельного чтения, Событие готовности снимка, Содержание, Сценарий демонстрации стенда, Шаг 1. Обновление источников (Airflow UI), Шаг 2. Источники через DBeaver (активация + просмотр) (+5 more)

### Community 23 - "temporal_extraction.py"
Cohesion: 0.16
Nodes (9): check_post_append_retry(), exercise(), load_module(), main(), metadata_row(), precision_fixtures(), render_source_reference(), source_count() (+1 more)

### Community 24 - "Spec Kit Workflow"
Cohesion: 0.06
Nodes (30): args, inputs, integration, spec, default, prompt, type, any (+22 more)

### Community 25 - "003-module-structure/plan.md"
Cohesion: 0.15
Nodes (13): ACT_SNAPSHOT_TABLES, Дополнение: пункты 3–5 плана от 2026-11-08, Исправления по третьему ревью, Итоговая проверка, Проверка уточнений после ревью, 001-act-snap-contract: задачи, README.md, Завершение пунктов 3–5 внешнего плана (+5 more)

### Community 26 - "Spec Kit Cycle Instructions"
Cohesion: 0.07
Nodes (29): 10. Обновление Spec Kit, 11. Взаимодействие с graphify и Pull Request, 12. Работа в ZCode Desktop (интерактивно), 1. Назначение, 2. Установка для сопровождающего (выполнена один раз), 3. Инструкция для разработчика, 4. Состояние цикла: каталоги, машины, ветки, 5. Состав артефактов (+21 more)

### Community 27 - "speckit-tasks"
Cohesion: 0.07
Nodes (33): contracts/ (interface contracts), plan.md (implementation plan), spec.md (feature specification), Модель состояния guard, Содержание, Состояния, Исследование, Источники и ограничения (+25 more)

### Community 29 - "RDBMS Dialect Adapter"
Cohesion: 0.06
Nodes (7): 2. Подключение новой JDBC-СУБД, Контракт диалекта, Проверки совместимости, RdbmsDialect, MssqlDialect, OracleDialect, TestBaseDialectContract

### Community 30 - "canonical"
Cohesion: 0.14
Nodes (6): canonical(), maximum(), window_bounds(), _plan_batch(), calculate_lower(), TestPositions

### Community 31 - "Shell Helper Scripts"
Cohesion: 0.12
Nodes (30): check-prerequisites.sh script, check_dir(), check_file(), find_specify_root(), format_speckit_command(), get_current_branch(), get_feature_paths(), get_invoke_separator() (+22 more)

### Community 32 - "incremental/load_entity.py"
Cohesion: 0.09
Nodes (11): IngestFullOperator, IngestIncrementalOperator, Airflow pool ingest_loads, load (DAG task), reader.py (shared JDBC reader), Backlog: Remove JDBC password from spark-submit arguments, _family(), _first_candidate() (+3 more)

### Community 33 - "guard.py"
Cohesion: 0.16
Nodes (8): _all_applications_completed(), _applications(), _get_master_state(), guard_spark_submit(), _kill_application(), _remaining(), _request(), SparkGuardError

### Community 34 - "Spec Kit Checklists"
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

### Community 38 - "check_snapshot_recovery"
Cohesion: 0.20
Nodes (9): check_snapshot_recovery(), allocate(), incremental(), projection(), registry(), snapshot(), register_virtual(), virtual_sources() (+1 more)

### Community 40 - "Nessie Catalog Config"
Cohesion: 0.09
Nodes (23): pg-catalog, nessie.catalog.default-warehouse, nessie.catalog.secrets.access-key.name, nessie.catalog.secrets.access-key.secret, nessie.catalog.service.s3.default-options.access-key, nessie.catalog.service.s3.default-options.auth-type, nessie.catalog.service.s3.default-options.endpoint, nessie.catalog.service.s3.default-options.path-style-access (+15 more)

### Community 41 - "Architecture Documentation"
Cohesion: 0.11
Nodes (21): pg-meta DDL (conf/pg-meta/init/01_ingestion.sql), Glossary (glossary.md), MSSQL dialect (ingestion/sources/rdbms/dialects/mssql.py), Oracle dialect (ingestion/sources/rdbms/dialects/oracle.py), Iceberg writer layer (ingestion/spark/raw/iceberg.py), Full-slice loader (ingestion/spark/raw/rdbms/full/load_entity.py), Incremental loader (ingestion/spark/raw/rdbms/incremental/load_entity.py), Incremental window reader (incremental/window.py) (+13 more)

### Community 43 - "Dev Tooling Documentation"
Cohesion: 0.13
Nodes (17): AGENTS.md, graphify-out/graph.json, graphify (CLI-утилита), AGENTS.md, .specify/feature.json (локальный указатель фичи), opencode, .opencode/commands/, specify CLI (GitHub Spec Kit) (+9 more)

### Community 44 - "Target Name Resolution Tests"
Cohesion: 0.17
Nodes (3): resolve_target_table_name(), _split_fragments(), TestResolveTargetTableName

### Community 45 - "key_metadata"
Cohesion: 0.17
Nodes (7): _degraded_metadata(), key_metadata(), KeyMetadata, fake_jvm(), TestCatalogAddressing, TestDegradedKeyMetadata, TestKeyMetadata

### Community 47 - "Post-Run Verification"
Cohesion: 0.17
Nodes (4): check_snapshot_publication(), main(), records(), require()

### Community 48 - "Init Services Compose"
Cohesion: 0.11
Nodes (19): nessie, silo-init, trino-init, condition, spark-master, trino, condition, command (+11 more)

### Community 49 - "pg-meta Logging Client"
Cohesion: 0.16
Nodes (6): allocate_process_id(), connection(), find_process_id(), log_dagrun(), log_task(), TestProcessAndEvents

### Community 50 - "Airflow Services Compose"
Cohesion: 0.11
Nodes (18): depends_on, condition, command, depends_on, image, networks, pull_policy, volumes (+10 more)

### Community 51 - "Docker Compose Layout"
Cohesion: 0.12
Nodes (16): name, networks, dlh-poc, services, spark-worker, trino-init, command, depends_on (+8 more)

### Community 52 - "Приоритет: важно"
Cohesion: 0.15
Nodes (13): Задача Jira, Задача Jira, Задача Jira, Изолировать Spark drivers через DockerOperator, Контекст, Контекст, Определить и проверить минимальные поддерживаемые версии компонентов, Предпочтительный путь решения (+5 more)

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

### Community 57 - "Приоритет: можно отложить"
Cohesion: 0.12
Nodes (17): Автоматически формировать теги DAG и исключать дубликаты, Бэклог, Выбирать JDBC-колонку по метрикам с управляемым повторным анализом, Контекст, Контекст, Оглавление, Предпочтительный путь решения, Предпочтительный путь решения (+9 more)

### Community 58 - "TestAppendRaw"
Cohesion: 0.05
Nodes (12): Задача Jira, Проблема, Проверки и критерии готовности, Рефакторинг и переорганизация кода: структура модулей, append_raw(), schema_columns(), main(), fake_pyspark_types() (+4 more)

### Community 59 - "Spec Kit Tasks Skill"
Cohesion: 0.12
Nodes (15): tasks.md (task list), $speckit-github-taskstoissues (github extension), speckit-checklist, Anti-Examples: What NOT To Do, Checklist Purpose: "Unit Tests for English", Example Checklist Types & Sample Items, Execution Steps, Post-Execution Checks (+7 more)

### Community 60 - "Code Style Guards"
Cohesion: 0.17
Nodes (3): TestNoHashComments, manipulates_sys_path(), TestNoSysPathManipulation

### Community 61 - "operators/__init__.py"
Cohesion: 0.16
Nodes (6): Назначение задач, Событие готовности Asset, publish_asset_event(), snapshot_has_current_process(), GenerateProcessIdOperator, TableLogOperator

### Community 62 - "read_window_frame"
Cohesion: 0.11
Nodes (8): position_select(), read_window_frame(), window_filters(), make_args(), make_batch(), TestPositionSelect, TestReadWindowFrame, TestWindowFilters

### Community 63 - "IngestFullOperator"
Cohesion: 0.15
Nodes (13): Жизненный цикл, Контекст, Контекст, Контракт оператора, Образ и зависимости, Предпочтительный путь решения, Проверки и критерии готовности, Риски и границы решения (+5 more)

### Community 64 - "Watermark"
Cohesion: 0.22
Nodes (11): Batch registry (ingestion/meta/registry.py), Incremental batch, Bootstrap (initial fill), ingestion.incremental_batches, incremental contract (canonical position type), ingestion.incremental_state, ingestion.incremental_state_migrations, Lookback (+3 more)

### Community 65 - "DAG Processor Service"
Cohesion: 0.20
Nodes (14): build, command, image, networks, pull_policy, volumes, build, build (+6 more)

### Community 66 - "test_dialects.py"
Cohesion: 0.07
Nodes (8): ConnectionSpec, all_dialects(), TestColumnProjection, TestConnections, TestContractsReExport, TestDayExpression, TestOracleIntegerKinds, TestPositionKind

### Community 69 - "Spec Kit Analyze Skill"
Cohesion: 0.14
Nodes (14): speckit-analyze, 7. Provide Next Actions, 8. Offer Remediation, 9. Check for extension hooks, .specify/extensions.yml (extension hooks registry), Analysis Guidelines, Context, Context Efficiency (+6 more)

### Community 71 - "Act Snap Plan Docs"
Cohesion: 0.10
Nodes (18): Asset (Airflow), ActSnapConfig (dataclass), DagConfig (базовый dataclass), MAX_ACTIVE_RUNS (константа генератора), Дополнительное покрытие пунктов 3–5, Проверка согласованности, PipelineManifest, Завершение проверки контракта (+10 more)

### Community 72 - "parse_args"
Cohesion: 0.13
Nodes (7): main(), managed_load(), parse_args(), build_argv(), TestMain, TestManagedLoad, TestParseArgs

### Community 74 - "Analysis Detection Passes"
Cohesion: 0.15
Nodes (13): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 4. Detection Passes (Token-Efficient Analysis), 5. Severity Assignment, 6. Produce Compact Analysis Report, A. Duplication Detection, B. Ambiguity Detection (+5 more)

### Community 75 - "Named Volumes"
Cohesion: 0.17
Nodes (12): name, name, volumes, airflow_logs, airflow_pg_data, pg_catalog_data, pg_meta_data, silo_data (+4 more)

### Community 76 - "source_position"
Cohesion: 0.11
Nodes (23): 7.1. Контракт, 7.2. Жизненный цикл загрузки, 7.3. `incremental_state` — состояние сущности, 7.4. `incremental_batches` — реестр партий, 7.5. Пример одного инкремента и повтора, 7.6. Смена контракта и ручные миграции, 7. Инкрементальная загрузка, meta.finalize_batch() (+15 more)

### Community 77 - "003-module-structure/spec.md"
Cohesion: 0.15
Nodes (12): Готовность фичи, Заметки, Полнота требований, Чек-лист качества спецификации: структура модулей загрузчика, FullEntityConfig, IncrementalEntityConfig, IngestAppendOperator (удаляется), IngestFullOperator (+4 more)

### Community 78 - "by_jdbc_url"
Cohesion: 0.13
Nodes (8): Регистрация и окружение, by_conn_type(), by_jdbc_url(), supported_conn_types(), supported_url_prefixes(), render_source_table(), TestRegistry, TestRenderSourceTable

### Community 79 - "source_position"
Cohesion: 0.20
Nodes (3): connection(), source_position(), TestSourcePosition

### Community 80 - "4. Detection Passes (Token-Efficient Analysis)"
Cohesion: 0.15
Nodes (13): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 4. Detection Passes (Token-Efficient Analysis), 5. Severity Assignment, 6. Produce Compact Analysis Report, A. Duplication Detection, B. Ambiguity Detection (+5 more)

### Community 81 - "load_operators"
Cohesion: 0.14
Nodes (4): fake_make_dsn(), load_meta_api(), load_operators(), TestGuardOperatorWiring

### Community 82 - "main"
Cohesion: 0.20
Nodes (3): compute_drift(), main(), TestCheckMode

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

### Community 88 - "Spec Kit Tasks Template"
Cohesion: 0.08
Nodes (21): Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, User Input, Checklist Format (REQUIRED), Completion Report (+13 more)

### Community 89 - "Project Constitution"
Cohesion: 0.18
Nodes (11): I. Граф кодовой базы прежде чтения файлов, II. Изменения подтверждаются тестами, III. Чистый Python без комментариев, IV. Дисциплина Git: ветка → Pull Request → мерж пользователем, V. Перестроение графа после каждого коммита, VI. Документация на русском языке, Базовые принципы, Конституция проекта datalake_stand (+3 more)

### Community 90 - "DATARCH-2244: Isolate Spark drivers via DockerOperator"
Cohesion: 0.27
Nodes (12): conf/spark/spark-defaults.conf, Global settings (ingestion/conf/settings.yaml), Sources registry (ingestion/conf/sources.yaml), load_sources(), Load operators module (ingestion/operators.py), DATARCH-2242: Split sources registry into YAML files, per-source JDBC parallelism, DATARCH-2244: Isolate Spark drivers via DockerOperator, DATARCH-2245: Handle hung Spark processes on task retry (+4 more)

### Community 91 - "Silo Service Compose"
Cohesion: 0.20
Nodes (10): silo, AWS_DEFAULT_REGION, silo-init, condition, depends_on, entrypoint, environment, image (+2 more)

### Community 92 - "asset_uri"
Cohesion: 0.27
Nodes (3): asset_uri(), snapshot_asset_uri(), TestAssetUri

### Community 93 - "Bank References Manifest"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 94 - "Broker References Manifest"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 95 - "check_partitioning"
Cohesion: 0.14
Nodes (9): column_position_kind(), ColumnMeta, choose(), source_kind(), check_partitioned_load_path(), check_partitioning(), derived_day_frame(), jdbc_count() (+1 more)

### Community 98 - "pg-meta DDL Tables"
Cohesion: 0.33
Nodes (8): airflow_dagruns_log_process_id_idx, airflow_dagruns_log_run_idx, airflow_tasks_log_run_idx, ingestion.airflow_dagruns_log, ingestion.airflow_tasks_log, ingestion.incremental_batches, ingestion.incremental_state, ingestion.incremental_state_migrations

### Community 99 - "Sydafa Service Compose"
Cohesion: 0.22
Nodes (9): start_period, sydafa, environment, healthcheck, image, networks, platform, ports (+1 more)

### Community 100 - "4. Таблицы raw-слоя: структура и наполнение"
Cohesion: 0.22
Nodes (9): 4. Таблицы raw-слоя: структура и наполнение, Гарантии записи, Запросы к raw в Trino, Имена и колонки, Как исключить повторные попытки из выборки, Как различать партии и попытки, Ограничение: последняя попытка прочитала пустое окно, Полный срез и инкремент (+1 more)

### Community 101 - "MetadataLocation"
Cohesion: 0.15
Nodes (4): MetadataLocation, TestMetadataLocation, CatalogDialect, TestColumnMetadata

### Community 102 - "ScriptedResult"
Cohesion: 0.07
Nodes (5): _primary_key(), _unique_single_columns(), ScriptedResult, TestCatalogPatternFiltering, TestKeyStructure

### Community 103 - "speckit.analyze.md"
Cohesion: 0.14
Nodes (12): 7. Provide Next Actions, 8. Offer Remediation, 9. Check for extension hooks, Analysis Guidelines, Context, Context Efficiency, Goal, Operating Constraints (+4 more)

### Community 104 - "Speckit Converge Phase"
Cohesion: 0.12
Nodes (15): 1. Initialize Convergence Context, 2. Load Artifacts (Progressive Disclosure), 3. Build the Intent Inventory, 4. Assess the Codebase and Classify Findings, 5. Assign Severity, 6. Present the In-Session Findings Summary, 7. Append Convergence Tasks (or report converged), 8. Provide Next Actions (Handoff) (+7 more)

### Community 105 - "Platform Component Overview"
Cohesion: 0.33
Nodes (9): Airflow — оркестрация загрузок, JDBC-драйверы MSSQL/Oracle в образах Airflow и Spark, Nessie — каталог Iceberg, pg-catalog — база метаданных Nessie, pg-meta — база журналов и состояния загрузок, silo — MinIO-совместимое S3-хранилище, Spark — вычисления и загрузка по JDBC, sydafa — сервис управления источниками (+1 more)

### Community 108 - "Ведение документации"
Cohesion: 0.25
Nodes (8): Ведение документации, Когда применять, Ответственность документов, Порядок работы и проверки, Содержание, Термины и глоссарий, Техническая точность, Язык и порядок изложения

### Community 109 - "run_spark_guard"
Cohesion: 0.29
Nodes (3): Контекст, _pgmeta_dsn(), run_spark_guard()

### Community 110 - "002-incremental-config/spec.md"
Cohesion: 0.29
Nodes (6): Инкремент (incremental), CLI-флаг --incremental-config, incremental.contract (YAML: source_column, position_type), Контракт инкрементальной загрузки, Требования, Уточнения

### Community 111 - "Spark Job Resources"
Cohesion: 0.25
Nodes (8): Выполнение и обмен данными, Задача Jira, Контекст, Память executor, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Управление ресурсами Spark на уровне отдельной джобы

### Community 112 - "depends_on"
Cohesion: 0.18
Nodes (11): condition, airflow-webserver, spark-master, spark-worker, sydafa, trino, condition, condition (+3 more)

### Community 113 - "Implementation Plan: [FEATURE]"
Cohesion: 0.22
Nodes (8): Complexity Tracking, Constitution Check, Documentation (this feature), Implementation Plan: [FEATURE], Project Structure, Source Code (repository root), Summary, Technical Context

### Community 114 - "test_meta_unit.py"
Cohesion: 0.15
Nodes (4): get_batch(), BatchTestCase, FakePg, TestGetBatch

### Community 115 - "DATARCH-2243: Templatize code in generator/generate_dags.py"
Cohesion: 0.40
Nodes (10): generate_dag_source(), generate_snapshot_dag_source(), build_create_table_sql(), build_rtas_sql(), build_extract_query(), read_bounds(), DATARCH-2243: Templatize code in generator/generate_dags.py, SQL-файлы и шаблоны (+2 more)

### Community 116 - "managed_snapshot"
Cohesion: 0.16
Nodes (5): current_snapshot_author(), managed_snapshot(), FakeCatalog, TestCurrentSnapshotAuthor, TestManagedSnapshot

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
Cohesion: 0.15
Nodes (13): 10. Известные ограничения, 1. Слои данных, 3. Структура сгенерированного DAG-а, 9. Типы данных и гарантии, dds / dma (планируется), ods (планируется), raw — данные источника без бизнес-преобразований, Архитектура datalake_stand (+5 more)

### Community 127 - "План реализации DATARCH-2245"
Cohesion: 0.18
Nodes (10): План реализации DATARCH-2245, Подход, Проверка конституции, Содержание, Структура и проверка, Технический контекст, Команды, Приёмка (+2 more)

### Community 129 - "Шаг 5. Повторный раунд: полный срез против инкремента"
Cohesion: 0.29
Nodes (7): 5.1. Подготовка показа pg-meta, 5.2. Новый запуск без изменения источника, 5.3. Clear только успешной load-задачи, 5.4. Полный Clear существующего запуска, 5.5. Новая загрузка после обновления источника, 5.6. Что объяснить про повтор после ошибки, Шаг 5. Повторный раунд: полный срез против инкремента

### Community 133 - "Шаг 7. Стресс-тест: эволюция схемы источника"
Cohesion: 0.29
Nodes (7): 7.1. Удаление колонки, 7.2. Переименование колонки, 7.3. Добавление колонки, 7.4. Настройка генераторов данных, 7.5. Проверка схемы источника, 7.6. Загрузка после изменения схемы, Шаг 7. Стресс-тест: эволюция схемы источника

### Community 134 - "Process ID Generation"
Cohesion: 0.33
Nodes (6): ingestion.airflow_dagruns_log, generate_process_id (Airflow task), GenerateProcessIdOperator, generate_process_id (DAG task), pg-meta metadata database, ingestion.process_id_seq

### Community 135 - "Environment Build Configuration"
Cohesion: 0.33
Nodes (6): conf/airflow/Dockerfile, conf/airflow/requirements.txt, conf/spark/Dockerfile, ingestion/generator/requirements.txt, E2E test suite (test/test.sh), DATARCH-2241: Define and verify minimal supported component versions

### Community 136 - "_apply_entity_fields"
Cohesion: 0.28
Nodes (3): _apply_entity_fields(), log_spark_task_event(), _strict_submit_options()

### Community 140 - "first_row"
Cohesion: 0.29
Nodes (3): first_row(), _row_size(), TestFirstRow

### Community 142 - "2. Манифесты и генерация DAG-ов"
Cohesion: 0.33
Nodes (6): 2. Манифесты и генерация DAG-ов, Манифест пайплайна, Настройки таблицы источника, Общие настройки и демо-набор, Подготовка DAG-ов, Реестр источников

### Community 144 - "DAG generator (ingestion/generator/generate_dags.py)"
Cohesion: 0.40
Nodes (5): Bank pipeline manifest (ingestion/conf/pipelines/bank/main.yaml), DAG generator (ingestion/generator/generate_dags.py), Manifest parser (ingestion/generator/manifest.py), Backlog: Auto-generate DAG tags and drop duplicates, Backlog: Split Airflow pools for loads vs service tasks

### Community 146 - "conf/airflow/requirements.txt — Airflow Image Dependencies"
Cohesion: 0.50
Nodes (4): conf/airflow/requirements.txt — Airflow Image Dependencies, apache-airflow-providers-apache-spark, apache-airflow-providers-trino, sydafa-airflow — Airflow Integration Package (SydafaActivateOperator, SydafaMigrateOperator)

### Community 148 - "7.7. DAG-и снимков и таблицы-снимки"
Cohesion: 0.29
Nodes (7): 7.7. DAG-и снимков и таблицы-снимки, Автор и время публикации, Атомарная замена данных, Контроль свежести снимков, Повтор после сбоя, Пустое чтение, Структура таблицы-снимка

### Community 149 - "Обработка подвисших процессов Spark при retry задач"
Cohesion: 0.33
Nodes (6): Задача Jira, Обработка подвисших процессов Spark при retry задач, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Реализация и граница решения

### Community 150 - "Шаблонизация кода в generator/generate_dags.py"
Cohesion: 0.29
Nodes (7): Генерация DAG, Загрузка и окружение, Задача Jira, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Шаблонизация кода в generator/generate_dags.py

### Community 151 - "Task Logging Tasks"
Cohesion: 0.67
Nodes (4): ingestion.airflow_tasks_log, log_end (DAG task), log_start (DAG task), TableLogOperator

### Community 152 - "5. Устойчивость к эволюции схемы источника"
Cohesion: 0.40
Nodes (5): 5. Устойчивость к эволюции схемы источника, Новые колонки, Отсутствующие колонки, Порядок колонок, Результат изменения схемы

### Community 157 - "Анализ согласованности перед реализацией"
Cohesion: 0.40
Nodes (4): Анализ согласованности перед реализацией, Результат, Содержание, Состояние проверок перед публикацией

### Community 158 - "Задачи DATARCH-2245"
Cohesion: 0.40
Nodes (4): Задачи DATARCH-2245, Приёмка, Реализация, Содержание

### Community 159 - "Проверка требований DATARCH-2245"
Cohesion: 0.50
Nodes (3): Проверка требований DATARCH-2245, Результат, Содержание

### Community 161 - "speckit-plan"
Cohesion: 0.18
Nodes (11): speckit-plan, Completion Report, Done When, Key rules, Mandatory Post-Execution Hooks, Outline, Phase 0: Outline & Research, Phase 1: Design & Contracts (+3 more)

### Community 168 - "check_spark_guard.py"
Cohesion: 0.13
Nodes (5): _master_url(), main(), orphan(), require(), snapshot()

### Community 173 - "8. База метаданных pg-meta: остальные объекты"
Cohesion: 0.18
Nodes (10): 8. База метаданных pg-meta: остальные объекты, Аудит миграций, Журналы событий, Запросы к метаданным, Последовательность идентификаторов, Запуск и подключения, Исходное состояние, Подготовка (+2 more)

### Community 174 - "Snapshot table (actual snapshot, __act_snap)"
Cohesion: 0.67
Nodes (3): Snapshot stream (incremental/load_act_snap.py), Increment (incremental column contract), Snapshot table (actual snapshot, __act_snap)

## Ambiguous Edges - Review These
- `tasks-template.md` → `speckit-tasks`  [AMBIGUOUS]
  .specify/templates/tasks-template.md · relation: references

## Knowledge Gaps
- **102 isolated node(s):** `SYDAFA_CONTAINER_PREFIX`, `dds / dma layers (planned)`, `Airflow pool ingest_loads`, `pg-catalog — база метаданных Nessie`, `CLI-флаг --incremental-config` (+97 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 1379 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **49 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `services` connect `Docker Compose Layout` to `DAG Processor Service`, `Sydafa Service Compose`, `Nessie Catalog Config`, `Compose Environment Vars`, `Init Services Compose`, `Airflow Services Compose`, `Postgres Services Compose`, `Test Runner Service`, `Silo Service Compose`?**
  _High betweenness centrality (0.129) - this node is a cross-community bridge._
- **Are the 16 inferred relationships involving `RdbmsDialect` (e.g. with `Контекст` and `Предпочтительный путь решения`) actually correct?**
  _`RdbmsDialect` has 16 INFERRED edges - model-reasoned connections that need verification._
- **What connects `SYDAFA_CONTAINER_PREFIX`, `dds / dma layers (planned)`, `Airflow pool ingest_loads` to the rest of the system?**
  _102 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `JDBC Partition Planning Tests` be split into smaller, more focused modules?**
  _Cohesion score 0.08106219426974144 - nodes in this community are weakly interconnected._
- **Why does `README — стенд хранилища и озера данных` connect `Project Agent Guides` to `Architecture Documentation`, `Dev Tooling Documentation`, `Project Documentation`, `conf/airflow/requirements.txt — Airflow Image Dependencies`, `Docker Compose Layout`, `Demo Walkthrough Docs`?**
  _High betweenness centrality (0.116) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `generate_all()` (e.g. with `IncrementalEntityConfig` and `SourceConfig`) actually correct?**
  _`generate_all()` has 2 INFERRED edges - model-reasoned connections that need verification._

### Low-confidence Hints
_AMBIGUOUS edges — the extractor was unsure. Verify before acting on these._

- **What is the exact relationship between `tasks-template.md` and `speckit-tasks`?**
  _Edge tagged AMBIGUOUS (relation: references) - confidence is low._