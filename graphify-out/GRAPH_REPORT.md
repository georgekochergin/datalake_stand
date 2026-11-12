# Graph Report - datalake_stand  (2026-11-12)

## Corpus Check
- 149 files · ~132,420 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 12 file(s) not represented in the graph (top: (none) 9, .properties 2, .conf 1)

## Summary
- 2740 nodes · 4858 edges · 166 communities (123 shown, 43 thin omitted)
- Extraction: 95% EXTRACTED · 5% INFERRED · 0% AMBIGUOUS · INFERRED: 233 edges (avg confidence: 0.9)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `d668d1c6`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- parse_args
- JDBC Partition Planning Tests
- Shared JDBC Reader
- pathlib
- Pipeline Manifest Loading
- TestPlanBatch
- TestGenerateSnapshotDagSource
- managed_snapshot
- Приоритет: можно отложить
- Batch Registry Tests
- base.py
- Airflow API Flow
- Connection Specs Docs
- Compose Environment Vars
- Project Documentation
- Project Agent Guides
- Table Naming Resolution
- Postgres Registry Tests
- generate_all
- Airflow Operators Package
- Incremental Batch Planning
- Demo Walkthrough Docs
- Demo Scenarios Docs
- Temporal Extraction Checks
- Spec Kit Workflow
- source_position
- Spec Kit Cycle Instructions
- Spec Templates
- Fake DB-API Doubles
- RDBMS Dialect Adapter
- Position Window Bounds
- Shell Helper Scripts
- Scripted JDBC Doubles
- Raw Iceberg Writer
- Spec Kit Checklists
- Implementation Plan Template
- reader.py
- Airflow Env Config
- manifest.py
- compatibility_projection
- Nessie Catalog Config
- Architecture Documentation
- test_jdbc.py
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
- Feature Convergence Logs
- Schema DDL Mapping Tests
- Spec Kit Tasks Skill
- Code Style Guards
- Act Snap Manifest Tests
- Window Filter Tests
- IncrementalEntityConfig
- managed_load
- DAG Processor Service
- Incremental Registry Docs
- TestGenerateDagSource
- Stand Config Tests
- Spec Kit Analyze Skill
- TestAppendRaw
- Act Snap Plan Docs
- parse_args
- Incremental Manifest Tests
- Analysis Detection Passes
- Named Volumes
- Incremental Config Docs
- 003-module-structure/spec.md
- by_conn_type
- Spec Kit Specify Skill
- Feature Branch Script
- Meta Connect Tests
- OracleDialect
- Postgres Services Compose
- asset_uri
- Test Runner Service
- Incremental Loading Docs
- Spec Kit Plan Skill
- Spec Kit Tasks Template
- Project Constitution
- DATARCH-2244: Isolate Spark drivers via DockerOperator
- Silo Service Compose
- Spark Driver Isolation
- Bank References Manifest
- Broker References Manifest
- source_kind
- Registry Transaction Tests
- Act Snap Operator Tests
- pg-meta DDL Tables
- Sydafa Service Compose
- Архитектура datalake_stand
- ColumnMeta
- TestKeyStructure
- speckit.analyze.md
- Speckit Converge Phase
- Platform Component Overview
- Implementation Plan Template
- Airflowignore Scanner Tests
- Documentation Guidelines
- check_snapshot_recovery
- FakeCursor
- Spark Job Resources
- depends_on
- Project Glossary
- Batch Revision Fence
- DATARCH-2243: Templatize code in generator/generate_dags.py
- Настройка graphify — графа кодовой базы
- Speckit Checklist Phase
- Feature Spec Template
- SQL Identifier Quoting
- Table Lifecycle Logging
- test_plugin_syntax.py
- Sources Configuration File
- first_row
- current_snapshot_author
- empty_incremental_window
- 7.7. DAG-и снимков и таблицы-снимки
- 002-incremental-config/spec.md
- main
- analysis.md
- TestPgMetaInitSql
- DAG Run Callbacks
- Asset Publication Policy
- Шаблонизация кода в generator/generate_dags.py
- Process ID Generation
- Environment Build Configuration
- _entity_block
- conf/airflow/requirements.txt — Airflow Image Dependencies
- Airflow Connection Initialization
- Temporal Column Rendering
- apply_partition_options
- Task Failure Logging
- Разделить реестр источников на YAML-файлы и настроить JDBC-параллелизм по источникам
- Quick Guidelines
- FakeSkip
- load_act_snap.py
- test_dialects.py
- Обработка подвисших процессов Spark при retry задач
- speckit.taskstoissues.md
- Task Logging Tasks
- DAG generator (ingestion/generator/generate_dags.py)
- Расширение стенда
- FakeOperator
- Task Generation Rules
- Meta Module Test Double
- Implementation Phases
- Bucket Init Script
- Auth Init Script
- Source Activation Operator
- Asset Event Callables
- Generator Dependencies
- Publish Asset Task
- Bootstrap Process
- CAS Revision Control

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
- `Проблема` --references--> `append_raw()`  [INFERRED]
  docs/backlog.md → ingestion/spark/raw/append.py
- `Настройки таблицы источника` --references--> `incremental()`  [INFERRED]
  docs/architecture.md → test/temporal_extraction.py
- `Независимые настройки снимка` --references--> `incremental()`  [INFERRED]
  docs/demo.md → test/temporal_extraction.py
- `Запуск и подключения` --references--> `meta()`  [INFERRED]
  docs/demo.md → test/unit/test_jdbc_partitioning.py

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

## Communities (166 total, 43 thin omitted)

### Community 0 - "parse_args"
Cohesion: 0.22
Nodes (3): parse_args(), build_argv(), TestParseArgs

### Community 1 - "JDBC Partition Planning Tests"
Cohesion: 0.07
Nodes (12): day_expression(), no_candidate_reason(), partition_name(), PartitionChoice, plan(), keys(), meta(), StubKindDialect (+4 more)

### Community 2 - "Shared JDBC Reader"
Cohesion: 0.08
Nodes (7): build_extract_query(), jdbc_reader(), read_extract_frame(), make_args(), RecordingReader, TestJdbcReader, TestReadExtractFrame

### Community 3 - "pathlib"
Cohesion: 0.07
Nodes (3): main(), cli_args(), TestOverwriteSnapshot

### Community 4 - "Pipeline Manifest Loading"
Cohesion: 0.08
Nodes (10): _expand_cron_field(), load_pipeline(), load_sources(), parse_schedule(), temporal_lookback_default(), TestGenerateAllValidation, TestLoadPipeline, TestLoadSources (+2 more)

### Community 7 - "managed_snapshot"
Cohesion: 0.23
Nodes (3): managed_snapshot(), FakeCatalog, TestManagedSnapshot

### Community 8 - "Приоритет: можно отложить"
Cohesion: 0.12
Nodes (17): Автоматически формировать теги DAG и исключать дубликаты, Бэклог, Выбирать JDBC-колонку по метрикам с управляемым повторным анализом, Контекст, Контекст, Оглавление, Предпочтительный путь решения, Предпочтительный путь решения (+9 more)

### Community 9 - "Batch Registry Tests"
Cohesion: 0.12
Nodes (5): finalize_batch(), start_batch(), BatchTestCase, TestBatchLifecycle, TestPlanBatch

### Community 10 - "base.py"
Cohesion: 0.14
Nodes (5): __getattr__(), decimal_kind(), fractional_kind(), integer_kind(), normalize_type_name()

### Community 11 - "Airflow API Flow"
Cohesion: 0.08
Nodes (21): load_migration_operations(), airflow_check_asset_publication(), airflow_token(), airflow_token_retry(), airflow_trigger(), airflow_trigger_retry(), airflow_unpause(), airflow_wait_dag_registered() (+13 more)

### Community 13 - "Compose Environment Vars"
Cohesion: 0.08
Nodes (36): environment, healthcheck, image, networks, volumes, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, POSTGRES_DB (+28 more)

### Community 14 - "Project Documentation"
Cohesion: 0.11
Nodes (18): AGENTS.md, conf/airflow/Dockerfile, conf/spark/Dockerfile, ConnectionSpec, .agents/skills/e2e-testing/SKILL.md, load_sources(), ingestion/operators/ (пакет), ingestion/conf/pipelines/ (+10 more)

### Community 15 - "Project Agent Guides"
Cohesion: 0.12
Nodes (25): AGENTS.md — инструкции проекта datalake_stand, Скилл backlog — ведение docs/backlog.md, Скилл e2e-testing — сквозное тестирование стенда, Скилл pull-request — создание Pull Request, пакет sydafa-airflow (SydafaActivateOperator), airflow/plugins/migrations/ — миграции источников bank/broker, conf/airflow/Dockerfile, conf/pg-meta/init/ — DDL pg-meta (+17 more)

### Community 16 - "Table Naming Resolution"
Cohesion: 0.07
Nodes (8): resolve_namespace(), resolve_snapshot_fully_qualified(), resolve_snapshot_table_name(), snapshot_alias(), _validate_identifier(), TestGenerateAll, TestResolveNamespace, TestSnapshotNames

### Community 17 - "Postgres Registry Tests"
Cohesion: 0.09
Nodes (3): 2. Установка утилиты, sql(), PostgresTests

### Community 18 - "generate_all"
Cohesion: 0.09
Nodes (10): compute_drift(), dag_id_for(), file_name_for(), generate_all(), main(), snapshot_dag_id(), SourceConfig, 003-module-structure: задачи (+2 more)

### Community 19 - "Airflow Operators Package"
Cohesion: 0.06
Nodes (17): Событие готовности Asset, Контекст, publish_asset_event(), snapshot_has_current_process(), _dagrun_event(), dagrun_failure_callback(), dagrun_success_callback(), GenerateProcessIdOperator (+9 more)

### Community 20 - "Incremental Batch Planning"
Cohesion: 0.10
Nodes (11): _row(), _batch_reason(), _check_batch(), _check_contract(), fail_batch(), _incremental_bounds(), _IncrementalConfig, _lock_batch() (+3 more)

### Community 21 - "Demo Walkthrough Docs"
Cohesion: 0.09
Nodes (26): Apache Airflow, Задача generate_process_id, Apache Iceberg, Таблица ingestion.incremental_batches, DAG ingest__rdbms__bank__main, ingest__rdbms__bank__main (generated DAG), pg-meta (реестр загрузок), Apache Spark (+18 more)

### Community 22 - "Demo Scenarios Docs"
Cohesion: 0.06
Nodes (31): 5.1. Подготовка показа pg-meta, 5.2. Новый запуск без изменения источника, 5.3. Clear только успешной load-задачи, 5.4. Полный Clear существующего запуска, 5.5. Новая загрузка после обновления источника, 5.6. Что объяснить про повтор после ошибки, 7.1. Удаление колонки, 7.2. Переименование колонки (+23 more)

### Community 23 - "Temporal Extraction Checks"
Cohesion: 0.12
Nodes (13): resolve_source_connection(), choose(), check_partitioned_load_path(), check_partitioning(), check_post_append_retry(), derived_day_frame(), exercise(), jdbc_count() (+5 more)

### Community 24 - "Spec Kit Workflow"
Cohesion: 0.06
Nodes (30): args, inputs, integration, spec, default, prompt, type, any (+22 more)

### Community 25 - "source_position"
Cohesion: 0.19
Nodes (4): Первичное наполнение и проверка источника, connection(), source_position(), TestSourcePosition

### Community 26 - "Spec Kit Cycle Instructions"
Cohesion: 0.07
Nodes (29): 10. Обновление Spec Kit, 11. Взаимодействие с graphify и Pull Request, 12. Работа в ZCode Desktop (интерактивно), 1. Назначение, 2. Установка для сопровождающего (выполнена один раз), 3. Инструкция для разработчика, 4. Состояние цикла: каталоги, машины, ветки, 5. Состав артефактов (+21 more)

### Community 27 - "Spec Templates"
Cohesion: 0.11
Nodes (27): contracts/ (interface contracts), data-model.md, plan.md (implementation plan), quickstart.md, research.md, speckit-implement, Completion Report, Done When (+19 more)

### Community 29 - "RDBMS Dialect Adapter"
Cohesion: 0.10
Nodes (7): 2. Подключение новой JDBC-СУБД, Контракт диалекта, Проверки совместимости, Регистрация и окружение, RdbmsDialect, source_count(), TestBaseDialectContract

### Community 30 - "Position Window Bounds"
Cohesion: 0.05
Nodes (15): canonical(), _format(), kind_info(), maximum(), _parse(), resolve_lookback(), window_bounds(), _plan_batch() (+7 more)

### Community 31 - "Shell Helper Scripts"
Cohesion: 0.16
Nodes (21): check-prerequisites.sh script, check_dir(), check_file(), find_specify_root(), format_speckit_command(), get_current_branch(), get_feature_paths(), get_invoke_separator() (+13 more)

### Community 33 - "Raw Iceberg Writer"
Cohesion: 0.07
Nodes (13): append_raw(), build_create_table_sql(), schema_columns(), table_location(), validate_target_table(), ingestion/iceberg.py (старый путь), ingestion/meta/ (пакет), ingestion/operators/ (пакет: __init__/process/assets/raw_rdbms) (+5 more)

### Community 34 - "Spec Kit Checklists"
Cohesion: 0.09
Nodes (26): checklists/requirements.md (spec quality checklist), .specify/extensions.yml — хуки расширений Spec Kit, .specify/init-options.json, speckit-clarify, Completion Report, Done When, Mandatory Post-Execution Hooks, Outline (+18 more)

### Community 35 - "Implementation Plan Template"
Cohesion: 0.08
Nodes (26): Dependencies & Execution Order, Format: `[ID] [P?] [Story] Description`, Implementation for User Story 1, Implementation for User Story 2, Implementation for User Story 3, Implementation Strategy, Incremental Delivery, MVP First (User Story 1 Only) (+18 more)

### Community 36 - "reader.py"
Cohesion: 0.11
Nodes (6): IngestFullOperator, IngestIncrementalOperator, Airflow pool ingest_loads, load (DAG task), reader.py (shared JDBC reader), Backlog: Remove JDBC password from spark-submit arguments

### Community 37 - "Airflow Env Config"
Cohesion: 0.26
Nodes (25): environment, environment, environment, environment, AIRFLOW__API_AUTH__JWT_SECRET, AIRFLOW__API__EXPOSE_CONFIG, AIRFLOW__API__SECRET_KEY, AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION (+17 more)

### Community 38 - "manifest.py"
Cohesion: 0.10
Nodes (16): Генерация и настройка, Предпочтительный путь решения, ActSnapConfig, AirflowConfig, DagConfig, IngestionSettings, load_settings(), _parse_act_snap() (+8 more)

### Community 40 - "Nessie Catalog Config"
Cohesion: 0.09
Nodes (23): pg-catalog, nessie.catalog.default-warehouse, nessie.catalog.secrets.access-key.name, nessie.catalog.secrets.access-key.secret, nessie.catalog.service.s3.default-options.access-key, nessie.catalog.service.s3.default-options.auth-type, nessie.catalog.service.s3.default-options.endpoint, nessie.catalog.service.s3.default-options.path-style-access (+15 more)

### Community 41 - "Architecture Documentation"
Cohesion: 0.11
Nodes (21): pg-meta DDL (conf/pg-meta/init/01_ingestion.sql), Glossary (glossary.md), MSSQL dialect (ingestion/sources/rdbms/dialects/mssql.py), Oracle dialect (ingestion/sources/rdbms/dialects/oracle.py), Iceberg writer layer (ingestion/spark/raw/iceberg.py), Full-slice loader (ingestion/spark/raw/rdbms/full/load_entity.py), Incremental loader (ingestion/spark/raw/rdbms/incremental/load_entity.py), Incremental window reader (incremental/window.py) (+13 more)

### Community 42 - "test_jdbc.py"
Cohesion: 0.16
Nodes (4): MetadataLocation, TestMetadataLocation, CatalogDialect, TestCatalogAddressing

### Community 43 - "Dev Tooling Documentation"
Cohesion: 0.13
Nodes (17): AGENTS.md, graphify-out/graph.json, graphify (CLI-утилита), AGENTS.md, .specify/feature.json (локальный указатель фичи), opencode, .opencode/commands/, specify CLI (GitHub Spec Kit) (+9 more)

### Community 44 - "Target Name Resolution Tests"
Cohesion: 0.12
Nodes (5): assert_unique_targets(), resolve_target_table_name(), _split_fragments(), TestAssertUniqueTargets, TestResolveTargetTableName

### Community 45 - "key_metadata"
Cohesion: 0.30
Nodes (4): key_metadata(), KeyMetadata, fake_jvm(), TestKeyMetadata

### Community 47 - "Post-Run Verification"
Cohesion: 0.14
Nodes (5): check_registry_recovery(), check_snapshot_publication(), main(), records(), require()

### Community 48 - "Init Services Compose"
Cohesion: 0.11
Nodes (19): nessie, silo-init, trino-init, condition, spark-master, trino, condition, command (+11 more)

### Community 49 - "pg-meta Logging Client"
Cohesion: 0.14
Nodes (6): allocate_process_id(), connection(), find_process_id(), log_dagrun(), log_task(), TestProcessAndEvents

### Community 50 - "Airflow Services Compose"
Cohesion: 0.11
Nodes (18): depends_on, condition, command, depends_on, image, networks, pull_policy, volumes (+10 more)

### Community 51 - "Docker Compose Layout"
Cohesion: 0.12
Nodes (16): name, networks, dlh-poc, services, spark-worker, trino-init, command, depends_on (+8 more)

### Community 52 - "Приоритет: важно"
Cohesion: 0.15
Nodes (13): Задача Jira, Задача Jira, Задача Jira, Изолировать Spark drivers через DockerOperator, Контекст, Определить и проверить минимальные поддерживаемые версии компонентов, Предпочтительный путь решения, Приоритет: важно (+5 more)

### Community 53 - "Spec Kit Converge Skill"
Cohesion: 0.12
Nodes (17): spec.md (feature specification), speckit-converge, 1. Initialize Convergence Context, 2. Load Artifacts (Progressive Disclosure), 3. Build the Intent Inventory, 4. Assess the Codebase and Classify Findings, 5. Assign Severity, 6. Present the In-Session Findings Summary (+9 more)

### Community 54 - "Bank Pipeline Manifest"
Cohesion: 0.12
Nodes (16): airflow, pool, position_type, source_column, entities, airflow, fqn, incremental (+8 more)

### Community 55 - "Broker Pipeline Manifest"
Cohesion: 0.12
Nodes (16): airflow, pool, position_type, source_column, entities, airflow, fqn, incremental (+8 more)

### Community 56 - "Constitution Templates"
Cohesion: 0.12
Nodes (15): Outline, Post-Execution Checks, Pre-Execution Checks, Scope Guard, User Input, Core Principles, Governance, [PRINCIPLE_1_NAME] (+7 more)

### Community 57 - "Feature Convergence Logs"
Cohesion: 0.15
Nodes (13): ACT_SNAPSHOT_TABLES, Дополнение: пункты 3–5 плана от 2026-11-08, Исправления по третьему ревью, Итоговая проверка, Проверка уточнений после ревью, 001-act-snap-contract: задачи, README.md, Завершение пунктов 3–5 внешнего плана (+5 more)

### Community 58 - "Schema DDL Mapping Tests"
Cohesion: 0.15
Nodes (3): fake_pyspark_types(), SchemaDouble, TestSchemaColumns

### Community 59 - "Spec Kit Tasks Skill"
Cohesion: 0.12
Nodes (15): tasks.md (task list), $speckit-github-taskstoissues (github extension), speckit-checklist, Anti-Examples: What NOT To Do, Checklist Purpose: "Unit Tests for English", Example Checklist Types & Sample Items, Execution Steps, Post-Execution Checks (+7 more)

### Community 60 - "Code Style Guards"
Cohesion: 0.17
Nodes (3): TestNoHashComments, manipulates_sys_path(), TestNoSysPathManipulation

### Community 62 - "Window Filter Tests"
Cohesion: 0.11
Nodes (8): position_select(), read_window_frame(), window_filters(), make_args(), make_batch(), TestPositionSelect, TestReadWindowFrame, TestWindowFilters

### Community 63 - "IncrementalEntityConfig"
Cohesion: 0.16
Nodes (16): Назначение задач, Границы рефакторинга, Контекст, Контекст, Контекст, Предпочтительный путь решения, 3. Новый тип источника, EntityConfig (+8 more)

### Community 65 - "DAG Processor Service"
Cohesion: 0.20
Nodes (14): build, command, image, networks, pull_policy, volumes, build, build (+6 more)

### Community 66 - "Incremental Registry Docs"
Cohesion: 0.20
Nodes (12): Batch registry (ingestion/meta/registry.py), Incremental batch, Bootstrap (initial fill), Compare-and-set (CAS) on revision, ingestion.incremental_batches, incremental contract (canonical position type), ingestion.incremental_state, ingestion.incremental_state_migrations (+4 more)

### Community 67 - "TestGenerateDagSource"
Cohesion: 0.10
Nodes (5): generate_dag_source(), PipelineManifest, validate_pipeline(), TestGenerateDagSource, TestValidatePipeline

### Community 69 - "Spec Kit Analyze Skill"
Cohesion: 0.14
Nodes (14): speckit-analyze, 7. Provide Next Actions, 8. Offer Remediation, 9. Check for extension hooks, .specify/extensions.yml (extension hooks registry), Analysis Guidelines, Context, Context Efficiency (+6 more)

### Community 71 - "Act Snap Plan Docs"
Cohesion: 0.13
Nodes (15): load_act_snap.py (Spark-приложение снимка), Asset (Airflow), Таблица-снимок (act_snap), PipelineManifest, Завершение проверки контракта, Конституция и уточнения, План реализации, Проверки (+7 more)

### Community 72 - "parse_args"
Cohesion: 0.13
Nodes (7): main(), managed_load(), parse_args(), build_argv(), TestMain, TestManagedLoad, TestParseArgs

### Community 74 - "Analysis Detection Passes"
Cohesion: 0.15
Nodes (13): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 4. Detection Passes (Token-Efficient Analysis), 5. Severity Assignment, 6. Produce Compact Analysis Report, A. Duplication Detection, B. Ambiguity Detection (+5 more)

### Community 75 - "Named Volumes"
Cohesion: 0.17
Nodes (12): name, name, volumes, airflow_logs, airflow_pg_data, pg_catalog_data, pg_meta_data, silo_data (+4 more)

### Community 76 - "Incremental Config Docs"
Cohesion: 0.18
Nodes (11): 7.1. Контракт, Выбор типа позиции, Выравнивание временной границы, Границы окна, Настройка lookback, Отступ по умолчанию и расписание, Передача конфигурации загрузчику, Требования к колонке инкремента (+3 more)

### Community 77 - "003-module-structure/spec.md"
Cohesion: 0.15
Nodes (12): Готовность фичи, Заметки, Полнота требований, Чек-лист качества спецификации: структура модулей загрузчика, FullEntityConfig, IncrementalEntityConfig, IngestAppendOperator (удаляется), IngestFullOperator (+4 more)

### Community 78 - "by_conn_type"
Cohesion: 0.18
Nodes (5): parse_source_table(), by_conn_type(), render_source_table(), TestParseSourceTable, TestRenderSourceTable

### Community 79 - "Spec Kit Specify Skill"
Cohesion: 0.17
Nodes (11): Completion Report, Done When, For AI Generation, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, Quick Guidelines, Section Requirements (+3 more)

### Community 80 - "Feature Branch Script"
Cohesion: 0.36
Nodes (9): clean_branch_name(), contains_non_ascii(), fit_branch_name(), generate_branch_name(), get_highest_from_specs(), is_feature_number_in_range(), create-new-feature.sh script, spec_prefix_exists() (+1 more)

### Community 81 - "Meta Connect Tests"
Cohesion: 0.17
Nodes (4): fake_make_dsn(), load_meta_api(), load_operators(), TestMetaConnect

### Community 83 - "Postgres Services Compose"
Cohesion: 0.18
Nodes (11): command, depends_on, image, networks, pull_policy, volumes, condition, airflow-postgres (+3 more)

### Community 84 - "asset_uri"
Cohesion: 0.21
Nodes (4): asset_uri(), snapshot_asset_uri(), _snapshot_entity_block(), TestAssetUri

### Community 85 - "Test Runner Service"
Cohesion: 0.18
Nodes (11): AIRFLOW_PASSWORD, AIRFLOW_USER, TRINO_PASSWORD, TRINO_USER, test-runner, build, environment, image (+3 more)

### Community 86 - "Incremental Loading Docs"
Cohesion: 0.31
Nodes (10): 7.2. Жизненный цикл загрузки, 7.3. `incremental_state` — состояние сущности, 7.4. `incremental_batches` — реестр партий, 7.5. Пример одного инкремента и повтора, 7.6. Смена контракта и ручные миграции, 7. Инкрементальная загрузка, meta.finalize_batch(), meta.get_batch() (+2 more)

### Community 87 - "Spec Kit Plan Skill"
Cohesion: 0.11
Nodes (16): Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, User Input, Completion Report, Done When (+8 more)

### Community 88 - "Spec Kit Tasks Template"
Cohesion: 0.11
Nodes (16): Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, User Input, Checklist Format (REQUIRED), Completion Report (+8 more)

### Community 89 - "Project Constitution"
Cohesion: 0.18
Nodes (11): I. Граф кодовой базы прежде чтения файлов, II. Изменения подтверждаются тестами, III. Чистый Python без комментариев, IV. Дисциплина Git: ветка → Pull Request → мерж пользователем, V. Перестроение графа после каждого коммита, VI. Документация на русском языке, Базовые принципы, Конституция проекта datalake_stand (+3 more)

### Community 90 - "DATARCH-2244: Isolate Spark drivers via DockerOperator"
Cohesion: 0.27
Nodes (12): conf/spark/spark-defaults.conf, Global settings (ingestion/conf/settings.yaml), Sources registry (ingestion/conf/sources.yaml), load_sources(), Load operators module (ingestion/operators.py), DATARCH-2242: Split sources registry into YAML files, per-source JDBC parallelism, DATARCH-2244: Isolate Spark drivers via DockerOperator, DATARCH-2245: Handle hung Spark processes on task retry (+4 more)

### Community 91 - "Silo Service Compose"
Cohesion: 0.20
Nodes (10): silo, AWS_DEFAULT_REGION, silo-init, condition, depends_on, entrypoint, environment, image (+2 more)

### Community 92 - "Spark Driver Isolation"
Cohesion: 0.29
Nodes (7): Жизненный цикл, Контракт оператора, Образ и зависимости, Предпочтительный путь решения, Проверки и критерии готовности, Риски и границы решения, Сеть и ресурсы

### Community 93 - "Bank References Manifest"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 94 - "Broker References Manifest"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 95 - "source_kind"
Cohesion: 0.25
Nodes (4): column_position_kind(), _family(), _first_candidate(), source_kind()

### Community 98 - "pg-meta DDL Tables"
Cohesion: 0.33
Nodes (8): airflow_dagruns_log_process_id_idx, airflow_dagruns_log_run_idx, airflow_tasks_log_run_idx, ingestion.airflow_dagruns_log, ingestion.airflow_tasks_log, ingestion.incremental_batches, ingestion.incremental_state, ingestion.incremental_state_migrations

### Community 99 - "Sydafa Service Compose"
Cohesion: 0.22
Nodes (9): start_period, sydafa, environment, healthcheck, image, networks, platform, ports (+1 more)

### Community 100 - "Архитектура datalake_stand"
Cohesion: 0.04
Nodes (47): 10. Известные ограничения, 1. Слои данных, 2. Манифесты и генерация DAG-ов, 3. Структура сгенерированного DAG-а, 4. Таблицы raw-слоя: структура и наполнение, 5. Устойчивость к эволюции схемы источника, 6.1. Порядок выполнения load-задачи, 6. Параллелизм Spark (+39 more)

### Community 101 - "ColumnMeta"
Cohesion: 0.21
Nodes (4): _column_metadata(), ColumnMeta, _optional_int(), TestColumnMetadata

### Community 102 - "TestKeyStructure"
Cohesion: 0.17
Nodes (3): _primary_key(), _unique_single_columns(), TestKeyStructure

### Community 103 - "speckit.analyze.md"
Cohesion: 0.08
Nodes (25): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 4. Detection Passes (Token-Efficient Analysis), 5. Severity Assignment, 6. Produce Compact Analysis Report, 7. Provide Next Actions, 8. Offer Remediation (+17 more)

### Community 104 - "Speckit Converge Phase"
Cohesion: 0.12
Nodes (15): 1. Initialize Convergence Context, 2. Load Artifacts (Progressive Disclosure), 3. Build the Intent Inventory, 4. Assess the Codebase and Classify Findings, 5. Assign Severity, 6. Present the In-Session Findings Summary, 7. Append Convergence Tasks (or report converged), 8. Provide Next Actions (Handoff) (+7 more)

### Community 105 - "Platform Component Overview"
Cohesion: 0.33
Nodes (9): Airflow — оркестрация загрузок, JDBC-драйверы MSSQL/Oracle в образах Airflow и Spark, Nessie — каталог Iceberg, pg-catalog — база метаданных Nessie, pg-meta — база журналов и состояния загрузок, silo — MinIO-совместимое S3-хранилище, Spark — вычисления и загрузка по JDBC, sydafa — сервис управления источниками (+1 more)

### Community 106 - "Implementation Plan Template"
Cohesion: 0.22
Nodes (8): Complexity Tracking, Constitution Check, Documentation (this feature), Implementation Plan: [FEATURE], Project Structure, Source Code (repository root), Summary, Technical Context

### Community 108 - "Documentation Guidelines"
Cohesion: 0.25
Nodes (8): Ведение документации, Когда применять, Ответственность документов, Порядок работы и проверки, Содержание, Термины и глоссарий, Техническая точность, Язык и порядок изложения

### Community 109 - "check_snapshot_recovery"
Cohesion: 0.21
Nodes (7): _overwrite_snapshot(), check_snapshot_recovery(), allocate(), incremental(), projection(), registry(), snapshot()

### Community 111 - "Spark Job Resources"
Cohesion: 0.25
Nodes (8): Выполнение и обмен данными, Задача Jira, Контекст, Память executor, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Управление ресурсами Spark на уровне отдельной джобы

### Community 112 - "depends_on"
Cohesion: 0.18
Nodes (11): condition, airflow-webserver, spark-master, spark-worker, sydafa, trino, condition, condition (+3 more)

### Community 113 - "Project Glossary"
Cohesion: 0.25
Nodes (8): Вычисления и ресурсы, Глоссарий, Данные и загрузки, Запуски и повторные попытки, Инкрементальная загрузка, Разработка и проверки, Содержание, Хранение и гарантии

### Community 115 - "DATARCH-2243: Templatize code in generator/generate_dags.py"
Cohesion: 0.27
Nodes (13): Snapshot stream (incremental/load_act_snap.py), Increment (incremental column contract), Snapshot table (actual snapshot, __act_snap), generate_dag_source(), generate_snapshot_dag_source(), build_create_table_sql(), build_rtas_sql(), build_extract_query() (+5 more)

### Community 116 - "Настройка graphify — графа кодовой базы"
Cohesion: 0.25
Nodes (8): 1. Назначение, 3. Переменные окружения, 4. Перестроение графа, 5. Артефакты `graphify-out/` и git, 6. Запросы по графу, Настройка graphify — графа кодовой базы, Результат настройки, Содержание

### Community 117 - "Speckit Checklist Phase"
Cohesion: 0.15
Nodes (11): Anti-Examples: What NOT To Do, Checklist Purpose: "Unit Tests for English", Example Checklist Types & Sample Items, Execution Steps, Post-Execution Checks, Pre-Execution Checks, User Input, [Category 1] (+3 more)

### Community 118 - "Feature Spec Template"
Cohesion: 0.15
Nodes (12): Assumptions, Edge Cases, Feature Specification: [FEATURE NAME], Functional Requirements, Key Entities *(include if feature involves data)*, Measurable Outcomes, Requirements *(mandatory)*, Success Criteria *(mandatory)* (+4 more)

### Community 122 - "Sources Configuration File"
Cohesion: 0.29
Nodes (6): bank, airflow_conn_id, type, broker, airflow_conn_id, type

### Community 123 - "first_row"
Cohesion: 0.29
Nodes (3): first_row(), _row_size(), TestFirstRow

### Community 126 - "7.7. DAG-и снимков и таблицы-снимки"
Cohesion: 0.29
Nodes (7): 7.7. DAG-и снимков и таблицы-снимки, Автор и время публикации, Атомарная замена данных, Контроль свежести снимков, Повтор после сбоя, Пустое чтение, Структура таблицы-снимка

### Community 127 - "002-incremental-config/spec.md"
Cohesion: 0.29
Nodes (6): Инкремент (incremental), CLI-флаг --incremental-config, incremental.contract (YAML: source_column, position_type), Контракт инкрементальной загрузки, Требования, Уточнения

### Community 129 - "analysis.md"
Cohesion: 0.33
Nodes (5): ActSnapConfig (dataclass), DagConfig (базовый dataclass), MAX_ACTIVE_RUNS (константа генератора), Дополнительное покрытие пунктов 3–5, Проверка согласованности

### Community 133 - "Шаблонизация кода в generator/generate_dags.py"
Cohesion: 0.29
Nodes (7): Генерация DAG, Загрузка и окружение, Задача Jira, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Шаблонизация кода в generator/generate_dags.py

### Community 134 - "Process ID Generation"
Cohesion: 0.33
Nodes (6): ingestion.airflow_dagruns_log, generate_process_id (Airflow task), GenerateProcessIdOperator, generate_process_id (DAG task), pg-meta metadata database, ingestion.process_id_seq

### Community 135 - "Environment Build Configuration"
Cohesion: 0.33
Nodes (6): conf/airflow/Dockerfile, conf/airflow/requirements.txt, conf/spark/Dockerfile, ingestion/generator/requirements.txt, E2E test suite (test/test.sh), DATARCH-2241: Define and verify minimal supported component versions

### Community 136 - "_entity_block"
Cohesion: 0.20
Nodes (4): _entity_block(), load_operator_name(), resolve_fully_qualified(), TestResolveFullyQualified

### Community 137 - "conf/airflow/requirements.txt — Airflow Image Dependencies"
Cohesion: 0.50
Nodes (4): conf/airflow/requirements.txt — Airflow Image Dependencies, apache-airflow-providers-apache-spark, apache-airflow-providers-trino, sydafa-airflow — Airflow Integration Package (SydafaActivateOperator, SydafaMigrateOperator)

### Community 140 - "apply_partition_options"
Cohesion: 0.14
Nodes (6): apply_partition_options(), bound_literal(), long_representable(), read_bounds(), TestBoundLiteral, TestReadBounds

### Community 142 - "Разделить реестр источников на YAML-файлы и настроить JDBC-параллелизм по источникам"
Cohesion: 0.50
Nodes (4): Задача Jira, Контекст, Проблема, Разделить реестр источников на YAML-файлы и настроить JDBC-параллелизм по источникам

### Community 143 - "Quick Guidelines"
Cohesion: 0.50
Nodes (4): For AI Generation, Quick Guidelines, Section Requirements, Success Criteria Guidelines

### Community 145 - "load_act_snap.py"
Cohesion: 0.10
Nodes (13): __act_snap auxiliary snapshot, append.py (raw-layer writer), dds / dma layers (planned), IngestActSnapOperator, ods layer (planned), raw layer, source schema evolution handling, quote_identifier() (+5 more)

### Community 148 - "test_dialects.py"
Cohesion: 0.06
Nodes (12): ConnectionSpec, all_dialects(), by_jdbc_url(), supported_conn_types(), supported_url_prefixes(), TestColumnProjection, TestConnections, TestContractsReExport (+4 more)

### Community 149 - "Обработка подвисших процессов Spark при retry задач"
Cohesion: 0.33
Nodes (6): Задача Jira, Обработка подвисших процессов Spark при retry задач, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Реализация и граница решения

### Community 150 - "speckit.taskstoissues.md"
Cohesion: 0.33
Nodes (5): Deprecation Notice, Outline, Post-Execution Checks, Pre-Execution Checks, User Input

### Community 151 - "Task Logging Tasks"
Cohesion: 0.67
Nodes (4): ingestion.airflow_tasks_log, log_end (DAG task), log_start (DAG task), TableLogOperator

### Community 154 - "DAG generator (ingestion/generator/generate_dags.py)"
Cohesion: 0.40
Nodes (5): Bank pipeline manifest (ingestion/conf/pipelines/bank/main.yaml), DAG generator (ingestion/generator/generate_dags.py), Manifest parser (ingestion/generator/manifest.py), Backlog: Auto-generate DAG tags and drop duplicates, Backlog: Split Airflow pools for loads vs service tasks

### Community 155 - "Расширение стенда"
Cohesion: 0.40
Nodes (5): 1. Выбор вида изменения, 4. Новый слой данных, 5. Порядок разработки, Расширение стенда, Содержание

### Community 158 - "Task Generation Rules"
Cohesion: 0.50
Nodes (4): Checklist Format (REQUIRED), Phase Structure, Task Generation Rules, Task Organization

### Community 161 - "Implementation Phases"
Cohesion: 0.67
Nodes (3): Phase 0: Outline & Research, Phase 1: Design & Contracts, Phases

## Ambiguous Edges - Review These
- `tasks-template.md` → `speckit-tasks`  [AMBIGUOUS]
  .specify/templates/tasks-template.md · relation: references

## Knowledge Gaps
- **102 isolated node(s):** `SYDAFA_CONTAINER_PREFIX`, `pg-catalog — база метаданных Nessie`, `GenerateProcessIdOperator`, `ingestion.process_id_seq`, `conf/airflow/requirements.txt` (+97 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 1287 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **43 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `services` connect `Docker Compose Layout` to `DAG Processor Service`, `Sydafa Service Compose`, `Nessie Catalog Config`, `Compose Environment Vars`, `Init Services Compose`, `Airflow Services Compose`, `Postgres Services Compose`, `Test Runner Service`, `Silo Service Compose`?**
  _High betweenness centrality (0.150) - this node is a cross-community bridge._
- **Are the 16 inferred relationships involving `RdbmsDialect` (e.g. with `Контекст` and `Предпочтительный путь решения`) actually correct?**
  _`RdbmsDialect` has 16 INFERRED edges - model-reasoned connections that need verification._
- **What connects `SYDAFA_CONTAINER_PREFIX`, `pg-catalog — база метаданных Nessie`, `GenerateProcessIdOperator` to the rest of the system?**
  _102 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `JDBC Partition Planning Tests` be split into smaller, more focused modules?**
  _Cohesion score 0.06874669487043893 - nodes in this community are weakly interconnected._
- **Why does `README — стенд хранилища и озера данных` connect `Project Agent Guides` to `Architecture Documentation`, `conf/airflow/requirements.txt — Airflow Image Dependencies`, `Dev Tooling Documentation`, `Project Documentation`, `Docker Compose Layout`, `Demo Walkthrough Docs`?**
  _High betweenness centrality (0.122) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `generate_all()` (e.g. with `IncrementalEntityConfig` and `SourceConfig`) actually correct?**
  _`generate_all()` has 2 INFERRED edges - model-reasoned connections that need verification._

### Low-confidence Hints
_AMBIGUOUS edges — the extractor was unsure. Verify before acting on these._

- **What is the exact relationship between `tasks-template.md` and `speckit-tasks`?**
  _Edge tagged AMBIGUOUS (relation: references) - confidence is low._