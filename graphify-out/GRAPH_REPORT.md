# Graph Report - datalake_stand  (2026-11-12)

## Corpus Check
- 149 files · ~132,712 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 12 file(s) not represented in the graph (top: (none) 9, .properties 2, .conf 1)

## Summary
- 2750 nodes · 4897 edges · 172 communities (129 shown, 43 thin omitted)
- Extraction: 95% EXTRACTED · 5% INFERRED · 0% AMBIGUOUS · INFERRED: 235 edges (avg confidence: 0.9)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `e12d4da3`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- parse_args
- JDBC Partition Planning Tests
- Shared JDBC Reader
- incremental/load_entity.py
- Pipeline Manifest Loading
- raw_rdbms.py
- TestGenerateSnapshotDagSource
- managed_snapshot
- Приоритет: можно отложить
- Batch Registry Tests
- build_create_table_sql
- Airflow API Flow
- test_stand_config.py
- Compose Environment Vars
- Project Documentation
- Project Agent Guides
- generate_dags.py
- Postgres Registry Tests
- generate_all
- Airflow Operators Package
- Incremental Batch Planning
- Demo Walkthrough Docs
- Demo Scenarios Docs
- temporal_extraction.py
- Spec Kit Workflow
- 003-module-structure/plan.md
- Spec Kit Cycle Instructions
- Spec Templates
- Fake DB-API Doubles
- RDBMS Dialect Adapter
- Position Window Bounds
- Shell Helper Scripts
- Scripted JDBC Doubles
- iceberg.py
- Spec Kit Checklists
- Implementation Plan Template
- full/load_entity.py
- Airflow Env Config
- load_settings
- compatibility_projection
- Nessie Catalog Config
- Architecture Documentation
- resolve_namespace
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
- TestPureHelpers
- Window Filter Tests
- IncrementalEntityConfig
- validate_target_table
- DAG Processor Service
- parse_args
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
- ConnectionSpec
- Postgres Services Compose
- TestGenerateAll
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
- jdbc_partitioning.py
- Registry Transaction Tests
- Act Snap Operator Tests
- pg-meta DDL Tables
- Sydafa Service Compose
- 4. Таблицы raw-слоя: структура и наполнение
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
- 6. Параллелизм Spark
- Sources Configuration File
- first_row
- current_snapshot_author
- Архитектура datalake_stand
- 7.7. DAG-и снимков и таблицы-снимки
- _apply_entity_fields
- 4. Detection Passes (Token-Efficient Analysis)
- speckit.clarify.md
- TestPgMetaInitSql
- DAG Run Callbacks
- Asset Publication Policy
- Execution Steps
- Process ID Generation
- Environment Build Configuration
- TestResolveFullyQualified
- speckit.implement.md
- Airflow Connection Initialization
- Temporal Column Rendering
- apply_partition_options
- Task Failure Logging
- 2. Манифесты и генерация DAG-ов
- Quick Guidelines
- FakeSkip
- load_act_snap.py
- Execution Steps
- 3. Структура сгенерированного DAG-а
- test_dialects.py
- Обработка подвисших процессов Spark при retry задач
- speckit.taskstoissues.md
- Task Logging Tasks
- 5. Устойчивость к эволюции схемы источника
- [CHECKLIST TYPE] Checklist: [FEATURE NAME]
- User Scenarios & Testing *(mandatory)*
- cli_args
- FakeOperator
- Specification Analysis Report
- Task Generation Rules
- TestColumnProjection
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
- `Первичное наполнение и проверка источника` --references--> `source_position()`  [INFERRED]
  docs/architecture.md → ingestion/spark/raw/rdbms/incremental/load_entity.py
- `Настройки таблицы источника` --references--> `incremental()`  [INFERRED]
  docs/architecture.md → test/temporal_extraction.py
- `Независимые настройки снимка` --references--> `incremental()`  [INFERRED]
  docs/demo.md → test/temporal_extraction.py

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

## Communities (172 total, 43 thin omitted)

### Community 0 - "parse_args"
Cohesion: 0.07
Nodes (10): empty_incremental_window(), main(), managed_load(), parse_args(), build_argv(), TestEmptyIncrementalWindow, TestMain, TestManagedLoad (+2 more)

### Community 1 - "JDBC Partition Planning Tests"
Cohesion: 0.08
Nodes (10): day_expression(), no_candidate_reason(), partition_name(), PartitionChoice, plan(), keys(), TestChoose, TestDayExpression (+2 more)

### Community 2 - "Shared JDBC Reader"
Cohesion: 0.08
Nodes (7): build_extract_query(), read_extract_frame(), make_args(), RecordingReader, TestJdbcReader, TestReadBounds, TestReadExtractFrame

### Community 3 - "incremental/load_entity.py"
Cohesion: 0.06
Nodes (4): decimal_kind(), fractional_kind(), integer_kind(), normalize_type_name()

### Community 4 - "Pipeline Manifest Loading"
Cohesion: 0.05
Nodes (13): _expand_cron_field(), load_pipeline(), load_sources(), parse_schedule(), _read_yaml(), temporal_lookback_default(), assert_unique_targets(), TestActSnapManifest (+5 more)

### Community 5 - "raw_rdbms.py"
Cohesion: 0.12
Nodes (5): validate(), _format(), kind_info(), _parse(), TestKindInfo

### Community 6 - "TestGenerateSnapshotDagSource"
Cohesion: 0.12
Nodes (7): Генерация и настройка, generate_snapshot_dag_source(), ActSnapConfig, AirflowConfig, DagConfig, SparkConfig, TestGenerateSnapshotDagSource

### Community 7 - "managed_snapshot"
Cohesion: 0.23
Nodes (3): managed_snapshot(), FakeCatalog, TestManagedSnapshot

### Community 8 - "Приоритет: можно отложить"
Cohesion: 0.14
Nodes (14): Автоматически формировать теги DAG и исключать дубликаты, Бэклог, Выбирать JDBC-колонку по метрикам с управляемым повторным анализом, Контекст, Контекст, Оглавление, Предпочтительный путь решения, Предпочтительный путь решения (+6 more)

### Community 9 - "Batch Registry Tests"
Cohesion: 0.12
Nodes (5): finalize_batch(), start_batch(), BatchTestCase, TestBatchLifecycle, TestPlanBatch

### Community 10 - "build_create_table_sql"
Cohesion: 0.19
Nodes (4): build_create_table_sql(), table_location(), TestBuildCreateTableSql, TestTableLocation

### Community 11 - "Airflow API Flow"
Cohesion: 0.07
Nodes (22): load_migration_operations(), airflow_check_asset_publication(), airflow_token(), airflow_token_retry(), airflow_trigger(), airflow_trigger_retry(), airflow_unpause(), airflow_wait_dag_registered() (+14 more)

### Community 13 - "Compose Environment Vars"
Cohesion: 0.08
Nodes (36): environment, healthcheck, image, networks, volumes, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, POSTGRES_DB (+28 more)

### Community 14 - "Project Documentation"
Cohesion: 0.07
Nodes (28): 1. Выбор вида изменения, 4. Новый слой данных, 5. Порядок разработки, AGENTS.md, conf/airflow/Dockerfile, conf/spark/Dockerfile, ConnectionSpec, .agents/skills/e2e-testing/SKILL.md (+20 more)

### Community 15 - "Project Agent Guides"
Cohesion: 0.10
Nodes (29): AGENTS.md — инструкции проекта datalake_stand, Скилл backlog — ведение docs/backlog.md, Скилл e2e-testing — сквозное тестирование стенда, Скилл pull-request — создание Pull Request, пакет sydafa-airflow (SydafaActivateOperator), airflow/plugins/migrations/ — миграции источников bank/broker, conf/airflow/Dockerfile, conf/airflow/requirements.txt — Airflow Image Dependencies (+21 more)

### Community 16 - "generate_dags.py"
Cohesion: 0.07
Nodes (15): asset_uri(), dag_id_for(), _entity_block(), file_name_for(), load_operator_name(), snapshot_asset_uri(), snapshot_dag_id(), _snapshot_entity_block() (+7 more)

### Community 18 - "generate_all"
Cohesion: 0.10
Nodes (6): compute_drift(), generate_all(), main(), TestCheckMode, TestGenerateAllValidation, TestConfiguration

### Community 19 - "Airflow Operators Package"
Cohesion: 0.06
Nodes (18): Событие готовности Asset, Контекст, Предпочтительный путь решения, Проблема, Убрать пароль JDBC-источника из аргументов spark-submit, publish_asset_event(), snapshot_has_current_process(), _dagrun_event() (+10 more)

### Community 20 - "Incremental Batch Planning"
Cohesion: 0.14
Nodes (9): _row(), _batch_reason(), _check_batch(), _check_contract(), fail_batch(), _IncrementalConfig, _lock_batch(), _lock_state() (+1 more)

### Community 21 - "Demo Walkthrough Docs"
Cohesion: 0.06
Nodes (41): Batch registry (ingestion/meta/registry.py), Incremental batch, Bootstrap (initial fill), Compare-and-set (CAS) on revision, ingestion.incremental_batches, incremental contract (canonical position type), ingestion.incremental_state, ingestion.incremental_state_migrations (+33 more)

### Community 22 - "Demo Scenarios Docs"
Cohesion: 0.05
Nodes (37): 8. База метаданных pg-meta: остальные объекты, Аудит миграций, Журналы событий, Запросы к метаданным, Последовательность идентификаторов, 5.1. Подготовка показа pg-meta, 5.2. Новый запуск без изменения источника, 5.3. Clear только успешной load-задачи (+29 more)

### Community 23 - "temporal_extraction.py"
Cohesion: 0.12
Nodes (12): check_partitioned_load_path(), check_post_append_retry(), exercise(), jdbc_count(), load_module(), main(), metadata_row(), precision_fixtures() (+4 more)

### Community 24 - "Spec Kit Workflow"
Cohesion: 0.06
Nodes (30): args, inputs, integration, spec, default, prompt, type, any (+22 more)

### Community 25 - "003-module-structure/plan.md"
Cohesion: 0.24
Nodes (9): ingestion/iceberg.py (старый путь), ingestion/meta/ (пакет), ingestion/operators/ (пакет: __init__/process/assets/raw_rdbms), raw_append.py (удаляется), 003-module-structure: задачи, cleanup(), run_runtime_checks(), test.sh script (+1 more)

### Community 26 - "Spec Kit Cycle Instructions"
Cohesion: 0.07
Nodes (29): 10. Обновление Spec Kit, 11. Взаимодействие с graphify и Pull Request, 12. Работа в ZCode Desktop (интерактивно), 1. Назначение, 2. Установка для сопровождающего (выполнена один раз), 3. Инструкция для разработчика, 4. Состояние цикла: каталоги, машины, ветки, 5. Состав артефактов (+21 more)

### Community 27 - "Spec Templates"
Cohesion: 0.11
Nodes (27): contracts/ (interface contracts), data-model.md, plan.md (implementation plan), quickstart.md, research.md, speckit-implement, Completion Report, Done When (+19 more)

### Community 29 - "RDBMS Dialect Adapter"
Cohesion: 0.08
Nodes (8): Контракт диалекта, RdbmsDialect, jdbc_reader(), read_bounds(), check_partitioning(), derived_day_frame(), TestBaseDialectContract, StubKindDialect

### Community 30 - "Position Window Bounds"
Cohesion: 0.06
Nodes (12): canonical(), maximum(), resolve_lookback(), window_bounds(), _plan_batch(), calculate_lower(), window_summary(), literal() (+4 more)

### Community 31 - "Shell Helper Scripts"
Cohesion: 0.16
Nodes (21): check-prerequisites.sh script, check_dir(), check_file(), find_specify_root(), format_speckit_command(), get_current_branch(), get_feature_paths(), get_invoke_separator() (+13 more)

### Community 33 - "iceberg.py"
Cohesion: 0.16
Nodes (3): append_raw(), schema_columns(), main()

### Community 34 - "Spec Kit Checklists"
Cohesion: 0.09
Nodes (26): checklists/requirements.md (spec quality checklist), .specify/extensions.yml — хуки расширений Spec Kit, .specify/init-options.json, speckit-clarify, Completion Report, Done When, Mandatory Post-Execution Hooks, Outline (+18 more)

### Community 35 - "Implementation Plan Template"
Cohesion: 0.08
Nodes (26): Dependencies & Execution Order, Format: `[ID] [P?] [Story] Description`, Implementation for User Story 1, Implementation for User Story 2, Implementation for User Story 3, Implementation Strategy, Incremental Delivery, MVP First (User Story 1 Only) (+18 more)

### Community 36 - "full/load_entity.py"
Cohesion: 0.18
Nodes (9): append.py (raw-layer writer), dds / dma layers (planned), IngestFullOperator, IngestIncrementalOperator, Airflow pool ingest_loads, load (DAG task), ods layer (planned), raw layer (+1 more)

### Community 37 - "Airflow Env Config"
Cohesion: 0.26
Nodes (25): environment, environment, environment, environment, AIRFLOW__API_AUTH__JWT_SECRET, AIRFLOW__API__EXPOSE_CONFIG, AIRFLOW__API__SECRET_KEY, AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION (+17 more)

### Community 38 - "load_settings"
Cohesion: 0.40
Nodes (3): Предпочтительный путь решения, IngestionSettings, load_settings()

### Community 40 - "Nessie Catalog Config"
Cohesion: 0.09
Nodes (23): pg-catalog, nessie.catalog.default-warehouse, nessie.catalog.secrets.access-key.name, nessie.catalog.secrets.access-key.secret, nessie.catalog.service.s3.default-options.access-key, nessie.catalog.service.s3.default-options.auth-type, nessie.catalog.service.s3.default-options.endpoint, nessie.catalog.service.s3.default-options.path-style-access (+15 more)

### Community 41 - "Architecture Documentation"
Cohesion: 0.11
Nodes (21): pg-meta DDL (conf/pg-meta/init/01_ingestion.sql), Glossary (glossary.md), MSSQL dialect (ingestion/sources/rdbms/dialects/mssql.py), Oracle dialect (ingestion/sources/rdbms/dialects/oracle.py), Iceberg writer layer (ingestion/spark/raw/iceberg.py), Full-slice loader (ingestion/spark/raw/rdbms/full/load_entity.py), Incremental loader (ingestion/spark/raw/rdbms/incremental/load_entity.py), Incremental window reader (incremental/window.py) (+13 more)

### Community 42 - "resolve_namespace"
Cohesion: 0.27
Nodes (3): resolve_namespace(), _validate_identifier(), TestResolveNamespace

### Community 43 - "Dev Tooling Documentation"
Cohesion: 0.13
Nodes (17): AGENTS.md, graphify-out/graph.json, graphify (CLI-утилита), AGENTS.md, .specify/feature.json (локальный указатель фичи), opencode, .opencode/commands/, specify CLI (GitHub Spec Kit) (+9 more)

### Community 44 - "Target Name Resolution Tests"
Cohesion: 0.17
Nodes (3): resolve_target_table_name(), _split_fragments(), TestResolveTargetTableName

### Community 45 - "key_metadata"
Cohesion: 0.25
Nodes (5): key_metadata(), KeyMetadata, fake_jvm(), TestCatalogAddressing, TestKeyMetadata

### Community 47 - "Post-Run Verification"
Cohesion: 0.14
Nodes (5): check_registry_recovery(), check_snapshot_publication(), main(), records(), require()

### Community 48 - "Init Services Compose"
Cohesion: 0.11
Nodes (19): nessie, silo-init, trino-init, condition, spark-master, trino, condition, command (+11 more)

### Community 49 - "pg-meta Logging Client"
Cohesion: 0.15
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
Cohesion: 0.20
Nodes (9): ACT_SNAPSHOT_TABLES, Дополнение: пункты 3–5 плана от 2026-11-08, Исправления по третьему ревью, Итоговая проверка, Проверка уточнений после ревью, 001-act-snap-contract: задачи, README.md, Завершение пунктов 3–5 внешнего плана (+1 more)

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
Cohesion: 0.20
Nodes (4): make_args(), make_batch(), TestReadWindowFrame, TestWindowFilters

### Community 63 - "IncrementalEntityConfig"
Cohesion: 0.09
Nodes (26): Назначение задач, Границы рефакторинга, Задача Jira, Контекст, Контекст, Контекст, Предпочтительный путь решения, Проблема (+18 more)

### Community 65 - "DAG Processor Service"
Cohesion: 0.20
Nodes (14): build, command, image, networks, pull_policy, volumes, build, build (+6 more)

### Community 66 - "parse_args"
Cohesion: 0.24
Nodes (3): main(), parse_args(), TestParseArgs

### Community 67 - "TestGenerateDagSource"
Cohesion: 0.09
Nodes (5): generate_dag_source(), PipelineManifest, validate_pipeline(), TestGenerateDagSource, TestValidatePipeline

### Community 69 - "Spec Kit Analyze Skill"
Cohesion: 0.14
Nodes (14): speckit-analyze, 7. Provide Next Actions, 8. Offer Remediation, 9. Check for extension hooks, .specify/extensions.yml (extension hooks registry), Analysis Guidelines, Context, Context Efficiency (+6 more)

### Community 71 - "Act Snap Plan Docs"
Cohesion: 0.10
Nodes (18): Asset (Airflow), ActSnapConfig (dataclass), DagConfig (базовый dataclass), MAX_ACTIVE_RUNS (константа генератора), Дополнительное покрытие пунктов 3–5, Проверка согласованности, PipelineManifest, Завершение проверки контракта (+10 more)

### Community 72 - "parse_args"
Cohesion: 0.11
Nodes (7): main(), managed_load(), parse_args(), build_argv(), TestMain, TestManagedLoad, TestParseArgs

### Community 74 - "Analysis Detection Passes"
Cohesion: 0.15
Nodes (13): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 4. Detection Passes (Token-Efficient Analysis), 5. Severity Assignment, 6. Produce Compact Analysis Report, A. Duplication Detection, B. Ambiguity Detection (+5 more)

### Community 75 - "Named Volumes"
Cohesion: 0.17
Nodes (12): name, name, volumes, airflow_logs, airflow_pg_data, pg_catalog_data, pg_meta_data, silo_data (+4 more)

### Community 76 - "Incremental Config Docs"
Cohesion: 0.17
Nodes (12): 7.1. Контракт, Выбор типа позиции, Выравнивание временной границы, Границы окна, Настройка lookback, Отступ по умолчанию и расписание, Первичное наполнение и проверка источника, Передача конфигурации загрузчику (+4 more)

### Community 77 - "003-module-structure/spec.md"
Cohesion: 0.15
Nodes (12): Готовность фичи, Заметки, Полнота требований, Чек-лист качества спецификации: структура модулей загрузчика, FullEntityConfig, IncrementalEntityConfig, IngestAppendOperator (удаляется), IngestFullOperator (+4 more)

### Community 78 - "by_conn_type"
Cohesion: 0.06
Nodes (15): resolve_source_connection(), connection(), by_conn_type(), by_jdbc_url(), supported_conn_types(), supported_url_prefixes(), source_kind(), source_position() (+7 more)

### Community 79 - "Spec Kit Specify Skill"
Cohesion: 0.17
Nodes (11): Completion Report, Done When, For AI Generation, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, Quick Guidelines, Section Requirements (+3 more)

### Community 80 - "Feature Branch Script"
Cohesion: 0.36
Nodes (9): clean_branch_name(), contains_non_ascii(), fit_branch_name(), generate_branch_name(), get_highest_from_specs(), is_feature_number_in_range(), create-new-feature.sh script, spec_prefix_exists() (+1 more)

### Community 81 - "Meta Connect Tests"
Cohesion: 0.17
Nodes (4): fake_make_dsn(), load_meta_api(), load_operators(), TestMetaConnect

### Community 82 - "ConnectionSpec"
Cohesion: 0.07
Nodes (7): 2. Подключение новой JDBC-СУБД, Проверки совместимости, Регистрация и окружение, ConnectionSpec, MssqlDialect, OracleDialect, TestConnections

### Community 83 - "Postgres Services Compose"
Cohesion: 0.18
Nodes (11): command, depends_on, image, networks, pull_policy, volumes, condition, airflow-postgres (+3 more)

### Community 85 - "Test Runner Service"
Cohesion: 0.18
Nodes (11): AIRFLOW_PASSWORD, AIRFLOW_USER, TRINO_PASSWORD, TRINO_USER, test-runner, build, environment, image (+3 more)

### Community 86 - "Incremental Loading Docs"
Cohesion: 0.31
Nodes (10): 7.2. Жизненный цикл загрузки, 7.3. `incremental_state` — состояние сущности, 7.4. `incremental_batches` — реестр партий, 7.5. Пример одного инкремента и повтора, 7.6. Смена контракта и ручные миграции, 7. Инкрементальная загрузка, meta.finalize_batch(), meta.get_batch() (+2 more)

### Community 87 - "Spec Kit Plan Skill"
Cohesion: 0.18
Nodes (10): Completion Report, Done When, Key rules, Mandatory Post-Execution Hooks, Outline, Phase 0: Outline & Research, Phase 1: Design & Contracts, Phases (+2 more)

### Community 88 - "Spec Kit Tasks Template"
Cohesion: 0.18
Nodes (10): Checklist Format (REQUIRED), Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Phase Structure, Pre-Execution Checks, Task Generation Rules (+2 more)

### Community 89 - "Project Constitution"
Cohesion: 0.18
Nodes (11): I. Граф кодовой базы прежде чтения файлов, II. Изменения подтверждаются тестами, III. Чистый Python без комментариев, IV. Дисциплина Git: ветка → Pull Request → мерж пользователем, V. Перестроение графа после каждого коммита, VI. Документация на русском языке, Базовые принципы, Конституция проекта datalake_stand (+3 more)

### Community 90 - "DATARCH-2244: Isolate Spark drivers via DockerOperator"
Cohesion: 0.22
Nodes (14): conf/spark/spark-defaults.conf, Global settings (ingestion/conf/settings.yaml), Sources registry (ingestion/conf/sources.yaml), Manifest parser (ingestion/generator/manifest.py), load_sources(), Load operators module (ingestion/operators.py), DATARCH-2242: Split sources registry into YAML files, per-source JDBC parallelism, DATARCH-2244: Isolate Spark drivers via DockerOperator (+6 more)

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

### Community 95 - "jdbc_partitioning.py"
Cohesion: 0.23
Nodes (5): reader.py (shared JDBC reader), column_position_kind(), choose(), _family(), _first_candidate()

### Community 98 - "pg-meta DDL Tables"
Cohesion: 0.33
Nodes (8): airflow_dagruns_log_process_id_idx, airflow_dagruns_log_run_idx, airflow_tasks_log_run_idx, ingestion.airflow_dagruns_log, ingestion.airflow_tasks_log, ingestion.incremental_batches, ingestion.incremental_state, ingestion.incremental_state_migrations

### Community 99 - "Sydafa Service Compose"
Cohesion: 0.22
Nodes (9): start_period, sydafa, environment, healthcheck, image, networks, platform, ports (+1 more)

### Community 100 - "4. Таблицы raw-слоя: структура и наполнение"
Cohesion: 0.22
Nodes (9): 4. Таблицы raw-слоя: структура и наполнение, Гарантии записи, Запросы к raw в Trino, Имена и колонки, Как исключить повторные попытки из выборки, Как различать партии и попытки, Ограничение: последняя попытка прочитала пустое окно, Полный срез и инкремент (+1 more)

### Community 101 - "ColumnMeta"
Cohesion: 0.21
Nodes (4): _column_metadata(), ColumnMeta, _optional_int(), TestColumnMetadata

### Community 102 - "TestKeyStructure"
Cohesion: 0.17
Nodes (3): _primary_key(), _unique_single_columns(), TestKeyStructure

### Community 103 - "speckit.analyze.md"
Cohesion: 0.22
Nodes (8): Analysis Guidelines, Context, Context Efficiency, Goal, Operating Constraints, Operating Principles, Pre-Execution Checks, User Input

### Community 104 - "Speckit Converge Phase"
Cohesion: 0.22
Nodes (8): 7. Append Convergence Tasks (or report converged), 8. Provide Next Actions (Handoff), 9. Check for extension hooks, Convergence Findings, Goal, Operating Constraints, Pre-Execution Checks, User Input

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
Cohesion: 0.20
Nodes (9): check_snapshot_recovery(), allocate(), incremental(), projection(), registry(), snapshot(), register_virtual(), virtual_sources() (+1 more)

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
Cohesion: 0.13
Nodes (24): Snapshot stream (incremental/load_act_snap.py), Increment (incremental column contract), Snapshot table (actual snapshot, __act_snap), Bank pipeline manifest (ingestion/conf/pipelines/bank/main.yaml), DAG generator (ingestion/generator/generate_dags.py), generate_dag_source(), generate_snapshot_dag_source(), build_create_table_sql() (+16 more)

### Community 116 - "Настройка graphify — графа кодовой базы"
Cohesion: 0.14
Nodes (10): 1. Назначение, 2. Установка утилиты, 3. Переменные окружения, 4. Перестроение графа, 5. Артефакты `graphify-out/` и git, 6. Запросы по графу, Настройка graphify — графа кодовой базы, Результат настройки (+2 more)

### Community 117 - "Speckit Checklist Phase"
Cohesion: 0.25
Nodes (7): Anti-Examples: What NOT To Do, Checklist Purpose: "Unit Tests for English", Example Checklist Types & Sample Items, Execution Steps, Post-Execution Checks, Pre-Execution Checks, User Input

### Community 118 - "Feature Spec Template"
Cohesion: 0.25
Nodes (7): Assumptions, Feature Specification: [FEATURE NAME], Functional Requirements, Key Entities *(include if feature involves data)*, Measurable Outcomes, Requirements *(mandatory)*, Success Criteria *(mandatory)*

### Community 121 - "6. Параллелизм Spark"
Cohesion: 0.22
Nodes (9): 6.1. Порядок выполнения load-задачи, 6. Параллелизм Spark, JDBC-партиции: несколько запросов к источнику, Как выбирается колонка разбиения, Как читать лог параллельной загрузки, Когда разбиение работает, Несколько загрузок и обработка внутри Spark, Ограничение: данные читаются в разные моменты (+1 more)

### Community 122 - "Sources Configuration File"
Cohesion: 0.29
Nodes (6): bank, airflow_conn_id, type, broker, airflow_conn_id, type

### Community 123 - "first_row"
Cohesion: 0.29
Nodes (3): first_row(), _row_size(), TestFirstRow

### Community 125 - "Архитектура datalake_stand"
Cohesion: 0.25
Nodes (8): 10. Известные ограничения, 1. Слои данных, 9. Типы данных и гарантии, dds / dma (планируется), ods (планируется), raw — данные источника без бизнес-преобразований, Архитектура datalake_stand, Содержание

### Community 126 - "7.7. DAG-и снимков и таблицы-снимки"
Cohesion: 0.29
Nodes (7): 7.7. DAG-и снимков и таблицы-снимки, Автор и время публикации, Атомарная замена данных, Контроль свежести снимков, Повтор после сбоя, Пустое чтение, Структура таблицы-снимка

### Community 128 - "4. Detection Passes (Token-Efficient Analysis)"
Cohesion: 0.29
Nodes (7): 4. Detection Passes (Token-Efficient Analysis), A. Duplication Detection, B. Ambiguity Detection, C. Underspecification, D. Constitution Alignment, E. Coverage Gaps, F. Inconsistency

### Community 129 - "speckit.clarify.md"
Cohesion: 0.29
Nodes (6): Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, User Input

### Community 133 - "Execution Steps"
Cohesion: 0.29
Nodes (7): 1. Initialize Convergence Context, 2. Load Artifacts (Progressive Disclosure), 3. Build the Intent Inventory, 4. Assess the Codebase and Classify Findings, 5. Assign Severity, 6. Present the In-Session Findings Summary, Execution Steps

### Community 134 - "Process ID Generation"
Cohesion: 0.33
Nodes (6): ingestion.airflow_dagruns_log, generate_process_id (Airflow task), GenerateProcessIdOperator, generate_process_id (DAG task), pg-meta metadata database, ingestion.process_id_seq

### Community 135 - "Environment Build Configuration"
Cohesion: 0.33
Nodes (6): conf/airflow/Dockerfile, conf/airflow/requirements.txt, conf/spark/Dockerfile, ingestion/generator/requirements.txt, E2E test suite (test/test.sh), DATARCH-2241: Define and verify minimal supported component versions

### Community 137 - "speckit.implement.md"
Cohesion: 0.29
Nodes (6): Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, User Input

### Community 140 - "apply_partition_options"
Cohesion: 0.22
Nodes (4): apply_partition_options(), bound_literal(), long_representable(), TestBoundLiteral

### Community 142 - "2. Манифесты и генерация DAG-ов"
Cohesion: 0.33
Nodes (6): 2. Манифесты и генерация DAG-ов, Манифест пайплайна, Настройки таблицы источника, Общие настройки и демо-набор, Подготовка DAG-ов, Реестр источников

### Community 143 - "Quick Guidelines"
Cohesion: 0.50
Nodes (4): For AI Generation, Quick Guidelines, Section Requirements, Success Criteria Guidelines

### Community 145 - "load_act_snap.py"
Cohesion: 0.19
Nodes (6): __act_snap auxiliary snapshot, IngestActSnapOperator, quote_identifier(), build_rtas_sql(), _overwrite_snapshot(), TestBuildRtasSql

### Community 146 - "Execution Steps"
Cohesion: 0.33
Nodes (6): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 5. Severity Assignment, 6. Produce Compact Analysis Report, Execution Steps

### Community 147 - "3. Структура сгенерированного DAG-а"
Cohesion: 0.40
Nodes (5): 3. Структура сгенерированного DAG-а, Зависимости задач, Идентификаторы и метаданные, Ограничение числа загрузок, Структура DAG-а снимка

### Community 148 - "test_dialects.py"
Cohesion: 0.06
Nodes (11): __getattr__(), MetadataLocation, parse_source_table(), all_dialects(), TestContractsReExport, TestDayExpression, TestMetadataLocation, TestOracleIntegerKinds (+3 more)

### Community 149 - "Обработка подвисших процессов Spark при retry задач"
Cohesion: 0.33
Nodes (6): Задача Jira, Обработка подвисших процессов Spark при retry задач, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Реализация и граница решения

### Community 150 - "speckit.taskstoissues.md"
Cohesion: 0.33
Nodes (5): Deprecation Notice, Outline, Post-Execution Checks, Pre-Execution Checks, User Input

### Community 151 - "Task Logging Tasks"
Cohesion: 0.67
Nodes (4): ingestion.airflow_tasks_log, log_end (DAG task), log_start (DAG task), TableLogOperator

### Community 152 - "5. Устойчивость к эволюции схемы источника"
Cohesion: 0.40
Nodes (5): 5. Устойчивость к эволюции схемы источника, Новые колонки, Отсутствующие колонки, Порядок колонок, Результат изменения схемы

### Community 153 - "[CHECKLIST TYPE] Checklist: [FEATURE NAME]"
Cohesion: 0.40
Nodes (4): [Category 1], [Category 2], [CHECKLIST TYPE] Checklist: [FEATURE NAME], Notes

### Community 154 - "User Scenarios & Testing *(mandatory)*"
Cohesion: 0.40
Nodes (5): Edge Cases, User Scenarios & Testing *(mandatory)*, User Story 1 - [Brief Title] (Priority: P1), User Story 2 - [Brief Title] (Priority: P2), User Story 3 - [Brief Title] (Priority: P3)

### Community 157 - "Specification Analysis Report"
Cohesion: 0.50
Nodes (4): 7. Provide Next Actions, 8. Offer Remediation, 9. Check for extension hooks, Specification Analysis Report

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
- **102 isolated node(s):** `SYDAFA_CONTAINER_PREFIX`, `pg-catalog — база метаданных Nessie`, `CLI-флаг --incremental-config`, `incremental.contract (YAML: source_column, position_type)`, `ActSnapConfig (dataclass)` (+97 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 1292 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **43 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `services` connect `Docker Compose Layout` to `DAG Processor Service`, `Sydafa Service Compose`, `Nessie Catalog Config`, `Compose Environment Vars`, `Init Services Compose`, `Airflow Services Compose`, `Postgres Services Compose`, `Test Runner Service`, `Silo Service Compose`?**
  _High betweenness centrality (0.149) - this node is a cross-community bridge._
- **Are the 16 inferred relationships involving `RdbmsDialect` (e.g. with `Контекст` and `Предпочтительный путь решения`) actually correct?**
  _`RdbmsDialect` has 16 INFERRED edges - model-reasoned connections that need verification._
- **What connects `SYDAFA_CONTAINER_PREFIX`, `pg-catalog — база метаданных Nessie`, `CLI-флаг --incremental-config` to the rest of the system?**
  _102 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `parse_args` be split into smaller, more focused modules?**
  _Cohesion score 0.06836158192090395 - nodes in this community are weakly interconnected._
- **Why does `README — стенд хранилища и озера данных` connect `Project Agent Guides` to `Architecture Documentation`, `Dev Tooling Documentation`, `Project Documentation`, `Docker Compose Layout`, `Demo Walkthrough Docs`?**
  _High betweenness centrality (0.120) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `generate_all()` (e.g. with `IncrementalEntityConfig` and `SourceConfig`) actually correct?**
  _`generate_all()` has 2 INFERRED edges - model-reasoned connections that need verification._

### Low-confidence Hints
_AMBIGUOUS edges — the extractor was unsure. Verify before acting on these._

- **What is the exact relationship between `tasks-template.md` and `speckit-tasks`?**
  _Edge tagged AMBIGUOUS (relation: references) - confidence is low._