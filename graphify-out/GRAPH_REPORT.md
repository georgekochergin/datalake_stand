# Graph Report - datalake_stand  (2026-11-12)

## Corpus Check
- 150 files · ~137,102 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 12 file(s) not represented in the graph (top: (none) 9, .properties 2, .conf 1)

## Summary
- 2814 nodes · 5046 edges · 171 communities (130 shown, 41 thin omitted)
- Extraction: 95% EXTRACTED · 5% INFERRED · 0% AMBIGUOUS · INFERRED: 236 edges (avg confidence: 0.9)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `8d9e2177`
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
- incremental/load_entity.py
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
- Spec Templates
- Fake DB-API Doubles
- RDBMS Dialect Adapter
- canonical
- Shell Helper Scripts
- Scripted JDBC Doubles
- check
- Spec Kit Checklists
- Implementation Plan Template
- Any
- Airflow Env Config
- check_snapshot_recovery
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
- Приоритет: можно отложить
- Schema DDL Mapping Tests
- Spec Kit Tasks Skill
- Code Style Guards
- source_ddl.py
- read_window_frame
- IncrementalEntityConfig
- Watermark
- DAG Processor Service
- test_dialects.py
- TestGenerateDagSource
- Stand Config Tests
- Spec Kit Analyze Skill
- TestAppendRaw
- Act Snap Plan Docs
- parse_args
- Incremental Manifest Tests
- Analysis Detection Passes
- Named Volumes
- source_position
- 003-module-structure/spec.md
- by_jdbc_url
- Spec Kit Specify Skill
- create-new-feature.sh
- Meta Connect Tests
- OracleDialect
- Postgres Services Compose
- TestGenerateAll
- Test Runner Service
- make_batch
- Spec Kit Plan Skill
- Spec Kit Tasks Template
- Project Constitution
- DATARCH-2244: Isolate Spark drivers via DockerOperator
- Silo Service Compose
- MssqlDialect
- Bank References Manifest
- Broker References Manifest
- choose
- Registry Transaction Tests
- Act Snap Operator Tests
- pg-meta DDL Tables
- Sydafa Service Compose
- 4. Таблицы raw-слоя: структура и наполнение
- MetadataLocation
- TestKeyStructure
- speckit.analyze.md
- Speckit Converge Phase
- Platform Component Overview
- migrations/__init__.py
- Airflowignore Scanner Tests
- Ведение документации
- _submit_args
- 002-incremental-config/spec.md
- Spark Job Resources
- depends_on
- Implementation Plan: [FEATURE]
- Batch Revision Fence
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
- evolution_incompatible
- trino_query
- Предпочтительный путь решения
- Шаг 5. Повторный раунд: полный срез против инкремента
- TestPgMetaInitSql
- DAG Run Callbacks
- Asset Publication Policy
- Шаг 7. Стресс-тест: эволюция схемы источника
- Process ID Generation
- Environment Build Configuration
- TestResolveFullyQualified
- bound_literal
- Airflow Connection Initialization
- Temporal Column Rendering
- analysis.md
- Task Failure Logging
- 2. Манифесты и генерация DAG-ов
- by_conn_type
- DAG generator (ingestion/generator/generate_dags.py)
- load_act_snap.py
- conf/airflow/requirements.txt — Airflow Image Dependencies
- Quick Guidelines
- all_dialects
- Обработка подвисших процессов Spark при retry задач
- speckit.taskstoissues.md
- Task Logging Tasks
- 5. Устойчивость к эволюции схемы источника
- TestCatalogPatternFiltering
- FakeOperator
- Task Generation Rules
- TestContractsReExport
- test_plugin_syntax.py
- Phases
- Bucket Init Script
- Auth Init Script
- Source Activation Operator
- Asset Event Callables
- Generator Dependencies
- Publish Asset Task
- Bootstrap Process
- CAS Revision Control
- 8. База метаданных pg-meta: остальные объекты
- Расширение стенда

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

## Communities (171 total, 41 thin omitted)

### Community 0 - "parse_args"
Cohesion: 0.06
Nodes (11): empty_incremental_window(), main(), managed_load(), parse_args(), build_argv(), TestEmptyIncrementalWindow, TestMain, TestManagedLoad (+3 more)

### Community 1 - "JDBC Partition Planning Tests"
Cohesion: 0.07
Nodes (11): day_expression(), no_candidate_reason(), partition_name(), PartitionChoice, plan(), keys(), StubKindDialect, TestChoose (+3 more)

### Community 2 - "Shared JDBC Reader"
Cohesion: 0.07
Nodes (11): apply_partition_options(), build_extract_query(), jdbc_reader(), long_representable(), read_bounds(), read_extract_frame(), make_args(), RecordingReader (+3 more)

### Community 3 - "pathlib"
Cohesion: 0.07
Nodes (3): main(), cli_args(), FakeSkip

### Community 4 - "Pipeline Manifest Loading"
Cohesion: 0.06
Nodes (13): _expand_cron_field(), load_pipeline(), load_sources(), parse_schedule(), temporal_lookback_default(), assert_unique_targets(), TestGenerateAllValidation, TestLoadPipeline (+5 more)

### Community 5 - "manifest.py"
Cohesion: 0.11
Nodes (15): Генерация и настройка, Предпочтительный путь решения, ActSnapConfig, AirflowConfig, DagConfig, IngestionSettings, load_settings(), _parse_act_snap() (+7 more)

### Community 7 - "incremental/load_entity.py"
Cohesion: 0.09
Nodes (5): reader.py (shared JDBC reader), decimal_kind(), fractional_kind(), integer_kind(), normalize_type_name()

### Community 8 - "positions.py"
Cohesion: 0.16
Nodes (4): validate(), kind_info(), _parse(), TestKindInfo

### Community 9 - "Batch Registry Tests"
Cohesion: 0.12
Nodes (5): finalize_batch(), start_batch(), BatchTestCase, TestBatchLifecycle, TestPlanBatch

### Community 10 - "validate_target_table"
Cohesion: 0.06
Nodes (19): append.py (raw-layer writer), dds / dma layers (planned), IngestFullOperator, IngestIncrementalOperator, Airflow pool ingest_loads, load (DAG task), ods layer (planned), raw layer (+11 more)

### Community 11 - "run_flow.py"
Cohesion: 0.16
Nodes (11): airflow_token(), airflow_token_retry(), airflow_trigger(), airflow_trigger_retry(), airflow_unpause(), airflow_wait_dag_registered(), http(), main() (+3 more)

### Community 13 - "Compose Environment Vars"
Cohesion: 0.08
Nodes (36): environment, healthcheck, image, networks, volumes, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, POSTGRES_DB (+28 more)

### Community 14 - "Project Documentation"
Cohesion: 0.10
Nodes (19): AGENTS.md, conf/airflow/Dockerfile, conf/spark/Dockerfile, ConnectionSpec, .agents/skills/e2e-testing/SKILL.md, load_sources(), ingestion/operators/ (пакет), ingestion/conf/pipelines/ (+11 more)

### Community 15 - "Project Agent Guides"
Cohesion: 0.12
Nodes (25): AGENTS.md — инструкции проекта datalake_stand, Скилл backlog — ведение docs/backlog.md, Скилл e2e-testing — сквозное тестирование стенда, Скилл pull-request — создание Pull Request, пакет sydafa-airflow (SydafaActivateOperator), airflow/plugins/migrations/ — миграции источников bank/broker, conf/airflow/Dockerfile, conf/pg-meta/init/ — DDL pg-meta (+17 more)

### Community 16 - "generate_dags.py"
Cohesion: 0.08
Nodes (14): asset_uri(), dag_id_for(), _entity_block(), file_name_for(), load_operator_name(), snapshot_asset_uri(), snapshot_dag_id(), _snapshot_entity_block() (+6 more)

### Community 17 - "Postgres Registry Tests"
Cohesion: 0.09
Nodes (3): 2. Установка утилиты, sql(), PostgresTests

### Community 18 - "generate_all"
Cohesion: 0.12
Nodes (5): compute_drift(), generate_all(), main(), TestCheckMode, TestConfiguration

### Community 19 - "process.py"
Cohesion: 0.09
Nodes (12): Событие готовности Asset, publish_asset_event(), snapshot_has_current_process(), _dagrun_event(), dagrun_failure_callback(), dagrun_success_callback(), GenerateProcessIdOperator, log_task_failed() (+4 more)

### Community 20 - "Incremental Batch Planning"
Cohesion: 0.10
Nodes (11): _row(), _batch_reason(), _check_batch(), _check_contract(), fail_batch(), _incremental_bounds(), _IncrementalConfig, _lock_batch() (+3 more)

### Community 21 - "Demo Walkthrough Docs"
Cohesion: 0.08
Nodes (27): Apache Airflow, Задача generate_process_id, Apache Iceberg, Таблица ingestion.incremental_batches, DAG ingest__rdbms__bank__main, ingest__rdbms__bank__main (generated DAG), pg-meta (реестр загрузок), Apache Spark (+19 more)

### Community 22 - "Сценарий демонстрации стенда"
Cohesion: 0.15
Nodes (13): Независимые настройки снимка, Ограничение числа загрузок, Проверка параллельного чтения, Событие готовности снимка, Содержание, Сценарий демонстрации стенда, Шаг 1. Обновление источников (Airflow UI), Шаг 2. Источники через DBeaver (активация + просмотр) (+5 more)

### Community 23 - "temporal_extraction.py"
Cohesion: 0.11
Nodes (14): ColumnMeta, check_partitioned_load_path(), check_partitioning(), check_post_append_retry(), derived_day_frame(), exercise(), jdbc_count(), load_module() (+6 more)

### Community 24 - "Spec Kit Workflow"
Cohesion: 0.06
Nodes (30): args, inputs, integration, spec, default, prompt, type, any (+22 more)

### Community 25 - "003-module-structure/plan.md"
Cohesion: 0.15
Nodes (13): ACT_SNAPSHOT_TABLES, Дополнение: пункты 3–5 плана от 2026-11-08, Исправления по третьему ревью, Итоговая проверка, Проверка уточнений после ревью, 001-act-snap-contract: задачи, README.md, Завершение пунктов 3–5 внешнего плана (+5 more)

### Community 26 - "Spec Kit Cycle Instructions"
Cohesion: 0.07
Nodes (29): 10. Обновление Spec Kit, 11. Взаимодействие с graphify и Pull Request, 12. Работа в ZCode Desktop (интерактивно), 1. Назначение, 2. Установка для сопровождающего (выполнена один раз), 3. Инструкция для разработчика, 4. Состояние цикла: каталоги, машины, ветки, 5. Состав артефактов (+21 more)

### Community 27 - "Spec Templates"
Cohesion: 0.11
Nodes (27): contracts/ (interface contracts), data-model.md, plan.md (implementation plan), quickstart.md, research.md, speckit-implement, Completion Report, Done When (+19 more)

### Community 28 - "Fake DB-API Doubles"
Cohesion: 0.08
Nodes (3): FakeConnection, FakeCursor, FakePg

### Community 29 - "RDBMS Dialect Adapter"
Cohesion: 0.10
Nodes (4): Контракт диалекта, __getattr__(), RdbmsDialect, TestBaseDialectContract

### Community 30 - "canonical"
Cohesion: 0.07
Nodes (12): canonical(), _format(), maximum(), resolve_lookback(), window_bounds(), _plan_batch(), calculate_lower(), window_summary() (+4 more)

### Community 31 - "Shell Helper Scripts"
Cohesion: 0.16
Nodes (21): check-prerequisites.sh script, check_dir(), check_file(), find_specify_root(), format_speckit_command(), get_current_branch(), get_feature_paths(), get_invoke_separator() (+13 more)

### Community 33 - "check"
Cohesion: 0.18
Nodes (10): airflow_check_asset_publication(), airflow_wait_run(), check(), evolution_ddl(), evolution_first_load(), evolution_load_refs(), evolution_prepare_sources(), evolution_widen() (+2 more)

### Community 34 - "Spec Kit Checklists"
Cohesion: 0.09
Nodes (26): checklists/requirements.md (spec quality checklist), .specify/extensions.yml — хуки расширений Spec Kit, .specify/init-options.json, speckit-clarify, Completion Report, Done When, Mandatory Post-Execution Hooks, Outline (+18 more)

### Community 35 - "Implementation Plan Template"
Cohesion: 0.08
Nodes (26): Dependencies & Execution Order, Format: `[ID] [P?] [Story] Description`, Implementation for User Story 1, Implementation for User Story 2, Implementation for User Story 3, Implementation Strategy, Incremental Delivery, MVP First (User Story 1 Only) (+18 more)

### Community 36 - "Any"
Cohesion: 0.17
Nodes (8): evolution_host(), evolution_source_check(), sydafa_get(), sydafa_migrate(), sydafa_post(), sydafa_request(), sydafa_rule_delete(), sydafa_rule_put()

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
Cohesion: 0.13
Nodes (5): resolve_snapshot_table_name(), resolve_target_table_name(), _split_fragments(), _validate_identifier(), TestResolveTargetTableName

### Community 45 - "key_metadata"
Cohesion: 0.16
Nodes (8): _degraded_metadata(), key_metadata(), KeyMetadata, CatalogDialect, fake_jvm(), TestCatalogAddressing, TestDegradedKeyMetadata, TestKeyMetadata

### Community 47 - "Post-Run Verification"
Cohesion: 0.14
Nodes (5): check_registry_recovery(), check_snapshot_publication(), main(), records(), require()

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

### Community 57 - "Приоритет: можно отложить"
Cohesion: 0.14
Nodes (14): Автоматически формировать теги DAG и исключать дубликаты, Бэклог, Выбирать JDBC-колонку по метрикам с управляемым повторным анализом, Контекст, Контекст, Оглавление, Предпочтительный путь решения, Предпочтительный путь решения (+6 more)

### Community 58 - "Schema DDL Mapping Tests"
Cohesion: 0.15
Nodes (3): fake_pyspark_types(), SchemaDouble, TestSchemaColumns

### Community 59 - "Spec Kit Tasks Skill"
Cohesion: 0.12
Nodes (15): tasks.md (task list), $speckit-github-taskstoissues (github extension), speckit-checklist, Anti-Examples: What NOT To Do, Checklist Purpose: "Unit Tests for English", Example Checklist Types & Sample Items, Execution Steps, Post-Execution Checks (+7 more)

### Community 60 - "Code Style Guards"
Cohesion: 0.17
Nodes (3): TestNoHashComments, manipulates_sys_path(), TestNoSysPathManipulation

### Community 61 - "source_ddl.py"
Cohesion: 0.21
Nodes (6): _connect(), execute(), mssql_dba_connect(), oracle_dba_connect(), query_scalar(), read_dba_secrets()

### Community 62 - "read_window_frame"
Cohesion: 0.24
Nodes (4): read_window_frame(), window_filters(), make_args(), TestReadWindowFrame

### Community 63 - "IncrementalEntityConfig"
Cohesion: 0.10
Nodes (22): Назначение задач, Границы рефакторинга, Задача Jira, Контекст, Контекст, Контекст, Предпочтительный путь решения, Проблема (+14 more)

### Community 64 - "Watermark"
Cohesion: 0.20
Nodes (12): Batch registry (ingestion/meta/registry.py), Incremental batch, Bootstrap (initial fill), Compare-and-set (CAS) on revision, ingestion.incremental_batches, incremental contract (canonical position type), ingestion.incremental_state, ingestion.incremental_state_migrations (+4 more)

### Community 65 - "DAG Processor Service"
Cohesion: 0.20
Nodes (14): build, command, image, networks, pull_policy, volumes, build, build (+6 more)

### Community 66 - "test_dialects.py"
Cohesion: 0.09
Nodes (7): resolve_source_connection(), ConnectionSpec, parse_source_table(), TestColumnProjection, TestConnections, TestOracleIntegerKinds, TestParseSourceTable

### Community 67 - "TestGenerateDagSource"
Cohesion: 0.10
Nodes (4): generate_dag_source(), PipelineManifest, validate_pipeline(), TestGenerateDagSource

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

### Community 76 - "source_position"
Cohesion: 0.05
Nodes (34): 7.1. Контракт, 7.2. Жизненный цикл загрузки, 7.3. `incremental_state` — состояние сущности, 7.4. `incremental_batches` — реестр партий, 7.5. Пример одного инкремента и повтора, 7.6. Смена контракта и ручные миграции, 7.7. DAG-и снимков и таблицы-снимки, 7. Инкрементальная загрузка (+26 more)

### Community 77 - "003-module-structure/spec.md"
Cohesion: 0.14
Nodes (13): At-least-once, Готовность фичи, Заметки, Полнота требований, Чек-лист качества спецификации: структура модулей загрузчика, FullEntityConfig, IncrementalEntityConfig, IngestAppendOperator (удаляется) (+5 more)

### Community 78 - "by_jdbc_url"
Cohesion: 0.16
Nodes (5): by_jdbc_url(), supported_conn_types(), supported_url_prefixes(), source_count(), TestRegistry

### Community 79 - "Spec Kit Specify Skill"
Cohesion: 0.17
Nodes (11): Completion Report, Done When, For AI Generation, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, Quick Guidelines, Section Requirements (+3 more)

### Community 80 - "create-new-feature.sh"
Cohesion: 0.36
Nodes (9): clean_branch_name(), contains_non_ascii(), fit_branch_name(), generate_branch_name(), get_highest_from_specs(), is_feature_number_in_range(), create-new-feature.sh script, spec_prefix_exists() (+1 more)

### Community 81 - "Meta Connect Tests"
Cohesion: 0.13
Nodes (5): fake_make_dsn(), load_meta_api(), load_operators(), TestMetaConnect, TestMetaDouble

### Community 83 - "Postgres Services Compose"
Cohesion: 0.18
Nodes (11): command, depends_on, image, networks, pull_policy, volumes, condition, airflow-postgres (+3 more)

### Community 85 - "Test Runner Service"
Cohesion: 0.18
Nodes (11): AIRFLOW_PASSWORD, AIRFLOW_USER, TRINO_PASSWORD, TRINO_USER, test-runner, build, environment, image (+3 more)

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

### Community 93 - "Bank References Manifest"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 94 - "Broker References Manifest"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 95 - "choose"
Cohesion: 0.24
Nodes (5): column_position_kind(), choose(), _family(), _first_candidate(), source_kind()

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
Cohesion: 0.21
Nodes (3): MetadataLocation, TestMetadataLocation, TestColumnMetadata

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

### Community 108 - "Ведение документации"
Cohesion: 0.25
Nodes (8): Ведение документации, Когда применять, Ответственность документов, Порядок работы и проверки, Содержание, Термины и глоссарий, Техническая точность, Язык и порядок изложения

### Community 109 - "_submit_args"
Cohesion: 0.20
Nodes (6): Контекст, Предпочтительный путь решения, Проблема, Убрать пароль JDBC-источника из аргументов spark-submit, _pgmeta_dsn(), _submit_args()

### Community 110 - "002-incremental-config/spec.md"
Cohesion: 0.22
Nodes (8): Канонический тип позиции, Инкремент (incremental), Позиция инкремента, CLI-флаг --incremental-config, incremental.contract (YAML: source_column, position_type), Контракт инкрементальной загрузки, Требования, Уточнения

### Community 111 - "Spark Job Resources"
Cohesion: 0.25
Nodes (8): Выполнение и обмен данными, Задача Jira, Контекст, Память executor, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Управление ресурсами Spark на уровне отдельной джобы

### Community 112 - "depends_on"
Cohesion: 0.18
Nodes (11): condition, airflow-webserver, spark-master, spark-worker, sydafa, trino, condition, condition (+3 more)

### Community 113 - "Implementation Plan: [FEATURE]"
Cohesion: 0.22
Nodes (8): Complexity Tracking, Constitution Check, Documentation (this feature), Implementation Plan: [FEATURE], Project Structure, Source Code (repository root), Summary, Technical Context

### Community 115 - "DATARCH-2243: Templatize code in generator/generate_dags.py"
Cohesion: 0.16
Nodes (20): Snapshot stream (incremental/load_act_snap.py), Increment (incremental column contract), Snapshot table (actual snapshot, __act_snap), generate_dag_source(), generate_snapshot_dag_source(), build_create_table_sql(), build_rtas_sql(), build_extract_query() (+12 more)

### Community 116 - "managed_snapshot"
Cohesion: 0.15
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

### Community 126 - "evolution_incompatible"
Cohesion: 0.25
Nodes (4): airflow_task_log(), airflow_task_states(), airflow_wait_run_failed(), evolution_incompatible()

### Community 127 - "trino_query"
Cohesion: 0.25
Nodes (4): raw_rows_with_process(), raw_snapshot_count(), raw_stats(), trino_query()

### Community 128 - "Предпочтительный путь решения"
Cohesion: 0.29
Nodes (7): Жизненный цикл, Контракт оператора, Образ и зависимости, Предпочтительный путь решения, Проверки и критерии готовности, Риски и границы решения, Сеть и ресурсы

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

### Community 140 - "analysis.md"
Cohesion: 0.33
Nodes (5): ActSnapConfig (dataclass), DagConfig (базовый dataclass), MAX_ACTIVE_RUNS (константа генератора), Дополнительное покрытие пунктов 3–5, Проверка согласованности

### Community 142 - "2. Манифесты и генерация DAG-ов"
Cohesion: 0.33
Nodes (6): 2. Манифесты и генерация DAG-ов, Манифест пайплайна, Настройки таблицы источника, Общие настройки и демо-набор, Подготовка DAG-ов, Реестр источников

### Community 143 - "by_conn_type"
Cohesion: 0.19
Nodes (5): by_conn_type(), position_select(), render_source_table(), TestRenderSourceTable, TestPositionSelect

### Community 144 - "DAG generator (ingestion/generator/generate_dags.py)"
Cohesion: 0.40
Nodes (5): Bank pipeline manifest (ingestion/conf/pipelines/bank/main.yaml), DAG generator (ingestion/generator/generate_dags.py), Manifest parser (ingestion/generator/manifest.py), Backlog: Auto-generate DAG tags and drop duplicates, Backlog: Split Airflow pools for loads vs service tasks

### Community 145 - "load_act_snap.py"
Cohesion: 0.09
Nodes (10): __act_snap auxiliary snapshot, IngestActSnapOperator, quote_identifier(), build_rtas_sql(), main(), _overwrite_snapshot(), parse_args(), TestBuildRtasSql (+2 more)

### Community 146 - "conf/airflow/requirements.txt — Airflow Image Dependencies"
Cohesion: 0.50
Nodes (4): conf/airflow/requirements.txt — Airflow Image Dependencies, apache-airflow-providers-apache-spark, apache-airflow-providers-trino, sydafa-airflow — Airflow Integration Package (SydafaActivateOperator, SydafaMigrateOperator)

### Community 147 - "Quick Guidelines"
Cohesion: 0.50
Nodes (4): For AI Generation, Quick Guidelines, Section Requirements, Success Criteria Guidelines

### Community 148 - "all_dialects"
Cohesion: 0.17
Nodes (3): all_dialects(), TestDayExpression, TestPositionKind

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

### Community 155 - "TestCatalogPatternFiltering"
Cohesion: 0.21
Nodes (4): _column_metadata(), _exact_pattern(), _optional_int(), TestCatalogPatternFiltering

### Community 158 - "Task Generation Rules"
Cohesion: 0.50
Nodes (4): Checklist Format (REQUIRED), Phase Structure, Task Generation Rules, Task Organization

### Community 161 - "Phases"
Cohesion: 0.67
Nodes (3): Phase 0: Outline & Research, Phase 1: Design & Contracts, Phases

### Community 173 - "8. База метаданных pg-meta: остальные объекты"
Cohesion: 0.18
Nodes (10): 8. База метаданных pg-meta: остальные объекты, Аудит миграций, Журналы событий, Запросы к метаданным, Последовательность идентификаторов, Запуск и подключения, Исходное состояние, Подготовка (+2 more)

### Community 174 - "Расширение стенда"
Cohesion: 0.25
Nodes (8): 1. Выбор вида изменения, 2. Подключение новой JDBC-СУБД, 4. Новый слой данных, 5. Порядок разработки, Проверки совместимости, Расширение стенда, Регистрация и окружение, Содержание

## Ambiguous Edges - Review These
- `tasks-template.md` → `speckit-tasks`  [AMBIGUOUS]
  .specify/templates/tasks-template.md · relation: references

## Knowledge Gaps
- **102 isolated node(s):** `SYDAFA_CONTAINER_PREFIX`, `pg-catalog — база метаданных Nessie`, `conf/spark/spark-defaults.conf`, `Backlog: Split Airflow pools for loads vs service tasks`, `.specify/memory/constitution.md (project constitution)` (+97 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 1320 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **41 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `services` connect `Docker Compose Layout` to `DAG Processor Service`, `Sydafa Service Compose`, `Nessie Catalog Config`, `Compose Environment Vars`, `Init Services Compose`, `Airflow Services Compose`, `Postgres Services Compose`, `Test Runner Service`, `Silo Service Compose`?**
  _High betweenness centrality (0.149) - this node is a cross-community bridge._
- **Are the 16 inferred relationships involving `RdbmsDialect` (e.g. with `Контекст` and `Предпочтительный путь решения`) actually correct?**
  _`RdbmsDialect` has 16 INFERRED edges - model-reasoned connections that need verification._
- **What connects `SYDAFA_CONTAINER_PREFIX`, `pg-catalog — база метаданных Nessie`, `conf/spark/spark-defaults.conf` to the rest of the system?**
  _102 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `parse_args` be split into smaller, more focused modules?**
  _Cohesion score 0.056140350877192984 - nodes in this community are weakly interconnected._
- **Why does `README — стенд хранилища и озера данных` connect `Project Agent Guides` to `Architecture Documentation`, `Dev Tooling Documentation`, `Project Documentation`, `conf/airflow/requirements.txt — Airflow Image Dependencies`, `Docker Compose Layout`, `Demo Walkthrough Docs`?**
  _High betweenness centrality (0.138) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `generate_all()` (e.g. with `IncrementalEntityConfig` and `SourceConfig`) actually correct?**
  _`generate_all()` has 2 INFERRED edges - model-reasoned connections that need verification._

### Low-confidence Hints
_AMBIGUOUS edges — the extractor was unsure. Verify before acting on these._

- **What is the exact relationship between `tasks-template.md` and `speckit-tasks`?**
  _Edge tagged AMBIGUOUS (relation: references) - confidence is low._