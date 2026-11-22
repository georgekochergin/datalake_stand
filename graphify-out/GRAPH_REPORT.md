# Graph Report - datalake_stand  (2026-11-22)

## Corpus Check
- 190 files · ~157,612 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 14 file(s) not represented in the graph (top: (none) 9, .properties 3, .conf 1)

## Summary
- 3165 nodes · 5738 edges · 217 communities (154 shown, 63 thin omitted)
- Extraction: 95% EXTRACTED · 5% INFERRED · 0% AMBIGUOUS · INFERRED: 285 edges (avg confidence: 0.9)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `f42fea3`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- managed_load
- keys
- make_args
- PostgresTests
- by_conn_type
- test_generate_dags.py
- manifest.py
- Приоритет: можно отложить
- TestBatchLifecycle
- TestSparkGuard
- run_flow.py
- ParallelE2ETests
- Compose Environment Vars
- IngestActSnapOperator
- README — стенд хранилища и озера данных
- load_pipeline
- TestIngestOperator
- make_batch
- temporal_extraction.py
- 001-act-snap-contract/converge.md
- Demo Walkthrough Docs
- check
- FakeCursor
- Spec Kit Workflow
- StandConfigTests
- Spec Kit Cycle Instructions
- speckit-implement
- check_extraction.py
- RdbmsDialect
- canonical
- Shell Helper Scripts
- speckit.converge.md
- guard.py
- speckit-specify
- Implementation Plan Template
- read_extract_frame
- Airflow Env Config
- manifest.py
- check_snapshot_recovery
- Nessie Catalog Config
- Architecture Documentation
- TestActSnapManifest
- Dev Tooling Documentation
- resolve_target_table_name
- key_metadata
- DATARCH-2243: Templatize code in generator/generate_dags.py
- evolution_widen
- Init Services Compose
- RecordingReader
- Airflow Services Compose
- Docker Compose Layout
- Приоритет: важно
- Execution Steps
- Bank Pipeline Manifest
- Broker Pipeline Manifest
- Constitution Templates
- raw_rdbms.py
- TestAppendRaw
- generate_all
- test_no_sys_path.py
- speckit-tasks
- log_step
- Any
- _column_metadata
- DAG Processor Service
- managed_load
- naming.py
- TestKeyStructure
- migrations/__init__.py
- TestSchemaColumns
- Act Snap Plan Docs
- speckit.taskstoissues.md
- meta/__init__.py
- Analysis Detection Passes
- Named Volumes
- 7.1. Контракт
- bound_literal
- Обработка подвисших процессов Spark при retry задач
- build_create_table_sql
- speckit.analyze.md
- ScriptedResult
- Тестирование datalake_stand
- Postgres Services Compose
- process.py
- Test Runner Service
- Спецификация: безопасный повтор Spark-загрузки
- create-new-feature.sh
- speckit.tasks.md
- Project Constitution
- IncrementalEntityConfig
- Silo Service Compose
- IngestFullOperator
- Bank References Manifest
- Broker References Manifest
- 003-module-structure/spec.md
- http
- connection
- pg-meta DDL Tables
- Sydafa Service Compose
- 4. Таблицы raw-слоя: структура и наполнение
- full/load_entity.py
- source_ddl.py
- check_spark_guard.py
- speckit-checklist
- Platform Component Overview
- TestTaskFailure
- base.py
- Ведение документации
- generate_dag_source
- TestIncrementalManifest
- compatibility_projection
- TestColumnProjection
- speckit.plan.md
- parse_args
- ConnectionSpec
- managed_snapshot
- iceberg.py
- Feature Spec Template
- test_operators.py
- test.sh
- Спецификация: упрощение чтения по JDBC и инкрементальной загрузки
- Sources Configuration File
- Настройка graphify — графа кодовой базы
- _apply_entity_fields
- constitution.md
- 7. Инкрементальная загрузка
- Управление ресурсами Spark на уровне отдельной джобы
- check_registry_recovery
- JdbcOperation
- FakePg
- TestIngestActSnapOperator
- TestAirflowIgnore
- 7. Практики прохождения цикла
- Process ID Generation
- validate_target_table
- IncrementalEntityConfig
- parse_source_table
- test_sql_logging.py
- TestQuoting
- get_batch
- TestTableLogOperator
- speckit-plan
- План: callbacks журнала загрузок
- TestGenerateAll
- 6. Параллелизм Spark
- spark-worker
- Диагностика шагов загрузки
- current_snapshot_author
- Сценарий демонстрации стенда
- Архитектура datalake_stand
- Task Logging Tasks
- Глоссарий
- _format_submit_command
- raw/__init__.py
- empty_incremental_window
- 7.7. DAG-и снимков и таблицы-снимки
- Анализ согласованности перед реализацией
- Задачи DATARCH-2245
- Проверка требований DATARCH-2245
- extending.md
- 4. Detection Passes (Token-Efficient Analysis)
- Bucket Init Script
- Auth Init Script
- Source Activation Operator
- Asset Event Callables
- Generator Dependencies
- load_act_snap.py
- Publish Asset Task
- Bootstrap Process
- CAS Revision Control
- TestPgMetaInitSql
- trino_query
- allocate
- 2. Манифесты и генерация DAG-ов
- Execution Steps
- cli_args
- speckit.implement.md
- incremental/load_entity.py
- TestAirflowInit
- 3. Структура сгенерированного DAG-а
- Decimal
- 5. Устойчивость к эволюции схемы источника
- 008-raw-utc-timestamps/plan.md
- 8. База метаданных pg-meta: остальные объекты
- _plan_batch
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
- TestPureHelpers
- jdbc_reader
- Execution Steps
- Проверка требований
- Модель данных
- 006-ingestion-step-logs/plan.md
- 006-ingestion-step-logs/tasks.md
- Определить и проверить минимальные поддерживаемые версии компонентов
- Specification Analysis Report
- TestDagrunCallbacks
- TestResolveFullyQualified
- Convergence Findings
- test_plugin_syntax.py
- Task Generation Rules
- TestOracleIntegerKinds
- TestPositionExpression
- TestContractsReExport
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
8. `managed_load()` - 26 edges
9. `TestLoadPipeline` - 25 edges
10. `ConnectionSpec` - 24 edges

## Surprising Connections (you probably didn't know these)
- `Память executor` --references--> `EntityConfig`  [INFERRED]
  docs/backlog.md → ingestion/generator/manifest.py
- `Решение` --references--> `log_task()`  [INFERRED]
  specs/005-load-lifecycle-callbacks/plan.md → ingestion/meta/__init__.py
- `Выводы` --references--> `log_task()`  [INFERRED]
  specs/005-load-lifecycle-callbacks/research.md → ingestion/meta/__init__.py
- `Требования` --references--> `IngestActSnapOperator`  [INFERRED]
  specs/001-act-snap-contract/spec.md → ingestion/operators/raw_rdbms.py
- `Проблема` --references--> `append_raw()`  [INFERRED]
  docs/backlog.md → ingestion/spark/raw/append.py

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

## Communities (217 total, 63 thin omitted)

### Community 0 - "managed_load"
Cohesion: 0.11
Nodes (6): main(), parse_args(), build_argv(), TestMain, TestParseArgs, TestPlanBatch

### Community 1 - "keys"
Cohesion: 0.09
Nodes (8): no_candidate_reason(), plan(), keys(), meta(), StubKindDialect, TestChoose, TestNoCandidateReason, TestPlan

### Community 5 - "by_conn_type"
Cohesion: 0.09
Nodes (12): 2. Подключение новой JDBC-СУБД, Проверки совместимости, Регистрация и окружение, by_conn_type(), by_jdbc_url(), supported_conn_types(), supported_url_prefixes(), position_select() (+4 more)

### Community 6 - "test_generate_dags.py"
Cohesion: 0.10
Nodes (8): asset_uri(), compute_drift(), main(), snapshot_asset_uri(), _snapshot_entity_block(), 003-module-structure: задачи, TestAssetUri, TestCheckMode

### Community 7 - "manifest.py"
Cohesion: 0.12
Nodes (7): Генерация и настройка, generate_snapshot_dag_source(), ActSnapConfig, AirflowConfig, DagConfig, SparkConfig, TestGenerateSnapshotDagSource

### Community 8 - "Приоритет: можно отложить"
Cohesion: 0.14
Nodes (14): Бэклог, Выбирать JDBC-колонку по метрикам с управляемым повторным анализом, Контекст, Контекст, Оглавление, Предпочтительный путь решения, Предпочтительный путь решения, Предпочтительный путь решения (+6 more)

### Community 9 - "TestBatchLifecycle"
Cohesion: 0.11
Nodes (5): finalize_batch(), start_batch(), BatchTestCase, TestBatchLifecycle, TestPlanBatch

### Community 10 - "TestSparkGuard"
Cohesion: 0.18
Nodes (4): app(), inventory(), load_guard(), TestSparkGuard

### Community 11 - "run_flow.py"
Cohesion: 0.14
Nodes (6): airflow_token(), airflow_token_retry(), check_load_trace(), logged_sql(), snapshot_dag_id(), wake_sources_and_wait_airflow()

### Community 12 - "ParallelE2ETests"
Cohesion: 0.08
Nodes (4): disable_retries(), prepare_workspace(), scale_migration(), ParallelE2ETests

### Community 13 - "Compose Environment Vars"
Cohesion: 0.08
Nodes (36): environment, healthcheck, image, networks, volumes, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, POSTGRES_DB (+28 more)

### Community 14 - "IngestActSnapOperator"
Cohesion: 0.24
Nodes (4): Контекст, _pgmeta_dsn(), run_spark_guard(), _submit_args()

### Community 15 - "README — стенд хранилища и озера данных"
Cohesion: 0.09
Nodes (26): AGENTS.md — инструкции проекта datalake_stand, Скилл backlog — ведение docs/backlog.md, пакет sydafa-airflow (SydafaActivateOperator), airflow/plugins/migrations/ — миграции источников bank/broker, conf/airflow/Dockerfile, conf/airflow/requirements.txt — Airflow Image Dependencies, apache-airflow-providers-apache-spark, apache-airflow-providers-trino (+18 more)

### Community 16 - "load_pipeline"
Cohesion: 0.07
Nodes (12): load_pipeline(), load_sources(), _read_yaml(), temporal_lookback_default(), assert_unique_targets(), TestGenerateAllValidation, TestLoadPipeline, TestLoadSources (+4 more)

### Community 18 - "make_batch"
Cohesion: 0.15
Nodes (5): read_window_frame(), make_args(), make_batch(), TestReadWindowFrame, TestWindowFilters

### Community 19 - "temporal_extraction.py"
Cohesion: 0.11
Nodes (14): ColumnMeta, check_partitioned_load_path(), check_post_append_retry(), check_raw_temporal(), exercise(), jdbc_count(), load_module(), main() (+6 more)

### Community 20 - "001-act-snap-contract/converge.md"
Cohesion: 0.20
Nodes (9): ACT_SNAPSHOT_TABLES, Дополнение: пункты 3–5 плана от 2026-11-08, Исправления по третьему ревью, Итоговая проверка, Проверка уточнений после ревью, 001-act-snap-contract: задачи, README.md, Завершение пунктов 3–5 внешнего плана (+1 more)

### Community 21 - "Demo Walkthrough Docs"
Cohesion: 0.08
Nodes (30): Apache Airflow, Задача generate_process_id, Apache Iceberg, Таблица ingestion.incremental_batches, Таблица ingestion.incremental_state, DAG ingest__rdbms__bank__main, ingest__rdbms__bank__main (generated DAG), load_act_snap.py (Spark-приложение снимка) (+22 more)

### Community 22 - "check"
Cohesion: 0.22
Nodes (6): airflow_task_states(), airflow_wait_run_failed(), check(), evolution_incompatible(), evolution_source_check(), validate_manifests_and_dags()

### Community 24 - "Spec Kit Workflow"
Cohesion: 0.06
Nodes (30): args, inputs, integration, spec, default, prompt, type, any (+22 more)

### Community 26 - "Spec Kit Cycle Instructions"
Cohesion: 0.07
Nodes (29): 10. Обновление Spec Kit, 11. Взаимодействие с graphify и Pull Request, 12. Работа в ZCode Desktop (интерактивно), 1. Назначение, 2. Установка для сопровождающего (выполнена один раз), 3. Инструкция для разработчика, 4. Состояние цикла: каталоги, машины, ветки, 5. Состав артефактов (+21 more)

### Community 27 - "speckit-implement"
Cohesion: 0.08
Nodes (24): Модель состояния guard, Содержание, Состояния, План реализации DATARCH-2245, Подход, Проверка конституции, Содержание, Структура и проверка (+16 more)

### Community 28 - "check_extraction.py"
Cohesion: 0.15
Nodes (6): resolve_source_connection(), check_logged_submit_replay(), check_snapshot_publication(), main(), records(), require()

### Community 29 - "RdbmsDialect"
Cohesion: 0.08
Nodes (10): Контракт диалекта, __getattr__(), RdbmsDialect, column_position_kind(), choose(), _family(), _first_candidate(), position_filter() (+2 more)

### Community 30 - "canonical"
Cohesion: 0.06
Nodes (14): canonical(), _format(), kind_info(), maximum(), _parse(), resolve_lookback(), window_bounds(), calculate_lower() (+6 more)

### Community 31 - "Shell Helper Scripts"
Cohesion: 0.16
Nodes (21): check-prerequisites.sh script, check_dir(), check_file(), find_specify_root(), format_speckit_command(), get_current_branch(), get_feature_paths(), get_invoke_separator() (+13 more)

### Community 32 - "speckit.converge.md"
Cohesion: 0.22
Nodes (8): 7. Append Convergence Tasks (or report converged), 8. Provide Next Actions (Handoff), 9. Check for extension hooks, Convergence Findings, Goal, Operating Constraints, Pre-Execution Checks, User Input

### Community 33 - "guard.py"
Cohesion: 0.13
Nodes (8): _all_applications_completed(), _applications(), _get_master_state(), guard_spark_submit(), _kill_application(), _remaining(), _request(), SparkGuardError

### Community 34 - "speckit-specify"
Cohesion: 0.07
Nodes (30): checklists/requirements.md (spec quality checklist), .specify/extensions.yml — хуки расширений Spec Kit, .specify/init-options.json, speckit-clarify, Completion Report, Done When, Mandatory Post-Execution Hooks, Outline (+22 more)

### Community 35 - "Implementation Plan Template"
Cohesion: 0.08
Nodes (26): Dependencies & Execution Order, Format: `[ID] [P?] [Story] Description`, Implementation for User Story 1, Implementation for User Story 2, Implementation for User Story 3, Implementation Strategy, Incremental Delivery, MVP First (User Story 1 Only) (+18 more)

### Community 36 - "read_extract_frame"
Cohesion: 0.14
Nodes (9): PartitionChoice, probe_upper(), apply_partition_options(), build_extract_query(), inspect_source(), probe_bounds(), read_extract_frame(), ReadSource (+1 more)

### Community 37 - "Airflow Env Config"
Cohesion: 0.26
Nodes (25): environment, environment, environment, environment, AIRFLOW__API_AUTH__JWT_SECRET, AIRFLOW__API__EXPOSE_CONFIG, AIRFLOW__API__SECRET_KEY, AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION (+17 more)

### Community 38 - "manifest.py"
Cohesion: 0.09
Nodes (14): Предпочтительный путь решения, _expand_cron_field(), IngestionSettings, load_settings(), _parse_act_snap(), _parse_dag_fields(), _parse_load_options(), parse_schedule() (+6 more)

### Community 39 - "check_snapshot_recovery"
Cohesion: 0.16
Nodes (9): source_rows(), check_snapshot_recovery(), incremental(), projection(), registry(), snapshot(), register_virtual(), virtual_sources() (+1 more)

### Community 40 - "Nessie Catalog Config"
Cohesion: 0.09
Nodes (23): pg-catalog, nessie.catalog.default-warehouse, nessie.catalog.secrets.access-key.name, nessie.catalog.secrets.access-key.secret, nessie.catalog.service.s3.default-options.access-key, nessie.catalog.service.s3.default-options.auth-type, nessie.catalog.service.s3.default-options.endpoint, nessie.catalog.service.s3.default-options.path-style-access (+15 more)

### Community 41 - "Architecture Documentation"
Cohesion: 0.09
Nodes (27): pg-meta DDL (conf/pg-meta/init/01_ingestion.sql), Glossary (glossary.md), Batch registry (ingestion/meta/registry.py), MSSQL dialect (ingestion/sources/rdbms/dialects/mssql.py), Oracle dialect (ingestion/sources/rdbms/dialects/oracle.py), Iceberg writer layer (ingestion/spark/raw/iceberg.py), Full-slice loader (ingestion/spark/raw/rdbms/full/load_entity.py), Incremental loader (ingestion/spark/raw/rdbms/incremental/load_entity.py) (+19 more)

### Community 43 - "Dev Tooling Documentation"
Cohesion: 0.10
Nodes (21): AGENTS.md, graphify-out/graph.json, graphify (CLI-утилита), AGENTS.md, .specify/feature.json (локальный указатель фичи), opencode, .opencode/commands/, specify CLI (GitHub Spec Kit) (+13 more)

### Community 44 - "resolve_target_table_name"
Cohesion: 0.17
Nodes (3): resolve_target_table_name(), _split_fragments(), TestResolveTargetTableName

### Community 45 - "key_metadata"
Cohesion: 0.08
Nodes (14): connection(), _degraded_metadata(), first_row(), key_metadata(), KeyMetadata, _primary_key(), _row_size(), _unique_single_columns() (+6 more)

### Community 46 - "DATARCH-2243: Templatize code in generator/generate_dags.py"
Cohesion: 0.17
Nodes (16): Snapshot stream (incremental/load_act_snap.py), Increment (incremental column contract), Snapshot table (actual snapshot, __act_snap), build_create_table_sql(), build_rtas_sql(), build_extract_query(), read_bounds(), SQL-файлы и шаблоны (+8 more)

### Community 47 - "evolution_widen"
Cohesion: 0.18
Nodes (7): evolution_ddl(), evolution_first_load(), evolution_host(), evolution_widen(), raw_column_types(), raw_rows_with_process(), run_source_schema_evolution()

### Community 48 - "Init Services Compose"
Cohesion: 0.07
Nodes (30): condition, airflow-webserver, nessie, silo-init, spark-master, spark-worker, sydafa, trino (+22 more)

### Community 50 - "Airflow Services Compose"
Cohesion: 0.12
Nodes (17): command, depends_on, image, networks, pull_policy, volumes, condition, command (+9 more)

### Community 51 - "Docker Compose Layout"
Cohesion: 0.12
Nodes (16): command, image, networks, ports, pull_policy, volumes, name, networks (+8 more)

### Community 52 - "Приоритет: важно"
Cohesion: 0.22
Nodes (9): Задача Jira, Задача Jira, Контекст, Приоритет: важно, Проблема, Проблема, Проверки и критерии готовности, Разделить реестр источников на YAML-файлы и настроить JDBC-параллелизм по источникам (+1 more)

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
Cohesion: 0.15
Nodes (6): dag_id_for(), file_name_for(), generate_all(), snapshot_dag_id(), SourceConfig, TestConfiguration

### Community 60 - "test_no_sys_path.py"
Cohesion: 0.17
Nodes (3): TestNoHashComments, manipulates_sys_path(), TestNoSysPathManipulation

### Community 61 - "speckit-tasks"
Cohesion: 0.07
Nodes (36): contracts/ (interface contracts), plan.md (implementation plan), spec.md (feature specification), tasks.md (task list), $speckit-github-taskstoissues (github extension), speckit-analyze, 7. Provide Next Actions, 8. Offer Remediation (+28 more)

### Community 62 - "log_step"
Cohesion: 0.17
Nodes (5): log_sql(), log_step(), append_raw(), AppendReason, schema_columns()

### Community 63 - "Any"
Cohesion: 0.22
Nodes (7): evolution_prepare_sources(), sydafa_get(), sydafa_migrate(), sydafa_post(), sydafa_request(), sydafa_rule_delete(), sydafa_rule_put()

### Community 64 - "_column_metadata"
Cohesion: 0.19
Nodes (4): _column_metadata(), _exact_pattern(), _optional_int(), TestColumnMetadata

### Community 65 - "DAG Processor Service"
Cohesion: 0.39
Nodes (9): build, build, build, build, context, dockerfile, build, build (+1 more)

### Community 67 - "naming.py"
Cohesion: 0.11
Nodes (8): resolve_fully_qualified(), resolve_namespace(), resolve_snapshot_fully_qualified(), resolve_snapshot_table_name(), snapshot_alias(), _validate_identifier(), TestResolveNamespace, TestSnapshotNames

### Community 70 - "TestSchemaColumns"
Cohesion: 0.15
Nodes (3): fake_pyspark_types(), SchemaDouble, TestSchemaColumns

### Community 71 - "Act Snap Plan Docs"
Cohesion: 0.10
Nodes (18): Asset (Airflow), ActSnapConfig (dataclass), DagConfig (базовый dataclass), MAX_ACTIVE_RUNS (константа генератора), Дополнительное покрытие пунктов 3–5, Проверка согласованности, PipelineManifest, Завершение проверки контракта (+10 more)

### Community 72 - "speckit.taskstoissues.md"
Cohesion: 0.33
Nodes (5): Deprecation Notice, Outline, Post-Execution Checks, Pre-Execution Checks, User Input

### Community 73 - "meta/__init__.py"
Cohesion: 0.11
Nodes (19): allocate_process_id(), DbConnect, DbCursor, DbRow, _execute(), find_process_id(), log_dagrun(), _log_statement() (+11 more)

### Community 74 - "Analysis Detection Passes"
Cohesion: 0.15
Nodes (13): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 4. Detection Passes (Token-Efficient Analysis), 5. Severity Assignment, 6. Produce Compact Analysis Report, A. Duplication Detection, B. Ambiguity Detection (+5 more)

### Community 75 - "Named Volumes"
Cohesion: 0.17
Nodes (12): name, name, volumes, airflow_logs, airflow_pg_data, pg_catalog_data, pg_meta_data, silo_data (+4 more)

### Community 76 - "7.1. Контракт"
Cohesion: 0.17
Nodes (12): 7.1. Контракт, Выбор типа позиции, Выравнивание временной границы, Границы окна, Настройка lookback, Отступ по умолчанию и расписание, Первичное наполнение и проверка источника, Передача конфигурации загрузчику (+4 more)

### Community 78 - "Обработка подвисших процессов Spark при retry задач"
Cohesion: 0.33
Nodes (6): Задача Jira, Обработка подвисших процессов Spark при retry задач, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Реализация и граница решения

### Community 79 - "build_create_table_sql"
Cohesion: 0.19
Nodes (4): build_create_table_sql(), table_location(), TestBuildCreateTableSql, TestTableLocation

### Community 80 - "speckit.analyze.md"
Cohesion: 0.22
Nodes (8): Analysis Guidelines, Context, Context Efficiency, Goal, Operating Constraints, Operating Principles, Pre-Execution Checks, User Input

### Community 82 - "Тестирование datalake_stand"
Cohesion: 0.22
Nodes (8): Изоляция и диагностика, Как расширять тестирование, Порядок запуска и бюджет, Содержание, Состав e2e-проверок, Состав pytest-проверок, Структура и ответственность, Тестирование datalake_stand

### Community 83 - "Postgres Services Compose"
Cohesion: 0.18
Nodes (11): command, depends_on, image, networks, pull_policy, volumes, condition, airflow-postgres (+3 more)

### Community 84 - "process.py"
Cohesion: 0.09
Nodes (12): Назначение задач, Событие готовности Asset, publish_asset_event(), snapshot_has_current_process(), _dagrun_event(), dagrun_failure_callback(), dagrun_success_callback(), GenerateProcessIdOperator (+4 more)

### Community 85 - "Test Runner Service"
Cohesion: 0.20
Nodes (10): AIRFLOW_PASSWORD, AIRFLOW_USER, TRINO_PASSWORD, TRINO_USER, test-runner, environment, image, networks (+2 more)

### Community 86 - "Спецификация: безопасный повтор Spark-загрузки"
Cohesion: 0.17
Nodes (11): US1: повтор после потери процесса задачи (P1), US2: запрет неподтверждённого запуска (P1), US3: предсказуемые повторы и диагностика (P2), Краевые случаи, Критерии успеха, Содержание, Спецификация: безопасный повтор Spark-загрузки, Сущности (+3 more)

### Community 87 - "create-new-feature.sh"
Cohesion: 0.36
Nodes (9): clean_branch_name(), contains_non_ascii(), fit_branch_name(), generate_branch_name(), get_highest_from_specs(), is_feature_number_in_range(), create-new-feature.sh script, spec_prefix_exists() (+1 more)

### Community 88 - "speckit.tasks.md"
Cohesion: 0.18
Nodes (10): Checklist Format (REQUIRED), Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Phase Structure, Pre-Execution Checks, Task Generation Rules (+2 more)

### Community 89 - "Project Constitution"
Cohesion: 0.18
Nodes (11): I. Граф кодовой базы прежде чтения файлов, II. Изменения подтверждаются тестами, III. Чистый Python без комментариев, IV. Дисциплина Git: ветка → Pull Request → мерж пользователем, V. Перестроение графа после каждого коммита, VI. Документация на русском языке, Базовые принципы, Конституция проекта datalake_stand (+3 more)

### Community 90 - "IncrementalEntityConfig"
Cohesion: 0.10
Nodes (4): generate_dag_source(), PipelineManifest, validate_pipeline(), TestGenerateDagSource

### Community 91 - "Silo Service Compose"
Cohesion: 0.20
Nodes (10): silo, AWS_DEFAULT_REGION, silo-init, condition, depends_on, entrypoint, environment, image (+2 more)

### Community 92 - "IngestFullOperator"
Cohesion: 0.11
Nodes (19): Границы рефакторинга, Жизненный цикл, Задача Jira, Изолировать Spark drivers через DockerOperator, Контекст, Контекст, Контракт оператора, Образ и зависимости (+11 more)

### Community 93 - "Bank References Manifest"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 94 - "Broker References Manifest"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 95 - "003-module-structure/spec.md"
Cohesion: 0.15
Nodes (12): Готовность фичи, Заметки, Полнота требований, Чек-лист качества спецификации: структура модулей загрузчика, FullEntityConfig, IncrementalEntityConfig, IngestAppendOperator (удаляется), IngestFullOperator (+4 more)

### Community 96 - "http"
Cohesion: 0.23
Nodes (11): airflow_check_asset_publication(), airflow_task_log(), airflow_trigger(), airflow_trigger_retry(), airflow_unpause(), airflow_wait_dag_registered(), airflow_wait_run(), evolution_load_refs() (+3 more)

### Community 97 - "connection"
Cohesion: 0.20
Nodes (3): connection(), DbConnection, TransactionTests

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
Cohesion: 0.15
Nodes (19): conf/spark/spark-defaults.conf, conf/airflow/Dockerfile, conf/airflow/requirements.txt, conf/spark/Dockerfile, Global settings (ingestion/conf/settings.yaml), Sources registry (ingestion/conf/sources.yaml), load_sources(), ingestion/generator/requirements.txt (+11 more)

### Community 102 - "source_ddl.py"
Cohesion: 0.21
Nodes (6): _connect(), execute(), mssql_dba_connect(), oracle_dba_connect(), query_scalar(), read_dba_secrets()

### Community 103 - "check_spark_guard.py"
Cohesion: 0.14
Nodes (5): main(), main(), orphan(), require(), snapshot()

### Community 104 - "speckit-checklist"
Cohesion: 0.10
Nodes (19): Anti-Examples: What NOT To Do, Checklist Purpose: "Unit Tests for English", Example Checklist Types & Sample Items, Execution Steps, Post-Execution Checks, Pre-Execution Checks, User Input, [Category 1] (+11 more)

### Community 105 - "Platform Component Overview"
Cohesion: 0.33
Nodes (9): Airflow — оркестрация загрузок, JDBC-драйверы MSSQL/Oracle в образах Airflow и Spark, Nessie — каталог Iceberg, pg-catalog — база метаданных Nessie, pg-meta — база журналов и состояния загрузок, silo — MinIO-совместимое S3-хранилище, Spark — вычисления и загрузка по JDBC, sydafa — сервис управления источниками (+1 more)

### Community 107 - "base.py"
Cohesion: 0.09
Nodes (7): MetadataLocation, decimal_kind(), fractional_kind(), integer_kind(), normalize_type_name(), OracleDialect, TestMetadataLocation

### Community 108 - "Ведение документации"
Cohesion: 0.25
Nodes (8): Ведение документации, Когда применять, Ответственность документов, Порядок работы и проверки, Содержание, Термины и глоссарий, Техническая точность, Язык и порядок изложения

### Community 109 - "generate_dag_source"
Cohesion: 0.20
Nodes (12): Bank pipeline manifest (ingestion/conf/pipelines/bank/main.yaml), DAG generator (ingestion/generator/generate_dags.py), generate_dag_source(), generate_snapshot_dag_source(), Manifest parser (ingestion/generator/manifest.py), Backlog: Auto-generate DAG tags and drop duplicates, DATARCH-2243: Templatize code in generator/generate_dags.py, Backlog: Split Airflow pools for loads vs service tasks (+4 more)

### Community 113 - "speckit.plan.md"
Cohesion: 0.22
Nodes (8): Complexity Tracking, Constitution Check, Documentation (this feature), Implementation Plan: [FEATURE], Project Structure, Source Code (repository root), Summary, Technical Context

### Community 114 - "parse_args"
Cohesion: 0.14
Nodes (6): managed_load(), parse_args(), build_argv(), TestMain, TestManagedLoad, TestParseArgs

### Community 115 - "ConnectionSpec"
Cohesion: 0.08
Nodes (5): ConnectionSpec, MssqlDialect, all_dialects(), TestConnections, TestPositionKind

### Community 116 - "managed_snapshot"
Cohesion: 0.26
Nodes (3): managed_snapshot(), FakeCatalog, TestManagedSnapshot

### Community 117 - "iceberg.py"
Cohesion: 0.17
Nodes (9): append.py (raw-layer writer), dds / dma layers (planned), ods layer (planned), raw layer, source schema evolution handling, ingestion/iceberg.py (старый путь), ingestion/meta/ (пакет), ingestion/operators/ (пакет: __init__/process/assets/raw_rdbms) (+1 more)

### Community 118 - "Feature Spec Template"
Cohesion: 0.15
Nodes (12): Assumptions, Edge Cases, Feature Specification: [FEATURE NAME], Functional Requirements, Key Entities *(include if feature involves data)*, Measurable Outcomes, Requirements *(mandatory)*, Success Criteria *(mandatory)* (+4 more)

### Community 119 - "test_operators.py"
Cohesion: 0.06
Nodes (11): fake_make_dsn(), FakeOperator, FakeSkip, load_meta_api(), load_operators(), test_driver_arguments_have_an_explicit_log_display_policy(), test_standard_jdbc_urls_are_visible_for_all_registered_dialects(), TestMetaConnect (+3 more)

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

### Community 124 - "_apply_entity_fields"
Cohesion: 0.29
Nodes (3): _apply_entity_fields(), log_spark_task_event(), _strict_submit_options()

### Community 125 - "constitution.md"
Cohesion: 0.07
Nodes (29): Скилл pull-request — создание Pull Request, graphify-out/graph.json — граф кодовой базы, Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, User Input (+21 more)

### Community 126 - "7. Инкрементальная загрузка"
Cohesion: 0.27
Nodes (11): 7.2. Жизненный цикл загрузки, 7.3. `incremental_state` — состояние сущности, 7.4. `incremental_batches` — реестр партий, 7.5. Пример одного инкремента и повтора, 7.6. Смена контракта и ручные миграции, 7. Инкрементальная загрузка, meta.finalize_batch(), meta.get_batch() (+3 more)

### Community 127 - "Управление ресурсами Spark на уровне отдельной джобы"
Cohesion: 0.25
Nodes (8): Выполнение и обмен данными, Задача Jira, Контекст, Память executor, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Управление ресурсами Spark на уровне отдельной джобы

### Community 128 - "check_registry_recovery"
Cohesion: 0.25
Nodes (4): check_registry_recovery(), arguments(), key(), sql()

### Community 129 - "JdbcOperation"
Cohesion: 0.22
Nodes (3): JdbcOperation, JdbcReadScope, JdbcReason

### Community 133 - "7. Практики прохождения цикла"
Cohesion: 0.22
Nodes (8): US1 — загрузка без обязательного журнала (P1), US2 — повторные попытки (P1), Границы, Критерии успеха, Пользовательские сценарии, Содержание, Спецификация: необязательный журнал загрузок, Требования

### Community 134 - "Process ID Generation"
Cohesion: 0.33
Nodes (6): ingestion.airflow_dagruns_log, generate_process_id (Airflow task), GenerateProcessIdOperator, generate_process_id (DAG task), pg-meta metadata database, ingestion.process_id_seq

### Community 136 - "IncrementalEntityConfig"
Cohesion: 0.20
Nodes (10): Контекст, _entity_block(), load_operator_name(), EntityConfig, FullEntityConfig, IncrementalEntityConfig, _parse_entity(), План (+2 more)

### Community 138 - "test_sql_logging.py"
Cohesion: 0.19
Nodes (8): format_sql_for_log(), restore_sql_blocks(), test_create_table_displays_one_column_per_line_and_separate_clauses(), test_nested_query_keeps_quoted_sql_intact(), test_pg_meta_commit_summary_keeps_only_changed_fields_at_info(), test_rendered_pg_meta_sql_is_not_executed(), test_rendering_failure_leaves_execution_unchanged(), test_sql_block_survives_line_oriented_driver_output()

### Community 142 - "speckit-plan"
Cohesion: 0.18
Nodes (11): speckit-plan, Completion Report, Done When, Key rules, Mandatory Post-Execution Hooks, Outline, Phase 0: Outline & Research, Phase 1: Design & Contracts (+3 more)

### Community 143 - "План: callbacks журнала загрузок"
Cohesion: 0.18
Nodes (8): Конституция, План: callbacks журнала загрузок, Проверки, Решение, Содержание, Выводы, Исследование, Содержание

### Community 145 - "6. Параллелизм Spark"
Cohesion: 0.22
Nodes (9): 6.1. Порядок выполнения load-задачи, 6. Параллелизм Spark, JDBC-партиции: несколько запросов к источнику, Как выбирается колонка разбиения, Как читать лог параллельной загрузки, Когда разбиение работает, Несколько загрузок и обработка внутри Spark, Ограничение: данные читаются в разные моменты (+1 more)

### Community 146 - "spark-worker"
Cohesion: 0.29
Nodes (7): spark-worker, command, depends_on, entrypoint, networks, ports, volumes

### Community 147 - "Диагностика шагов загрузки"
Cohesion: 0.40
Nodes (4): Диагностика шагов загрузки, Приёмка, Требования, Цель

### Community 149 - "Сценарий демонстрации стенда"
Cohesion: 0.06
Nodes (31): 5.1. Подготовка показа pg-meta, 5.2. Новый запуск без изменения источника, 5.3. Clear только успешной load-задачи, 5.4. Полный Clear существующего запуска, 5.5. Новая загрузка после обновления источника, 5.6. Что объяснить про повтор после ошибки, 7.1. Удаление колонки, 7.2. Переименование колонки (+23 more)

### Community 150 - "Архитектура datalake_stand"
Cohesion: 0.25
Nodes (8): 10. Известные ограничения, 1. Слои данных, 9. Типы данных и гарантии, dds / dma (планируется), ods (планируется), raw — данные источника без бизнес-преобразований, Архитектура datalake_stand, Содержание

### Community 151 - "Task Logging Tasks"
Cohesion: 0.67
Nodes (4): ingestion.airflow_tasks_log, log_end (DAG task), log_start (DAG task), TableLogOperator

### Community 152 - "Глоссарий"
Cohesion: 0.25
Nodes (8): Вычисления и ресурсы, Глоссарий, Данные и загрузки, Запуски и повторные попытки, Инкрементальная загрузка, Разработка и проверки, Содержание, Хранение и гарантии

### Community 156 - "7.7. DAG-и снимков и таблицы-снимки"
Cohesion: 0.29
Nodes (7): 7.7. DAG-и снимков и таблицы-снимки, Автор и время публикации, Атомарная замена данных, Контроль свежести снимков, Повтор после сбоя, Пустое чтение, Структура таблицы-снимка

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
Cohesion: 0.07
Nodes (28): 1. Выбор вида изменения, 4. Новый слой данных, 5. Порядок разработки, AGENTS.md, conf/airflow/Dockerfile, conf/spark/Dockerfile, ConnectionSpec, .agents/skills/e2e-testing/SKILL.md (+20 more)

### Community 161 - "4. Detection Passes (Token-Efficient Analysis)"
Cohesion: 0.29
Nodes (7): 4. Detection Passes (Token-Efficient Analysis), A. Duplication Detection, B. Ambiguity Detection, C. Underspecification, D. Constitution Alignment, E. Coverage Gaps, F. Inconsistency

### Community 168 - "load_act_snap.py"
Cohesion: 0.11
Nodes (9): __act_snap auxiliary snapshot, IngestActSnapOperator, quote_identifier(), build_rtas_sql(), main(), _overwrite_snapshot(), parse_args(), TestBuildRtasSql (+1 more)

### Community 173 - "trino_query"
Cohesion: 0.25
Nodes (4): initial_raw_stats(), raw_snapshot_count(), raw_stats(), trino_query()

### Community 175 - "2. Манифесты и генерация DAG-ов"
Cohesion: 0.33
Nodes (6): 2. Манифесты и генерация DAG-ов, Манифест пайплайна, Настройки таблицы источника, Общие настройки и демо-набор, Подготовка DAG-ов, Реестр источников

### Community 176 - "Execution Steps"
Cohesion: 0.29
Nodes (7): 1. Initialize Convergence Context, 2. Load Artifacts (Progressive Disclosure), 3. Build the Intent Inventory, 4. Assess the Codebase and Classify Findings, 5. Assign Severity, 6. Present the In-Session Findings Summary, Execution Steps

### Community 178 - "speckit.implement.md"
Cohesion: 0.29
Nodes (6): Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, User Input

### Community 179 - "incremental/load_entity.py"
Cohesion: 0.14
Nodes (7): IngestFullOperator, IngestIncrementalOperator, Airflow pool ingest_loads, load (DAG task), reader.py (shared JDBC reader), configure_driver_logging(), main()

### Community 181 - "3. Структура сгенерированного DAG-а"
Cohesion: 0.40
Nodes (5): 3. Структура сгенерированного DAG-а, Зависимости задач, Идентификаторы и метаданные, Ограничение числа загрузок, Структура DAG-а снимка

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

### Community 199 - "Execution Steps"
Cohesion: 0.33
Nodes (6): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 5. Severity Assignment, 6. Produce Compact Analysis Report, Execution Steps

### Community 200 - "Проверка требований"
Cohesion: 0.50
Nodes (3): Проверка требований, Результат, Содержание

### Community 201 - "Модель данных"
Cohesion: 0.50
Nodes (3): Журнал, Модель данных, Содержание

### Community 204 - "Определить и проверить минимальные поддерживаемые версии компонентов"
Cohesion: 0.40
Nodes (5): Задача Jira, Контекст, Определить и проверить минимальные поддерживаемые версии компонентов, Предпочтительный путь решения, Проблема

### Community 205 - "Specification Analysis Report"
Cohesion: 0.50
Nodes (4): 7. Provide Next Actions, 8. Offer Remediation, 9. Check for extension hooks, Specification Analysis Report

### Community 208 - "Convergence Findings"
Cohesion: 0.50
Nodes (4): 7. Append Convergence Tasks (or report converged), 8. Provide Next Actions (Handoff), 9. Check for extension hooks, Convergence Findings

### Community 210 - "Task Generation Rules"
Cohesion: 0.50
Nodes (4): Checklist Format (REQUIRED), Phase Structure, Task Generation Rules, Task Organization

## Ambiguous Edges - Review These
- `tasks-template.md` → `speckit-tasks`  [AMBIGUOUS]
  .specify/templates/tasks-template.md · relation: references

## Knowledge Gaps
- **104 isolated node(s):** `TEST_FAILURE_LOG_DIR`, `conf/spark/spark-defaults.conf`, `pg-catalog — база метаданных Nessie`, `ingestion/iceberg.py (старый путь)`, `ingestion/meta/ (пакет)` (+99 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 1529 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **63 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AGENTS.md — инструкции проекта datalake_stand` connect `README — стенд хранилища и озера данных` to `extending.md`, `Architecture Documentation`, `Dev Tooling Documentation`, `constitution.md`?**
  _High betweenness centrality (0.140) - this node is a cross-community bridge._
- **Are the 29 inferred relationships involving `RdbmsDialect` (e.g. with `Контекст` and `Предпочтительный путь решения`) actually correct?**
  _`RdbmsDialect` has 29 INFERRED edges - model-reasoned connections that need verification._
- **What connects `TEST_FAILURE_LOG_DIR`, `conf/spark/spark-defaults.conf`, `pg-catalog — база метаданных Nessie` to the rest of the system?**
  _104 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `managed_load` be split into smaller, more focused modules?**
  _Cohesion score 0.11428571428571428 - nodes in this community are weakly interconnected._
- **Why does `services` connect `Docker Compose Layout` to `Sydafa Service Compose`, `Nessie Catalog Config`, `Compose Environment Vars`, `Init Services Compose`, `Airflow Services Compose`, `Postgres Services Compose`, `spark-worker`, `Test Runner Service`, `Silo Service Compose`?**
  _High betweenness centrality (0.111) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `generate_all()` (e.g. with `IncrementalEntityConfig` and `SourceConfig`) actually correct?**
  _`generate_all()` has 2 INFERRED edges - model-reasoned connections that need verification._

### Low-confidence Hints
_AMBIGUOUS edges — the extractor was unsure. Verify before acting on these._

- **What is the exact relationship between `tasks-template.md` and `speckit-tasks`?**
  _Edge tagged AMBIGUOUS (relation: references) - confidence is low._