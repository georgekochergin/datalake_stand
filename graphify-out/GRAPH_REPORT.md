# Graph Report - datalake-pr70  (2026-11-18)

## Corpus Check
- 178 files · ~150,652 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 14 file(s) not represented in the graph (top: (none) 10, .properties 2, .conf 1)

## Summary
- 3052 nodes · 5405 edges · 207 communities (154 shown, 53 thin omitted)
- Extraction: 95% EXTRACTED · 4% INFERRED · 0% AMBIGUOUS · INFERRED: 243 edges (avg confidence: 0.9)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `155a9bf5`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- parse_args
- keys
- make_args
- PostgresTests
- test_generate_dags.py
- validate_target_table
- manifest.py
- Приоритет: можно отложить
- .plan
- TestSparkGuard
- run_flow.py
- ParallelE2ETests
- silo
- extending.md
- AGENTS.md — инструкции проекта datalake_stand
- load_pipeline
- TestIngestOperator
- read_window_frame
- base.py
- 001-act-snap-contract/converge.md
- demo.md
- managed_load
- TestParseArgs
- steps
- load_operators
- Настройка GitHub Spec Kit — спецификации изменений
- speckit-plan
- check_extraction.py
- RdbmsDialect
- canonical
- common.sh
- operators/__init__.py
- guard.py
- speckit-specify
- Tasks: [FEATURE NAME]
- choose
- environment
- _overwrite_snapshot
- check_snapshot_recovery
- environment
- architecture.md
- TestActSnapManifest
- speckit.md
- resolve_target_table_name
- key_metadata
- TestKeyStructure
- PartitionChoice
- spark-master
- raw_rdbms.py
- airflow-webserver
- services
- Приоритет: важно
- Execution Steps
- entities
- entities
- speckit.constitution.md
- DATARCH-2243: Templatize code in generator/generate_dags.py
- TestAppendRaw
- generate_all
- test_no_sys_path.py
- bound_literal
- _dagrun_event
- IngestFullOperator
- speckit.converge.md
- airflow-scheduler
- by_conn_type
- generate_dags.py
- Snapshot table (actual snapshot, __act_snap)
- speckit-tasks
- TestSchemaColumns
- 001-act-snap-contract/analysis.md
- constitution.md
- log_task
- 4. Detection Passes (Token-Efficient Analysis)
- volumes
- 7.1. Контракт
- Чек-лист качества спецификации: структура модулей загрузчика
- test_dialects.py
- create-new-feature.sh
- speckit.analyze.md
- ScriptedResult
- Тестирование datalake_stand
- airflow-init
- FakeConnection
- test-runner
- Спецификация: безопасный повтор Spark-загрузки
- speckit.plan.md
- speckit.tasks.md
- Базовые принципы
- TestGenerateDagSource
- silo-init
- Предпочтительный путь решения
- entities
- entities
- StandConfigTests
- connection
- TestIncrementalManifest
- 01_ingestion.sql
- sydafa
- 4. Таблицы raw-слоя: структура и наполнение
- DATARCH-2244: Isolate Spark drivers via DockerOperator
- OracleDialect
- migrations/__init__.py
- FakeCursor
- Spark — вычисления и загрузка по JDBC
- asset_uri
- .columns
- Ведение документации
- plan
- meta/registry.py
- Watermark
- depends_on
- Implementation Plan: [FEATURE]
- parse_args
- MetadataLocation
- managed_snapshot
- speckit-checklist
- Feature Specification: [FEATURE NAME]
- TestPureHelpers
- test.sh
- 6. Параллелизм Spark
- sources.yaml
- Настройка graphify — графа кодовой базы
- TestManifestShapeRejections
- Архитектура datalake_stand
- 7. Инкрементальная загрузка
- Управление ресурсами Spark на уровне отдельной джобы
- Шаблонизация кода в generator/generate_dags.py
- by_jdbc_url
- TestGenerateAll
- TestIngestActSnapOperator
- TestAirflowIgnore
- Спецификация: необязательный журнал загрузок
- generate_process_id (DAG task)
- DATARCH-2241: Define and verify minimal supported component versions
- current_snapshot_author
- Шаг 7. Стресс-тест: эволюция схемы источника
- check_spark_guard.py
- TestQuoting
- Глоссарий
- TestTaskLifecycleCallbacks
- 2. Манифесты и генерация DAG-ов
- Обработка подвисших процессов Spark при retry задач
- DAG generator (ingestion/generator/generate_dags.py)
- first_row
- empty_incremental_window
- ColumnMeta
- 7.7. DAG-и снимков и таблицы-снимки
- Сценарий демонстрации стенда
- assert_unique_targets
- ingestion.airflow_tasks_log
- 5. Устойчивость к эволюции схемы источника
- compatibility_projection
- no_candidate_reason
- _apply_entity_fields
- Execution Steps
- Анализ согласованности перед реализацией
- Задачи DATARCH-2245
- Проверка требований DATARCH-2245
- resolve_source_connection
- speckit.implement.md
- init-bucket.sh
- init-auth.sh
- activate_source (DAG task)
- publish_asset_event (callable)
- ingestion/generator/requirements.txt — DAG Generator Dependencies
- 002-incremental-config/spec.md
- publish_asset (DAG task)
- Bootstrap
- CAS / CAS revision
- 4. Detection Passes (Token-Efficient Analysis)
- append.py (raw-layer writer)
- _column_metadata
- TestDagrunCallbacks
- incremental/load_entity.py
- TestPublishAsset
- 3. Структура сгенерированного DAG-а
- Execution Steps
- TestAirflowInit
- TestPositionExpression
- TestTaskFailure
- TestPgMetaInitSql
- Specification Analysis Report
- TestContractsReExport
- ConnectionSpec
- Выполнено
- План: параллельный e2e
- Спецификация: параллельный e2e за семь минут
- Задачи: параллельный e2e
- Анализ согласованности
- 8. База метаданных pg-meta: остальные объекты
- Расширение стенда
- Контракт callbacks
- Проверка
- Задачи
- StubKindDialect
- test_plugin_syntax.py
- conf/airflow/requirements.txt — Airflow Image Dependencies
- Проверка требований
- Модель данных
- FakeOperator
- FakeSkip
- TestMetaConnect
- TestMetaDouble
- meta

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
- `Требования` --references--> `IngestActSnapOperator`  [INFERRED]
  specs/001-act-snap-contract/spec.md → ingestion/operators/raw_rdbms.py
- `Проблема` --references--> `append_raw()`  [INFERRED]
  docs/backlog.md → ingestion/spark/raw/append.py
- `Первичное наполнение и проверка источника` --references--> `source_position()`  [INFERRED]
  docs/architecture.md → ingestion/spark/raw/rdbms/incremental/load_entity.py
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

## Communities (207 total, 53 thin omitted)

### Community 0 - "parse_args"
Cohesion: 0.08
Nodes (9): connection(), main(), parse_args(), source_position(), build_argv(), TestMain, TestParseArgs, TestPlanBatch (+1 more)

### Community 2 - "make_args"
Cohesion: 0.08
Nodes (8): build_extract_query(), jdbc_reader(), read_extract_frame(), make_args(), RecordingReader, TestJdbcReader, TestReadBounds, TestReadExtractFrame

### Community 5 - "test_generate_dags.py"
Cohesion: 0.11
Nodes (8): Генерация и настройка, generate_snapshot_dag_source(), ActSnapConfig, AirflowConfig, DagConfig, IncrementalEntityConfig, SparkConfig, TestGenerateSnapshotDagSource

### Community 6 - "validate_target_table"
Cohesion: 0.07
Nodes (9): append_raw(), build_create_table_sql(), schema_columns(), table_location(), validate_target_table(), main(), TestBuildCreateTableSql, TestTableLocation (+1 more)

### Community 7 - "manifest.py"
Cohesion: 0.10
Nodes (13): _expand_cron_field(), IngestionSettings, load_settings(), _parse_act_snap(), _parse_dag_fields(), _parse_entity(), _parse_load_options(), parse_schedule() (+5 more)

### Community 8 - "Приоритет: можно отложить"
Cohesion: 0.12
Nodes (17): Автоматически формировать теги DAG и исключать дубликаты, Бэклог, Выбирать JDBC-колонку по метрикам с управляемым повторным анализом, Контекст, Контекст, Оглавление, Предпочтительный путь решения, Предпочтительный путь решения (+9 more)

### Community 9 - ".plan"
Cohesion: 0.12
Nodes (5): finalize_batch(), start_batch(), BatchTestCase, TestBatchLifecycle, TestPlanBatch

### Community 10 - "TestSparkGuard"
Cohesion: 0.18
Nodes (4): app(), inventory(), load_guard(), TestSparkGuard

### Community 11 - "run_flow.py"
Cohesion: 0.05
Nodes (45): airflow_check_asset_publication(), airflow_task_log(), airflow_task_states(), airflow_token(), airflow_token_retry(), airflow_trigger(), airflow_trigger_retry(), airflow_unpause() (+37 more)

### Community 12 - "ParallelE2ETests"
Cohesion: 0.09
Nodes (4): scale_migration(), allocate(), ParallelE2ETests, token()

### Community 13 - "silo"
Cohesion: 0.08
Nodes (36): environment, healthcheck, image, networks, volumes, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, POSTGRES_DB (+28 more)

### Community 14 - "extending.md"
Cohesion: 0.08
Nodes (26): AGENTS.md, conf/airflow/Dockerfile, conf/spark/Dockerfile, ConnectionSpec, .agents/skills/e2e-testing/SKILL.md, load_sources(), ingestion/operators/ (пакет), ingestion/conf/pipelines/ (+18 more)

### Community 15 - "AGENTS.md — инструкции проекта datalake_stand"
Cohesion: 0.10
Nodes (22): AGENTS.md — инструкции проекта datalake_stand, Скилл backlog — ведение docs/backlog.md, пакет sydafa-airflow (SydafaActivateOperator), airflow/plugins/migrations/ — миграции источников bank/broker, conf/airflow/Dockerfile, conf/pg-meta/init/ — DDL pg-meta, conf/spark/Dockerfile, conf/sydafa/sources.json — конфигурация источников (+14 more)

### Community 16 - "load_pipeline"
Cohesion: 0.16
Nodes (5): load_pipeline(), load_sources(), TestLoadPipeline, TestLoadSources, write_temp_yaml()

### Community 18 - "read_window_frame"
Cohesion: 0.16
Nodes (6): read_window_frame(), window_filters(), make_args(), make_batch(), TestReadWindowFrame, TestWindowFilters

### Community 19 - "base.py"
Cohesion: 0.16
Nodes (5): __getattr__(), decimal_kind(), fractional_kind(), integer_kind(), normalize_type_name()

### Community 20 - "001-act-snap-contract/converge.md"
Cohesion: 0.20
Nodes (9): ACT_SNAPSHOT_TABLES, Дополнение: пункты 3–5 плана от 2026-11-08, Исправления по третьему ревью, Итоговая проверка, Проверка уточнений после ревью, 001-act-snap-contract: задачи, README.md, Завершение пунктов 3–5 внешнего плана (+1 more)

### Community 21 - "demo.md"
Cohesion: 0.07
Nodes (31): Apache Airflow, Задача generate_process_id, Apache Iceberg, Таблица ingestion.incremental_batches, DAG ingest__rdbms__bank__main, ingest__rdbms__bank__main (generated DAG), load_act_snap.py (Spark-приложение снимка), pg-meta (реестр загрузок) (+23 more)

### Community 24 - "steps"
Cohesion: 0.06
Nodes (30): args, inputs, integration, spec, default, prompt, type, any (+22 more)

### Community 25 - "load_operators"
Cohesion: 0.15
Nodes (4): fake_make_dsn(), load_meta_api(), load_operators(), TestGuardOperatorWiring

### Community 26 - "Настройка GitHub Spec Kit — спецификации изменений"
Cohesion: 0.07
Nodes (29): 10. Обновление Spec Kit, 11. Взаимодействие с graphify и Pull Request, 12. Работа в ZCode Desktop (интерактивно), 1. Назначение, 2. Установка для сопровождающего (выполнена один раз), 3. Инструкция для разработчика, 4. Состояние цикла: каталоги, машины, ветки, 5. Состав артефактов (+21 more)

### Community 27 - "speckit-plan"
Cohesion: 0.07
Nodes (25): План реализации DATARCH-2245, Подход, Проверка конституции, Содержание, Структура и проверка, Технический контекст, Команды, Приёмка (+17 more)

### Community 28 - "check_extraction.py"
Cohesion: 0.11
Nodes (8): check_registry_recovery(), arguments(), key(), check_snapshot_publication(), main(), records(), require(), sql()

### Community 29 - "RdbmsDialect"
Cohesion: 0.10
Nodes (5): Контракт диалекта, RdbmsDialect, check_partitioning(), source_count(), TestBaseDialectContract

### Community 30 - "canonical"
Cohesion: 0.05
Nodes (15): canonical(), _format(), kind_info(), maximum(), _parse(), resolve_lookback(), window_bounds(), _plan_batch() (+7 more)

### Community 31 - "common.sh"
Cohesion: 0.16
Nodes (21): check-prerequisites.sh script, check_dir(), check_file(), find_specify_root(), format_speckit_command(), get_current_branch(), get_feature_paths(), get_invoke_separator() (+13 more)

### Community 32 - "operators/__init__.py"
Cohesion: 0.25
Nodes (4): Назначение задач, Событие готовности Asset, publish_asset_event(), snapshot_has_current_process()

### Community 33 - "guard.py"
Cohesion: 0.17
Nodes (8): _all_applications_completed(), _applications(), _get_master_state(), guard_spark_submit(), _kill_application(), _remaining(), _request(), SparkGuardError

### Community 34 - "speckit-specify"
Cohesion: 0.07
Nodes (30): checklists/requirements.md (spec quality checklist), .specify/extensions.yml — хуки расширений Spec Kit, .specify/init-options.json, speckit-clarify, Completion Report, Done When, Mandatory Post-Execution Hooks, Outline (+22 more)

### Community 35 - "Tasks: [FEATURE NAME]"
Cohesion: 0.08
Nodes (26): Dependencies & Execution Order, Format: `[ID] [P?] [Story] Description`, Implementation for User Story 1, Implementation for User Story 2, Implementation for User Story 3, Implementation Strategy, Incremental Delivery, MVP First (User Story 1 Only) (+18 more)

### Community 36 - "choose"
Cohesion: 0.24
Nodes (5): column_position_kind(), choose(), _family(), _first_candidate(), source_kind()

### Community 37 - "environment"
Cohesion: 0.26
Nodes (25): environment, environment, environment, environment, AIRFLOW__API_AUTH__JWT_SECRET, AIRFLOW__API__EXPOSE_CONFIG, AIRFLOW__API__SECRET_KEY, AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION (+17 more)

### Community 38 - "_overwrite_snapshot"
Cohesion: 0.13
Nodes (6): quote_identifier(), build_rtas_sql(), _overwrite_snapshot(), cli_args(), TestBuildRtasSql, TestOverwriteSnapshot

### Community 39 - "check_snapshot_recovery"
Cohesion: 0.10
Nodes (16): check_post_append_retry(), check_snapshot_recovery(), incremental(), projection(), registry(), snapshot(), exercise(), load_module() (+8 more)

### Community 40 - "environment"
Cohesion: 0.09
Nodes (23): pg-catalog, nessie.catalog.default-warehouse, nessie.catalog.secrets.access-key.name, nessie.catalog.secrets.access-key.secret, nessie.catalog.service.s3.default-options.access-key, nessie.catalog.service.s3.default-options.auth-type, nessie.catalog.service.s3.default-options.endpoint, nessie.catalog.service.s3.default-options.path-style-access (+15 more)

### Community 41 - "architecture.md"
Cohesion: 0.11
Nodes (21): pg-meta DDL (conf/pg-meta/init/01_ingestion.sql), Glossary (glossary.md), MSSQL dialect (ingestion/sources/rdbms/dialects/mssql.py), Oracle dialect (ingestion/sources/rdbms/dialects/oracle.py), Iceberg writer layer (ingestion/spark/raw/iceberg.py), Full-slice loader (ingestion/spark/raw/rdbms/full/load_entity.py), Incremental loader (ingestion/spark/raw/rdbms/incremental/load_entity.py), Incremental window reader (incremental/window.py) (+13 more)

### Community 43 - "speckit.md"
Cohesion: 0.12
Nodes (18): AGENTS.md, graphify-out/graph.json, graphify (CLI-утилита), AGENTS.md, .specify/feature.json (локальный указатель фичи), opencode, .opencode/commands/, specify CLI (GitHub Spec Kit) (+10 more)

### Community 44 - "resolve_target_table_name"
Cohesion: 0.17
Nodes (3): resolve_target_table_name(), _split_fragments(), TestResolveTargetTableName

### Community 45 - "key_metadata"
Cohesion: 0.17
Nodes (7): _degraded_metadata(), key_metadata(), KeyMetadata, fake_jvm(), TestCatalogAddressing, TestDegradedKeyMetadata, TestKeyMetadata

### Community 46 - "TestKeyStructure"
Cohesion: 0.17
Nodes (3): _primary_key(), _unique_single_columns(), TestKeyStructure

### Community 47 - "PartitionChoice"
Cohesion: 0.16
Nodes (7): day_expression(), partition_name(), PartitionChoice, apply_partition_options(), long_representable(), derived_day_frame(), TestDayExpression

### Community 48 - "spark-master"
Cohesion: 0.11
Nodes (19): nessie, silo-init, trino-init, condition, spark-master, trino, condition, command (+11 more)

### Community 49 - "raw_rdbms.py"
Cohesion: 0.15
Nodes (4): log_task_event(), _pull_process_id(), _validate_process_id(), SparkGuardStatus

### Community 50 - "airflow-webserver"
Cohesion: 0.11
Nodes (18): command, depends_on, image, networks, pull_policy, volumes, condition, depends_on (+10 more)

### Community 51 - "services"
Cohesion: 0.12
Nodes (16): name, networks, dlh-poc, services, spark-worker, trino-init, command, depends_on (+8 more)

### Community 52 - "Приоритет: важно"
Cohesion: 0.22
Nodes (9): Задача Jira, Задача Jira, Контекст, Определить и проверить минимальные поддерживаемые версии компонентов, Предпочтительный путь решения, Приоритет: важно, Проблема, Проблема (+1 more)

### Community 53 - "Execution Steps"
Cohesion: 0.16
Nodes (11): 1. Initialize Convergence Context, 2. Load Artifacts (Progressive Disclosure), 3. Build the Intent Inventory, 4. Assess the Codebase and Classify Findings, 5. Assign Severity, 6. Present the In-Session Findings Summary, 7. Append Convergence Tasks (or report converged), 8. Provide Next Actions (Handoff) (+3 more)

### Community 54 - "entities"
Cohesion: 0.12
Nodes (16): airflow, pool, position_type, source_column, entities, airflow, fqn, incremental (+8 more)

### Community 55 - "entities"
Cohesion: 0.12
Nodes (16): airflow, pool, position_type, source_column, entities, airflow, fqn, incremental (+8 more)

### Community 56 - "speckit.constitution.md"
Cohesion: 0.12
Nodes (15): Outline, Post-Execution Checks, Pre-Execution Checks, Scope Guard, User Input, Core Principles, Governance, [PRINCIPLE_1_NAME] (+7 more)

### Community 57 - "DATARCH-2243: Templatize code in generator/generate_dags.py"
Cohesion: 0.40
Nodes (10): generate_dag_source(), generate_snapshot_dag_source(), build_create_table_sql(), build_rtas_sql(), build_extract_query(), read_bounds(), DATARCH-2243: Templatize code in generator/generate_dags.py, SQL-файлы и шаблоны (+2 more)

### Community 59 - "generate_all"
Cohesion: 0.10
Nodes (6): compute_drift(), generate_all(), main(), TestCheckMode, TestGenerateAllValidation, TestConfiguration

### Community 60 - "test_no_sys_path.py"
Cohesion: 0.17
Nodes (3): TestNoHashComments, manipulates_sys_path(), TestNoSysPathManipulation

### Community 62 - "_dagrun_event"
Cohesion: 0.15
Nodes (5): _dagrun_event(), dagrun_failure_callback(), dagrun_success_callback(), GenerateProcessIdOperator, _meta_connect()

### Community 63 - "IngestFullOperator"
Cohesion: 0.13
Nodes (19): Границы рефакторинга, Задача Jira, Контекст, Контекст, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Рефакторинг и переорганизация кода: структура модулей (+11 more)

### Community 64 - "speckit.converge.md"
Cohesion: 0.22
Nodes (8): 7. Append Convergence Tasks (or report converged), 8. Provide Next Actions (Handoff), 9. Check for extension hooks, Convergence Findings, Goal, Operating Constraints, Pre-Execution Checks, User Input

### Community 65 - "airflow-scheduler"
Cohesion: 0.19
Nodes (15): build, build, build, command, image, networks, pull_policy, volumes (+7 more)

### Community 66 - "by_conn_type"
Cohesion: 0.16
Nodes (6): by_conn_type(), position_select(), render_source_table(), render_source_reference(), TestRenderSourceTable, TestPositionSelect

### Community 67 - "generate_dags.py"
Cohesion: 0.06
Nodes (17): Предпочтительный путь решения, dag_id_for(), _entity_block(), file_name_for(), load_operator_name(), snapshot_dag_id(), _snapshot_entity_block(), SourceConfig (+9 more)

### Community 68 - "Snapshot table (actual snapshot, __act_snap)"
Cohesion: 0.67
Nodes (3): Snapshot stream (incremental/load_act_snap.py), Increment (incremental column contract), Snapshot table (actual snapshot, __act_snap)

### Community 69 - "speckit-tasks"
Cohesion: 0.06
Nodes (44): contracts/ (interface contracts), plan.md (implementation plan), spec.md (feature specification), tasks.md (task list), Модель состояния guard, Содержание, Состояния, speckit-analyze (+36 more)

### Community 70 - "TestSchemaColumns"
Cohesion: 0.15
Nodes (3): fake_pyspark_types(), SchemaDouble, TestSchemaColumns

### Community 71 - "001-act-snap-contract/analysis.md"
Cohesion: 0.10
Nodes (18): Asset (Airflow), ActSnapConfig (dataclass), DagConfig (базовый dataclass), MAX_ACTIVE_RUNS (константа генератора), Дополнительное покрытие пунктов 3–5, Проверка согласованности, PipelineManifest, Завершение проверки контракта (+10 more)

### Community 72 - "constitution.md"
Cohesion: 0.13
Nodes (13): Скилл pull-request — создание Pull Request, graphify-out/graph.json — граф кодовой базы, Deprecation Notice, Outline, Post-Execution Checks, Pre-Execution Checks, User Input, $speckit-github-taskstoissues (github extension) (+5 more)

### Community 73 - "log_task"
Cohesion: 0.15
Nodes (9): log_task(), Конституция, План: callbacks журнала загрузок, Проверки, Решение, Содержание, Выводы, Исследование (+1 more)

### Community 74 - "4. Detection Passes (Token-Efficient Analysis)"
Cohesion: 0.15
Nodes (13): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 4. Detection Passes (Token-Efficient Analysis), 5. Severity Assignment, 6. Produce Compact Analysis Report, A. Duplication Detection, B. Ambiguity Detection (+5 more)

### Community 75 - "volumes"
Cohesion: 0.17
Nodes (12): name, name, volumes, airflow_logs, airflow_pg_data, pg_catalog_data, pg_meta_data, silo_data (+4 more)

### Community 76 - "7.1. Контракт"
Cohesion: 0.17
Nodes (12): 7.1. Контракт, Выбор типа позиции, Выравнивание временной границы, Границы окна, Настройка lookback, Отступ по умолчанию и расписание, Первичное наполнение и проверка источника, Передача конфигурации загрузчику (+4 more)

### Community 77 - "Чек-лист качества спецификации: структура модулей загрузчика"
Cohesion: 0.40
Nodes (4): Готовность фичи, Заметки, Полнота требований, Чек-лист качества спецификации: структура модулей загрузчика

### Community 78 - "test_dialects.py"
Cohesion: 0.15
Nodes (4): parse_source_table(), TestColumnProjection, TestOracleIntegerKinds, TestParseSourceTable

### Community 79 - "create-new-feature.sh"
Cohesion: 0.36
Nodes (9): clean_branch_name(), contains_non_ascii(), fit_branch_name(), generate_branch_name(), get_highest_from_specs(), is_feature_number_in_range(), create-new-feature.sh script, spec_prefix_exists() (+1 more)

### Community 80 - "speckit.analyze.md"
Cohesion: 0.22
Nodes (8): Analysis Guidelines, Context, Context Efficiency, Goal, Operating Constraints, Operating Principles, Pre-Execution Checks, User Input

### Community 82 - "Тестирование datalake_stand"
Cohesion: 0.22
Nodes (8): Изоляция и диагностика, Как расширять тестирование, Порядок запуска и бюджет, Содержание, Состав e2e-проверок, Состав pytest-проверок, Структура и ответственность, Тестирование datalake_stand

### Community 83 - "airflow-init"
Cohesion: 0.18
Nodes (11): command, depends_on, image, networks, pull_policy, volumes, condition, airflow-postgres (+3 more)

### Community 85 - "test-runner"
Cohesion: 0.20
Nodes (10): AIRFLOW_PASSWORD, AIRFLOW_USER, TRINO_PASSWORD, TRINO_USER, test-runner, environment, image, networks (+2 more)

### Community 86 - "Спецификация: безопасный повтор Spark-загрузки"
Cohesion: 0.17
Nodes (11): US1: повтор после потери процесса задачи (P1), US2: запрет неподтверждённого запуска (P1), US3: предсказуемые повторы и диагностика (P2), Краевые случаи, Критерии успеха, Содержание, Спецификация: безопасный повтор Spark-загрузки, Сущности (+3 more)

### Community 87 - "speckit.plan.md"
Cohesion: 0.07
Nodes (27): Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, User Input, Completion Report, Done When (+19 more)

### Community 88 - "speckit.tasks.md"
Cohesion: 0.18
Nodes (10): Checklist Format (REQUIRED), Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Phase Structure, Pre-Execution Checks, Task Generation Rules (+2 more)

### Community 89 - "Базовые принципы"
Cohesion: 0.18
Nodes (11): I. Граф кодовой базы прежде чтения файлов, II. Изменения подтверждаются тестами, III. Чистый Python без комментариев, IV. Дисциплина Git: ветка → Pull Request → мерж пользователем, V. Перестроение графа после каждого коммита, VI. Документация на русском языке, Базовые принципы, Конституция проекта datalake_stand (+3 more)

### Community 90 - "TestGenerateDagSource"
Cohesion: 0.09
Nodes (5): generate_dag_source(), PipelineManifest, validate_pipeline(), TestGenerateDagSource, TestValidatePipeline

### Community 91 - "silo-init"
Cohesion: 0.20
Nodes (10): silo, AWS_DEFAULT_REGION, silo-init, condition, depends_on, entrypoint, environment, image (+2 more)

### Community 92 - "Предпочтительный путь решения"
Cohesion: 0.18
Nodes (11): Жизненный цикл, Задача Jira, Изолировать Spark drivers через DockerOperator, Контекст, Контракт оператора, Образ и зависимости, Предпочтительный путь решения, Проблема (+3 more)

### Community 93 - "entities"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 94 - "entities"
Cohesion: 0.20
Nodes (9): pool, entities, airflow, fqn, spark, name, source_id, jdbc_partitioning (+1 more)

### Community 96 - "connection"
Cohesion: 0.13
Nodes (6): allocate_process_id(), connection(), find_process_id(), log_dagrun(), TransactionTests, TestProcessAndEvents

### Community 98 - "01_ingestion.sql"
Cohesion: 0.33
Nodes (8): airflow_dagruns_log_process_id_idx, airflow_dagruns_log_run_idx, airflow_tasks_log_run_idx, ingestion.airflow_dagruns_log, ingestion.airflow_tasks_log, ingestion.incremental_batches, ingestion.incremental_state, ingestion.incremental_state_migrations

### Community 99 - "sydafa"
Cohesion: 0.22
Nodes (9): start_period, sydafa, environment, healthcheck, image, networks, platform, ports (+1 more)

### Community 100 - "4. Таблицы raw-слоя: структура и наполнение"
Cohesion: 0.22
Nodes (9): 4. Таблицы raw-слоя: структура и наполнение, Гарантии записи, Запросы к raw в Trino, Имена и колонки, Как исключить повторные попытки из выборки, Как различать партии и попытки, Ограничение: последняя попытка прочитала пустое окно, Полный срез и инкремент (+1 more)

### Community 101 - "DATARCH-2244: Isolate Spark drivers via DockerOperator"
Cohesion: 0.24
Nodes (13): conf/spark/spark-defaults.conf, Global settings (ingestion/conf/settings.yaml), Sources registry (ingestion/conf/sources.yaml), load_sources(), Load operators module (ingestion/operators.py), DATARCH-2242: Split sources registry into YAML files, per-source JDBC parallelism, DATARCH-2244: Isolate Spark drivers via DockerOperator, DATARCH-2245: Handle hung Spark processes on task retry (+5 more)

### Community 105 - "Spark — вычисления и загрузка по JDBC"
Cohesion: 0.33
Nodes (9): Airflow — оркестрация загрузок, JDBC-драйверы MSSQL/Oracle в образах Airflow и Spark, Nessie — каталог Iceberg, pg-catalog — база метаданных Nessie, pg-meta — база журналов и состояния загрузок, silo — MinIO-совместимое S3-хранилище, Spark — вычисления и загрузка по JDBC, sydafa — сервис управления источниками (+1 more)

### Community 106 - "asset_uri"
Cohesion: 0.27
Nodes (3): asset_uri(), snapshot_asset_uri(), TestAssetUri

### Community 108 - "Ведение документации"
Cohesion: 0.25
Nodes (8): Ведение документации, Когда применять, Ответственность документов, Порядок работы и проверки, Содержание, Термины и глоссарий, Техническая точность, Язык и порядок изложения

### Community 110 - "meta/registry.py"
Cohesion: 0.11
Nodes (11): _row(), _batch_reason(), _check_batch(), _check_contract(), fail_batch(), get_batch(), _IncrementalConfig, _lock_batch() (+3 more)

### Community 111 - "Watermark"
Cohesion: 0.22
Nodes (11): Batch registry (ingestion/meta/registry.py), Incremental batch, Bootstrap (initial fill), ingestion.incremental_batches, incremental contract (canonical position type), ingestion.incremental_state, ingestion.incremental_state_migrations, Lookback (+3 more)

### Community 112 - "depends_on"
Cohesion: 0.18
Nodes (11): condition, airflow-webserver, spark-master, spark-worker, sydafa, trino, condition, condition (+3 more)

### Community 113 - "Implementation Plan: [FEATURE]"
Cohesion: 0.22
Nodes (8): Complexity Tracking, Constitution Check, Documentation (this feature), Implementation Plan: [FEATURE], Project Structure, Source Code (repository root), Summary, Technical Context

### Community 114 - "parse_args"
Cohesion: 0.11
Nodes (7): main(), managed_load(), parse_args(), build_argv(), TestMain, TestManagedLoad, TestParseArgs

### Community 115 - "MetadataLocation"
Cohesion: 0.22
Nodes (3): MetadataLocation, TestMetadataLocation, CatalogDialect

### Community 116 - "managed_snapshot"
Cohesion: 0.26
Nodes (3): managed_snapshot(), FakeCatalog, TestManagedSnapshot

### Community 117 - "speckit-checklist"
Cohesion: 0.10
Nodes (19): Anti-Examples: What NOT To Do, Checklist Purpose: "Unit Tests for English", Example Checklist Types & Sample Items, Execution Steps, Post-Execution Checks, Pre-Execution Checks, User Input, [Category 1] (+11 more)

### Community 118 - "Feature Specification: [FEATURE NAME]"
Cohesion: 0.15
Nodes (12): Assumptions, Edge Cases, Feature Specification: [FEATURE NAME], Functional Requirements, Key Entities *(include if feature involves data)*, Measurable Outcomes, Requirements *(mandatory)*, Success Criteria *(mandatory)* (+4 more)

### Community 120 - "test.sh"
Cohesion: 0.36
Nodes (8): cleanup(), configure_stack(), finish(), run_post_checks(), run_runtime_checks(), run_stack(), test.sh script, TEST_FAILURE_LOG_DIR

### Community 121 - "6. Параллелизм Spark"
Cohesion: 0.22
Nodes (9): 6.1. Порядок выполнения load-задачи, 6. Параллелизм Spark, JDBC-партиции: несколько запросов к источнику, Как выбирается колонка разбиения, Как читать лог параллельной загрузки, Когда разбиение работает, Несколько загрузок и обработка внутри Spark, Ограничение: данные читаются в разные моменты (+1 more)

### Community 122 - "sources.yaml"
Cohesion: 0.29
Nodes (6): bank, airflow_conn_id, type, broker, airflow_conn_id, type

### Community 123 - "Настройка graphify — графа кодовой базы"
Cohesion: 0.22
Nodes (9): 1. Назначение, 2. Установка утилиты, 3. Переменные окружения, 4. Перестроение графа, 5. Артефакты `graphify-out/` и git, 6. Запросы по графу, Настройка graphify — графа кодовой базы, Результат настройки (+1 more)

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

### Community 129 - "by_jdbc_url"
Cohesion: 0.09
Nodes (8): all_dialects(), by_jdbc_url(), supported_conn_types(), supported_url_prefixes(), read_bounds(), TestDayExpression, TestPositionKind, TestRegistry

### Community 133 - "Спецификация: необязательный журнал загрузок"
Cohesion: 0.22
Nodes (8): US1 — загрузка без обязательного журнала (P1), US2 — повторные попытки (P1), Границы, Критерии успеха, Пользовательские сценарии, Содержание, Спецификация: необязательный журнал загрузок, Требования

### Community 134 - "generate_process_id (DAG task)"
Cohesion: 0.33
Nodes (6): ingestion.airflow_dagruns_log, generate_process_id (Airflow task), GenerateProcessIdOperator, generate_process_id (DAG task), pg-meta metadata database, ingestion.process_id_seq

### Community 135 - "DATARCH-2241: Define and verify minimal supported component versions"
Cohesion: 0.33
Nodes (6): conf/airflow/Dockerfile, conf/airflow/requirements.txt, conf/spark/Dockerfile, ingestion/generator/requirements.txt, E2E test suite (test/test.sh), DATARCH-2241: Define and verify minimal supported component versions

### Community 137 - "Шаг 7. Стресс-тест: эволюция схемы источника"
Cohesion: 0.29
Nodes (7): 7.1. Удаление колонки, 7.2. Переименование колонки, 7.3. Добавление колонки, 7.4. Настройка генераторов данных, 7.5. Проверка схемы источника, 7.6. Загрузка после изменения схемы, Шаг 7. Стресс-тест: эволюция схемы источника

### Community 138 - "check_spark_guard.py"
Cohesion: 0.09
Nodes (7): main(), main(), orphan(), require(), snapshot(), disable_retries(), prepare_workspace()

### Community 140 - "Глоссарий"
Cohesion: 0.25
Nodes (8): Вычисления и ресурсы, Глоссарий, Данные и загрузки, Запуски и повторные попытки, Инкрементальная загрузка, Разработка и проверки, Содержание, Хранение и гарантии

### Community 142 - "2. Манифесты и генерация DAG-ов"
Cohesion: 0.33
Nodes (6): 2. Манифесты и генерация DAG-ов, Манифест пайплайна, Настройки таблицы источника, Общие настройки и демо-набор, Подготовка DAG-ов, Реестр источников

### Community 143 - "Обработка подвисших процессов Spark при retry задач"
Cohesion: 0.33
Nodes (6): Задача Jira, Обработка подвисших процессов Spark при retry задач, Предпочтительный путь решения, Проблема, Проверки и критерии готовности, Реализация и граница решения

### Community 144 - "DAG generator (ingestion/generator/generate_dags.py)"
Cohesion: 0.40
Nodes (5): Bank pipeline manifest (ingestion/conf/pipelines/bank/main.yaml), DAG generator (ingestion/generator/generate_dags.py), Manifest parser (ingestion/generator/manifest.py), Backlog: Auto-generate DAG tags and drop duplicates, Backlog: Split Airflow pools for loads vs service tasks

### Community 145 - "first_row"
Cohesion: 0.29
Nodes (3): first_row(), _row_size(), TestFirstRow

### Community 147 - "ColumnMeta"
Cohesion: 0.33
Nodes (3): ColumnMeta, check_partitioned_load_path(), jdbc_count()

### Community 148 - "7.7. DAG-и снимков и таблицы-снимки"
Cohesion: 0.29
Nodes (7): 7.7. DAG-и снимков и таблицы-снимки, Автор и время публикации, Атомарная замена данных, Контроль свежести снимков, Повтор после сбоя, Пустое чтение, Структура таблицы-снимка

### Community 149 - "Сценарий демонстрации стенда"
Cohesion: 0.08
Nodes (24): 5.1. Подготовка показа pg-meta, 5.2. Новый запуск без изменения источника, 5.3. Clear только успешной load-задачи, 5.4. Полный Clear существующего запуска, 5.5. Новая загрузка после обновления источника, 5.6. Что объяснить про повтор после ошибки, Запуск и подключения, Исходное состояние (+16 more)

### Community 151 - "ingestion.airflow_tasks_log"
Cohesion: 0.67
Nodes (4): ingestion.airflow_tasks_log, log_end (DAG task), log_start (DAG task), TableLogOperator

### Community 152 - "5. Устойчивость к эволюции схемы источника"
Cohesion: 0.40
Nodes (5): 5. Устойчивость к эволюции схемы источника, Новые колонки, Отсутствующие колонки, Порядок колонок, Результат изменения схемы

### Community 155 - "_apply_entity_fields"
Cohesion: 0.28
Nodes (3): _apply_entity_fields(), log_spark_task_event(), _strict_submit_options()

### Community 156 - "Execution Steps"
Cohesion: 0.29
Nodes (7): 1. Initialize Convergence Context, 2. Load Artifacts (Progressive Disclosure), 3. Build the Intent Inventory, 4. Assess the Codebase and Classify Findings, 5. Assign Severity, 6. Present the In-Session Findings Summary, Execution Steps

### Community 157 - "Анализ согласованности перед реализацией"
Cohesion: 0.29
Nodes (6): Анализ согласованности перед реализацией, Исправления ревью PR 69, Итог converge, Результат, Содержание, Состояние проверок перед публикацией

### Community 158 - "Задачи DATARCH-2245"
Cohesion: 0.40
Nodes (4): Задачи DATARCH-2245, Приёмка, Реализация, Содержание

### Community 159 - "Проверка требований DATARCH-2245"
Cohesion: 0.50
Nodes (3): Проверка требований DATARCH-2245, Результат, Содержание

### Community 160 - "resolve_source_connection"
Cohesion: 0.21
Nodes (5): Контекст, _pgmeta_dsn(), resolve_source_connection(), run_spark_guard(), _submit_args()

### Community 161 - "speckit.implement.md"
Cohesion: 0.29
Nodes (6): Completion Report, Done When, Mandatory Post-Execution Hooks, Outline, Pre-Execution Checks, User Input

### Community 168 - "002-incremental-config/spec.md"
Cohesion: 0.33
Nodes (5): CLI-флаг --incremental-config, incremental.contract (YAML: source_column, position_type), Контракт инкрементальной загрузки, Требования, Уточнения

### Community 172 - "4. Detection Passes (Token-Efficient Analysis)"
Cohesion: 0.29
Nodes (7): 4. Detection Passes (Token-Efficient Analysis), A. Duplication Detection, B. Ambiguity Detection, C. Underspecification, D. Constitution Alignment, E. Coverage Gaps, F. Inconsistency

### Community 173 - "append.py (raw-layer writer)"
Cohesion: 0.33
Nodes (7): __act_snap auxiliary snapshot, append.py (raw-layer writer), dds / dma layers (planned), IngestActSnapOperator, ods layer (planned), raw layer, source schema evolution handling

### Community 174 - "_column_metadata"
Cohesion: 0.21
Nodes (4): _column_metadata(), _exact_pattern(), _optional_int(), TestCatalogPatternFiltering

### Community 176 - "incremental/load_entity.py"
Cohesion: 0.09
Nodes (8): IngestFullOperator, IngestIncrementalOperator, Airflow pool ingest_loads, load (DAG task), reader.py (shared JDBC reader), Backlog: Remove JDBC password from spark-submit arguments, main(), parse_args()

### Community 178 - "3. Структура сгенерированного DAG-а"
Cohesion: 0.40
Nodes (5): 3. Структура сгенерированного DAG-а, Зависимости задач, Идентификаторы и метаданные, Ограничение числа загрузок, Структура DAG-а снимка

### Community 179 - "Execution Steps"
Cohesion: 0.33
Nodes (6): 1. Initialize Analysis Context, 2. Load Artifacts (Progressive Disclosure), 3. Build Semantic Models, 5. Severity Assignment, 6. Produce Compact Analysis Report, Execution Steps

### Community 184 - "Specification Analysis Report"
Cohesion: 0.50
Nodes (4): 7. Provide Next Actions, 8. Offer Remediation, 9. Check for extension hooks, Specification Analysis Report

### Community 186 - "ConnectionSpec"
Cohesion: 0.12
Nodes (6): 2. Подключение новой JDBC-СУБД, Проверки совместимости, Регистрация и окружение, ConnectionSpec, MssqlDialect, TestConnections

### Community 187 - "Выполнено"
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

### Community 192 - "8. База метаданных pg-meta: остальные объекты"
Cohesion: 0.40
Nodes (5): 8. База метаданных pg-meta: остальные объекты, Аудит миграций, Журналы событий, Запросы к метаданным, Последовательность идентификаторов

### Community 193 - "Расширение стенда"
Cohesion: 0.40
Nodes (5): 1. Выбор вида изменения, 4. Новый слой данных, 5. Порядок разработки, Расширение стенда, Содержание

### Community 194 - "Контракт callbacks"
Cohesion: 0.40
Nodes (4): Контракт callbacks, Развёртывание, События, Содержание

### Community 195 - "Проверка"
Cohesion: 0.40
Nodes (4): Команды, Ожидания, Проверка, Содержание

### Community 196 - "Задачи"
Cohesion: 0.40
Nodes (4): Задачи, Реализация, Результаты проверки, Содержание

### Community 199 - "conf/airflow/requirements.txt — Airflow Image Dependencies"
Cohesion: 0.50
Nodes (4): conf/airflow/requirements.txt — Airflow Image Dependencies, apache-airflow-providers-apache-spark, apache-airflow-providers-trino, sydafa-airflow — Airflow Integration Package (SydafaActivateOperator, SydafaMigrateOperator)

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
- **104 isolated node(s):** `TEST_FAILURE_LOG_DIR`, `conf/spark/spark-defaults.conf`, `pg-catalog — база метаданных Nessie`, `Bootstrap (initial fill)`, `ingestion.incremental_state_migrations` (+99 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 1459 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **53 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AGENTS.md — инструкции проекта datalake_stand` connect `AGENTS.md — инструкции проекта datalake_stand` to `constitution.md`, `architecture.md`, `speckit.md`, `extending.md`, `speckit.plan.md`?**
  _High betweenness centrality (0.148) - this node is a cross-community bridge._
- **Are the 16 inferred relationships involving `RdbmsDialect` (e.g. with `Контекст` and `Предпочтительный путь решения`) actually correct?**
  _`RdbmsDialect` has 16 INFERRED edges - model-reasoned connections that need verification._
- **What connects `TEST_FAILURE_LOG_DIR`, `conf/spark/spark-defaults.conf`, `pg-catalog — база метаданных Nessie` to the rest of the system?**
  _104 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `parse_args` be split into smaller, more focused modules?**
  _Cohesion score 0.07547169811320754 - nodes in this community are weakly interconnected._
- **Why does `services` connect `services` to `airflow-scheduler`, `sydafa`, `environment`, `silo`, `spark-master`, `airflow-webserver`, `airflow-init`, `test-runner`, `silo-init`?**
  _High betweenness centrality (0.114) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `generate_all()` (e.g. with `IncrementalEntityConfig` and `SourceConfig`) actually correct?**
  _`generate_all()` has 2 INFERRED edges - model-reasoned connections that need verification._

### Low-confidence Hints
_AMBIGUOUS edges — the extractor was unsure. Verify before acting on these._

- **What is the exact relationship between `tasks-template.md` and `speckit-tasks`?**
  _Edge tagged AMBIGUOUS (relation: references) - confidence is low._