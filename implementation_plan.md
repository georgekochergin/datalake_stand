# Raw-Layer Ingestion Framework — Implementation Plan

- **Provenance**: `__process_id`/logging pattern from the local clone `~/git/dbt_dag_generator` (`python/rnd/libs/logging.py`, `python/rnd/dbt_utils/dbt_dag_generator.py`); stand layout facts from the repo itself.
- **Repo constraints (AGENTS.md)**: no `#` comments in Python (docstrings only); branches `kodik/*`; PR mandatory; agent never merges; every change ships with unit or e2e tests; `./test/test.sh` must end with `FAIL=0`.

---

## 1. Concept — the raw layer

### 1.1 What the raw layer is

The datalake's first layer is a **raw layer**: an exact mirror of source entities plus technical metadata columns — nothing else. No business logic, no merging, no column selection, no transformations. Adding a new table to the lake is a one-line manifest change.

All processing belongs to future layers built **on top of** raw: the **ods layer** (processing of raw tables — describing needed columns, merge strategies, conformed models) is the first of them and is explicitly out of scope here; `dds` and `dma` follow. Raw never changes retroactively when those layers appear — they read raw, they don't redefine it.

### 1.2 Layer layout ("layer = catalog")

- One bucket `datalake`; each layer is a Nessie warehouse and an engine catalog. Phase 1 creates warehouse `raw` → `s3://datalake/raw`. The `ods`/`dds`/`dma` warehouses are added when those layers are built.
- Framework-owned identifiers: `raw.<source_id>.<schema>__<table>` (one schema per source instance); Kafka in future: `raw.<source_id>.<topic_resolved>`.
- Engines address exactly `catalog.schema.table` (Trino, DBeaver, BI), so the layer occupies the catalog slot; the physical layout mirrors the logical one automatically (warehouse root → namespace folder → table folder under `datalake/raw/<source_id>/<table>/{data,metadata}`).
- All writes go to the Nessie default branch (`main`) — no per-run branches.

### 1.3 Stack and sources

- **Stack**: Airflow 3.3.2 (LocalExecutor, connections via `AIRFLOW_CONN_*` env), Spark 3.5.5 standalone (`spark-master` + `spark-worker`, iceberg-spark-runtime 1.8.1 + MSSQL/Oracle JDBC jars baked into the images), Nessie 0.99 REST catalog backed by `pg-catalog` (Postgres 16, db `nessie`), dedicated `pg-meta` Postgres for ingestion operational state, SILO (S3-compatible, bucket `datalake`), Trino 479 (Iceberg REST catalog `raw`).
- **Sources**: sydafa — `bank` (MSSQL, schema `retail_bank`), `broker` (Oracle, schema `trading_core`); each with `clients`, `accounts`, `deals`, `transactions`. **Kafka topics appear as sources in future** — the design carries this constraint: the source `type` drives validation and reader selection, and everything outside the reader (metadata columns, append path, DDL, registry, assets) is source-kind-agnostic (§3.6).
- **Greenfield**: no ingestion code exists; `spark/` and `trino/` are gitignored skeletons.

## 2. Naming rules

Module `ingestion/lib/naming.py`:

1. One delimiter: **every dot becomes `__`** — the structural `schema.table` separator of an RDBMS FQN, dots embedded in quoted fragments, and dots in a Kafka topic name alike.
2. RDBMS FQN = exactly 2 fragments (quote-aware tokenization, unquoting, lowercase folding); a Kafka topic = one flat fragment. Result validated against `^[a-z][a-z0-9_]*$`; uniqueness of resolved targets asserted per pipeline.

| Input | Target |
|---|---|
| `retail_bank.clients` | `retail_bank__clients` |
| `retail_bank."sub.clients"` | `retail_bank__sub__clients` |
| `"abc_d.ef_g".tbl` | `abc_d__ef_g__tbl` |
| Kafka topic `payments.card_events.v1` | `payments__card_events__v1` |

Fully qualified name: `raw.<source_id>.<schema>__<table>`. The two dots are the engine's `catalog.schema.table` separators (structural, never encoded); the only encoded characters come from the entity FQN itself. Asset outlets use the raw entity FQN: `Asset("raw://<source_id>/<entity_fqn>")` — stable scheduling identity, percent-encoded where needed; the physical table is discoverable via the registry and Trino.

## 3. Loading strategy — append-only, schema-change-tolerant

### 3.1 Core mechanics

Every load **appends** rows; rows are never updated or deleted in place; full history is retained as data.

- **First load** emits a generated DDL and appends:
  `CREATE TABLE <target> (<columns from the discovered df.schema>, __load_dttm timestamptz, __process_id bigint) USING iceberg PARTITIONED BY (days(__load_dttm)) TBLPROPERTIES ('write.spark.accept-any-schema'='true')`
  (the column list is generated at runtime from the discovered schema — never declared in YAML; SQL DDL because `DataFrameWriterV2.partitionedBy()` supports identity transforms only; `timestamptz` matches what Spark's `current_timestamp()` produces — Iceberg maps Spark `TimestampType` to `timestamptz`, and declaring zone-less `timestamp` would make appends hit the §3.2 type-change fail-fast; day partitions are UTC-day boundaries).
- **Every subsequent load**: `df.writeTo(target).option("mergeSchema", "true").append()`.
- The two schema knobs that make this tolerant are explained below.
- **Partitioning is framework-fixed**: `days(__load_dttm)` — the load timestamp is the natural analytics dimension; business-driven partitioning belongs to the future ods/dds layers. No user-facing partitioning config.

#### The two schema knobs, explained

Orientation: a Spark **DataFrame** is the in-memory table about to be written; its schema comes from the JDBC read of the source, so it is always *exactly what the source looks like right now*. When appending to an Iceberg table, Iceberg first validates the DataFrame schema against the table schema. **By default the validation is strict**: the DataFrame must carry exactly the table's column set (matched by name) — a missing column or an extra column aborts the write with a schema-compatibility error. Any source schema change would break loading. Two knobs relax this:

1. **`write.spark.accept-any-schema`** — a **table property**: a flag stored in the table's own metadata, set once in the CREATE DDL, applying to every future writer of the table. Effect: validation becomes by-name and the DataFrame may be **missing** table columns — the missing columns are stored as `NULL` in the appended rows. It never adds columns: an extra DataFrame column is still an error at this level.
2. **`mergeSchema`** — a **write option**, passed on each append (`.option("mergeSchema", "true")`). Effect: immediately before writing, Iceberg compares the DataFrame schema to the table schema and **adds** every DataFrame column the table lacks (the equivalent of `ALTER TABLE ADD COLUMN`, executed inside the write commit). Existing rows read the new columns as `NULL`.

Together: new source columns are added, dropped source columns are NULL-filled, alignment is always by name — and nothing else ever happens to the schema (no removal, no retype).

Walkthrough — table created earlier with `client_id, full_name, client_type, status, __load_dttm, __process_id`; the "next load's DataFrame" column is what the source looks like today:

| Next load's DataFrame | Default Iceberg | With both knobs |
|---|---|---|
| same columns (any order) | ok — matched by name | ok |
| new column `email` | error — extra column | `email` added by `mergeSchema`; old rows read `NULL`, new rows carry values |
| `status` dropped at source | error — missing column | accepted; `status` column retained, new rows get `NULL` |
| rename `client_type` → `client_segment` | error | looks like add+drop: `client_segment` added (history `NULL`), `client_type` retained (new rows `NULL`) |
| `amount` type changed | error | **not automatic** — compatible change needs an explicit `ALTER TABLE`; incompatible fails fast (§3.2 boundary) |

Physically, an append writes only **new Parquet files** plus one metadata commit (a new snapshot); files of previous loads are never touched — a later load cannot corrupt earlier history.

### 3.2 Reaction to source schema evolution

| Source change | Effect on the raw table |
|---|---|
| add column | auto-added (`mergeSchema`); historical rows read `NULL` |
| drop column | column retained; new rows get `NULL` |
| rename | not recognized: old column retained (new rows `NULL`), new column added (history `NULL`) |
| type change | not automatic: compatible → explicit `ALTER TABLE`; incompatible → the write fails fast |
| column order | no effect (Iceberg tracks columns by field id / name) |

Automation boundary: **add only — no drop, no change**. No schema surprise ever breaks a write and no data is ever silently lost; drop/rename/type-change require a deliberate explicit `ALTER TABLE`.

### 3.3 Load portions and meta-columns

Every appended row carries exactly two metadata columns with complementary roles:

- `__load_dttm` (timestamptz) — wall clock of the load; the **analyst-facing time dimension**; each load writes one constant value for all its rows.
- `__process_id` (BIGINT) — identity of the DAG run from the pg-meta sequence; the **exact run identity** (timestamps drift across tasks within a run; the sequence value does not) and the join key to the registry.

One load = one portion = one (`__process_id`, `__load_dttm`) pair; the pair identifies the portion in data, in the registry, and in Airflow logs.

### 3.4 Working with raw tables (analytics guide)

```sql
SELECT * FROM raw.bank.retail_bank__clients
WHERE __load_dttm = (SELECT max(__load_dttm) FROM raw.bank.retail_bank__clients);
```
— **current state** (the latest snapshot; equivalently filter on `max(__process_id)`).

```sql
SELECT * FROM raw.bank.retail_bank__clients WHERE __load_dttm >= current_date;
```
— **today's loaded data** (prunes to the day partition).

```sql
SELECT ... FROM raw.bank.retail_bank__clients WHERE __process_id = :pid
```
— **an exact portion** (joins to `airflow_tasks_log` for timings/status of that load).

Diffs between portions, deleted-at-source rows (absent from the latest snapshot but present in history), and per-run audit are plain SQL over these two columns. Optional `__latest` views wrapping the current-state query are a future feature.

### 3.5 Full-snapshot mode (default) vs incremental mode (`incremental_column`)

| | full snapshot (default) | incremental (`incremental_column: <col>`) |
|---|---|---|
| read | whole source table each run | `WHERE <col> > last_watermark`, pushed down via a `dbtable` subquery |
| portion | complete mirror state per run | only new/changed rows per run |
| deletes at source | reflected (deleted rows absent from the new snapshot) | **not captured** (rows vanished before the watermark are never re-read) |
| watermark | none | stored on pg-meta (§7); advanced only after a successful commit — at-least-once |
| bootstrap | — | first run without a stored watermark reads everything |
| use when | controlled volumes (default) | large, append-dominated sources |

`incremental_column` must be monotonically increasing (`updated_at`, autoincrement id); it is also the first-choice parallel-read column (§4).

### 3.6 Kafka outlook (topics as future sources — same strategy)

Kafka fits the raw model natively: a topic is an append-only stream.

- **Entity**: one topic = one flat entity; FQN = the raw topic name (`payments.card_events.v1` → `payments__card_events__v1` in the source's namespace); the manifest key is the topic itself.
- **Portions**: each run appends the offset window `(last_offset, now]` — the offset is the topic's natural `incremental_column` (a system column, not a payload field), stored in `table_watermarks` exactly like an RDBMS watermark.
- **Schema**: from Schema Registry (Avro/Protobuf) or payload inspection — feeds the same §3.2 evolution rules; `mergeSchema` appends are identical.
- **Metadata/DDL/partitioning**: unchanged (`__load_dttm`/`__process_id`, `days(__load_dttm)`).
- Open decision when Kafka lands: topic names with `-` or uppercase fail identifier validation — reject vs map.

## 4. Spark read parallelism

The parallel-read column is **auto-evaluated — never a config knob**. Priority order:

1. `incremental_column` if set (monotonic numeric — always safe to partition on);
2. primary-key columns in `KEY_SEQ` order;
3. unique-index columns;
4. first candidate whose type is numeric/date/timestamp wins; string/GUID columns are skipped;
5. no suitable column → single-partition read.

Mechanics: candidate columns come from the JDBC `DatabaseMetaData` API (`getPrimaryKeys` + `getIndexInfo` ordering + `getColumns` types) — the dialect-free standard, unlike `INFORMATION_SCHEMA` (not implemented by Oracle). For the chosen column: `SELECT min(col), max(col)` on the source → JDBC read with `partitionColumn/lowerBound/upperBound/numPartitions` (default 4). The job keeps `--partition-column` as a CLI debug override only — no YAML surface.

## 5. Configuration schemas

### 5.1 `ingestion/sources.yaml`

Root is a mapping; keys are source ids matching `^[a-z][a-z0-9_]*$`; unknown keys rejected at every level.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `type` | enum `rdbms` \| `kafka` | yes | Coarse kind (not a dialect): selects FQN validation rules (2 fragments vs flat topic), legal entity keys, and the reader path. `rdbms` now; `kafka` when Kafka lands (§3.6); future kinds open. |
| `airflow_conn_id` | string | yes | Airflow Connection ID holding host/credentials. For `rdbms` the connection scheme must be a known JDBC dialect (`mssql://`, `oracle://`, …); the operator asserts scheme and `type` agree at task runtime. |

`type` exists because the generator/validator runs connection-free at build time and needs the kind; the concrete dialect is derived at runtime from the connection scheme (adding PostgreSQL/MySQL later = a new connection, zero vocabulary change).

### 5.2 `ingestion/pipelines/<source_id>.yaml` (DAG config)

One file = one DAG. Root mapping; unknown keys rejected at every level.

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `source_id` | string | yes | — | Must exist in `sources.yaml`; defines the target namespace and source config for all entities. |
| `schedule` | string \| null | no | `null` (manual) | Airflow schedule expression or `null`. |
| `catchup` | bool | no | `false` | Airflow catchup. |
| `tags` | list[string] | no | `[]` | Airflow tags. |
| `entities` | list[mapping] | yes | — | Non-empty; duplicate `fqn` rejected. Keys per entry below. |

Entity entry keys:

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `fqn` | string | yes | — | `rdbms`: `<schema>.<table>` (exactly 2 fragments, quoted fragments allowed). `kafka`: the raw topic name. Resolved by §2; used verbatim in the asset URI. |
| `incremental_column` | string | no | full snapshot | Monotonically increasing source column switching the entity to incremental mode (§3.5): read `WHERE col > last_watermark` via `dbtable` subquery; watermark on pg-meta; at-least-once; source deletes not captured. Also the first-choice parallel-read column (§4). |

Example:

```yaml
source_id: bank
schedule: "@daily"
catchup: false
tags:
  - ingestion
  - bank
  - mssql

entities:
  - fqn: retail_bank.clients                    (full-snapshot append, default)
  - fqn: retail_bank.transactions
    incremental_column: transaction_id          (incremental append)
```

### 5.3 Authoring conventions

Block notation only (`-` + newline) — no flow lists (`[a, b]`). Scalars are preferred over single-field dicts.

## 6. DAG generation and structure

### 6.1 Generation model

- DAGs are generated statically at build time — no dynamic generation at Airflow parse time. The host/CI script `ingestion/generate_dags.py` parses `sources.yaml` + `pipelines/*.yaml` and writes one fully-expanded, parameter-inlined `.py` file per pipeline into `airflow/dags/generated/` (gitignored artifact; every task, TaskGroup and dependency is a literal; no YAML parsing at parse time). This is the future CI/CD hook: CI runs the generator and `--check` mode (regenerate in memory, diff against disk, exit ≠ 0 on drift); today both run manually.
- Generated files carry only `conn_id` literals and static parameters — never credentials; credentials resolve at task runtime from Airflow Connections.
- Shared runtime (operators, registry access) lives in `airflow/plugins/ingestion/`, importable from DAGs.
- The test rig needs no compose changes (`test.sh` copies the working tree into `test/.workspace/`).

### 6.2 DAG and TaskGroup structure

```text
DAG ingest_bank   (schedule from YAML, catchup=False, max_active_runs=1, tags from YAML,
                   on_success/on_failure callbacks → append dagruns_log END/FAILED)
├── activate_source      SydafaActivateOperator (conn_id=sydafa_default, source=bank, ttl_minutes=15)
├── generate_process_id  bootstrap DDL → nextval → INSERT airflow_dagruns_log(START) → XCom push
└── TaskGroup "ingest_group"
    ├── TaskGroup "load_retail_bank__clients"
    │   ├── log_start   INSERT airflow_tasks_log(START)
    │   ├── load        IngestAppendOperator → /opt/spark-jobs/raw/load_entity.py
    │   │               outlets=[Asset("raw://bank/retail_bank.clients")]
    │   └── log_end     INSERT airflow_tasks_log(END);
    │                   if incremental_column set → advance watermark (§7.2) via Trino + pg-meta
    └── ... one TaskGroup per entity, in parallel
activate_source >> generate_process_id >> each entity group; inside a group: log_start >> load >> log_end
```

`activate_source` wakes the lazily-started sydafa DB; `generate_process_id` precedes every load. `max_active_runs=1` serializes runs so latest-snapshot reads and watermarks are unambiguous. Entities load in parallel. The per-entity TaskGroup exists so extra steps (quality checks, sensors) can be appended alongside `load` later.

## 7. Metadata store — standalone `pg-meta`

### 7.1 Instance

Dedicated Postgres service (db `meta`, schema `ingestion`), decoupled from both the lake and Nessie's own `pg-catalog`: the sequence cannot live in the lake (no atomic increment on object storage), and operational telemetry must survive lake outages (independent failure domain from Nessie/Trino/S3/Spark). All operational state lives here: the sequence, DAG/task lifecycle logs, incremental watermarks. DDL is self-bootstrapped (idempotent `CREATE … IF NOT EXISTS` inside the first task). Access from Airflow via `PostgresHook` (provider ships with the image).

### 7.2 Tables

```sql
CREATE SCHEMA IF NOT EXISTS ingestion;
CREATE SEQUENCE IF NOT EXISTS ingestion.process_id_seq;

CREATE TABLE IF NOT EXISTS ingestion.airflow_dagruns_log (
  id bigserial PRIMARY KEY,
  process_id bigint NOT NULL,
  dag_id text NOT NULL,
  run_id text NOT NULL,
  execution_date timestamptz,
  ts timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL);
CREATE INDEX IF NOT EXISTS airflow_dagruns_log_process_id_idx
  ON ingestion.airflow_dagruns_log (process_id);

CREATE TABLE IF NOT EXISTS ingestion.airflow_tasks_log (
  id bigserial PRIMARY KEY,
  process_id bigint NOT NULL,
  source_id text NOT NULL,
  entity_fqn text NOT NULL,
  datalake_table text NOT NULL,
  ts timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL);
CREATE INDEX IF NOT EXISTS airflow_tasks_log_run_idx
  ON ingestion.airflow_tasks_log (process_id, entity_fqn);

CREATE TABLE IF NOT EXISTS ingestion.table_watermarks (
  source_id text NOT NULL,
  entity_fqn text NOT NULL,
  watermark_value text NOT NULL,
  process_id bigint NOT NULL,
  updated_dttm timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source_id, entity_fqn));
```

- `__process_id = SELECT nextval('ingestion.process_id_seq')` — one per DAG run, monotonic BIGINT, pushed via XCom; task retries reuse it, a new run gets a new one.
- Both log tables are append-only event logs (insert-only, matching the reference pattern): dagrun events START (`generate_process_id`) / END (success callback) / FAILED (failure callback); task events START (`log_start`) / END (`log_end`) / FAILED (load `on_failure_callback`). Current status = latest event per `(process_id, entity_fqn)`; duration = `ts(END) − ts(START)`; retries just append duplicate events.
- `table_watermarks`: one row per incremental entity; read before the load (passed to the Spark job as an arg); **advanced only after a successful commit** by `log_end`, computed as `SELECT max(<incremental_column>) FROM <raw table> WHERE __process_id = <pid>` via TrinoHook — at-least-once semantics.

## 8. Phase 1 — implementation scope

### 8.1 Scope (the working fundament)

1. Logging on standalone `pg-meta`: DAG lifecycle, task lifecycle, process-id sequence, incremental watermarks.
2. Append-only schema-change-tolerant loading — **full-snapshot append** (default strategy).
3. Append-only schema-change-tolerant loading — **incremental append** via `incremental_column`.
4. Spark multithreaded reading via JDBC partitioning (auto-evaluated column, §4).
5. Static DAG generation by script (future CI/CD hook).

RDBMS sources only are *implemented*; the code must not assume RDBMS-only (§1.3, §3.6): source `type` drives validation/reader dispatch, and everything after the read is kind-agnostic.

### 8.2 Components

| Artifact | Path | Tracked | Content |
|---|---|---|---|
| Sources registry | `ingestion/sources.yaml` | yes | `bank`, `broker` (`type: rdbms` + conn id) |
| Pipeline manifests | `ingestion/pipelines/{bank,broker}.yaml` | yes | bank: full-snapshot entities; broker: `incremental_column` entities (both modes exercised) |
| Shared lib | `ingestion/lib/{naming.py, manifest.py}` | yes | FQN resolution; manifest dataclasses + strict validation |
| Generator | `ingestion/generate_dags.py` | yes | YAML → deterministic DAG emission; `--check` drift mode |
| Spark job | `spark/raw/load_entity.py` | yes (`.gitignore` whitelist) | arg-only append loader, both modes; no YAML, no Airflow imports. Spark jobs are organized by layer — `spark/<layer>/<job>.py` (future: `spark/ods/…`, `spark/marts/…`, shared `spark/common/`) |
| Airflow runtime | `airflow/plugins/ingestion/{operators.py, registry.py}` | yes | `GenerateProcessIdOperator`, `TableLogOperator`, `IngestAppendOperator(SparkSubmitOperator)` |
| Stand wiring | `conf/` + `docker-compose.yaml` | yes | bucket `datalake`, warehouse/catalog `raw`, `pg-meta`, connections (Step 0, §9) |
| Generated DAGs | `airflow/dags/generated/ingest__{bank,broker}.py` | **no** | static DAGs |
| Tests | `test/unit/` + `test/run_flow.py` + `test/test.sh` | yes | unit + e2e (§8.5) |

### 8.3 Spark job CLI (all params supplied by the generated DAG; credentials resolved by the operator)

```
load_entity.py --jdbc-url <url> --jdbc-user <u> --jdbc-password <p>
                  --source-table <schema.table> --target-table raw.<source_id>.<schema>__<table>
                  --process-id <int8>
                  [--incremental-column <col> --watermark-value <v>]
                  [--partition-column <col>] [--num-partitions <n>]
```

Read (full or `WHERE col > watermark` via subquery, auto-parallelized) → add `__load_dttm` + `__process_id` → first-load DDL or `mergeSchema` append → log rows appended.

### 8.4 Airflow connections (compose `&airflow-env`)

```
AIRFLOW_CONN_BANK_SOURCE:     "mssql://sydafa:<pass>@sydafa-src-mssql-mssql:1433/retail_bank"
AIRFLOW_CONN_BROKER_SOURCE:   "oracle://sydafa:<pass>@sydafa-src-oracle-oracle:1521/<service>"
AIRFLOW_CONN_INGESTION_META:  "postgres://<user>:<pass>@pg-meta:5432/meta"
```

The operator renders JDBC URLs from Connection fields (mssql → `jdbc:sqlserver://host:1433;databaseName=<schema>;encrypt=false`; oracle → `jdbc:oracle:thin:@//host:1521/<service>`) and asserts the scheme matches the declared `type`.

### 8.5 e2e scenario (Trino REST via stdlib `urllib`, same technique as the existing Airflow steps)

After `update_source_{bank,broker}` succeed: trigger `ingest_bank` (full-snapshot) and `ingest_broker` (incremental), poll to success, then per table:

- first run: `count(*)` equals the expected source count (dims 100/100/1000, fact 25000); `count(distinct __process_id) = 1`; `max(__process_id)` identical across all tables of the run and greater than the previous run's;
- second `ingest_bank` trigger (no source change in between): total `count(*)` doubles (append semantics), rows of the latest snapshot (`max(__load_dttm)`) still equal the source count, history preserved with the old `__process_id`;
- second `ingest_broker` trigger after a source migration: only the migrated delta is appended (rows with the new `__process_id` ≈ migrated row counts), watermark advanced in `table_watermarks`;
- migrated source updates visible in the latest snapshot.

### 8.6 Assumptions

- `pg-meta` credentials in `.env` (`PG_META_USER`/`PG_META_PASSWORD`); the test rig inherits the service/volume via compose project isolation.
- Oracle service name pinned from the activate response / sources.json on first real run.
- Generator needs host `python3` + PyYAML (or a throwaway python container).
- Layout risk: "layer = catalog" depends on the Step 0 multi-warehouse spike (Nessie 0.99 / Trino 479 / Iceberg 1.8.1). Fallback: single catalog with flattened `raw__<source>` schemas — localized changes (§2 constant + Step 0 configs).

### 8.7 Delivery — one PR, logical commits

Single branch `kodik/ingestion-raw-phase1`, **one pull request**, commits in logical blocks reviewed one by one (each code commit ships with its tests):

1. `docs` — this plan.
2. `step0` — stand re-wiring: `conf/` + `docker-compose.yaml` + `.env` (bucket `datalake`, warehouse/catalog `raw`, `pg-meta`, Trino connection) — followed by the Step 0 spike (§9).
3. `lib` — `ingestion/lib/naming.py` + unit tests.
4. `manifest` — `ingestion/lib/manifest.py` + `sources.yaml` + `pipelines/*.yaml` + unit tests.
5. `loader` — `spark/raw/load_entity.py` + `.gitignore` whitelist + unit tests + style guard + `test.sh` unittest wiring.
6. `orchestration` — Airflow plugins (operators, registry, watermark advance) + `AIRFLOW_CONN_*` env.
7. `generator` — `ingestion/generate_dags.py` + generated-DAG ignore rule + determinism tests.
8. `e2e` — `run_flow.py` steps (Trino assertions incl. incremental) + `test.sh` generator invocation + docs refresh.

## 9. PR1 build sheet (implement first)

### 9.0 Definition of done

- `python3 -m unittest discover -s test/unit -v` — all green;
- `./test/test.sh` — `FAIL=0`, exit 0;
- smoke per source verified via Trino: full-snapshot append (twice → doubling, latest snapshot intact) and incremental append (second run appends only the delta);
- single PR `kodik/ingestion-raw-phase1` with logical commits (§8.7), reviewed commit-by-commit; **never merged** by the agent (user merges).

**Step 0 — stand re-wiring (before any coding; then rebuild `spark-master`/`spark-worker` + the Airflow image and restart):**

1. `conf/silo/init-bucket.sh`: create `s3://datalake`.
2. `docker-compose.yaml` (`nessie`): warehouse `raw` → `s3://datalake/raw`, set as default (future layers add their own warehouses).
3. `conf/spark/spark-defaults.conf`: replace `spark.sql.catalog.iceberg.*` with `spark.sql.catalog.ods`-style block for `raw` (same URI/impl/S3 settings + `spark.sql.catalog.raw.warehouse raw`).
4. `conf/trino/catalog/`: replace `iceberg.properties` with `raw.properties` (`iceberg.rest-catalog.warehouse=raw`).
5. `docker-compose.yaml` (`&airflow-env`): `AIRFLOW_CONN_TRINO_DEFAULT` catalog → `/raw`.
6. `docker-compose.yaml`: add `pg-meta` (postgres:16.15-bookworm, volume `pg_meta_data`, `POSTGRES_DB: meta`, creds from `.env`, `pg_isready` healthcheck, no ports; Airflow `depends_on: pg-meta (service_healthy)`).

Spike (≈1h): from `airflow-scheduler` create `raw.bank.__spike` via spark-sql; `SELECT` it from the `trino` container; confirm `datalake/raw/bank/__spike/` in the silo console; drop it. On any failure — stop, fall back to the flattened layout (§8.6) before coding.

### 9.1 `.gitignore`

After `/spark/*` and `!/spark/.gitkeep` add: `!/spark/raw/` and `!/spark/raw/**`. (`ingestion/` root is tracked by default; `airflow/dags/generated/` ignore lands in PR2.)

### 9.2 `ingestion/lib/naming.py`

```python
"""FQN-to-target table name resolution for raw-layer Iceberg tables."""
```

```python
CATALOG = "raw"
DELIMITER = "__"

def resolve_target_table_name(fqn: str) -> str
def resolve_namespace(source_id: str) -> str
def resolve_fully_qualified(source_id: str, fqn: str) -> str
def assert_unique_targets(fqns: list[str]) -> None
```

Rules: quote-aware tokenization into exactly 2 fragments (RDBMS; a Kafka topic is one flat fragment); unquote, lowercase, replace every inner `.` with `__`; join with `__`; validate `^[a-z][a-z0-9_]*$` (fail-fast `ValueError`); `resolve_fully_qualified` → `raw.<ns>.<table>`; `assert_unique_targets` raises on collisions. Error cases → unit tests: empty; 1 fragment; 3 fragments; leading/trailing dot; unbalanced quotes; empty quoted fragment; invalid chars; duplicates; lowercase folding (`RetailBank.Clients` → `retail_bank__clients`); known quirk `"a._b"` → `a___b` (never split on `__`).

### 9.3 `ingestion/lib/manifest.py`

```python
"""Strict loading and validation of ingestion source and pipeline manifests."""
```

```python
@dataclass(frozen=True)
class SourceConfig:
    source_id: str
    type: str
    airflow_conn_id: str

@dataclass(frozen=True)
class EntityConfig:
    fqn: str
    target_table: str
    incremental_column: str | None

@dataclass(frozen=True)
class PipelineManifest:
    source_id: str
    schedule: str | None
    catchup: bool
    tags: tuple[str, ...]
    entities: tuple[EntityConfig, ...]

def load_sources(path) -> dict[str, SourceConfig]
def load_pipeline(path) -> PipelineManifest
def validate_pipeline(sources, pipeline) -> None
```

Validation (fail-fast `ValueError`, precise messages): sources — `type` ∈ {`rdbms`, `kafka`} (kafka rejected until it lands), `airflow_conn_id` non-empty; pipeline — unknown top-level keys rejected, `source_id` must exist in the registry, `schedule` non-empty or absent, `tags` list of strings, `entities` non-empty list; each entity — required non-empty `fqn`, unknown entity keys rejected, duplicate FQNs fail-fast, `incremental_column` if present must match `^[a-z_][a-z0-9_]*$`; FQNs resolved through naming; targets unique. `yaml` imported lazily inside load functions (module imports without PyYAML); tests build objects directly, one load test uses a temp file with `skipUnless`.

### 9.4 `spark/raw/load_entity.py`

```python
"""Append-only entity loader: JDBC source -> raw-layer Iceberg with metadata columns."""
```

```python
def parse_args(argv: list[str]) -> argparse.Namespace
def resolve_driver(jdbc_url: str) -> str
def validate_target_table(target: str) -> tuple[str, str]
def build_read_query(source_table: str, incremental_column: str | None, watermark_value: str | None) -> str
def pick_partition_column(candidates: list[tuple[str, str]]) -> str | None
def detect_key_columns(spark, jdbc_url, user, password, source_table) -> list[tuple[str, str]]

def main(argv: list[str] | None = None) -> None
```

- `parse_args`: required `--jdbc-url`, `--jdbc-user`, `--jdbc-password`, `--source-table`, `--target-table`, `--process-id` (int > 0); optional `--incremental-column`, `--watermark-value`, `--partition-column` (debug-only, no YAML surface), `--num-partitions` (default 4).
- `resolve_driver`: `jdbc:sqlserver:` → `com.microsoft.sqlserver.jdbc.SQLServerDriver`; `jdbc:oracle:` → `oracle.jdbc.OracleDriver`; else `ValueError`.
- `validate_target_table`: exactly 3 dot-parts, first part `raw`, namespace/table match `^[a-z][a-z0-9_]*$`.
- `build_read_query`: full mode → the bare table; incremental mode → `(SELECT * FROM <table> WHERE <col> > <watermark>) AS q` (watermark omitted → bare table = bootstrap read). Pure, unit-tested.
- `pick_partition_column(candidates)`: `(name, jdbc_type)` pairs ordered PK-first then unique-index; first numeric/date/timestamp wins, else `None`. Pure, unit-tested.
- `detect_key_columns`: JVM interop over `spark._jvm.java.sql.DriverManager…getMetaData()` combining `getPrimaryKeys` + `getIndexInfo(unique=true)` + `getColumns`; any exception → empty list (single-partition fallback).
- `main`: parse/validate → SparkSession → `CREATE NAMESPACE IF NOT EXISTS raw.<ns>` → log start → choose partition column (`--partition-column` → `--incremental-column` → `pick_partition_column(detect_key_columns(…))`); if chosen → `SELECT min/max` bounds → partitioned JDBC read of `build_read_query(...)`; else single-stream → `withColumn("__load_dttm", F.current_timestamp())`, `withColumn("__process_id", F.lit(pid).cast("long"))` → if `not spark.catalog.tableExists(target)` run the generated §3.1 DDL via `spark.sql` → `df.writeTo(target).option("mergeSchema","true").append()` → log `load finished … rows_appended=<df.count()>` (+ `new_watermark=<max incremental col>` in incremental mode, informational).
- No YAML, no Airflow imports, no `#` comments. `__init__.py` files are empty-with-docstring.

### 9.5 `test/unit/` (stdlib `unittest`)

| File | Cases |
|---|---|
| `test_naming.py` | §2 examples, all §9.2 error cases, uniqueness, composition |
| `test_manifest.py` | valid objects; unknown keys; missing source; empty entities; missing `fqn`; duplicate FQN; `incremental_column` valid/invalid identifier; lazy-YAML load (`skipUnless`) |
| `test_load_entity.py` | `parse_args` incl. incremental args, bad `--process-id`; `resolve_driver` 3 cases; `validate_target_table` ok/wrong prefix/2/4 parts/bad identifier; `build_read_query` full / incremental / bootstrap (no watermark); `pick_partition_column` priority + skip + none |
| `test_code_style.py` | zero `tokenize.COMMENT` tokens in `ingestion/` + `spark/raw/` (shebang exempt) |

Tests put the repo root on `sys.path`; `load_entity.py` loads via `importlib.util.spec_from_file_location`.

### 9.6 Commands

`python3 -m unittest discover -s test/unit -v`; `./test/test.sh`. `test.sh` edit: in static validation, after `--validate-manifests-only`, add the unittest run with failure aborting the script — that one insertion only.

### 9.7 Smoke (live stand; after Step 0 + image rebuild)

```bash
curl -X POST "http://localhost:8090/sources/bank/activate?ttl_minutes=15"

docker compose exec airflow-scheduler spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  /opt/spark-jobs/raw/load_entity.py \
  --jdbc-url "jdbc:sqlserver://sydafa-src-mssql-mssql:1433;databaseName=retail_bank;encrypt=false" \
  --jdbc-user sydafa --jdbc-password "<reader password from conf/sydafa/sources.json>" \
  --source-table retail_bank.clients \
  --target-table raw.bank.retail_bank__clients \
  --process-id 1
```

Run twice; in Trino: `SELECT count(*), count(distinct __process_id) FROM raw.bank.retail_bank__clients` → `(100, 1)` then `(200, 2)`; the §3.4 latest-snapshot query → `100` both times. Incremental smoke (broker, service name from the activate response): add `--incremental-column client_id` → first run loads all rows; re-run → `rows_appended=0` and the count is unchanged (watermark gate works).

### 9.8 Git

One branch `kodik/ingestion-raw-phase1`, one PR for the whole phase, commits per §8.7 — the PR is created after the first commits and left for the user to review commit-by-commit and merge.

## 10. Future features (out of Phase 1)

- **ods layer**: processing of raw tables — column selection, merge/upsert strategies, conformed models; reads raw, writes its own catalog.
- **Kafka sources**: §3.6 — offset windows as the natural incremental column, Schema Registry schemas, flat topic entities.
- Optional `__latest` views; table maintenance (`expire_snapshots`, `rewrite_data_files`); Spark worker/shuffle tuning; reject-vs-map for Kafka `-`/uppercase topic names.
- Registry monitoring/dashboards; Dataset-driven Mart DAGs on the `raw://` assets; optional inter-table topology in the generator.
