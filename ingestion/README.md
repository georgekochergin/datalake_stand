# Raw-Layer Ingestion Framework

Loads source entities into the raw layer — `raw.<source_id>.<table>` — as an
append-only, schema-change-tolerant mirror with `__load_dttm` / `__process_id`
metadata columns. Full design, schemas, and semantics: `implementation_plan.md`
at the repo root.

## Layout

- `sources.yaml` — source registry (`type: rdbms` + `airflow_conn_id`).
- `pipelines/<source_id>.yaml` — DAG configs: entities list; optional scalar
  `incremental_column` switches an entity from full-snapshot to incremental.
- `lib/` — naming (every dot becomes `__`) and strict manifest validation.
- `generate_dags.py` — renders static DAG files into `airflow/dags/generated/`.

## Commands

```bash
python3 ingestion/generate_dags.py          (re)generate DAG files
python3 ingestion/generate_dags.py --check  drift gate: exit 1 if files differ
python3 -m unittest discover -s test/unit   framework unit tests
./test/test.sh                              full isolated e2e (runs the generator first)
```

## Runtime pieces (outside this directory)

- `spark/raw/load_entity.py` — the loader (JDBC read, auto-parallel, first-load
  DDL partitioned by `days(__load_dttm)` with `accept-any-schema`, then
  `mergeSchema` appends).
- `airflow/plugins/ingestion/` — operators (process_id, task logging with
  watermark advance, SparkSubmit wrapper) and the pg-meta registry
  (`pg-meta` service, db `meta`).
