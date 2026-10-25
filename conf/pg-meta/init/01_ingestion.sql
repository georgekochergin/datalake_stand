-- Operational schema of the ingestion framework on pg-meta.
-- Executed once by the postgres entrypoint on first startup (empty data dir).

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
