-- Operational schema of the ingestion framework on pg-meta.
-- Executed once by the postgres entrypoint on first startup (empty data dir).
-- The incremental registry (incremental_state, incremental_batches,
-- incremental_state_migrations) tracks incremental entities only: full-slice
-- entities and snapshot flows do not write it.

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
CREATE INDEX IF NOT EXISTS airflow_dagruns_log_run_idx
  ON ingestion.airflow_dagruns_log (dag_id, run_id);

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

CREATE TABLE IF NOT EXISTS ingestion.incremental_state (
  source_id text NOT NULL,
  entity_fqn text NOT NULL,
  incremental_column text NOT NULL,
  incremental_type text NOT NULL,
  watermark text,
  revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
  last_process_id bigint CHECK (last_process_id > 0),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (source_id, entity_fqn)
);

CREATE TABLE IF NOT EXISTS ingestion.incremental_batches (
  process_id bigint NOT NULL CHECK (process_id > 0),
  dag_id text NOT NULL,
  run_id text NOT NULL,
  source_id text NOT NULL,
  entity_fqn text NOT NULL,
  target_table text NOT NULL,
  reason text NOT NULL CHECK (reason IN ('bootstrap', 'regular')),
  incremental_column text NOT NULL,
  incremental_type text NOT NULL,
  expected_revision bigint NOT NULL CHECK (expected_revision > 0),
  watermark_before text,
  lookback_size numeric(38,0) NOT NULL DEFAULT 0 CHECK (lookback_size >= 0),
  lookback_unit text CHECK (lookback_unit IN ('h', 'd')),
  lower_bound text,
  lower_bound_op text NOT NULL CHECK (lower_bound_op IN ('>', '>=')),
  upper_bound text,
  empty_at_plan boolean NOT NULL DEFAULT false,
  status text NOT NULL DEFAULT 'PLANNED' CHECK (status IN ('PLANNED', 'RUNNING', 'FAILED', 'SUCCEEDED')),
  attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  row_count bigint CHECK (row_count >= 0),
  observed_max text,
  watermark_after text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz,
  PRIMARY KEY (process_id, source_id, entity_fqn),
  FOREIGN KEY (source_id, entity_fqn) REFERENCES ingestion.incremental_state,
  CHECK (reason <> 'bootstrap' OR (watermark_before IS NULL AND lower_bound IS NULL)),
  CHECK (reason <> 'regular' OR watermark_before IS NOT NULL),
  CHECK (status <> 'SUCCEEDED' OR (row_count IS NOT NULL AND finished_at IS NOT NULL)),
  CHECK (status = 'SUCCEEDED' OR (row_count IS NULL AND observed_max IS NULL AND watermark_after IS NULL))
);

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'ingestion.incremental_state'::regclass AND conname = 'incremental_state_last_process_fk') THEN
    ALTER TABLE ingestion.incremental_state ADD CONSTRAINT incremental_state_last_process_fk
      FOREIGN KEY (last_process_id, source_id, entity_fqn)
      REFERENCES ingestion.incremental_batches (process_id, source_id, entity_fqn)
      DEFERRABLE INITIALLY DEFERRED;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS ingestion.incremental_state_migrations (
  migration_id bigserial PRIMARY KEY CHECK (migration_id > 0),
  source_id text NOT NULL,
  entity_fqn text NOT NULL,
  old_revision bigint NOT NULL CHECK (old_revision > 0),
  new_revision bigint NOT NULL CHECK (new_revision = old_revision + 1),
  old_column text NOT NULL,
  new_column text NOT NULL,
  old_type text NOT NULL,
  new_type text NOT NULL,
  old_watermark text,
  new_watermark text,
  performed_by text NOT NULL CHECK (length(btrim(performed_by)) > 0),
  reason_text text NOT NULL CHECK (length(btrim(reason_text)) > 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (source_id, entity_fqn) REFERENCES ingestion.incremental_state,
  UNIQUE (source_id, entity_fqn, new_revision)
);
