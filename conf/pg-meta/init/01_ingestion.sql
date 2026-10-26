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
CREATE UNIQUE INDEX IF NOT EXISTS airflow_dagruns_log_start_idx
  ON ingestion.airflow_dagruns_log (dag_id, run_id) WHERE status = 'START';

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

CREATE TABLE IF NOT EXISTS ingestion.extraction_state (
  source_id text NOT NULL,
  entity_fqn text NOT NULL,
  configured_mode text NOT NULL CHECK (configured_mode IN ('incremental', 'full_snapshot')),
  incremental_column text,
  incremental_type text,
  watermark text,
  revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
  last_batch_id bigint CHECK (last_batch_id > 0),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (source_id, entity_fqn),
  CHECK ((configured_mode = 'incremental' AND incremental_column IS NOT NULL AND incremental_type IS NOT NULL)
      OR (configured_mode = 'full_snapshot' AND incremental_column IS NULL AND incremental_type IS NULL AND watermark IS NULL))
);

CREATE TABLE IF NOT EXISTS ingestion.extraction_batches (
  batch_id bigserial PRIMARY KEY CHECK (batch_id > 0),
  process_id bigint NOT NULL CHECK (process_id > 0),
  dag_id text NOT NULL,
  run_id text NOT NULL,
  source_id text NOT NULL,
  entity_fqn text NOT NULL,
  target_table text NOT NULL,
  mode text NOT NULL CHECK (mode IN ('incremental', 'full_snapshot')),
  reason text NOT NULL CHECK (reason IN ('bootstrap', 'regular', 'manual_full')),
  incremental_column text,
  incremental_type text,
  expected_revision bigint NOT NULL CHECK (expected_revision > 0),
  watermark_before text,
  lookback_size numeric(38,0) NOT NULL DEFAULT 0 CHECK (lookback_size >= 0),
  lookback_unit text CHECK (lookback_unit IN ('h', 'd')),
  lower_bound text,
  upper_bound text,
  empty_at_plan boolean NOT NULL DEFAULT false,
  status text NOT NULL DEFAULT 'PLANNED' CHECK (status IN ('PLANNED', 'RUNNING', 'FAILED', 'SUCCEEDED')),
  attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  row_count bigint CHECK (row_count >= 0),
  observed_max text,
  watermark_after text,
  reason_text text,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz,
  UNIQUE (dag_id, run_id, source_id, entity_fqn),
  FOREIGN KEY (source_id, entity_fqn) REFERENCES ingestion.extraction_state,
  CHECK ((incremental_column IS NULL) = (incremental_type IS NULL)),
  CHECK (mode <> 'incremental' OR (incremental_column IS NOT NULL AND reason = 'regular' AND watermark_before IS NOT NULL)),
  CHECK (reason <> 'bootstrap' OR (mode = 'full_snapshot' AND incremental_column IS NOT NULL AND watermark_before IS NULL AND lower_bound IS NULL)),
  CHECK (reason <> 'manual_full' OR (mode = 'full_snapshot' AND lower_bound IS NULL AND upper_bound IS NULL AND reason_text IS NOT NULL AND length(btrim(reason_text)) > 0)),
  CHECK (incremental_column IS NOT NULL OR (mode = 'full_snapshot' AND watermark_before IS NULL AND watermark_after IS NULL AND lower_bound IS NULL AND upper_bound IS NULL AND lookback_size = 0 AND lookback_unit IS NULL)),
  CHECK (status <> 'SUCCEEDED' OR (row_count IS NOT NULL AND finished_at IS NOT NULL AND last_error IS NULL)),
  CHECK (status = 'SUCCEEDED' OR (row_count IS NULL AND observed_max IS NULL AND watermark_after IS NULL))
);

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'ingestion.extraction_state'::regclass AND conname = 'extraction_state_last_batch_fk') THEN
    ALTER TABLE ingestion.extraction_state ADD CONSTRAINT extraction_state_last_batch_fk
      FOREIGN KEY (last_batch_id) REFERENCES ingestion.extraction_batches (batch_id)
      DEFERRABLE INITIALLY DEFERRED;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS ingestion.extraction_state_migrations (
  migration_id bigserial PRIMARY KEY CHECK (migration_id > 0),
  source_id text NOT NULL,
  entity_fqn text NOT NULL,
  old_revision bigint NOT NULL CHECK (old_revision > 0),
  new_revision bigint NOT NULL CHECK (new_revision = old_revision + 1),
  old_mode text NOT NULL CHECK (old_mode IN ('incremental', 'full_snapshot')),
  new_mode text NOT NULL CHECK (new_mode IN ('incremental', 'full_snapshot')),
  old_column text,
  new_column text,
  old_type text,
  new_type text,
  old_watermark text,
  new_watermark text,
  performed_by text NOT NULL CHECK (length(btrim(performed_by)) > 0),
  reason_text text NOT NULL CHECK (length(btrim(reason_text)) > 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (source_id, entity_fqn) REFERENCES ingestion.extraction_state,
  UNIQUE (source_id, entity_fqn, new_revision),
  CHECK ((old_mode = 'incremental' AND old_column IS NOT NULL AND old_type IS NOT NULL)
      OR (old_mode = 'full_snapshot' AND old_column IS NULL AND old_type IS NULL AND old_watermark IS NULL)),
  CHECK ((new_mode = 'incremental' AND new_column IS NOT NULL AND new_type IS NOT NULL)
      OR (new_mode = 'full_snapshot' AND new_column IS NULL AND new_type IS NULL AND new_watermark IS NULL))
);
