"""Unit tests for the incremental window loader (spark/raw/rdbms/incremental/load_entity.py)."""

import sys
import json
import os
import types
import unittest
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.spark.raw.rdbms.incremental import load_entity

MSSQL_URL = "jdbc:sqlserver://host:1433;databaseName=retail_bank"
ORACLE_URL = "jdbc:oracle:thin:@//host:1521/sydafa"

BASE_ARGS = [
    "--jdbc-url",
    MSSQL_URL,
    "--jdbc-user",
    "sydafa",
    "--jdbc-password",
    "secret",
    "--source-table",
    "retail_bank.transactions",
    "--target-table",
    "raw.bank.retail_bank__transactions",
    "--process-id",
    "42",
    "--incremental-config",
    json.dumps({"contract": {"source_column": "_batch_id", "position_type": "bigint"},
                "lookback": {"size": 0, "unit": None}}),
]


def build_argv(*extra: str) -> list[str]:
    """Base CLI arguments plus extras."""
    return BASE_ARGS + list(extra)


class TestParseArgs(unittest.TestCase):
    """CLI argument parsing and validation."""

    def test_happy_path(self):
        args = load_entity.parse_args(build_argv())
        self.assertEqual(args.source_table, "retail_bank.transactions")
        self.assertEqual(args.target_table, "raw.bank.retail_bank__transactions")
        self.assertEqual(args.process_id, 42)
        self.assertEqual(args.incremental_column, "_batch_id")
        self.assertEqual(args.num_partitions, 4)

    def test_incremental_column_required(self):
        argv = BASE_ARGS[:-2]
        with self.assertRaises(SystemExit):
            load_entity.parse_args(argv)

    def test_managed_incremental_args(self):
        args = load_entity.parse_args(build_argv(
            "--source-id", "bank", "--dag-id", "dag", "--run-id", "run",
            "--incremental-config", json.dumps({"contract": {"source_column": "updated_at", "position_type": "timestamp_local(7)"}, "lookback": {"size": 3, "unit": "h"}}),
        ))
        self.assertEqual((args.source_id, args.dag_id, args.run_id), ("bank", "dag", "run"))
        self.assertEqual((args.lookback_size, args.lookback_unit), (3, "h"))
        self.assertEqual(args.position_type, "timestamp_local(7)")
        self.assertFalse(hasattr(args, "force_full"))

    def test_lookback_given_defaults_off(self):
        args = load_entity.parse_args(build_argv("--source-id", "bank"))
        self.assertFalse(hasattr(args, "lookback_given"))
        self.assertEqual(args.lookback_size, 0)
        self.assertIsNone(args.lookback_unit)

    def test_unknown_lookback_unit_rejected(self):
        with self.assertRaises(SystemExit):
            load_entity.parse_args(build_argv("--lookback-unit", "minutes"))

    def test_missing_required_argument(self):
        with self.assertRaises(SystemExit):
            load_entity.parse_args(build_argv()[2:])

    def test_zero_process_id(self):
        with self.assertRaises(ValueError):
            load_entity.parse_args(build_argv("--process-id", "0"))

    def test_negative_process_id(self):
        with self.assertRaises(ValueError):
            load_entity.parse_args(build_argv("--process-id", "-1"))

    def test_zero_num_partitions(self):
        with self.assertRaises(ValueError):
            load_entity.parse_args(build_argv("--num-jdbc-partitions", "0"))

    def test_jdbc_partitioning_switch(self):
        self.assertFalse(load_entity.parse_args(build_argv()).no_jdbc_partitioning)
        args = load_entity.parse_args(
            build_argv("--no-jdbc-partitioning", "--num-jdbc-partitions", "7")
        )
        self.assertTrue(args.no_jdbc_partitioning)
        self.assertEqual(args.num_partitions, 7)

    def test_invalid_incremental_column(self):
        with self.assertRaises(ValueError):
            load_entity.parse_args(build_argv("--incremental-config", json.dumps({"contract": {"source_column": "Client-Id", "position_type": "bigint"}, "lookback": {"size": 0, "unit": None}})))


class TestMain(unittest.TestCase):
    """The entry point requires a managed source and dispatches only to managed_load."""

    def test_missing_source_id_rejected(self):
        with self.assertRaisesRegex(ValueError, "source-id"):
            load_entity.main(build_argv())

    def test_dispatches_to_managed_load(self):
        spark = MagicMock()
        session = MagicMock()
        session.builder.appName.return_value.getOrCreate.return_value = spark
        sql = types.ModuleType("pyspark.sql")
        sql.SparkSession = session
        pyspark = types.ModuleType("pyspark")
        pyspark.sql = sql
        managed = MagicMock()
        with patch.dict(sys.modules, {"pyspark": pyspark, "pyspark.sql": sql}), patch.object(
            load_entity, "managed_load", managed
        ):
            load_entity.main(build_argv("--source-id", "bank", "--dag-id", "dag", "--run-id", "run"))
        spark.stop.assert_called_once()
        managed.assert_called_once()
        self.assertIs(managed.call_args.args[0], spark)
        self.assertEqual(managed.call_args.args[1].source_id, "bank")


class TestEmptyIncrementalWindow(unittest.TestCase):
    """Only a zero-lookback window with equal typed bounds is provably empty."""

    def batch(self, lower="10", upper="10", lookback=0):
        return {"lower_bound": lower, "upper_bound": upper, "lookback_size": lookback}

    def test_equal_bounds_with_zero_lookback_is_empty(self):
        self.assertTrue(load_entity.empty_incremental_window(self.batch()))
        self.assertTrue(load_entity.empty_incremental_window(self.batch(lower="2026-10-26", upper="2026-10-26")))

    def test_lookback_window_with_equal_bounds_is_not_empty(self):
        self.assertFalse(load_entity.empty_incremental_window(self.batch(lookback=3)))
        self.assertFalse(load_entity.empty_incremental_window(self.batch(lower=None, lookback=3)))

    def test_strict_window_with_distinct_bounds_is_not_empty(self):
        for changes in ({"lower": "7"}, {"upper": "12"}, {"lower": None}, {"lower": None, "upper": None}):
            with self.subTest(changes=changes):
                self.assertFalse(load_entity.empty_incremental_window(self.batch(**changes)))


class TestSourcePosition(unittest.TestCase):
    """Source-side incremental inspection over a stubbed JDBC layer."""

    def args(self, **extra):
        argv = build_argv(*sum(([k, v] for k, v in extra.items()), []))
        return load_entity.parse_args(argv)

    def inspect(self, metadata, row):
        spark = MagicMock()
        with patch.object(load_entity.jdbc, "connection") as managed, patch.object(
            load_entity.jdbc, "column_metadata", return_value=metadata
        ) as columns, patch.object(load_entity.jdbc, "first_row", return_value=row) as first:
            result = load_entity.source_position(spark, self.args())
        self.assertEqual(managed.call_count, 2)
        self.assertIn("FROM [retail_bank].[transactions]", columns.call_args.args[1])
        return result, first.call_args.args[1]

    def test_regular_inspection(self):
        result, sql = self.inspect([("_batch_id", "bigint", 19, 0)], ["10", "5", "5"])
        self.assertEqual(result, ("bigint", "10", False, False))
        self.assertEqual(sql, "SELECT MAX([_batch_id]), COUNT(*), COUNT([_batch_id]) FROM [retail_bank].[transactions]")

    def test_type_mismatch_prevents_aggregates(self):
        with patch.object(load_entity.jdbc, "connection"), patch.object(
            load_entity.jdbc, "column_metadata", return_value=[("_batch_id", "int", 10, 0)]
        ), patch.object(load_entity.jdbc, "first_row") as aggregate:
            with self.assertRaisesRegex(ValueError, "expected=bigint actual=integer"):
                load_entity.source_position(MagicMock(), self.args())
        aggregate.assert_not_called()

    def test_retry_checks_metadata_without_replanning(self):
        from ingestion.meta import registry

        args = self.args()
        batch = {"target_table": args.target_table, "incremental_column": "_batch_id",
                 "incremental_type": "bigint", "status": "FAILED"}
        with patch.object(registry, "get_batch", return_value=batch), patch.object(
            load_entity, "source_kind", return_value="bigint"
        ) as metadata, patch.object(load_entity, "source_position") as aggregate:
            self.assertIs(load_entity._plan_batch(MagicMock(), args, MagicMock()), batch)
        metadata.assert_called_once()
        aggregate.assert_not_called()

    def test_empty_and_null_diagnostics(self):
        self.assertEqual(self.inspect([("_batch_id", "bigint", 19, 0)], ["0", "0", "0"])[0][2:], (True, False))
        self.assertEqual(self.inspect([("_batch_id", "bigint", 19, 0)], ["5", "5", "4"])[0][2:], (False, True))

    def test_oracle_temporal_kind(self):
        spark = MagicMock()
        args = load_entity.parse_args(build_argv("--jdbc-url", ORACLE_URL))
        args.position_type = "timestamp_instant(6)"
        with patch.object(load_entity.jdbc, "connection"), patch.object(
            load_entity.jdbc, "column_metadata", return_value=[("_batch_id", "TIMESTAMP(6) WITH TIME ZONE", 0, 6)]
        ), patch.object(load_entity.jdbc, "first_row", return_value=["2026-01-01T00:00:00.000000Z", "3", "3"]) as first:
            result = load_entity.source_position(spark, args)
        self.assertEqual(result[0], "timestamp_instant(6)")
        self.assertIn("TO_CHAR", first.call_args.args[1])


class TestPlanBatch(unittest.TestCase):
    """Batch resolution and planning before any source window is read."""

    def setUp(self):
        from ingestion.meta import registry

        self.registry = registry
        self.spark = MagicMock()

    def args(self, *extra):
        argv = build_argv("--source-id", "bank", "--dag-id", "dag", "--run-id", "run", *extra)
        return load_entity.parse_args(argv)

    def plan(self, batch, planned=None, position=None, args=None):
        with patch.object(self.registry, "get_batch", return_value=batch) as got, patch.object(
            self.registry, "plan_batch", return_value=planned
        ) as plan, patch.object(load_entity, "source_position", return_value=position) as probe, patch.object(
            load_entity, "source_kind", return_value="bigint"
        ):
            result = load_entity._plan_batch(self.spark, args or self.args(), lambda: None)
        return result, got, plan, probe

    def test_incremental_entity_captures_detected_position(self):
        planned = {"status": "PLANNED"}
        args = self.args()
        result, _, plan, probe = self.plan(None, planned=planned,
                                           position=("bigint", "10", False, False), args=args)
        self.assertIs(result, planned)
        self.assertEqual(
            (plan.call_args.kwargs["incremental_type"], plan.call_args.kwargs["upper_bound"],
             plan.call_args.kwargs["empty_at_plan"]),
            ("bigint", "10", False),
        )
        probe.assert_called_once()

    def test_default_lookback_resolved_by_runtime_kind(self):
        args = self.args()
        _, _, plan, _ = self.plan(None, planned={"status": "PLANNED"},
                                  position=("bigint", "10", False, False), args=args)
        self.assertEqual((plan.call_args.kwargs["lookback_size"],
                          plan.call_args.kwargs["lookback_unit"]), (0, None))
        timestamp_args = self.args("--incremental-config", json.dumps({"contract": {"source_column": "updated_at", "position_type": "timestamp_instant(6)"}, "lookback": {"size": 3, "unit": "h"}}))
        _, _, plan, _ = self.plan(None, planned={"status": "PLANNED"},
                                  position=("timestamp_instant(6)", "2026-01-01T00:00:00.000000Z", False, False),
                                  args=timestamp_args)
        self.assertEqual((plan.call_args.kwargs["lookback_size"],
                          plan.call_args.kwargs["lookback_unit"]), (3, "h"))

    def test_succeeded_batch_short_circuits_without_source(self):
        batch = {"target_table": "raw.bank.retail_bank__transactions",
                 "incremental_column": "_batch_id", "incremental_type": "bigint", "status": "SUCCEEDED",
                 "row_count": 5, "finished_at": "2026-11-06"}
        args = self.args()
        with self.assertLogs(load_entity.LOGGER, level="INFO") as logs:
            result, _, plan, probe = self.plan(batch, args=args)
        self.assertIs(result, batch)
        plan.assert_not_called()
        probe.assert_not_called()
        self.assertTrue(any("repeat read skipped" in line for line in logs.output))

    def test_existing_batch_target_mismatch_rejected(self):
        batch = {"target_table": "raw.bank.other", "status": "PLANNED"}
        args = self.args()
        with self.assertRaisesRegex(ValueError, "target does not match"):
            self.plan(batch, args=args)

    def test_retry_after_manifest_column_change_rejected(self):
        for status in ("PLANNED", "RUNNING", "FAILED", "SUCCEEDED"):
            with self.subTest(status=status):
                batch = {"target_table": "raw.bank.retail_bank__transactions",
                         "incremental_column": "_batch_id", "status": status,
                         "row_count": 5, "finished_at": "2026-11-07"}
                args = self.args()
                args.incremental_column = "other_position"
                with self.assertRaisesRegex(ValueError, "incremental column does not match"):
                    self.plan(batch, args=args)

    def test_retry_replays_frozen_window_without_source_access(self):
        batch = {"target_table": "raw.bank.retail_bank__transactions",
                 "incremental_column": "_batch_id", "incremental_type": "bigint",
                 "status": "RUNNING"}
        args = self.args()
        result, _, plan, probe = self.plan(batch, position=("bigint", "99", False, True), args=args)
        self.assertIs(result, batch)
        plan.assert_not_called()
        probe.assert_not_called()

    def test_null_incremental_value_rejected_for_new_batch(self):
        args = self.args()
        with self.assertRaisesRegex(ValueError, "NULL incremental"):
            self.plan(None, position=("bigint", "10", False, True), args=args)

    def test_calculate_lower_rejects_regression(self):
        _, _, plan, _ = self.plan(None, planned={"status": "PLANNED"},
                                  position=("bigint", "10", False, False), args=self.args())
        calculate_lower = plan.call_args.kwargs["calculate_lower"]
        with self.assertRaisesRegex(ValueError, "SOURCE_POSITION_REGRESSION"):
            calculate_lower("12")
        self.assertEqual(calculate_lower("8"), ("8", ">"))

    def test_calculate_lower_bootstrap_has_no_lower(self):
        args = self.args()
        _, _, plan, _ = self.plan(None, planned={"status": "PLANNED"},
                                  position=("bigint", "10", False, False), args=args)
        self.assertEqual(plan.call_args.kwargs["calculate_lower"](None), (None, ">"))

    def test_calculate_lower_empty_source_with_watermark_rejected(self):
        args = self.args()
        _, _, plan, _ = self.plan(None, planned={"status": "PLANNED"},
                                  position=("bigint", None, True, False), args=args)
        with self.assertRaisesRegex(ValueError, "SOURCE_POSITION_REGRESSION"):
            plan.call_args.kwargs["calculate_lower"]("12")

    def test_invalid_lookback_rejected_before_planning(self):
        with self.assertRaisesRegex(ValueError, "forbids a unit"):
            self.args("--incremental-config", json.dumps({"contract": {"source_column": "_batch_id", "position_type": "bigint"}, "lookback": {"size": 1, "unit": "h"}}))


class TestManagedLoad(unittest.TestCase):
    """Managed execution: planning, empty windows, finalization, and failure."""

    def setUp(self):
        from ingestion.meta import registry

        self.registry = registry
        self.spark = MagicMock()
        self.source = MagicMock()
        psycopg2 = types.ModuleType("psycopg2")
        psycopg2.connect = MagicMock(return_value=MagicMock())
        self.psycopg2 = psycopg2

    def test_missing_dsn_fails_with_explicit_error(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "INGESTION_META_DSN is not set"):
                load_entity.managed_load(self.spark, self.args())

    def test_registry_connection_uses_provided_dsn(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.dict(
            os.environ, {"INGESTION_META_DSN": "host=meta-dsn"}
        ), patch.object(
            load_entity, "_plan_batch", return_value=self.batch(status="SUCCEEDED")
        ) as plan:
            load_entity.managed_load(self.spark, self.args())
            plan.call_args.args[2]()
        self.psycopg2.connect.assert_called_once_with("host=meta-dsn")

    def batch(self, **changes):
        values = {"process_id": 7, "status": "RUNNING", "incremental_type": "bigint",
                  "lower_bound": "5", "lower_bound_op": ">",
                  "upper_bound": "10", "lookback_size": 0, "empty_at_plan": False,
                  "watermark_before": None, "reason": "bootstrap"}
        values.update(changes)
        return values

    def args(self, *extra):
        return load_entity.parse_args(build_argv(
            "--source-id", "bank", "--dag-id", "dag", "--run-id", "run", "--process-id", "7", *extra
        ))

    def load(self, batch, args=None, appended=(5, "10"), append_error=None):
        """Run managed_load with every collaborator patched; return the patchers' mocks."""
        args = args or self.args()
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.dict(
            os.environ, {"INGESTION_META_DSN": "host=meta-dsn"}
        ), patch.object(
            load_entity, "_plan_batch", return_value=batch
        ), patch.object(load_entity.reader, "read_extract_frame", return_value=self.source) as read_frame, patch.object(
            load_entity.window,
            "window_summary",
            return_value=(appended[0], appended[1], self.source),
        ), patch.object(
            load_entity.append,
            "append_raw",
            **({"side_effect": append_error} if append_error else {}),
        ) as append, patch.object(self.registry, "start_batch") as started, patch.object(
            self.registry, "finalize_batch"
        ) as finalize, patch.object(self.registry, "fail_batch") as failed:
            load_entity.managed_load(self.spark, args)
        return types.SimpleNamespace(read_frame=read_frame, append=append, started=started,
                                     finalize=finalize, failed=failed)

    def test_succeeded_batch_is_noop(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.object(
            load_entity, "_plan_batch", return_value=self.batch(status="SUCCEEDED")
        ):
            load_entity.managed_load(self.spark, self.args())
        self.source.unpersist.assert_not_called()

    def test_provably_empty_window_finalizes_without_reading(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.object(
            load_entity, "_plan_batch", return_value=self.batch(lower_bound="10")
        ), patch.object(load_entity.reader, "read_extract_frame") as read_frame, patch.object(
            self.registry, "finalize_batch"
        ) as finalize:
            load_entity.managed_load(self.spark, self.args())
        read_frame.assert_not_called()
        self.assertEqual(
            (finalize.call_args.kwargs["row_count"], finalize.call_args.kwargs["observed_max"]),
            (0, None),
        )

    def test_lookback_window_with_equal_bounds_is_read(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.object(
            load_entity, "_plan_batch",
            return_value=self.batch(lower_bound="10", lookback_size=3, reason="regular",
                                    watermark_before="10")
        ), patch.object(load_entity.reader, "read_extract_frame", return_value=self.source) as read_frame, patch.object(
            load_entity.window, "window_summary", return_value=(2, "10", self.source)
        ), patch.object(load_entity.append, "append_raw"), patch.object(
            self.registry, "start_batch"), patch.object(self.registry, "finalize_batch"):
            load_entity.managed_load(self.spark, self.args())
        read_frame.assert_called_once()

    def test_success_finalizes_with_typed_maximum(self):
        run = self.load(self.batch())
        run.started.assert_called_once_with(process_id=7, source_id="bank",
                                            entity_fqn="retail_bank.transactions", db_connect=ANY)
        run.failed.assert_not_called()
        self.assertEqual(run.finalize.call_args.kwargs["watermark_after"], "10")
        self.source.unpersist.assert_called_once()

    def test_regular_window_never_regresses_watermark(self):
        run = self.load(self.batch(watermark_before="12", reason="regular"))
        self.assertEqual(run.finalize.call_args.kwargs["watermark_after"], "12")

    def test_failure_path_records_failed_status_and_reraises(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.object(
            load_entity, "_plan_batch", return_value=self.batch()
        ), patch.object(load_entity.reader, "read_extract_frame", return_value=self.source), patch.object(
            load_entity.window, "window_summary", return_value=(5, "10", self.source)
        ), patch.object(
            load_entity.append, "append_raw", side_effect=RuntimeError("secret boom")
        ), patch.object(self.registry, "start_batch"), patch.object(self.registry, "fail_batch") as failed:
            with self.assertRaisesRegex(RuntimeError, "secret boom"):
                load_entity.managed_load(self.spark, self.args())
        self.assertEqual(
            {key: failed.call_args.kwargs[key] for key in ("process_id", "source_id", "entity_fqn")},
            {"process_id": 7, "source_id": "bank", "entity_fqn": "retail_bank.transactions"},
        )
        self.assertNotIn("error", failed.call_args.kwargs)
        self.source.unpersist.assert_called_once()

    def test_failure_recording_failure_still_reraises(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.object(
            load_entity, "_plan_batch", return_value=self.batch()
        ), patch.object(load_entity.reader, "read_extract_frame", return_value=self.source), patch.object(
            load_entity.window, "window_summary", return_value=(5, "10", self.source)
        ), patch.object(
            load_entity.append, "append_raw", side_effect=RuntimeError("secret boom")
        ), patch.object(self.registry, "start_batch"), patch.object(
            self.registry, "fail_batch", side_effect=RuntimeError("meta down")
        ):
            with self.assertRaisesRegex(RuntimeError, "secret boom"), self.assertLogs(load_entity.LOGGER, level="WARNING"):
                load_entity.managed_load(self.spark, self.args())


if __name__ == "__main__":
    unittest.main()
