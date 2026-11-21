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
        session.builder.getOrCreate.return_value = spark
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
        return {"lower_bound": lower, "upper_bound": upper, "empty_at_plan": False,
                "lower_bound_op": ">=" if lookback else ">"}

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


class TestPlanBatch(unittest.TestCase):
    """Batch resolution and planning before any source window is read."""

    def setUp(self):
        from ingestion.meta import registry

        self.registry = registry
        self.spark = MagicMock()

    def args(self, *extra):
        argv = build_argv("--source-id", "bank", "--dag-id", "dag", "--run-id", "run", *extra)
        return load_entity.parse_args(argv)

    def plan(self, batch, args=None, watermark=None, row=("10",), meta=None):
        args = args or self.args()
        info = meta or load_entity.jdbc.ColumnMeta("_batch_id", "bigint", 19, 0, True)
        keys = load_entity.jdbc.KeyMetadata((), (), {info.name: info})

        def persist(**kwargs):
            lower, op = kwargs["calculate_lower"](watermark)
            return {"lower_bound": lower, "lower_bound_op": op, "upper_bound": kwargs["probe_upper"](lower, op)}

        with patch.object(self.registry, "get_batch", return_value=batch), patch.object(
            self.registry, "plan_batch", side_effect=persist
        ) as plan, patch.object(load_entity.jdbc, "key_metadata", return_value=keys) as catalog, patch.object(
            load_entity.jdbc, "connection"
        ), patch.object(load_entity.jdbc, "first_row", return_value=row) as query:
            result, source, bounds = load_entity._plan_batch(self.spark, args, lambda: None)
        return result, catalog, plan, query, bounds

    def test_regular_probe_uses_locked_watermark(self):
        result, catalog, _, query, bounds = self.plan(None, watermark="8")
        self.assertEqual(result, {"lower_bound": "8", "lower_bound_op": ">", "upper_bound": "10"})
        catalog.assert_called_once()
        query.assert_called_once()
        self.assertEqual(query.call_args.args[1],
                         "SELECT MAX([_batch_id]) AS max_increment_col FROM [retail_bank].[transactions] WHERE [_batch_id] > 8")
        self.assertEqual(bounds, (None, None))

    def test_nullability_check_is_opt_out_only(self):
        info = load_entity.jdbc.ColumnMeta("_batch_id", "bigint", 19, 0, False)
        with self.assertRaisesRegex(ValueError, "does not confirm NOT NULL"):
            self.plan(None, meta=info)
        args = self.args("--incremental-config", json.dumps({"contract": {
            "source_column": "_batch_id", "position_type": "bigint", "check_not_null": False,
        }, "lookback": {"size": 0, "unit": None}}))
        _, _, _, query, _ = self.plan(None, args=args, meta=info)
        query.assert_called_once()
        self.assertTrue(query.call_args.args[1].endswith("WHERE [_batch_id] IS NOT NULL"))
        wrong_type = load_entity.jdbc.ColumnMeta("_batch_id", "int", 10, 0, False)
        with self.assertRaisesRegex(ValueError, "Incremental type mismatch"):
            self.plan(None, args=args, meta=wrong_type)

    def test_empty_slice_and_lower_max_are_not_errors(self):
        result, _, _, _, _ = self.plan(None, watermark="12", row=(None,))
        self.assertIsNone(result["upper_bound"])
        args = self.args()
        args.lookback_size = 5
        result, _, _, query, _ = self.plan(None, args=args, watermark="12", row=("10",))
        self.assertEqual(result["upper_bound"], "10")
        self.assertTrue(query.call_args.args[1].endswith("WHERE [_batch_id] > 7"))

    def test_succeeded_batch_short_circuits_without_source(self):
        batch = {"target_table": "raw.bank.retail_bank__transactions",
                 "incremental_column": "_batch_id", "incremental_type": "bigint", "status": "SUCCEEDED",
                 "row_count": 5, "finished_at": "2026-11-06"}
        args = self.args()
        with self.assertLogs(load_entity.LOGGER, level="INFO") as logs:
            result, catalog, plan, probe, bounds = self.plan(batch, args=args)
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

    def test_retry_checks_type_without_reprobing_incremental_maximum(self):
        batch = {"target_table": "raw.bank.retail_bank__transactions",
                 "incremental_column": "_batch_id", "incremental_type": "bigint", "status": "RUNNING"}
        result, catalog, plan, probe, bounds = self.plan(batch)
        self.assertIs(result, batch)
        self.assertIsNone(bounds)
        catalog.assert_called_once()
        plan.assert_not_called()
        probe.assert_not_called()

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
            load_entity, "_plan_batch", return_value=(self.batch(status="SUCCEEDED"), None, None)
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
            load_entity, "_plan_batch", return_value=(batch, None, None)
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
            load_entity, "_plan_batch", return_value=(self.batch(status="SUCCEEDED"), None, None)
        ):
            load_entity.managed_load(self.spark, self.args())
        self.source.unpersist.assert_not_called()

    def test_provably_empty_window_finalizes_without_reading(self):
        with self.assertLogs(load_entity.LOGGER, level="INFO") as logs, patch.dict(
            sys.modules, {"psycopg2": self.psycopg2}
        ), patch.object(
            load_entity, "_plan_batch", return_value=(self.batch(upper_bound=None, empty_at_plan=True), None, None)
        ), patch.object(load_entity.reader, "read_extract_frame") as read_frame, patch.object(
            self.registry, "finalize_batch"
        ) as finalize:
            load_entity.managed_load(self.spark, self.args())
        read_frame.assert_not_called()
        self.assertTrue(any(
            "No source rows match the incremental window. JDBC read and Iceberg append skipped. reason=empty_window"
            in line for line in logs.output
        ), logs.output)
        self.assertEqual(
            (finalize.call_args.kwargs["row_count"], finalize.call_args.kwargs["observed_max"]),
            (0, None),
        )

    def test_lookback_window_with_equal_bounds_is_read(self):
        with patch.dict(sys.modules, {"psycopg2": self.psycopg2}), patch.object(
            load_entity, "_plan_batch",
            return_value=(self.batch(lower_bound="10", lower_bound_op=">=", lookback_size=3,
                                     reason="regular", watermark_before="10"), None, None)
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
            load_entity, "_plan_batch", return_value=(self.batch(), None, None)
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
            load_entity, "_plan_batch", return_value=(self.batch(), None, None)
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
