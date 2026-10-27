"""Behavioral ingestion operator tests using scoped Airflow module doubles."""

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


class FakeOperator:
    """Preserve constructor arguments and record Spark submission."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.on_failure_callback = kwargs.get("on_failure_callback")
        self.env_vars = kwargs.get("env_vars")
        self.submissions = []

    def execute(self, context):
        self.submissions.append((list(self.application_args), context))


def load_operators():
    """Import under a private package and restore every mocked module afterwards."""
    modules = {}
    for name in (
        "airflow", "airflow.providers", "airflow.providers.apache", "airflow.providers.apache.spark",
        "airflow.providers.apache.spark.operators", "airflow.providers.apache.spark.operators.spark_submit",
        "airflow.sdk", "_operator_test_ingestion",
    ):
        modules[name] = types.ModuleType(name)
    modules["airflow.sdk"].BaseHook = MagicMock()
    modules["airflow.sdk"].BaseOperator = FakeOperator
    modules["airflow.providers.apache.spark.operators.spark_submit"].SparkSubmitOperator = FakeOperator
    modules["_operator_test_ingestion"].registry = MagicMock()
    path = Path(__file__).resolve().parents[2] / "airflow/plugins/ingestion/operators.py"
    spec = importlib.util.spec_from_file_location("_operator_test_ingestion.operators", path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module


class TestIngestOperator(unittest.TestCase):
    """Validate runtime connection compatibility, CLI arguments and run-scoped IDs."""

    def setUp(self):
        self.module = load_operators()
        self.module.BaseHook.get_connection.return_value = types.SimpleNamespace(
            conn_type="mssql", conn_id="source", host="host", port=None,
            schema="db", login="reader", password="secret",
        )
        self.meta = types.SimpleNamespace(host="pg", port=None, schema="metadata", login="writer", password="pg-secret")
        source = self.module.BaseHook.get_connection.return_value
        self.module.BaseHook.get_connection.side_effect = lambda conn_id: self.meta if conn_id == "ingestion_meta" else source
        self.context = {"ti": MagicMock(), "dag_run": types.SimpleNamespace(dag_id="dag", run_id="run", conf={})}
        self.context["ti"].xcom_pull.return_value = 42

    def operator(self, **kwargs):
        return self.module.IngestAppendOperator(
            task_id="load", source_id="bank", airflow_conn_id="source", fqn="s.t",
            target_table="raw.bank.s__t", **kwargs,
        )

    def test_defaults_and_explicit_options(self):
        operator = self.operator()
        operator.execute(self.context)
        args = operator.application_args
        self.assertEqual(args[args.index("--num-partitions") + 1], "4")
        self.assertNotIn("--no-parallelism", args)
        self.assertEqual(operator.source_type, "rdbms")
        self.assertEqual(operator.application, "/opt/spark-jobs/raw/load_entity.py")
        self.assertEqual(operator.deploy_mode, "client")
        operator = self.operator(parallelism=False, num_partitions=7, incremental_column="_batch_id", lookback_size=5)
        operator.execute(self.context)
        self.assertIn("--no-parallelism", operator.application_args)
        self.assertEqual(operator.application_args[-2:], ["--incremental-column", "_batch_id"])
        self.assertIn("--lookback-size", operator.application_args)
        self.assertEqual(operator.application_args[operator.application_args.index("--lookback-size") + 1], "5")
        self.assertNotIn("--watermark-value", operator.application_args)
        self.module.registry.get_watermark.assert_not_called()
        self.assertEqual(operator.application_args[operator.application_args.index("--num-partitions") + 1], "7")

    def test_oracle_connection(self):
        self.module.BaseHook.get_connection.return_value.conn_type = "oracle"
        operator = self.operator()
        operator.execute(self.context)
        self.assertIn("jdbc:oracle:thin:@//host:1521/db", operator.application_args)

    def test_run_identity_and_metadata_credentials_are_forwarded(self):
        operator = self.operator(env_vars={"EXISTING": "kept"}, lookback_size=3, lookback_unit="h",
                                 lookback_specified=True)
        operator.execute(self.context)
        args = operator.application_args
        for flag, value in (("--source-id", "bank"), ("--dag-id", "dag"), ("--run-id", "run"), ("--lookback-unit", "h")):
            self.assertEqual(args[args.index(flag) + 1], value)
        self.assertIn("--lookback-given", args)
        self.assertEqual(operator.env_vars, {"EXISTING": "kept", "PGHOST": "pg", "PGPORT": "5432", "PGDATABASE": "metadata", "PGUSER": "writer", "PGPASSWORD": "pg-secret"})
        self.assertNotIn("pg-secret", args)

    def test_unspecified_lookback_omits_given_flag(self):
        for kwargs in ({}, {"lookback_size": 0}, {"lookback_size": 5}):
            with self.subTest(kwargs=kwargs):
                operator = self.operator(**kwargs)
                operator.execute(self.context)
                self.assertNotIn("--lookback-given", operator.application_args)

    def test_force_full_override_applies_only_to_selected_entity(self):
        for fqns in (["s.t"], ["s.other"]):
            with self.subTest(fqns=fqns):
                self.context["dag_run"].conf = {"force_full_snapshot_fqns": fqns, "reason": "repair"}
                operator = self.operator(incremental_column="_batch_id")
                operator.execute(self.context)
                self.assertEqual("--force-full" in operator.application_args, fqns == ["s.t"])
                self.assertEqual("--reason" in operator.application_args, fqns == ["s.t"])
                if fqns == ["s.t"]:
                    self.assertEqual(operator.application_args[operator.application_args.index("--reason") + 1], "repair")

    def test_strict_constructor_types(self):
        for kwargs in (
            {"parallelism": "false"}, {"parallelism": 0}, {"parallelism": None},
            {"num_partitions": True}, {"num_partitions": 0}, {"num_partitions": -1},
            {"num_partitions": 4.0}, {"num_partitions": "4"}, {"source_type": None},
            {"lookback_size": -1}, {"lookback_size": "2"}, {"lookback_size": True},
            {"lookback_specified": "yes"}, {"lookback_specified": None}, {"lookback_specified": 1},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.operator(**kwargs)

    def test_runtime_rejects_source_and_connection_mismatch(self):
        operator = self.operator(source_type="file")
        with self.assertRaisesRegex(ValueError, "source_type"):
            operator.execute(self.context)
        self.assertFalse(operator.submissions)
        self.module.BaseHook.get_connection.return_value.conn_type = "postgres"
        with self.assertRaisesRegex(ValueError, "supported JDBC dialect"):
            self.operator().execute(self.context)

    def test_missing_or_invalid_pid_fails_before_submit(self):
        for value in (None, "None", True, 1.5, 0, -1):
            self.context["ti"].xcom_pull.return_value = value
            operator = self.operator()
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "process_id"):
                operator.execute(self.context)
            self.assertFalse(operator.submissions)

    def test_same_pid_across_loads_retries_and_different_runs(self):
        generate = self.module.GenerateProcessIdOperator()
        self.module.registry.get_or_create_process_id.side_effect = lambda pg, **kw: {"run_a": 101, "run_b": 102}[kw["run_id"]]
        ids = []
        for run_id in ("run_a", "run_b"):
            dag_run = types.SimpleNamespace(dag_id="dag", run_id=run_id, logical_date="date", conf={})
            process_id = generate.execute({"dag_run": dag_run})
            self.assertEqual(generate.execute({"dag_run": dag_run}), process_id)
            ids.append(process_id)
            self.context["ti"].xcom_pull.return_value = process_id
            for operator in (self.operator(), self.operator()):
                for attempt in range(2):
                    operator.execute(self.context)
                    args = operator.application_args
                    self.assertEqual(args[args.index("--process-id") + 1], str(process_id))
        self.assertEqual(ids, [101, 102])
        self.assertEqual([call.kwargs["run_id"] for call in self.module.registry.get_or_create_process_id.call_args_list], [
            "run_a", "run_a", "run_b", "run_b"
        ])
        self.module.registry.next_process_id.assert_not_called()


class TestRunConfValidation(unittest.TestCase):
    """Reject unsafe manual overrides before allocating a process ID."""

    def setUp(self):
        self.module = load_operators()
        self.operator = self.module.GenerateProcessIdOperator(entities=("s.t", "s.other"))

    def execute(self, conf):
        return self.operator.execute({"dag_run": types.SimpleNamespace(
            dag_id="dag", run_id="run", logical_date="date", conf=conf,
        )})

    def test_empty_and_valid_overrides(self):
        for conf in (None, {}, {"force_full_snapshot_fqns": []}, {"reason": "scheduled"},
                     {"force_full_snapshot_fqns": ["s.t", "s.other"], "reason": "repair"}):
            with self.subTest(conf=conf):
                self.module.registry.get_or_create_process_id.reset_mock()
                self.module.registry.get_or_create_process_id.return_value = 42
                self.assertEqual(self.execute(conf), 42)
                self.module.registry.get_or_create_process_id.assert_called_once_with(
                    "ingestion_meta", dag_id="dag", run_id="run", execution_date="date")

    def test_invalid_conf_never_allocates_process_id(self):
        for conf in (
            {"unknown": True}, {"force_full_snapshot_fqns": ["s.unknown"], "reason": "repair"},
            {"force_full_snapshot_fqns": ["s.t", "s.t"], "reason": "repair"},
            {"force_full_snapshot_fqns": "s.t", "reason": "repair"},
            {"force_full_snapshot_fqns": [1], "reason": "repair"},
            {"force_full_snapshot_fqns": None}, {"force_full_snapshot_fqns": ["s.t"]},
            {"force_full_snapshot_fqns": ["s.t"], "reason": "  "},
            {"force_full_snapshot_fqns": ["s.t"], "reason": 123}, {"reason": None},
            ["invalid"], "invalid", 1,
        ):
            with self.subTest(conf=conf), self.assertRaises(ValueError):
                self.execute(conf)
            self.module.registry.get_or_create_process_id.assert_not_called()


class TestTableLogOperator(unittest.TestCase):
    """Lifecycle logging does not own watermark reads or persistence."""

    def setUp(self):
        self.module = load_operators()
        self.events = []
        self.module.registry.log_task.side_effect = lambda **kwargs: self.events.append(kwargs["status"])

    def operator(self, **kwargs):
        return self.module.TableLogOperator(
            task_id="log", source_id="bank", entity_fqn="s.t", datalake_table="raw.bank.s__t",
            process_id="42", **kwargs,
        )

    def test_end_only_logs_lifecycle(self):
        self.operator(status="END").execute({})
        self.assertEqual(self.events, ["END"])
        self.module.registry.log_task.assert_called_once_with(
            pg_conn_id="ingestion_meta", process_id=42, source_id="bank", entity_fqn="s.t",
            datalake_table="raw.bank.s__t", status="END",
        )
        self.module.registry.get_watermark.assert_not_called()
        self.module.registry.set_watermark.assert_not_called()
        self.module.registry.plan_batch.assert_not_called()
        self.module.registry.start_batch.assert_not_called()
        self.module.registry.finalize_batch.assert_not_called()
        self.module.registry.fail_batch.assert_not_called()

    def test_logging_interface_has_no_extraction_parameters(self):
        import inspect

        parameters = inspect.signature(self.module.TableLogOperator.__init__).parameters
        self.assertNotIn("incremental_column", parameters)
        self.assertNotIn("trino_conn_id", parameters)

    def test_log_failure_is_propagated(self):
        self.module.registry.log_task.side_effect = RuntimeError("log failed")
        with self.assertRaisesRegex(RuntimeError, "log failed"):
            self.operator(status="END").execute({})

    def test_start_and_snapshot_end_do_not_query_watermark(self):
        self.operator(status="START").execute({})
        self.operator(status="END").execute({})
        self.module.registry.get_watermark.assert_not_called()
        self.module.registry.set_watermark.assert_not_called()
        self.assertEqual(self.events, ["START", "END"])

    def test_invalid_pid_fails_cleanly(self):
        operator = self.operator(status="END")
        operator.process_id = None
        with self.assertRaisesRegex(ValueError, "process_id"):
            operator.execute({})
        self.assertFalse(self.events)


if __name__ == "__main__":
    unittest.main()
