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
        self.submissions = []

    def execute(self, context):
        self.submissions.append((list(self.application_args), context))


def load_operators():
    """Import under a private package and restore every mocked module afterwards."""
    modules = {}
    for name in (
        "airflow", "airflow.providers", "airflow.providers.apache", "airflow.providers.apache.spark",
        "airflow.providers.apache.spark.operators", "airflow.providers.apache.spark.operators.spark_submit",
        "airflow.providers.trino", "airflow.providers.trino.hooks", "airflow.providers.trino.hooks.trino",
        "airflow.sdk", "_operator_test_ingestion",
    ):
        modules[name] = types.ModuleType(name)
    modules["airflow.sdk"].BaseHook = MagicMock()
    modules["airflow.sdk"].BaseOperator = FakeOperator
    modules["airflow.providers.apache.spark.operators.spark_submit"].SparkSubmitOperator = FakeOperator
    modules["airflow.providers.trino.hooks.trino"].TrinoHook = MagicMock()
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
        self.context = {"ti": MagicMock()}
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
        operator = self.operator(parallelism=False, num_partitions=7, incremental_column="_batch_id")
        self.module.registry.get_watermark.return_value = "100"
        operator.execute(self.context)
        self.assertIn("--no-parallelism", operator.application_args)
        self.assertEqual(operator.application_args[-4:], ["--incremental-column", "_batch_id", "--watermark-value", "100"])
        self.assertEqual(operator.application_args[operator.application_args.index("--num-partitions") + 1], "7")

    def test_oracle_connection(self):
        self.module.BaseHook.get_connection.return_value.conn_type = "oracle"
        operator = self.operator()
        operator.execute(self.context)
        self.assertIn("jdbc:oracle:thin:@//host:1521/db", operator.application_args)

    def test_strict_constructor_types(self):
        for kwargs in (
            {"parallelism": "false"}, {"parallelism": 0}, {"parallelism": None},
            {"num_partitions": True}, {"num_partitions": 0}, {"num_partitions": -1},
            {"num_partitions": 4.0}, {"num_partitions": "4"}, {"source_type": None},
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
        self.module.registry.next_process_id.side_effect = [101, 102]
        ids = []
        for run_id in ("run_a", "run_b"):
            dag_run = types.SimpleNamespace(dag_id="dag", run_id=run_id, logical_date="date")
            process_id = generate.execute({"dag_run": dag_run})
            ids.append(process_id)
            self.context["ti"].xcom_pull.return_value = process_id
            for operator in (self.operator(), self.operator()):
                for attempt in range(2):
                    operator.execute(self.context)
                    args = operator.application_args
                    self.assertEqual(args[args.index("--process-id") + 1], str(process_id))
        self.assertEqual(ids, [101, 102])
        self.assertEqual([call.kwargs["run_id"] for call in self.module.registry.log_dagrun.call_args_list], ["run_a", "run_b"])


class TestTableLogOperator(unittest.TestCase):
    """END is emitted only after watermark retrieval and persistence succeed."""

    def setUp(self):
        self.module = load_operators()
        self.events = []
        self.module.registry.set_watermark.side_effect = lambda **kwargs: self.events.append("watermark")
        self.module.registry.log_task.side_effect = lambda **kwargs: self.events.append(kwargs["status"])
        self.hook = self.module.TrinoHook.return_value
        self.hook.get_first.side_effect = lambda query: self.events.append("query") or (100,)

    def operator(self, **kwargs):
        return self.module.TableLogOperator(
            task_id="log", source_id="bank", entity_fqn="s.t", datalake_table="raw.bank.s__t",
            process_id="42", **kwargs,
        )

    def test_watermark_before_end_and_pid_filter(self):
        self.operator(status="END", incremental_column="_batch_id").execute({})
        self.assertEqual(self.events, ["query", "watermark", "END"])
        self.hook.get_first.assert_called_once_with(
            "SELECT max(_batch_id) FROM raw.bank.s__t WHERE __process_id = 42"
        )
        self.assertEqual(self.module.registry.set_watermark.call_args.kwargs["process_id"], 42)

    def test_null_watermark_still_logs_end_without_saving(self):
        self.hook.get_first.side_effect = None
        self.hook.get_first.return_value = (None,)
        self.operator(status="END", incremental_column="_batch_id").execute({})
        self.module.registry.set_watermark.assert_not_called()
        self.assertEqual(self.events, ["END"])

    def test_query_or_save_failure_never_logs_end(self):
        for stage in ("query", "save"):
            with self.subTest(stage=stage):
                self.module.registry.log_task.reset_mock()
                if stage == "query":
                    self.hook.get_first.side_effect = RuntimeError("query failed")
                else:
                    self.hook.get_first.side_effect = None
                    self.hook.get_first.return_value = (100,)
                    self.module.registry.set_watermark.side_effect = RuntimeError("save failed")
                with self.assertRaises(RuntimeError):
                    self.operator(status="END", incremental_column="_batch_id").execute({})
                self.module.registry.log_task.assert_not_called()

    def test_start_and_snapshot_end_do_not_query_watermark(self):
        self.operator(status="START", incremental_column="_batch_id").execute({})
        self.operator(status="END").execute({})
        self.hook.get_first.assert_not_called()
        self.assertEqual(self.events, ["START", "END"])

    def test_invalid_pid_fails_cleanly(self):
        operator = self.operator(status="END", incremental_column="_batch_id")
        operator.process_id = None
        with self.assertRaisesRegex(ValueError, "process_id"):
            operator.execute({})
        self.assertFalse(self.events)


if __name__ == "__main__":
    unittest.main()
