"""Behavioral ingestion operator tests using scoped Airflow module doubles."""

import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


class FakeSkip(Exception):
    """Stand-in for AirflowSkipException in the scoped module doubles."""


class FakeOperator:
    """Preserve constructor arguments and record Spark submission."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.on_failure_callback = kwargs.get("on_failure_callback")
        self.env_vars = kwargs.get("env_vars")
        self.submissions = []

    def execute(self, context):
        self.submissions.append((list(self.application_args), context))


def fake_make_dsn(**kwargs):
    """Render a deterministic DSN string from the connection fields."""
    return "host={host} port={port} dbname={dbname} user={user} password={password}".format(**kwargs)


def load_operators():
    """Import the operators package under a private name and restore mocked modules."""
    import ingestion.sources
    import ingestion.spark.raw.iceberg

    modules = {}
    for name in (
        "airflow", "airflow.providers", "airflow.providers.apache", "airflow.providers.apache.spark",
        "airflow.providers.apache.spark.operators", "airflow.providers.apache.spark.operators.spark_submit",
        "airflow.sdk", "airflow.sdk.exceptions", "psycopg2", "psycopg2.extensions",
        "_operator_test_ingestion",
    ):
        modules[name] = types.ModuleType(name)
    modules["airflow.sdk"].BaseHook = MagicMock()
    modules["airflow.sdk"].BaseOperator = FakeOperator
    modules["airflow.sdk.exceptions"].AirflowSkipException = FakeSkip
    modules["airflow.providers.apache.spark.operators.spark_submit"].SparkSubmitOperator = FakeOperator
    modules["psycopg2.extensions"].make_dsn = fake_make_dsn
    modules["ingestion"] = types.ModuleType("ingestion")
    modules["ingestion"].meta = MagicMock()
    modules["ingestion"].sources = ingestion.sources
    modules["ingestion"].spark = ingestion.spark
    modules["ingestion.meta"] = modules["ingestion"].meta
    package = Path(__file__).resolve().parents[2] / "ingestion" / "operators"
    spec = importlib.util.spec_from_file_location(
        "_operator_test_ingestion.operators", package / "__init__.py",
        submodule_search_locations=[str(package)],
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {**modules, spec.name: module}):
        spec.loader.exec_module(module)
    module.BaseHook = modules["airflow.sdk"].BaseHook
    module.meta = modules["ingestion"].meta
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
        self.config = {"contract": {"source_column": "_batch_id", "position_type": "bigint"}, "lookback": {"size": 0, "unit": None}}

    def full_operator(self, **kwargs):
        return self.module.IngestFullOperator(
            task_id="load", source_id="bank", airflow_conn_id="source", fqn="s.t",
            target_table="raw.bank.s__t", **kwargs,
        )

    def incremental_operator(self, **kwargs):
        return self.module.IngestIncrementalOperator(
            task_id="load", source_id="bank", airflow_conn_id="source", fqn="s.t",
            target_table="raw.bank.s__t",
            incremental_config=kwargs.pop("incremental_config", self.config),
            **kwargs,
        )

    def test_full_defaults(self):
        operator = self.full_operator()
        operator.execute(self.context)
        args = operator.application_args
        self.assertEqual(operator.application, "/opt/spark-jobs/raw/rdbms/full/load_entity.py")
        self.assertEqual(operator.deploy_mode, "client")
        self.assertEqual(operator.source_type, "rdbms")
        self.assertEqual(args[args.index("--num-jdbc-partitions") + 1], "4")
        self.assertNotIn("--no-jdbc-partitioning", args)
        self.assertIn("jdbc:sqlserver://host:1433;databaseName=db;encrypt=false", args)

    def test_full_operator_has_no_pg_meta_transport(self):
        operator = self.full_operator(env_vars={"EXISTING": "kept"})
        operator.execute(self.context)
        args = operator.application_args
        for flag in ("--incremental-config", "--incremental-column", "--lookback-size", "--lookback-unit", "--lookback-given"):
            self.assertNotIn(flag, args)
        self.assertEqual(operator.env_vars, {"EXISTING": "kept"})
        self.module.meta.get_watermark.assert_not_called()

    def test_full_operator_rejects_incremental_parameters(self):
        import inspect

        parameters = inspect.signature(self.module.IngestFullOperator.__init__).parameters
        self.assertNotIn("incremental_config", parameters)
        self.assertNotIn("pg_conn_id", parameters)

    def test_incremental_defaults_and_config_json(self):
        operator = self.incremental_operator()
        operator.execute(self.context)
        self.assertEqual(operator.application, "/opt/spark-jobs/raw/rdbms/incremental/load_entity.py")
        args = operator.application_args
        self.assertEqual(json.loads(args[args.index("--incremental-config") + 1]), self.config)
        self.assertNotIn("--watermark-value", args)
        self.module.meta.get_watermark.assert_not_called()

    def test_partitioning_arguments_for_full_and_incremental_loads(self):
        for make in (self.full_operator, self.incremental_operator):
            for enabled in (False, True):
                with self.subTest(operator=make.__name__, enabled=enabled):
                    operator = make(jdbc_partitioning=enabled, num_partitions=7)
                    operator.execute(self.context)
                    args = operator.application_args
                    if enabled:
                        self.assertEqual(args[args.index("--num-jdbc-partitions") + 1], "7")
                        self.assertNotIn("--no-jdbc-partitioning", args)
                    else:
                        self.assertIn("--no-jdbc-partitioning", args)
                        self.assertNotIn("--num-jdbc-partitions", args)

    def test_oracle_connection(self):
        self.module.BaseHook.get_connection.return_value.conn_type = "oracle"
        operator = self.full_operator()
        operator.execute(self.context)
        self.assertIn("jdbc:oracle:thin:@//host:1521/db", operator.application_args)

    def test_run_identity_and_metadata_credentials_are_forwarded(self):
        operator = self.incremental_operator(
            env_vars={"EXISTING": "kept"},
            incremental_config={"contract": {"source_column": "updated_at", "position_type": "timestamp_local(7)"}, "lookback": {"size": 3, "unit": "h"}},
        )
        operator.execute(self.context)
        args = operator.application_args
        for flag, value in (("--source-id", "bank"), ("--dag-id", "dag"), ("--run-id", "run")):
            self.assertEqual(args[args.index(flag) + 1], value)
        self.assertIn("--incremental-config", args)
        self.assertEqual(operator.env_vars, {
            "EXISTING": "kept",
            "INGESTION_META_DSN": "host=pg port=5432 dbname=metadata user=writer password=pg-secret",
        })
        self.assertNotIn("pg-secret", args)

    def test_unspecified_lookback_omits_given_flag(self):
        for size in (0, 5):
            with self.subTest(size=size):
                operator = self.incremental_operator(
                    incremental_config={"contract": {"source_column": "_batch_id", "position_type": "bigint"}, "lookback": {"size": size, "unit": None}}
                )
                operator.execute(self.context)
                self.assertNotIn("--lookback-given", operator.application_args)
                self.assertNotIn("--lookback-size", operator.application_args)
                self.assertNotIn("--lookback-unit", operator.application_args)

    def test_hourly_temporal_default_is_not_explicit_lookback(self):
        operator = self.incremental_operator(
            incremental_config={"contract": {"source_column": "updated_at", "position_type": "timestamp_local(7)"}, "lookback": {"size": 3, "unit": "h"}}
        )
        operator.execute(self.context)
        args = operator.application_args
        self.assertEqual(json.loads(args[args.index("--incremental-config") + 1])["lookback"], {"size": 3, "unit": "h"})
        self.assertNotIn("--lookback-size", args)
        self.assertNotIn("--lookback-unit", args)

    def test_explicit_zero_lookback_omits_unit(self):
        operator = self.incremental_operator(
            incremental_config={"contract": {"source_column": "_batch_id", "position_type": "bigint"}, "lookback": {"size": 0, "unit": None}}
        )
        operator.execute(self.context)
        args = operator.application_args
        self.assertEqual(json.loads(args[args.index("--incremental-config") + 1])["lookback"], {"size": 0, "unit": None})
        self.assertNotIn("--lookback-given", args)
        self.assertNotIn("--lookback-unit", args)

    def test_no_force_full_flags_remain(self):
        operator = self.incremental_operator()
        self.context["dag_run"].conf = {"force_full_snapshot_fqns": ["s.t"], "reason": "repair"}
        operator.execute(self.context)
        self.assertNotIn("--force-full", operator.application_args)
        self.assertNotIn("--reason", operator.application_args)

    def test_strict_constructor_types(self):
        for kwargs in (
            {"jdbc_partitioning": "false"}, {"jdbc_partitioning": 0}, {"jdbc_partitioning": None},
            {"num_partitions": True}, {"num_partitions": 0}, {"num_partitions": -1},
            {"num_partitions": 4.0}, {"num_partitions": "4"}, {"source_type": None},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.full_operator(**kwargs)
        for kwargs in ({"incremental_config": {}}, {"incremental_config": True}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.incremental_operator(**kwargs)

    def test_runtime_rejects_source_and_connection_mismatch(self):
        operator = self.full_operator(source_type="file")
        with self.assertRaisesRegex(ValueError, "source_type"):
            operator.execute(self.context)
        self.assertFalse(operator.submissions)
        self.module.BaseHook.get_connection.return_value.conn_type = "postgres"
        with self.assertRaisesRegex(ValueError, "supported JDBC dialect"):
            self.full_operator().execute(self.context)

    def test_missing_or_invalid_pid_fails_before_submit(self):
        for value in (None, "None", True, 1.5, 0, -1):
            self.context["ti"].xcom_pull.return_value = value
            operator = self.full_operator()
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "process_id"):
                operator.execute(self.context)
            self.assertFalse(operator.submissions)

    def test_every_execution_allocates_fresh_process_id(self):
        generate = self.module.GenerateProcessIdOperator()
        self.module.meta.allocate_process_id.side_effect = iter([101, 102, 103])
        ids = []
        for run_id in ("run_a", "run_a", "run_b"):
            dag_run = types.SimpleNamespace(dag_id="dag", run_id=run_id, logical_date="date", conf={})
            ids.append(generate.execute({"dag_run": dag_run}))
        self.assertEqual(ids, [101, 102, 103])
        self.assertEqual(
            [(call.kwargs["dag_id"], call.kwargs["run_id"], call.kwargs["execution_date"])
             for call in self.module.meta.allocate_process_id.call_args_list],
            [("dag", "run_a", "date")] * 2 + [("dag", "run_b", "date")],
        )

    def test_generated_task_has_no_entities_parameter(self):
        import inspect

        parameters = inspect.signature(self.module.GenerateProcessIdOperator.__init__).parameters
        self.assertNotIn("entities", parameters)



class TestTableLogOperator(unittest.TestCase):
    """Lifecycle logging does not own watermark reads or persistence."""

    def setUp(self):
        self.module = load_operators()
        self.events = []
        self.module.meta.log_task.side_effect = lambda **kwargs: self.events.append(kwargs["status"])

    def operator(self, **kwargs):
        return self.module.TableLogOperator(
            task_id="log", source_id="bank", entity_fqn="s.t", datalake_table="raw.bank.s__t",
            process_id="42", **kwargs,
        )

    def test_end_only_logs_lifecycle(self):
        self.operator(status="END").execute({})
        self.assertEqual(self.events, ["END"])
        kwargs = self.module.meta.log_task.call_args.kwargs
        self.assertEqual({key: value for key, value in kwargs.items() if key != "db_connect"}, {
            "process_id": 42, "source_id": "bank", "entity_fqn": "s.t",
            "datalake_table": "raw.bank.s__t", "status": "END",
        })
        self.module.meta.get_watermark.assert_not_called()
        self.module.meta.set_watermark.assert_not_called()
        self.module.meta.plan_batch.assert_not_called()
        self.module.meta.start_batch.assert_not_called()
        self.module.meta.finalize_batch.assert_not_called()
        self.module.meta.fail_batch.assert_not_called()

    def test_logging_interface_has_no_extraction_parameters(self):
        import inspect

        parameters = inspect.signature(self.module.TableLogOperator.__init__).parameters
        self.assertNotIn("incremental_column", parameters)
        self.assertNotIn("trino_conn_id", parameters)

    def test_log_failure_is_propagated(self):
        self.module.meta.log_task.side_effect = RuntimeError("log failed")
        with self.assertRaisesRegex(RuntimeError, "log failed"):
            self.operator(status="END").execute({})

    def test_start_and_snapshot_end_do_not_query_watermark(self):
        self.operator(status="START").execute({})
        self.operator(status="END").execute({})
        self.module.meta.get_watermark.assert_not_called()
        self.module.meta.set_watermark.assert_not_called()
        self.assertEqual(self.events, ["START", "END"])

    def test_invalid_pid_fails_cleanly(self):
        operator = self.operator(status="END")
        operator.process_id = None
        with self.assertRaisesRegex(ValueError, "process_id"):
            operator.execute({})
        self.assertFalse(self.events)


class TestDagrunCallbacks(unittest.TestCase):
    """DAG-level callbacks append END/FAILED for the run's allocated process."""

    def setUp(self):
        self.module = load_operators()

    def context(self):
        return {"dag_run": types.SimpleNamespace(dag_id="dag", run_id="run")}

    def test_success_appends_end_event(self):
        self.module.meta.find_process_id.return_value = 7
        self.module.dagrun_success_callback(self.context())
        kwargs = self.module.meta.log_dagrun.call_args.kwargs
        self.assertEqual((kwargs["process_id"], kwargs["dag_id"], kwargs["status"]), (7, "dag", "END"))

    def test_failure_appends_failed_event(self):
        self.module.meta.find_process_id.return_value = 7
        self.module.dagrun_failure_callback(self.context())
        self.assertEqual(self.module.meta.log_dagrun.call_args.kwargs["status"], "FAILED")

    def test_callback_without_process_id_is_noop(self):
        self.module.meta.find_process_id.return_value = None
        self.module.dagrun_success_callback(self.context())
        self.module.dagrun_failure_callback(self.context())
        self.module.meta.log_dagrun.assert_not_called()


class TestTaskFailure(unittest.TestCase):
    """The load on-failure callback logs FAILED only with an allocated process."""

    def setUp(self):
        self.module = load_operators()

    def context(self, process_id):
        context = {"ti": MagicMock()}
        context["ti"].xcom_pull.return_value = process_id
        return context

    def test_failure_logs_task_event(self):
        self.module.log_task_failed(self.context(42), "bank", "s.t", "raw.bank.s__t")
        kwargs = self.module.meta.log_task.call_args.kwargs
        self.assertEqual({key: value for key, value in kwargs.items() if key != "db_connect"}, {
            "process_id": 42, "source_id": "bank", "entity_fqn": "s.t",
            "datalake_table": "raw.bank.s__t", "status": "FAILED",
        })
        self.assertTrue(callable(kwargs["db_connect"]))

    def test_failure_without_process_id_is_silent(self):
        self.module.log_task_failed(self.context(None), "bank", "s.t", "raw.bank.s__t")
        self.module.meta.log_task.assert_not_called()


class TestMetaConnect(unittest.TestCase):
    """pg-meta connections resolve through the Airflow Postgres provider."""

    def test_hook_connection_forwarded(self):
        module = load_operators()
        hook = MagicMock()
        modules = {}
        for name in (
            "airflow", "airflow.providers", "airflow.providers.postgres",
            "airflow.providers.postgres.hooks", "airflow.providers.postgres.hooks.postgres",
        ):
            modules[name] = types.ModuleType(name)
        modules["airflow.providers.postgres.hooks.postgres"].PostgresHook = hook
        with patch.dict(sys.modules, modules):
            module.process._meta_connect("custom_meta")
        hook.assert_called_once_with(postgres_conn_id="custom_meta")
        hook.return_value.get_conn.assert_called_once_with()


class TestIngestActSnapOperator(unittest.TestCase):
    """The snapshot submission: CLI arguments, marker channel, no pg-meta."""

    def setUp(self):
        self.module = load_operators()
        self.module.BaseHook.get_connection.return_value = types.SimpleNamespace(
            conn_type="mssql", conn_id="source", host="host", port=None,
            schema="db", login="reader", password="secret",
        )
        self.context = {"ti": MagicMock(), "dag_run": types.SimpleNamespace(dag_id="dag", run_id="run", conf={})}
        self.context["ti"].xcom_pull.return_value = 42

    def operator(self, **kwargs):
        return self.module.IngestActSnapOperator(
            task_id="load", source_id="bank", airflow_conn_id="source", fqn="s.t",
            target_table="raw.bank.s__t__act_snap", **kwargs,
        )

    def test_application_path_and_arguments(self):
        operator = self.operator()
        self.assertEqual(operator.application, "/opt/spark-jobs/raw/rdbms/incremental/load_act_snap.py")
        self.assertEqual(operator.deploy_mode, "client")
        self.assertIsNone(operator.execute(self.context))
        args = operator.application_args
        self.assertEqual(args[args.index("--source-table") + 1], "s.t")
        self.assertEqual(args[args.index("--target-table") + 1], "raw.bank.s__t__act_snap")
        self.assertEqual(args[args.index("--process-id") + 1], "42")
        self.assertEqual(args[args.index("--num-jdbc-partitions") + 1], "4")
        self.assertNotIn("--no-jdbc-partitioning", args)
        self.assertIn("jdbc:sqlserver://host:1433;databaseName=db;encrypt=false", args)
        self.assertTrue(operator.submissions)

    def test_partitioning_disabled_appends_flag(self):
        operator = self.operator(jdbc_partitioning=False, num_partitions=7)
        operator.execute(self.context)
        self.assertIn("--no-jdbc-partitioning", operator.application_args)
        self.assertNotIn("--num-jdbc-partitions", operator.application_args)

    def test_partitioning_enabled_forwards_partition_count(self):
        operator = self.operator(num_partitions=7)
        operator.execute(self.context)
        args = operator.application_args
        self.assertEqual(args[args.index("--num-jdbc-partitions") + 1], "7")
        self.assertNotIn("--no-jdbc-partitioning", args)

    def test_no_pg_meta_environment_forwarded(self):
        operator = self.operator()
        operator.execute(self.context)
        self.assertNotIn("PGHOST", operator.env_vars or {})
        self.assertNotIn("INGESTION_META_DSN", operator.env_vars or {})
        self.assertNotIn("--lookback-size", operator.application_args)
        self.assertNotIn("--incremental-column", operator.application_args)

    def test_no_marker_protocol(self):
        operator = self.operator()
        operator.execute(self.context)
        self.assertNotIn("--result-marker", operator.application_args)
        self.assertTrue(operator.submissions)

    def test_strict_constructor_types(self):
        for kwargs in (
            {"jdbc_partitioning": "false"}, {"num_partitions": 0}, {"num_partitions": True},
            {"source_type": None},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.operator(**kwargs)

    def test_missing_pid_fails_before_submit(self):
        self.context["ti"].xcom_pull.return_value = None
        operator = self.operator()
        with self.assertRaisesRegex(ValueError, "process_id"):
            operator.execute(self.context)
        self.assertFalse(operator.submissions)


class TestPublishAsset(unittest.TestCase):
    """Publication policy is independent of Spark and the generated task structure."""

    def setUp(self):
        self.module = load_operators()
        self.context = {"ti": MagicMock()}
        self.context["ti"].xcom_pull.return_value = 42

    def test_regular_load_has_no_condition(self):
        with patch.object(self.module.assets, "snapshot_has_current_process") as condition:
            self.module.publish_asset_event(**self.context)
        condition.assert_not_called()
        self.context["ti"].xcom_pull.assert_not_called()

    def test_snapshot_policy(self):
        for result in (True, False):
            with patch.object(self.module.assets, "snapshot_has_current_process", return_value=result) as condition:
                if result:
                    self.module.publish_asset_event("raw.bank.s__t__act_snap", **self.context)
                else:
                    with self.assertRaises(FakeSkip):
                        self.module.publish_asset_event("raw.bank.s__t__act_snap", **self.context)
                condition.assert_called_once_with("raw.bank.s__t__act_snap", 42)

    def test_missing_process_is_error(self):
        self.context["ti"].xcom_pull.return_value = None
        with self.assertRaisesRegex(ValueError, "process_id"):
            self.module.publish_asset_event("raw.bank.s__t__act_snap", **self.context)

    def test_single_row_query_and_errors(self):
        hook_module = types.ModuleType("airflow.providers.trino.hooks.trino")
        hook_module.TrinoHook = MagicMock()
        hook = hook_module.TrinoHook.return_value
        with patch.dict(sys.modules, {"airflow.providers.trino.hooks.trino": hook_module}):
            for row, expected in ((None, False), ([], False), ((41,), False), ((42,), True)):
                hook.get_first.return_value = row
                self.assertEqual(self.module.snapshot_has_current_process("raw.bank.s__t__act_snap", 42), expected)
            hook.get_first.assert_called_with("SELECT __process_id FROM raw.bank.s__t__act_snap LIMIT 1")
            hook.get_first.side_effect = RuntimeError("Trino unavailable")
            with self.assertRaisesRegex(RuntimeError, "Trino unavailable"):
                self.module.publish_asset_event("raw.bank.s__t__act_snap", **self.context)


if __name__ == "__main__":
    unittest.main()
