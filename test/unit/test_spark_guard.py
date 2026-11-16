"""Admission guard tests with deterministic HTTP inventories and monotonic time."""

import importlib.util
from pathlib import Path
import types
import unittest
from unittest.mock import MagicMock, patch

from test_operators import load_operators


def load_guard():
    """Load a fresh guard without Airflow or test module replacements."""
    path = Path(__file__).resolve().parents[2] / "ingestion/spark/guard.py"
    spec = importlib.util.spec_from_file_location("_test_spark_guard", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def app(application_id="old", name="raw.bank.s__t", state="RUNNING"):
    """Build an application record compatible with the master JSON contract."""
    return {"id": application_id, "name": name, "state": state}


def inventory(active=(), completed=()):
    """Build a complete master application inventory."""
    return {"activeapps": list(active), "completedapps": list(completed)}


class TestSparkGuard(unittest.TestCase):
    """Verify fail-closed admission, exact targeting, once-only kill and deadlines."""

    def setUp(self):
        self.guard = load_guard()
        self.request = patch.object(self.guard, "_get_master_state").start()
        self.kill = patch.object(self.guard, "_kill_application").start()
        self.addCleanup(patch.stopall)

    def execute(self, *responses):
        self.request.side_effect = responses
        return self.guard.guard_spark_submit("http://spark-master:8080", "raw.bank.s__t")

    def test_empty_and_other_target_do_not_kill(self):
        self.assertEqual(self.execute(inventory([app(name="raw.bank.s__t__act_snap")])), [])
        self.assertEqual(self.request.call_count, 1)
        self.assertEqual(self.request.call_args.args[0], "http://spark-master:8080")
        self.kill.assert_not_called()

    def test_multiple_ids_killed_once_and_finished_race_allowed(self):
        old, second = app(), app("second")
        with patch.object(self.guard.time, "sleep"):
            result = self.execute(
                inventory([old, second]), inventory([old, second]),
                inventory(completed=[app(state="KILLED"), app("second", state="FINISHED")]),
            )
        self.assertEqual(result, ["old", "second"])
        self.assertEqual([call.args[1] for call in self.kill.call_args_list], ["old", "second"])

    def test_repeated_invocation_observes_current_state(self):
        self.execute(inventory([app()]), inventory(completed=[app(state="KILLED")]))
        self.assertEqual(self.execute(inventory()), [])

    def test_unknown_missing_duplicate_and_new_id_refuse(self):
        cases = (
            None, {}, {"activeapps": []}, inventory([{}]),
            inventory([app(), app()]), inventory([app(state="UNKNOWN")]),
        )
        for response in cases:
            with self.subTest(response=response), self.assertRaises(self.guard.SparkGuardError):
                self.execute(response)
        for after in (
            inventory(), inventory(completed=[app(state="FAILED")]),
            inventory([app(), app("new")]),
            inventory(completed=[app(name="raw.bank.other", state="KILLED")]),
        ):
            with self.subTest(after=after), self.assertRaises(self.guard.SparkGuardError):
                self.execute(inventory([app()]), after)

    def test_http_200_without_state_change_exhausts_budget(self):
        now = [0.0]
        self.guard.GUARD_TIMEOUT = 3
        def sleep(seconds):
            now[0] += seconds
        self.request.return_value = inventory([app()])
        with patch.object(self.guard.time, "monotonic", side_effect=lambda: now[0]), patch.object(self.guard.time, "sleep", side_effect=sleep):
            with self.assertRaises(self.guard.SparkGuardError):
                self.guard.guard_spark_submit("http://spark-master:8080", "raw.bank.s__t")
        self.assertEqual(now[0], 3)
        self.kill.assert_called_once()

    def test_target_is_compared_verbatim_without_format_validation(self):
        target = "catalog.Custom.Table"
        self.request.side_effect = (
            inventory([app(name=target)]),
            inventory(completed=[app(name=target, state="KILLED")]),
        )
        self.assertEqual(self.guard.guard_spark_submit("http://spark-master:8080", target), ["old"])
        self.kill.assert_called_once()

    def test_http_errors_and_json_errors_are_sanitized(self):
        guard = load_guard()
        for body in (b"not json", b"{}"):
            response = MagicMock()
            response.__enter__.return_value = response
            response.status = 200
            response.read.return_value = body
            with patch.object(guard, "HTTPConnection") as connection:
                connection.return_value.getresponse.return_value = response
                if body == b"not json":
                    with self.assertRaises(guard.SparkGuardError):
                        guard._get_master_state("http://master", guard.time.monotonic() + 2)
                else:
                    self.assertEqual(guard._get_master_state("http://master", guard.time.monotonic() + 2), {})
                self.assertEqual(connection.return_value.request.call_args.args, ("GET", "/json/"))
                self.assertLessEqual(connection.call_args.kwargs["timeout"], 2)
        with patch.object(guard, "HTTPConnection") as connection:
            connection.return_value.connect.side_effect = OSError("secret")
            with self.assertRaises(guard.SparkGuardError) as raised:
                guard._get_master_state("http://master", guard.time.monotonic() + 5)
            self.assertNotIn("secret", str(raised.exception))

    def test_kill_redirect_is_not_followed_and_get_redirect_refuses(self):
        guard = load_guard()
        response = MagicMock()
        response.__enter__.return_value = response
        response.status = 302
        with patch.object(guard, "HTTPConnection") as connection:
            connection.return_value.getresponse.return_value = response
            guard._kill_application("http://master", "old", guard.time.monotonic() + 5)
            self.assertEqual(connection.return_value.request.call_count, 1)
            self.assertEqual(connection.return_value.request.call_args.args, ("POST", "/app/kill/"))
            self.assertEqual(connection.return_value.request.call_args.kwargs["body"], b"id=old&terminate=true")
            response.read.assert_not_called()
            with self.assertRaises(guard.SparkGuardError):
                guard._get_master_state("http://master", guard.time.monotonic() + 5)

    def test_budget_is_checked_after_reading_complete_body(self):
        guard = load_guard()
        now = [0.0]
        response = MagicMock()
        response.__enter__.return_value = response
        response.status = 200
        def read():
            now[0] += 6
            return b"{}"
        response.read.side_effect = read
        with patch.object(guard, "HTTPConnection") as connection, patch.object(guard.time, "monotonic", side_effect=lambda: now[0]):
            connection.return_value.getresponse.return_value = response
            with self.assertRaises(guard.SparkGuardError):
                guard._get_master_state("http://master", 120)
            response.read.assert_called_once_with()
            connection.return_value.close.assert_called_once()


class TestGuardOperatorWiring(unittest.TestCase):
    """Each execute, including repeated execution, guards before any submission."""

    def test_missing_connection_refuses_before_guard_or_submit(self):
        module = load_operators()
        for missing in ("spark_main_submit", "spark_main_master_http"):
            operator = module.IngestFullOperator(task_id="load", source_id="bank", airflow_conn_id="source", fqn="s.t", target_table="raw.bank.s__t")
            def get_connection(key):
                if key == missing:
                    raise LookupError("missing connection")
                return types.SimpleNamespace(schema="http", host="spark-master", port=8080)
            module.BaseHook.get_connection.side_effect = get_connection
            with self.assertRaises(LookupError):
                operator.execute({})
            self.assertEqual(operator.submissions, [])
        module.spark_guard.guard_spark_submit.assert_not_called()

    def test_connection_fields_form_url_without_custom_validation(self):
        module = load_operators()
        module.BaseHook.get_connection.return_value = types.SimpleNamespace(
            schema="http", host="spark-master", port=8080,
        )
        operator = module.IngestFullOperator(task_id="load", source_id="bank", airflow_conn_id="source", fqn="s.t", target_table="raw.bank.s__t")
        module.raw_rdbms.run_spark_guard(operator)
        module.spark_guard.guard_spark_submit.assert_called_once_with(
            "http://spark-master:8080", "raw.bank.s__t",
        )
        self.assertIs(operator._guard_result, module.raw_rdbms.SparkGuardStatus.ADMITTED)
        self.assertEqual(
            [call.args[0] for call in module.BaseHook.get_connection.call_args_list],
            ["spark_main_submit", "spark_main_master_http"],
        )

    def test_all_three_operators_refuse_and_repeat_guard(self):
        module = load_operators()
        config = {"contract": {"source_column": "_batch_id", "position_type": "bigint"}, "lookback": {"size": 0, "unit": None}}
        for cls in (module.IngestFullOperator, module.IngestIncrementalOperator, module.IngestActSnapOperator):
            extra = {"incremental_config": config} if cls is module.IngestIncrementalOperator else {}
            operator = cls(task_id="load", source_id="bank", airflow_conn_id="source", fqn="s.t", target_table="raw.bank.s__t", **extra)
            self.assertEqual(operator.conn_id, "spark_main_submit")
            with patch.object(module.spark_guard, "guard_spark_submit", side_effect=RuntimeError("refused")) as guard:
                for _ in range(2):
                    with self.assertRaises(RuntimeError):
                        operator.execute({})
                self.assertEqual(guard.call_count, 2)
                self.assertEqual(operator.submissions, [])
                self.assertIs(operator._guard_result, module.raw_rdbms.SparkGuardStatus.REFUSED)
            self.assertIn(module.raw_rdbms.log_spark_task_event, operator.on_failure_callback)
            self.assertIn(module.raw_rdbms.log_spark_task_event, operator.on_retry_callback)

    def test_callbacks_preserve_existing_and_never_call_master(self):
        module = load_operators()
        existing = MagicMock()
        operator = module.IngestFullOperator(task_id="load", source_id="bank", airflow_conn_id="source", fqn="s.t", target_table="raw.bank.s__t", on_failure_callback=existing)
        context = {"task": operator, "exception": RuntimeError("password=secret")}
        with patch.object(module.spark_guard, "_request") as request:
            for callback in operator.on_failure_callback:
                callback(context)
            request.assert_not_called()
        existing.assert_called_once_with(context)

    def test_default_failure_callback_still_logs_failed(self):
        module = load_operators()
        operator = module.IngestFullOperator(task_id="load", source_id="bank", airflow_conn_id="source", fqn="s.t", target_table="raw.bank.s__t")
        ti = MagicMock()
        ti.xcom_pull.return_value = 42
        for callback in operator.on_failure_callback:
            callback({"ti": ti, "task": operator})
        self.assertEqual(module.meta.log_task.call_args.kwargs["status"], "FAILED")
        self.assertEqual(module.meta.log_task.call_args.kwargs["process_id"], 42)

    def test_all_entrypoints_use_verbatim_target_application_name(self):
        import ast

        root = Path(__file__).resolve().parents[2] / "ingestion/spark/raw/rdbms"
        for relative in ("full/load_entity.py", "incremental/load_entity.py", "incremental/load_act_snap.py"):
            with self.subTest(entrypoint=relative):
                tree = ast.parse((root / relative).read_text())
                calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "appName"]
                self.assertEqual(len(calls), 1)
                self.assertEqual(ast.unparse(calls[0].args[0]), "args.target_table")


if __name__ == "__main__":
    unittest.main()
