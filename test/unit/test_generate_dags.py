"""Unit tests for ingestion.generate_dags."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.generate_dags import (
    asset_uri,
    compute_drift,
    generate_all,
    generate_dag_source,
    main,
)
from ingestion.lib.manifest import EntityConfig, PipelineManifest, SourceConfig

BANK = SourceConfig(source_id="bank", type="rdbms", airflow_conn_id="bank_source")
PIPELINE = PipelineManifest(
    source_id="bank",
    schedule=None,
    catchup=False,
    tags=("ingestion", "bank"),
    entities=(
        EntityConfig("retail_bank.clients", "retail_bank__clients", None),
        EntityConfig("retail_bank.transactions", "retail_bank__transactions", "_batch_id", lookback_size=12,
                     lookback_unit="h", lookback_specified=True),
    ),
)


class TestGenerateDagSource(unittest.TestCase):
    """Rendering of a single pipeline into a literal DAG module."""

    def setUp(self):
        self.source = generate_dag_source(BANK, PIPELINE)

    def test_dag_header(self):
        self.assertIn('dag_id="ingest__rdbms__bank"', self.source)
        self.assertIn("schedule=None", self.source)
        self.assertIn("catchup=False", self.source)
        self.assertIn("max_active_runs=1", self.source)
        self.assertIn("tags=['ingestion', 'bank']", self.source)
        self.assertIn("on_success_callback=dagrun_success_callback", self.source)
        self.assertIn("on_failure_callback=dagrun_failure_callback", self.source)

    def test_pre_pipeline_tasks(self):
        self.assertIn('source="bank"', self.source)
        self.assertIn("ttl_minutes=15", self.source)
        self.assertIn('GenerateProcessIdOperator(task_id="generate_process_id", entities=', self.source)
        self.assertIn("generate_process_id >> activate_source >> ingest_group", self.source)

    def test_no_dag_params(self):
        """The trigger dialog must open with an active button, without required fields."""
        self.assertNotIn("Param(", self.source)
        self.assertNotIn("params={", self.source)

    def test_entity_groups_and_assets(self):
        self.assertIn('group_id="load_retail_bank__clients"', self.source)
        self.assertIn('group_id="load_retail_bank__transactions"', self.source)
        self.assertIn('Asset("raw://bank/retail_bank.clients")', self.source)
        self.assertIn('Asset("raw://bank/retail_bank.transactions")', self.source)
        self.assertIn('datalake_table="raw.bank.retail_bank__transactions"', self.source)

    def test_assets_are_published_only_by_separate_task(self):
        import ast

        calls = [node for node in ast.walk(ast.parse(self.source)) if isinstance(node, ast.Call)]
        publishers = [node for node in calls if any(keyword.arg == "outlets" for keyword in node.keywords)]
        self.assertEqual(len(publishers), 2)
        for node in publishers:
            self.assertEqual(node.func.id, "EmptyOperator")
            keywords = {keyword.arg: keyword.value for keyword in node.keywords}
            self.assertEqual(ast.literal_eval(keywords["task_id"]), "publish_asset")
            self.assertNotIn("trigger_rule", keywords)

    def test_publication_requires_successful_end_logging(self):
        self.assertEqual(self.source.count("log_start >> load >> log_end >> publish_asset"), 2)

    def test_load_tasks_share_pool(self):
        self.assertIn('pool="default_pool"', self.source)
        self.assertEqual(self.source.count('pool_slots=1'), 2)

    def test_incremental_column_only_where_configured(self):
        self.assertEqual(self.source.count('incremental_column="_batch_id"'), 1)
        self.assertIn('entity_fqn="retail_bank.clients"', self.source)
        self.assertEqual(self.source.count("lookback_size=12"), 1)
        self.assertEqual(self.source.count("lookback_unit='h'"), 1)
        self.assertEqual(self.source.count("lookback_specified=True"), 1)
        self.assertNotIn("lookback_specified=False", self.source)

    def test_no_hash_comments(self):
        self.assertNotIn("\n#", self.source)
        self.assertNotIn(" #", self.source)

    def test_schedule_literal_rendered(self):
        pipeline = PipelineManifest(
            source_id="bank",
            schedule="@daily",
            catchup=False,
            tags=(),
            entities=(EntityConfig("retail_bank.clients", "retail_bank__clients", None),),
        )
        self.assertIn("schedule='@daily'", generate_dag_source(BANK, pipeline))

    def test_deterministic(self):
        self.assertEqual(self.source, generate_dag_source(BANK, PIPELINE))


class TestAssetUri(unittest.TestCase):
    """Asset URI composition and percent-encoding."""

    def test_plain(self):
        self.assertEqual(asset_uri("bank", "retail_bank.clients"), "raw://bank/retail_bank.clients")

    def test_quoted_fqn_percent_encoded(self):
        self.assertEqual(asset_uri("x", '"a.b"'), "raw://x/%22a.b%22")


class TestGenerateAll(unittest.TestCase):
    """Rendering of the real repository manifests."""

    @classmethod
    def setUpClass(cls):
        repo_root = Path(__file__).resolve().parents[2]
        from ingestion.lib.manifest import load_sources

        cls.sources = load_sources(repo_root / "ingestion" / "sources.yaml")
        cls.generated = generate_all(cls.sources, repo_root / "ingestion" / "pipelines")

    def test_covers_both_pipelines(self):
        self.assertEqual(set(self.generated), {
            f"ingest__rdbms__{source}__{pipeline}.py"
            for source in ("bank", "broker") for pipeline in ("main", "refs")
        })

    def test_broker_is_incremental(self):
        broker = self.generated["ingest__rdbms__broker__main.py"]
        self.assertIn('incremental_column="_batch_id"', broker)
        self.assertIn('entity_fqn="trading_core.transactions"', broker)
        self.assertIn('Asset("raw://broker/trading_core.deals")', broker)

    def test_bank_mixed_modes(self):
        bank = self.generated["ingest__rdbms__bank__main.py"]
        self.assertEqual(bank.count('incremental_column="_batch_id"'), 1)

    def test_demonstration_weights_and_parallelism(self):
        for name, content in self.generated.items():
            self.assertEqual(content.count('pool="ingest_loads"'), 2)
            self.assertEqual(content.count("parallelism=False"), 1 if "__main" in name else 2)
            self.assertEqual(content.count("pool_slots=2"), 1 if "__main" in name else 0)

    def test_generated_modules_parse(self):
        import ast

        for name, content in self.generated.items():
            ast.parse(content, filename=name)

    def test_deterministic(self):
        repo_root = Path(__file__).resolve().parents[2]
        again = generate_all(self.sources, repo_root / "ingestion" / "pipelines")
        self.assertEqual(self.generated, again)


class TestCheckMode(unittest.TestCase):
    """Drift detection and the --check exit code."""

    def test_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            repo_root = Path(__file__).resolve().parents[2]
            argv = ["--output", str(output)]
            self.assertEqual(main(argv), 0)
            self.assertEqual(main(argv + ["--check"]), 0)
            (output / "stale.py").write_text("x = 1\n", encoding="utf-8")
            drift = main(argv + ["--check"])
            self.assertEqual(drift, 1)

    def test_missing_directory_is_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "generated"
            self.assertTrue(compute_drift({"a.py": "x = 1\n"}, output))


if __name__ == "__main__":
    unittest.main()
