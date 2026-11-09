"""Unit tests for ingestion.generator.generate_dags."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.generator.generate_dags import (
    asset_uri,
    compute_drift,
    generate_all,
    generate_dag_source,
    generate_snapshot_dag_source,
    main,
    snapshot_asset_uri,
    snapshot_dag_id,
)
from ingestion.generator.manifest import ActSnapConfig, AirflowConfig, SparkConfig, EntityConfig, PipelineManifest, SourceConfig
from ingestion.generator.naming import snapshot_alias

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
        self.assertIn('GenerateProcessIdOperator(task_id="generate_process_id")', self.source)
        self.assertNotIn("entities=", self.source)
        self.assertIn("generate_process_id >> activate_source >> ingest_group", self.source)
        self.assertIn("    generate_process_id >> ingest_group\n", self.source)

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
            self.assertEqual(node.func.id, "PythonOperator")
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
            schedule="30 2 * * *",
            catchup=False,
            tags=(),
            entities=(EntityConfig("retail_bank.clients", "retail_bank__clients", None),),
        )
        self.assertIn("schedule='30 2 * * *'", generate_dag_source(BANK, pipeline))

    def test_default_lookback_baked_for_incremental_entity(self):
        pipeline = PipelineManifest(
            source_id="bank",
            schedule=None,
            catchup=False,
            tags=(),
            entities=(EntityConfig("retail_bank.transactions", "retail_bank__transactions",
                                   "_batch_id", lookback_size=3, lookback_unit="d"),),
        )
        source = generate_dag_source(BANK, pipeline)
        self.assertIn("lookback_size=3", source)
        self.assertIn("lookback_unit='d'", source)
        self.assertIn("lookback_specified=False", source)

    def test_deterministic(self):
        self.assertEqual(self.source, generate_dag_source(BANK, PIPELINE))


class TestAssetUri(unittest.TestCase):
    """Asset URI composition and percent-encoding."""

    def test_plain(self):
        self.assertEqual(asset_uri("bank", "retail_bank.clients"), "raw://bank/retail_bank.clients")

    def test_quoted_fqn_percent_encoded(self):
        self.assertEqual(asset_uri("x", '"a.b"'), "raw://x/%22a.b%22")

    def test_snapshot_plain(self):
        self.assertEqual(
            snapshot_asset_uri("bank", "retail_bank.transactions"),
            "raw://bank/retail_bank.transactions/act_snap",
        )

    def test_snapshot_quoted_fqn_percent_encoded(self):
        self.assertEqual(snapshot_asset_uri("x", '"a.b"'), "raw://x/%22a.b%22/act_snap")


SNAPSHOT_PIPELINE = PipelineManifest(
    source_id="bank",
    schedule=None,
    catchup=False,
    tags=("ingestion", "bank"),
    entities=(
        EntityConfig(
            "retail_bank.transactions", "retail_bank__transactions", "_batch_id",
            pool="ingest_loads", pool_slots=2,
        ),
    ),
)


class TestGenerateSnapshotDagSource(unittest.TestCase):
    """Rendering of a single snapshot DAG module."""

    def setUp(self):
        self.source = generate_snapshot_dag_source(BANK, SNAPSHOT_PIPELINE, SNAPSHOT_PIPELINE.entities[0], "main")

    def test_snapshot_group_waits_directly_for_process_generator(self):
        self.assertIn("generate_process_id >> activate_source >> snap_group", self.source)
        self.assertIn("    generate_process_id >> snap_group\n", self.source)

    def test_dag_header(self):
        expected = f"ingest__rdbms__bank__main__act_snap_{snapshot_alias('retail_bank.transactions')}"
        self.assertEqual(snapshot_dag_id(BANK, "main", "retail_bank.transactions"), expected)
        self.assertIn(f'dag_id="{expected}"', self.source)
        self.assertIn("schedule=None", self.source)
        self.assertIn("catchup=False", self.source)
        self.assertIn("max_active_runs=1", self.source)
        self.assertIn(
            "tags=['act_snap', 'bank', 'retail_bank__transactions']", self.source
        )
        self.assertIn(
            "description='Actual snapshot for source bank entity retail_bank.transactions (generated)'",
            self.source,
        )

    def test_schedule_literal_rendered(self):
        entity = EntityConfig(
            "retail_bank.transactions", "retail_bank__transactions", "_batch_id",
            act_snap=ActSnapConfig(schedule="30 2 * * *", tags=("nightly",)),
        )
        rendered = generate_snapshot_dag_source(BANK, SNAPSHOT_PIPELINE, entity, "main")
        self.assertIn("schedule='30 2 * * *'", rendered)
        self.assertIn("'nightly'", rendered)

    def test_pipeline_suffix_dropped_for_default_name(self):
        rendered = generate_snapshot_dag_source(BANK, SNAPSHOT_PIPELINE, SNAPSHOT_PIPELINE.entities[0], "bank")
        self.assertIn('dag_id="ingest__rdbms__bank__act_snap_', rendered)

    def test_task_group_and_operators(self):
        self.assertIn('group_id="snap_retail_bank__transactions"', self.source)
        self.assertIn('IngestActSnapOperator(', self.source)
        self.assertIn('datalake_table="raw.bank.retail_bank__transactions__act_snap"', self.source)
        self.assertIn('target_table="raw.bank.retail_bank__transactions__act_snap"', self.source)
        self.assertIn("generate_process_id >> activate_source >> snap_group", self.source)

    def test_pool_has_independent_defaults(self):
        self.assertIn('pool="default_pool"', self.source)
        self.assertIn("pool_slots=1", self.source)

    def test_pool_override(self):
        entity = EntityConfig(
            "retail_bank.transactions", "retail_bank__transactions", "_batch_id",
            pool="ingest_loads", pool_slots=2,
            jdbc_partitioning=True,
            act_snap=ActSnapConfig(airflow=AirflowConfig("snapshot_loads", 1),
                                   spark=SparkConfig(False)),
        )
        rendered = generate_snapshot_dag_source(BANK, SNAPSHOT_PIPELINE, entity, "main")
        self.assertEqual(rendered.count('pool="snapshot_loads"'), 1)
        self.assertIn("pool_slots=1", rendered)
        self.assertNotIn('pool="ingest_loads"', rendered)
        self.assertIn("jdbc_partitioning=False", rendered)

    def test_asset_published_only_by_conditional_task(self):
        import ast

        calls = [node for node in ast.walk(ast.parse(self.source)) if isinstance(node, ast.Call)]
        publishers = [node for node in calls if any(keyword.arg == "outlets" for keyword in node.keywords)]
        self.assertEqual(len(publishers), 1)
        node = publishers[0]
        self.assertEqual(node.func.id, "PythonOperator")
        keywords = {keyword.arg: keyword.value for keyword in node.keywords}
        self.assertEqual(ast.literal_eval(keywords["op_kwargs"]), {"target_table": "raw.bank.retail_bank__transactions__act_snap"})
        self.assertEqual(keywords["python_callable"].id, "publish_asset_event")
        self.assertIn('Asset("raw://bank/retail_bank.transactions/act_snap")', self.source)

    def test_logging_chain(self):
        self.assertEqual(self.source.count("log_start >> load >> log_end >> publish_asset"), 1)

    def test_no_dag_params_and_no_hash_comments(self):
        self.assertNotIn("Param(", self.source)
        self.assertNotIn("params={", self.source)
        self.assertNotIn("\n#", self.source)
        self.assertNotIn(" #", self.source)

    def test_deterministic(self):
        again = generate_snapshot_dag_source(BANK, SNAPSHOT_PIPELINE, SNAPSHOT_PIPELINE.entities[0], "main")
        self.assertEqual(self.source, again)


class TestGenerateAll(unittest.TestCase):
    """Rendering of the real repository manifests."""

    @classmethod
    def setUpClass(cls):
        repo_root = Path(__file__).resolve().parents[2]
        from ingestion.generator.manifest import load_sources

        cls.sources = load_sources(repo_root / "ingestion" / "conf" / "sources.yaml")
        cls.generated = generate_all(cls.sources, repo_root / "ingestion" / "conf" / "pipelines")

    def test_covers_both_pipelines(self):
        expected = {
            f"ingest__rdbms__{source}__{pipeline}.py"
            for source in ("bank", "broker") for pipeline in ("main", "refs")
        }
        expected.update(
            f"ingest__rdbms__{source}__main__act_snap_{alias}.py"
            for source, alias in (
                ("bank", snapshot_alias("retail_bank.transactions")),
                ("broker", snapshot_alias("trading_core.transactions")),
            )
        )
        self.assertEqual(set(self.generated), expected)

    def test_snapshot_files_only_for_incremental_entities(self):
        snapshots = {name for name in self.generated if "__act_snap_" in name}
        self.assertEqual(len(snapshots), 2)
        for name in snapshots:
            self.assertTrue(name.startswith("ingest__rdbms__"), name)
            self.assertNotIn("refs", name)

    def test_broker_is_incremental(self):
        broker = self.generated["ingest__rdbms__broker__main.py"]
        self.assertIn('incremental_column="_batch_id"', broker)
        self.assertIn('entity_fqn="trading_core.transactions"', broker)
        self.assertIn('Asset("raw://broker/trading_core.deals")', broker)
        self.assertIn("lookback_size=3", broker)
        self.assertIn("lookback_unit='d'", broker)

    def test_bank_mixed_modes(self):
        bank = self.generated["ingest__rdbms__bank__main.py"]
        self.assertEqual(bank.count('incremental_column="_batch_id"'), 1)

    def test_demonstration_weights_and_partitioning(self):
        for name, content in self.generated.items():
            if "__act_snap_" in name:
                self.assertEqual(content.count('pool="ingest_loads"'), 1)
                self.assertEqual(content.count("jdbc_partitioning=False"), 0)
                self.assertEqual(content.count("pool_slots=2"), 1)
            elif "__main" in name:
                self.assertEqual(content.count('pool="ingest_loads"'), 2)
                self.assertEqual(content.count("jdbc_partitioning=False"), 1)
                self.assertEqual(content.count("pool_slots=2"), 1)
            else:
                self.assertEqual(content.count('pool="ingest_loads"'), 2)
                self.assertEqual(content.count("jdbc_partitioning=False"), 2)
                self.assertEqual(content.count("pool_slots=2"), 0)

    def test_generated_modules_parse(self):
        import ast

        for name, content in self.generated.items():
            ast.parse(content, filename=name)

    def test_deterministic(self):
        repo_root = Path(__file__).resolve().parents[2]
        again = generate_all(self.sources, repo_root / "ingestion" / "conf" / "pipelines")
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

    def test_changed_content_is_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            (output / "a.py").write_text("x = 2\n", encoding="utf-8")
            self.assertEqual(compute_drift({"a.py": "x = 1\n"}, output), ["differs: " + str(output / "a.py")])


class TestGenerateAllValidation(unittest.TestCase):
    """Pipeline tree and rendering preconditions."""

    def test_empty_pipelines_dir_rejected(self):
        from ingestion.generator.manifest import load_sources

        repo_root = Path(__file__).resolve().parents[2]
        sources = load_sources(repo_root / "ingestion" / "conf" / "sources.yaml")
        with tempfile.TemporaryDirectory() as tmp, self.assertRaisesRegex(ValueError, "no pipeline manifests"):
            generate_all(sources, Path(tmp))

    def test_full_slice_entity_named_like_snapshot_table_is_allowed(self):
        import yaml

        from ingestion.generator.manifest import load_sources

        repo_root = Path(__file__).resolve().parents[2]
        sources = load_sources(repo_root / "ingestion" / "conf" / "sources.yaml")
        document = {
            "source_id": "bank",
            "entities": [{"fqn": "s.t"}, {"fqn": "s.t__act_snap"}],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "main.yaml"
            path.write_text(yaml.safe_dump(document), encoding="utf-8")
            generated = generate_all(sources, Path(tmp))
        self.assertEqual(set(generated), {"ingest__rdbms__bank__main.py"})

    def test_snapshot_collision_detected_when_full_slice_comes_first(self):
        import yaml

        from ingestion.generator.manifest import load_sources

        repo_root = Path(__file__).resolve().parents[2]
        sources = load_sources(repo_root / "ingestion" / "conf" / "sources.yaml")
        document = {
            "source_id": "bank",
            "entities": [
                {"fqn": "s.t__act_snap"},
                {"fqn": "s.t", "incremental": {"source_column": "_batch_id"}},
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "main.yaml"
            path.write_text(yaml.safe_dump(document), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "target collision raw.bank.s__t__act_snap"):
                generate_all(sources, Path(tmp))

    def test_snapshot_target_collision_rejected(self):
        import yaml

        from ingestion.generator.manifest import load_sources

        repo_root = Path(__file__).resolve().parents[2]
        sources = load_sources(repo_root / "ingestion" / "conf" / "sources.yaml")
        document = {
            "source_id": "bank",
            "entities": [
                {"fqn": "s.t", "incremental": {"source_column": "_batch_id"}},
                {"fqn": "s.t__act_snap"},
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pipeline.yaml"
            path.write_text(yaml.safe_dump(document), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "target collision raw.bank.s__t__act_snap"):
                generate_all(sources, Path(tmp))


if __name__ == "__main__":
    unittest.main()
