"""Unit tests for ingestion.lib.manifest."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.lib import manifest

try:
    import yaml

    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

BANK_SOURCE = manifest.SourceConfig(
    source_id="bank",
    type="rdbms",
    airflow_conn_id="bank_source",
)


def write_temp_yaml(content: str) -> Path:
    """Write YAML content to a temporary file and return its path."""
    directory = tempfile.mkdtemp()
    path = Path(directory) / "manifest.yaml"
    path.write_text(content, encoding="utf-8")
    return path


class TestLoadSources(unittest.TestCase):
    """Loading and validation of the sources registry."""

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_valid_sources(self):
        path = write_temp_yaml(
            "bank:\n"
            "  type: rdbms\n"
            "  airflow_conn_id: bank_source\n"
        )
        sources = manifest.load_sources(path)
        self.assertEqual(set(sources), {"bank"})
        self.assertEqual(sources["bank"], BANK_SOURCE)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_unknown_source_key(self):
        path = write_temp_yaml(
            "bank:\n"
            "  type: rdbms\n"
            "  airflow_conn_id: bank_source\n"
            "  schema_registry_conn_id: registry\n"
        )
        with self.assertRaises(ValueError):
            manifest.load_sources(path)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_kafka_type_rejected(self):
        path = write_temp_yaml(
            "kafka_payments:\n"
            "  type: kafka\n"
            "  airflow_conn_id: kafka_source\n"
        )
        with self.assertRaises(ValueError):
            manifest.load_sources(path)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_missing_conn_id(self):
        path = write_temp_yaml("bank:\n  type: rdbms\n")
        with self.assertRaises(ValueError):
            manifest.load_sources(path)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_invalid_source_id(self):
        path = write_temp_yaml(
            "Bank-Source:\n"
            "  type: rdbms\n"
            "  airflow_conn_id: bank_source\n"
        )
        with self.assertRaises(ValueError):
            manifest.load_sources(path)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_empty_registry(self):
        path = write_temp_yaml("")
        with self.assertRaises(ValueError):
            manifest.load_sources(path)


class TestLoadPipeline(unittest.TestCase):
    """Loading and validation of pipeline manifests."""

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_valid_full_snapshot_pipeline(self):
        path = write_temp_yaml(
            "source_id: bank\n"
            "schedule: \"@daily\"\n"
            "catchup: false\n"
            "tags:\n  - ingestion\n  - bank\n"
            "entities:\n"
            "  - fqn: retail_bank.clients\n"
            "  - fqn: retail_bank.transactions\n"
            "    incremental_column: transaction_id\n"
        )
        pipeline = manifest.load_pipeline(path)
        self.assertEqual(pipeline.source_id, "bank")
        self.assertEqual(pipeline.schedule, "@daily")
        self.assertFalse(pipeline.catchup)
        self.assertEqual(pipeline.tags, ("ingestion", "bank"))
        self.assertEqual(len(pipeline.entities), 2)
        self.assertEqual(pipeline.entities[0].target_table, "retail_bank__clients")
        self.assertIsNone(pipeline.entities[0].incremental_column)
        self.assertEqual(pipeline.entities[1].incremental_column, "transaction_id")

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_defaults(self):
        path = write_temp_yaml(
            "source_id: bank\n"
            "entities:\n"
            "  - fqn: retail_bank.clients\n"
        )
        pipeline = manifest.load_pipeline(path)
        self.assertIsNone(pipeline.schedule)
        self.assertFalse(pipeline.catchup)
        self.assertEqual(pipeline.tags, ())

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_unknown_top_level_key(self):
        path = write_temp_yaml(
            "source_id: bank\n"
            "unknown_key: 1\n"
            "entities:\n"
            "  - fqn: retail_bank.clients\n"
        )
        with self.assertRaises(ValueError):
            manifest.load_pipeline(path)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_empty_entities(self):
        path = write_temp_yaml("source_id: bank\nentities: []\n")
        with self.assertRaises(ValueError):
            manifest.load_pipeline(path)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_missing_fqn(self):
        path = write_temp_yaml(
            "source_id: bank\n"
            "entities:\n"
            "  - incremental_column: client_id\n"
        )
        with self.assertRaises(ValueError):
            manifest.load_pipeline(path)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_duplicate_fqn(self):
        path = write_temp_yaml(
            "source_id: bank\n"
            "entities:\n"
            "  - fqn: retail_bank.clients\n"
            "  - fqn: retail_bank.clients\n"
        )
        with self.assertRaises(ValueError):
            manifest.load_pipeline(path)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_unknown_entity_key(self):
        path = write_temp_yaml(
            "source_id: bank\n"
            "entities:\n"
            "  - fqn: retail_bank.clients\n"
            "    strategy: overwrite\n"
        )
        with self.assertRaises(ValueError):
            manifest.load_pipeline(path)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_leading_underscore_incremental_column(self):
        path = write_temp_yaml(
            "source_id: bank\n"
            "entities:\n"
            "  - fqn: retail_bank.transactions\n"
            "    incremental_column: _batch_id\n"
        )
        pipeline = manifest.load_pipeline(path)
        self.assertEqual(pipeline.entities[0].incremental_column, "_batch_id")

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_invalid_incremental_column(self):
        path = write_temp_yaml(
            "source_id: bank\n"
            "entities:\n"
            "  - fqn: retail_bank.clients\n"
            "    incremental_column: Client-Id\n"
        )
        with self.assertRaises(ValueError):
            manifest.load_pipeline(path)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_invalid_fqn_propagates_from_naming(self):
        path = write_temp_yaml(
            "source_id: bank\n"
            "entities:\n"
            "  - fqn: a.b.c\n"
        )
        with self.assertRaises(ValueError):
            manifest.load_pipeline(path)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_duplicate_targets(self):
        path = write_temp_yaml(
            "source_id: bank\n"
            "entities:\n"
            "  - fqn: x.a__b\n"
            "  - fqn: x.\"a.b\"\n"
        )
        with self.assertRaises(ValueError):
            manifest.load_pipeline(path)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_empty_schedule(self):
        path = write_temp_yaml(
            "source_id: bank\n"
            "schedule: \"\"\n"
            "entities:\n"
            "  - fqn: retail_bank.clients\n"
        )
        with self.assertRaises(ValueError):
            manifest.load_pipeline(path)


class TestValidatePipeline(unittest.TestCase):
    """Cross-validation of a pipeline manifest against the sources registry."""

    def test_valid_pipeline_passes(self):
        sources = {"bank": BANK_SOURCE}
        pipeline = manifest.PipelineManifest(
            source_id="bank",
            schedule="@daily",
            catchup=False,
            tags=("ingestion",),
            entities=(manifest.EntityConfig("retail_bank.clients", "retail_bank__clients", None),),
        )
        manifest.validate_pipeline(sources, pipeline)

    def test_missing_source_fails(self):
        sources = {"bank": BANK_SOURCE}
        pipeline = manifest.PipelineManifest(
            source_id="broker",
            schedule=None,
            catchup=False,
            tags=(),
            entities=(manifest.EntityConfig("trading_core.clients", "trading_core__clients", None),),
        )
        with self.assertRaises(ValueError):
            manifest.validate_pipeline(sources, pipeline)

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_real_repo_manifests_are_valid(self):
        repo_root = Path(__file__).resolve().parents[2]
        sources = manifest.load_sources(repo_root / "ingestion" / "sources.yaml")
        paths = sorted((repo_root / "ingestion" / "pipelines").rglob("*.yaml"))
        self.assertEqual(len(paths), 4)
        for pipeline_path in paths:
            pipeline = manifest.load_pipeline(pipeline_path)
            manifest.validate_pipeline(sources, pipeline)
            self.assertTrue(pipeline.entities)


if __name__ == "__main__":
    unittest.main()
