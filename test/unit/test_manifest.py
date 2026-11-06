"""Unit tests for ingestion.generator.manifest."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.generator import manifest

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
            "    incremental: {source_column: transaction_id}\n"
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
            "  - incremental: {source_column: client_id}\n"
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
            "    incremental: {source_column: _batch_id}\n"
        )
        pipeline = manifest.load_pipeline(path)
        self.assertEqual(pipeline.entities[0].incremental_column, "_batch_id")

    @unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
    def test_invalid_incremental_column(self):
        path = write_temp_yaml(
            "source_id: bank\n"
            "entities:\n"
            "  - fqn: retail_bank.clients\n"
            "    incremental: {source_column: Client-Id}\n"
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


@unittest.skipUnless(YAML_AVAILABLE, "PyYAML is not installed")
class TestManifestShapeRejections(unittest.TestCase):
    """Strict rejection of malformed manifest shapes."""

    def load_sources(self, content):
        return manifest.load_sources(write_temp_yaml(content))

    def load_pipeline(self, content):
        return manifest.load_pipeline(write_temp_yaml(content))

    def test_source_config_not_mapping(self):
        with self.assertRaisesRegex(ValueError, "config must be a mapping"):
            self.load_sources("bank: bank_source\n")

    def test_source_type_not_rdbms(self):
        with self.assertRaisesRegex(ValueError, "type must be 'rdbms'"):
            self.load_sources("bank:\n  type: files\n  airflow_conn_id: c\n")

    def test_pipeline_not_mapping(self):
        with self.assertRaisesRegex(ValueError, "must be a mapping"):
            self.load_pipeline("- bank\n")

    def test_pipeline_source_id_not_string(self):
        with self.assertRaisesRegex(ValueError, "source_id"):
            self.load_pipeline("source_id: 7\nentities:\n  - fqn: s.t\n")

    def test_pipeline_catchup_not_boolean(self):
        with self.assertRaisesRegex(ValueError, "catchup"):
            self.load_pipeline("source_id: bank\ncatchup: 1\nentities:\n  - fqn: s.t\n")

    def test_pipeline_tags_not_string_list(self):
        with self.assertRaisesRegex(ValueError, "tags"):
            self.load_pipeline("source_id: bank\ntags: [1]\nentities:\n  - fqn: s.t\n")

    def test_entity_not_mapping(self):
        with self.assertRaisesRegex(ValueError, "entity #1: must be a mapping"):
            self.load_pipeline("source_id: bank\nentities:\n  - s.t\n")


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
        sources = manifest.load_sources(repo_root / "ingestion" / "conf" / "sources.yaml")
        paths = sorted((repo_root / "ingestion" / "conf" / "pipelines").rglob("*.yaml"))
        self.assertEqual(len(paths), 4)
        for pipeline_path in paths:
            pipeline = manifest.load_pipeline(pipeline_path)
            manifest.validate_pipeline(sources, pipeline)
            self.assertTrue(pipeline.entities)


if __name__ == "__main__":
    unittest.main()
