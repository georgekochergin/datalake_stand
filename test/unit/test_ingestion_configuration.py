"""Recursive pipeline ownership and strict execution configuration tests."""

import ast
import tempfile
import unittest
from pathlib import Path

import yaml

from ingestion.generator.generate_dags import generate_all, main
from ingestion.generator.manifest import SourceConfig, load_pipeline, load_settings


class TestConfiguration(unittest.TestCase):
    """Validate manifest contracts before any generated artifacts are written."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.sources = {name: SourceConfig(name, "rdbms", name) for name in ("bank", "broker")}

    def write(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(value), encoding="utf-8")
        return path

    def pipeline(self, name, source="bank", fqn="dbo.clients", **entity):
        return self.write(name, {"source_id": source, "entities": [{"fqn": fqn, **entity}]})

    def test_recursive_same_stems_different_sources(self):
        self.pipeline("bank/main.yaml")
        self.pipeline("broker/main.yaml", "broker")
        self.assertEqual(set(generate_all(self.sources, self.root)), {
            "ingest__rdbms__bank__main.py", "ingest__rdbms__broker__main.py",
        })

    def test_collision_reports_both_paths(self):
        first = self.pipeline("a/main.yaml")
        second = self.pipeline("b/main.yaml", fqn="dbo.accounts")
        with self.assertRaises(ValueError) as raised:
            generate_all(self.sources, self.root)
        self.assertIn(str(first), str(raised.exception))
        self.assertIn(str(second), str(raised.exception))

    def test_normalized_target_collision(self):
        self.pipeline("a/main.yaml", fqn='"DBO".clients')
        self.pipeline("b/refs.yaml", fqn="dbo.clients")
        with self.assertRaisesRegex(ValueError, "target collision"):
            generate_all(self.sources, self.root)

    def test_suffix_omitted_and_quotes_parse(self):
        self.pipeline("nested/bank.yaml", fqn='dbo."sub.clients"')
        generated = generate_all(self.sources, self.root)
        self.assertEqual(list(generated), ["ingest__rdbms__bank.py"])
        ast.parse(next(iter(generated.values())))

    def test_invalid_stem(self):
        self.pipeline("invalid-name.yaml")
        with self.assertRaisesRegex(ValueError, "stem"):
            generate_all(self.sources, self.root)

    def test_pool_fields_independent(self):
        for pool, expected in (({}, ("default_pool", 1)), ({"name": "heavy"}, ("heavy", 1)), ({"slots": 3}, ("default_pool", 3))):
            with self.subTest(pool=pool):
                path = self.pipeline("pipeline.yaml", airflow={"pool": pool})
                entity = load_pipeline(path).entities[0]
                self.assertEqual((entity.pool, entity.pool_slots), expected)

    def test_invalid_nested_settings(self):
        invalid = [
            {"spark": {"parallelism": 1}}, {"spark": {"parallelism": "false"}},
            {"spark": {"other": True}}, {"spark": None},
            {"airflow": {"other": 1}}, {"airflow": {"pool": []}},
            {"airflow": {"pool": {"name": " "}}},
            *({"airflow": {"pool": {"slots": value}}} for value in (True, 0, -1, "2", 1.5)),
            {"airflow": {"pool": {"unknown": 1}}},
        ]
        for config in invalid:
            with self.subTest(config=config):
                with self.assertRaises(ValueError):
                    load_pipeline(self.pipeline("pipeline.yaml", **config))

    def test_settings_strict_and_propagated(self):
        self.pipeline("pipelines/bank.yaml", spark={"parallelism": False})
        path = self.write("settings.yaml", {"spark": {"jdbc": {"num_partitions": 7}}})
        self.assertEqual(load_settings(path).num_partitions, 7)
        content = next(iter(generate_all(self.sources, self.root / "pipelines", path).values()))
        self.assertIn("num_partitions=7", content)
        self.assertIn("parallelism=False", content)
        for value in (True, 0, -2, "4", None):
            self.write("settings.yaml", {"spark": {"jdbc": {"num_partitions": value}}})
            with self.subTest(value=value), self.assertRaises(ValueError):
                load_settings(path)
        self.write("settings.yaml", {"spark": {"jdbc": {"num_partitions": 4, "other": 1}}})
        with self.assertRaises(ValueError):
            load_settings(path)

    def test_regeneration_preserves_unowned_files(self):
        output = self.root / "output"
        output.mkdir()
        stale = output / "ingest__old.py"
        stale.write_text('"""Generated DAG for pipeline old."""\n', encoding="utf-8")
        custom = output / "ingest__custom.py"
        custom.write_text('"""User module."""\n', encoding="utf-8")
        main(["--output", str(output)])
        self.assertFalse(stale.exists())
        self.assertTrue(custom.exists())

    def test_conflicting_output_fails_before_cleanup(self):
        output = self.root / "output"
        output.mkdir()
        stale = output / "ingest__old.py"
        stale.write_text('"""Generated DAG for pipeline old."""\n', encoding="utf-8")
        custom = output / "ingest__rdbms__bank__main.py"
        custom.write_text('"""User module."""\n', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "non-generated"):
            main(["--output", str(output)])
        self.assertTrue(stale.exists())
        self.assertEqual(custom.read_text(), '"""User module."""\n')
