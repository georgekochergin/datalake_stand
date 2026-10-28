"""Strict nested incremental manifest parsing and lookback boundaries."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.generator import manifest


class TestIncrementalManifest(unittest.TestCase):
    """Preserve internal entity fields while rejecting invalid YAML contracts."""

    def load_entity(self, fields):
        import yaml

        document = {"source_id": "bank", "entities": [{"fqn": "s.t", **fields}]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pipeline.yaml"
            path.write_text(yaml.safe_dump(document), encoding="utf-8")
            return manifest.load_pipeline(path).entities[0]

    def test_snapshot_defaults(self):
        entity = self.load_entity({})
        self.assertIsNone(entity.incremental_column)
        self.assertEqual(entity.lookback_size, 0)
        self.assertIsNone(entity.lookback_unit)
        self.assertFalse(entity.lookback_specified)

    def test_nested_source_column_and_default_lookback(self):
        for column in ("_batch_id", "updated_at"):
            with self.subTest(column=column):
                entity = self.load_entity({"incremental": {"source_column": column}})
                self.assertEqual(entity.incremental_column, column)
                self.assertEqual(entity.lookback_size, 0)
                self.assertIsNone(entity.lookback_unit)
                self.assertFalse(entity.lookback_specified)

    def test_numeric_and_temporal_lookback(self):
        for size, unit in ((0, None), (10, None), (0, "h"), (2, "h"), (3, "d"), (10 ** 38 - 1, None)):
            lookback = {"size": size}
            if unit is not None:
                lookback["unit"] = unit
            with self.subTest(size=size, unit=unit):
                entity = self.load_entity({"incremental": {"source_column": "updated_at", "lookback": lookback}})
                self.assertEqual(entity.lookback_size, size)
                self.assertEqual(entity.lookback_unit, unit)
                self.assertTrue(entity.lookback_specified)

    def test_legacy_and_unknown_entity_keys_rejected(self):
        for fields in ({"incremental_column": "_batch_id"}, {"lookback": {"size": 1}},
                       {"incremental": {"source_column": "_batch_id"}, "incremental_column": "_batch_id"}):
            with self.subTest(fields=fields), self.assertRaisesRegex(ValueError, "unknown keys"):
                self.load_entity(fields)

    def test_incremental_requires_mapping_and_valid_column(self):
        for incremental in (None, False, [], "_batch_id", {}, {"source_column": None},
                            {"source_column": True}, {"source_column": 1}, {"source_column": ""},
                            {"source_column": "Updated_At"}, {"source_column": "updated-at"},
                            {"source_column": "updated_at; DROP TABLE x"},
                            {"source_column": "updated_at", "unknown": 1}):
            with self.subTest(incremental=incremental), self.assertRaises(ValueError):
                self.load_entity({"incremental": incremental})

    def test_lookback_requires_strict_mapping(self):
        for lookback in (None, False, [], 1, "1", {}, {"unit": "h"}, {"size": 1, "unknown": 1}):
            with self.subTest(lookback=lookback), self.assertRaises(ValueError):
                self.load_entity({"incremental": {"source_column": "updated_at", "lookback": lookback}})

    def test_lookback_size_rejects_bool_fraction_and_overflow(self):
        for size in (None, True, False, -1, 1.0, 0.5, "1", [], {}, 10 ** 38):
            with self.subTest(size=size), self.assertRaisesRegex(ValueError, "lookback.size"):
                self.load_entity({"incremental": {"source_column": "_batch_id", "lookback": {"size": size}}})

    def test_lookback_unit_rejects_invalid_or_explicit_null(self):
        for unit in (None, "", "H", "hours", "m", 1, True, [], {}):
            with self.subTest(unit=unit), self.assertRaisesRegex(ValueError, "lookback.unit"):
                self.load_entity({"incremental": {"source_column": "updated_at", "lookback": {"size": 1, "unit": unit}}})


if __name__ == "__main__":
    unittest.main()
