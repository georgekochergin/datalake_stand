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
                self.assertEqual((entity.lookback_size, entity.lookback_unit), (3, "d"))
                self.assertFalse(entity.lookback_specified)

    def test_numeric_and_temporal_lookback(self):
        for size, unit, stored in ((0, None, None), (10, None, None), (0, "h", None),
                                   (2, "h", "h"), (3, "d", "d"), (10 ** 38 - 1, None, None)):
            lookback = {"size": size}
            if unit is not None:
                lookback["unit"] = unit
            with self.subTest(size=size, unit=unit):
                entity = self.load_entity({"incremental": {"source_column": "updated_at", "lookback": lookback}})
                self.assertEqual(entity.lookback_size, size)
                self.assertEqual(entity.lookback_unit, stored)
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


class TestFullSnapshotManifest(unittest.TestCase):
    """The incremental.full_snapshot block: schedule and pool allowlist parsing."""

    def load_entity(self, fields):
        import yaml

        document = {"source_id": "bank", "entities": [{"fqn": "s.t", **fields}]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pipeline.yaml"
            path.write_text(yaml.safe_dump(document), encoding="utf-8")
            return manifest.load_pipeline(path).entities[0]

    def test_snapshot_defaults(self):
        entity = self.load_entity({"incremental": {"source_column": "_batch_id"}})
        self.assertIsNone(entity.snapshot_schedule)
        self.assertIsNone(entity.snapshot_pool)
        self.assertIsNone(entity.snapshot_pool_slots)

    def test_full_snapshot_with_schedule_and_pool(self):
        entity = self.load_entity({"incremental": {
            "source_column": "_batch_id",
            "full_snapshot": {"schedule": "30 2 * * *", "pool": {"name": "snapshot_loads", "slots": 2}},
        }})
        self.assertEqual(entity.snapshot_schedule, "30 2 * * *")
        self.assertEqual(entity.snapshot_pool, "snapshot_loads")
        self.assertEqual(entity.snapshot_pool_slots, 2)

    def test_full_snapshot_empty_block_and_null_schedule(self):
        entity = self.load_entity({"incremental": {
            "source_column": "_batch_id", "full_snapshot": {"schedule": None},
        }})
        self.assertIsNone(entity.snapshot_schedule)
        self.assertIsNone(entity.snapshot_pool)

    def test_pool_name_alone_defaults_slots(self):
        entity = self.load_entity({"incremental": {
            "source_column": "_batch_id", "full_snapshot": {"pool": {"name": "snapshot_loads"}},
        }})
        self.assertEqual(entity.snapshot_pool, "snapshot_loads")
        self.assertEqual(entity.snapshot_pool_slots, 1)

    def test_invalid_schedule_rejected(self):
        for schedule in ("", "nonsense", "61 * * * *", "* * *", 5, True):
            with self.subTest(schedule=schedule), self.assertRaisesRegex(ValueError, "schedule"):
                self.load_entity({"incremental": {
                    "source_column": "_batch_id", "full_snapshot": {"schedule": schedule},
                }})

    def test_invalid_pool_rejected(self):
        for pool in ("busy", [], {"name": ""}, {"name": " "}, {"name": 5}, {"slots": 2},
                     {"name": "p", "slots": 0}, {"name": "p", "slots": True}, {"name": "p", "extra": 1}):
            with self.subTest(pool=pool), self.assertRaisesRegex(ValueError, "pool"):
                self.load_entity({"incremental": {"source_column": "_batch_id", "full_snapshot": {"pool": pool}}})

    def test_unknown_keys_rejected(self):
        with self.assertRaisesRegex(ValueError, "unknown keys"):
            self.load_entity({"incremental": {
                "source_column": "_batch_id", "full_snapshot": {"cron": "0 3 * * *"},
            }})

    def test_entity_level_full_snapshot_rejected_with_hint(self):
        with self.assertRaisesRegex(ValueError, "full_snapshot is configured inside the incremental block"):
            self.load_entity({"full_snapshot": {"schedule": "0 3 * * *"}})

    def test_full_snapshot_requires_source_column(self):
        with self.assertRaisesRegex(ValueError, "incremental.source_column"):
            self.load_entity({"incremental": {"full_snapshot": {"schedule": "0 3 * * *"}}})


if __name__ == "__main__":
    unittest.main()
