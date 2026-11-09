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


class TestActSnapManifest(unittest.TestCase):
    """Independent restricted snapshot DAG configuration."""

    def load_entity(self, fields):
        import yaml

        document = {"source_id": "bank", "entities": [{"fqn": "s.t", **fields}]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pipeline.yaml"
            path.write_text(yaml.safe_dump(document), encoding="utf-8")
            return manifest.load_pipeline(path).entities[0]

    def test_snapshot_defaults(self):
        entity = self.load_entity({"incremental": {"source_column": "_batch_id"}})
        self.assertEqual(entity.act_snap, manifest.ActSnapConfig())

    def test_act_snap_with_schedule_and_pool(self):
        entity = self.load_entity({"incremental": {
            "source_column": "_batch_id",
            "act_snap": {"schedule": "30 2 * * *", "tags": ["nightly"],
                         "airflow": {"pool": {"name": "snapshot_loads", "slots": 2}}},
        }})
        self.assertEqual(entity.act_snap.schedule, "30 2 * * *")
        self.assertEqual(entity.act_snap.tags, ("nightly",))
        self.assertEqual(entity.act_snap.airflow.pool, "snapshot_loads")
        self.assertEqual(entity.act_snap.airflow.pool_slots, 2)

    def test_act_snap_empty_block_and_null_schedule(self):
        entity = self.load_entity({"incremental": {
            "source_column": "_batch_id", "act_snap": {"schedule": None},
        }})
        self.assertEqual(entity.act_snap, manifest.ActSnapConfig())

    def test_pool_name_alone_defaults_slots(self):
        entity = self.load_entity({"incremental": {
            "source_column": "_batch_id", "act_snap": {"airflow": {"pool": {"name": "snapshot_loads"}}},
        }})
        self.assertEqual(entity.act_snap.airflow.pool, "snapshot_loads")
        self.assertEqual(entity.act_snap.airflow.pool_slots, 1)

    def test_invalid_schedule_rejected(self):
        for schedule in ("", "nonsense", "61 * * * *", "* * *", 5, True):
            with self.subTest(schedule=schedule), self.assertRaisesRegex(ValueError, "schedule"):
                self.load_entity({"incremental": {
                    "source_column": "_batch_id", "act_snap": {"schedule": schedule},
                }})

    def test_invalid_pool_rejected(self):
        for pool in ("busy", [], {"name": ""}, {"name": " "}, {"name": 5},
                     {"name": "p", "slots": 0}, {"name": "p", "slots": True}, {"name": "p", "extra": 1}):
            with self.subTest(pool=pool), self.assertRaisesRegex(ValueError, "pool"):
                self.load_entity({"incremental": {"source_column": "_batch_id", "act_snap": {"airflow": {"pool": pool}}}})

    def test_unknown_keys_rejected(self):
        for key in ("fqn", "entities", "incremental", "catchup", "max_active_runs", "pool", "cron"):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "unknown keys"):
                self.load_entity({"incremental": {"source_column": "_batch_id", "act_snap": {key: None}}})

    def test_entity_level_act_snap_rejected_with_hint(self):
        with self.assertRaisesRegex(ValueError, "act_snap is configured inside the incremental block"):
            self.load_entity({"act_snap": {"schedule": "0 3 * * *"}})

    def test_act_snap_requires_source_column(self):
        with self.assertRaisesRegex(ValueError, "incremental.source_column"):
            self.load_entity({"incremental": {"act_snap": {"schedule": "0 3 * * *"}}})

    def test_old_name_rejected(self):
        with self.assertRaisesRegex(ValueError, "full_snapshot"):
            self.load_entity({"incremental": {"source_column": "id", "full_snapshot": {}}})

    def test_independent_reading_and_pool_defaults(self):
        entity = self.load_entity({"spark": {"jdbc_partitioning": False},
                                   "airflow": {"pool": {"name": "main", "slots": 2}},
                                   "incremental": {"source_column": "id"}})
        self.assertTrue(entity.act_snap.spark.jdbc_partitioning)
        self.assertEqual(entity.act_snap.airflow, manifest.AirflowConfig())
        entity = self.load_entity({"incremental": {"source_column": "id",
                                   "act_snap": {"spark": {"jdbc_partitioning": False}}}})
        self.assertTrue(entity.jdbc_partitioning)
        self.assertFalse(entity.act_snap.spark.jdbc_partitioning)

    def test_strict_snapshot_options(self):
        for options in ({"spark": {"jdbc_partitioning": "false"}}, {"tags": "nightly"},
                        {"spark": {"parallelism": True}}, {"airflow": {"extra": 1}}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.load_entity({"incremental": {"source_column": "id", "act_snap": options}})

    def test_restricted_dataclass_contract(self):
        from dataclasses import fields

        self.assertEqual({item.name for item in fields(manifest.ActSnapConfig)},
                         {"schedule", "tags", "spark", "airflow"})
        self.assertTrue(issubclass(manifest.PipelineManifest, manifest.DagConfig))
        self.assertTrue(issubclass(manifest.ActSnapConfig, manifest.DagConfig))
        self.assertEqual({item.name for item in fields(manifest.PipelineManifest)},
                         {"source_id", "schedule", "tags", "catchup", "entities"})


if __name__ == "__main__":
    unittest.main()
