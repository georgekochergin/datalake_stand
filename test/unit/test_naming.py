"""Unit tests for ingestion.lib.naming."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.lib import naming


class TestResolveTargetTableName(unittest.TestCase):
    """Resolution of two-fragment RDBMS FQNs to target table names."""

    def test_plain_fqn(self):
        self.assertEqual(naming.resolve_target_table_name("retail_bank.clients"), "retail_bank__clients")

    def test_quoted_fragment_with_dot(self):
        self.assertEqual(
            naming.resolve_target_table_name('retail_bank."sub.clients"'),
            "retail_bank__sub__clients",
        )

    def test_quoted_schema_with_dot(self):
        self.assertEqual(naming.resolve_target_table_name('"abc_d.ef_g".tbl'), "abc_d__ef_g__tbl")

    def test_lowercase_folding(self):
        self.assertEqual(naming.resolve_target_table_name("RetailBank.Clients"), "retailbank__clients")

    def test_encoded_fragment_contains_delimiter(self):
        self.assertEqual(naming.resolve_target_table_name('x."a._b"'), "x__a___b")

    def test_empty_fqn(self):
        with self.assertRaises(ValueError):
            naming.resolve_target_table_name("")

    def test_single_fragment(self):
        with self.assertRaises(ValueError):
            naming.resolve_target_table_name("clients")

    def test_three_fragments(self):
        with self.assertRaises(ValueError):
            naming.resolve_target_table_name("a.b.c")

    def test_leading_dot(self):
        with self.assertRaises(ValueError):
            naming.resolve_target_table_name(".clients")

    def test_trailing_dot(self):
        with self.assertRaises(ValueError):
            naming.resolve_target_table_name("retail_bank.clients.")

    def test_double_dot(self):
        with self.assertRaises(ValueError):
            naming.resolve_target_table_name("retail_bank..clients")

    def test_unbalanced_quotes(self):
        with self.assertRaises(ValueError):
            naming.resolve_target_table_name('retail_bank."clients')

    def test_empty_quoted_fragment(self):
        with self.assertRaises(ValueError):
            naming.resolve_target_table_name('retail_bank.""')

    def test_invalid_characters(self):
        with self.assertRaises(ValueError):
            naming.resolve_target_table_name("retail_bank.clie-nts")


class TestResolveNamespace(unittest.TestCase):
    """Resolution of source ids to raw-layer namespaces."""

    def test_plain_source_id(self):
        self.assertEqual(naming.resolve_namespace("bank"), "bank")

    def test_lowercase_folding(self):
        self.assertEqual(naming.resolve_namespace("Broker"), "broker")

    def test_invalid_source_id(self):
        with self.assertRaises(ValueError):
            naming.resolve_namespace("bank-1")

    def test_empty_source_id(self):
        with self.assertRaises(ValueError):
            naming.resolve_namespace("")


class TestResolveFullyQualified(unittest.TestCase):
    """Composition of catalog, namespace, and target table name."""

    def test_composition(self):
        self.assertEqual(
            naming.resolve_fully_qualified("bank", "retail_bank.clients"),
            "raw.bank.retail_bank__clients",
        )

    def test_catalog_constant(self):
        self.assertEqual(naming.CATALOG, "raw")


class TestAssertUniqueTargets(unittest.TestCase):
    """Pairwise-uniqueness of resolved target table names."""

    def test_unique_targets_pass(self):
        naming.assert_unique_targets(["retail_bank.clients", "retail_bank.accounts"])

    def test_duplicate_targets_fail(self):
        with self.assertRaises(ValueError):
            naming.assert_unique_targets(["x.a__b", 'x."a.b"'])

    def test_identical_fqns_fail(self):
        with self.assertRaises(ValueError):
            naming.assert_unique_targets(["retail_bank.clients", "retail_bank.clients"])


if __name__ == "__main__":
    unittest.main()
