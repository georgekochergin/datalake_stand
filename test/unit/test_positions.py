"""Focused lossless position codec and lookback tests."""

import sys
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dlh_ingestion.sources.rdbms import positions


class TestKindInfo(unittest.TestCase):
    """Kind introspection shared by dialects and the loader."""

    def test_supported_kinds(self):
        for kind, family, precision in (
            ("smallint", "integral", 0),
            ("bigint", "integral", 0),
            ("decimal(18,0)", "integral", 0),
            ("date", "date", 0),
            ("timestamp_local(7)", "local", 7),
            ("timestamp_instant(0)", "instant", 0),
        ):
            self.assertEqual(positions.kind_info(kind)[:2], (family, precision))

    def test_unsupported_kind(self):
        for kind in ("", "boolean", "decimal(10,2)", "timestamp_local(10)", "timestamp_utc(3)", 5, None):
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                positions.kind_info(kind)


class TestPositions(unittest.TestCase):
    """Typed normalization, range validation and exact ordering."""

    def test_integral_boundaries(self):
        for kind, minimum, maximum in (
            ("smallint", -32768, 32767),
            ("integer", -(2**31), 2**31 - 1),
            ("bigint", -(2**63), 2**63 - 1),
            ("decimal(38,0)", -(10**38 - 1), 10**38 - 1),
        ):
            for value in (minimum, maximum, 0):
                self.assertEqual(positions.canonical(value, kind), str(value))
            for value in (minimum - 1, maximum + 1):
                with self.assertRaises(ValueError):
                    positions.canonical(value, kind)
        self.assertEqual(positions.canonical(Decimal("10.00"), "bigint"), "10")

    def test_reject_noncanonical_numeric_and_sql(self):
        for value in (True, 1.0, "01", "+1", "-0", "1e2", "1.0", " 1", "1; SELECT 1", Decimal("NaN"), Decimal("1.1")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                positions.canonical(value, "bigint")

    def test_precision_seven_and_nine(self):
        for precision in (7, 9):
            kind = f"timestamp_local({precision})"
            fraction = "123456789"[:precision]
            value = f"2026-10-26T12:34:56.{fraction}"
            self.assertEqual(positions.canonical(value.replace("T", " "), kind), value)
            self.assertEqual(positions.lower_bound(value, kind, 1, "h"), f"2026-10-26T11:34:56.{fraction}")
        with self.assertRaises(ValueError):
            positions.canonical("2026-10-26T12:34:56.1234567", "timestamp_local(6)")
        self.assertEqual(positions.canonical("2026-10-26 12:34:56.1", "timestamp_local(9)"), "2026-10-26T12:34:56.100000000")

    def test_all_precisions_and_native_values(self):
        for precision in range(10):
            expected = "2026-10-26T00:00:00" + ("." + "0" * precision if precision else "")
            self.assertEqual(positions.canonical(datetime(2026, 10, 26), f"timestamp_local({precision})"), expected)
        self.assertEqual(positions.canonical(date(2026, 10, 26), "date"), "2026-10-26")
        self.assertEqual(positions.canonical(datetime(2026, 10, 26, tzinfo=timezone.utc), "timestamp_instant(0)"), "2026-10-26T00:00:00Z")

    def test_offsets_and_java_strings(self):
        kind = "timestamp_instant(9)"
        for offset in ("+02:00", "+0200", "+02"):
            self.assertEqual(positions.canonical(f"2026-10-26 01:30:00.123456789 {offset}", kind), "2026-10-25T23:30:00.123456789Z")
        self.assertEqual(positions.canonical("2026-10-26T23:30:00-02:00", "timestamp_instant(0)"), "2026-10-27T01:30:00Z")

    def test_dst_has_no_implicit_timezone(self):
        self.assertEqual(positions.lower_bound("2026-03-29T03:30:00", "timestamp_local(0)", 1, "h"), "2026-03-29T02:30:00")
        self.assertEqual(positions.lower_bound("2026-03-29T03:30:00+02:00", "timestamp_instant(0)", 1, "d"), "2026-03-28T01:30:00Z")
        self.assertEqual(positions.maximum("2026-10-25T02:30:00+02:00", "2026-10-25T02:15:00+01:00", "timestamp_instant(0)"), "2026-10-25T01:15:00Z")

    def test_invalid_temporal_values(self):
        for value, kind in (
            ("2026-02-30", "date"),
            ("2026-10-26T24:00:00", "timestamp_local(0)"),
            ("2026-10-26T00:00:60Z", "timestamp_instant(0)"),
            ("2026-10-26T00:00:00+18:01", "timestamp_instant(0)"),
            ("2026-10-26T00:00:00Z", "timestamp_local(0)"),
            ("2026-10-26T00:00:00", "timestamp_instant(0)"),
            ("0001-01-01T00:00:00+01:00", "timestamp_instant(0)"),
            ("2026-10-26'; DROP TABLE x", "date"),
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                positions.canonical(value, kind)

    def test_maximum_and_nulls(self):
        self.assertEqual(positions.maximum("9", "10", "bigint"), "10")
        self.assertEqual(positions.maximum("-10", "-2", "bigint"), "-2")
        self.assertEqual(positions.maximum(None, "10", "bigint"), "10")
        self.assertIsNone(positions.maximum(None, None, "bigint"))
        self.assertIsNone(positions.canonical(None, "date"))
        self.assertEqual(positions.maximum("2026-01-01T00:00:00.123456788", "2026-01-01T00:00:00.123456789", "timestamp_local(9)"), "2026-01-01T00:00:00.123456789")

    def test_minimum_underflow_and_large_lookback(self):
        for value, kind, unit in (
            ("-32768", "smallint", None),
            ("0001-01-01", "date", "d"),
            ("0001-01-01T00:00:00.000000000", "timestamp_local(9)", "h"),
            ("0001-01-01T00:00:00Z", "timestamp_instant(0)", "d"),
        ):
            self.assertIsNone(positions.lower_bound(value, kind, 1, unit))
            self.assertIsNone(positions.lower_bound(value, kind, 10**38 - 1, unit))
            self.assertEqual(positions.lower_bound(value, kind), value)
        self.assertEqual(positions.lower_bound("-32767", "smallint", 1), "-32768")
        self.assertEqual(positions.lower_bound("2024-03-01", "date", 1, "d"), "2024-02-29")

    def test_lookback_validation_even_with_null_watermark(self):
        for size in (-1, True, "1", 1.0, 10**38):
            with self.assertRaises(ValueError):
                positions.lower_bound(None, "bigint", size)
        for kind, size, unit in (("bigint", 0, "d"), ("date", 0, "h"), ("date", 1, None), ("timestamp_local(0)", 1, "m")):
            with self.assertRaises(ValueError):
                positions.lower_bound(None, kind, size, unit)

    def test_configured_lookback_requires_temporal_unit_even_at_zero_size(self):
        kinds = {"date": "2026-10-26", "timestamp_local(6)": "2026-10-26T12:00:00.000000",
                 "timestamp_instant(9)": "2026-10-26T12:00:00.000000000Z"}
        for kind, position in kinds.items():
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, "unit"):
                positions.lower_bound(None, kind, 0, None, specified=True)
            for unit in (("d",) if kind == "date" else ("h", "d")):
                with self.subTest(kind=kind, unit=unit):
                    self.assertEqual(positions.lower_bound(position, kind, 0, unit, specified=True), position)
        self.assertEqual(positions.lower_bound("-5", "bigint", 0, None, specified=True), "-5")


if __name__ == "__main__":
    unittest.main()
