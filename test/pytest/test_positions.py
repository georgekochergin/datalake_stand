"""Focused lossless position codec and lookback tests."""

import sys
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.sources.rdbms import positions


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
            self.assertEqual(
                positions.window_bounds(value, kind, 1, "h"),
                (f"2026-10-26T11:00:00." + "0" * precision, ">="),
            )
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

    def test_alignment_in_the_values_own_calendar(self):
        """Instants align on the UTC grid; local positions on wall-clock digits."""
        self.assertEqual(
            positions.window_bounds("2026-10-26T12:34:56", "timestamp_local(0)", 3, "h"),
            ("2026-10-26T09:00:00", ">="),
        )
        self.assertEqual(
            positions.window_bounds("2026-10-26T15:34:56+03:00", "timestamp_instant(0)", 3, "h"),
            ("2026-10-26T09:00:00Z", ">="),
        )
        self.assertEqual(
            positions.window_bounds("2026-10-26T12:34:56", "timestamp_local(0)", 3, "d"),
            ("2026-10-23T00:00:00", ">="),
        )
        self.assertEqual(
            positions.window_bounds("2026-10-26T12:34:56Z", "timestamp_instant(0)", 3, "d"),
            ("2026-10-23T00:00:00Z", ">="),
        )
        self.assertEqual(
            positions.window_bounds("2026-10-26T12:34:56", "timestamp_local(0)", 1, "h"),
            ("2026-10-26T11:00:00", ">="),
        )

    def test_dst_has_no_implicit_timezone(self):
        self.assertEqual(
            positions.window_bounds("2026-03-29T03:30:00", "timestamp_local(0)", 1, "h"),
            ("2026-03-29T02:00:00", ">="),
        )
        self.assertEqual(
            positions.window_bounds("2026-03-29T03:30:00+02:00", "timestamp_instant(0)", 1, "d"),
            ("2026-03-28T00:00:00Z", ">="),
        )
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
            (datetime(2026, 10, 26, 12, 0), "date"),
            (5, "timestamp_local(0)"),
            ("yesterday", "timestamp_local(0)"),
            ("2026-10-26 12:00", "timestamp_local(0)"),
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
            self.assertEqual(positions.window_bounds(value, kind, 1, unit), (None, ">=" if unit else ">"))
            self.assertEqual(positions.window_bounds(value, kind, 10**38 - 1, unit), (None, ">=" if unit else ">"))
            self.assertEqual(positions.window_bounds(value, kind), (value, ">"))
        self.assertEqual(positions.window_bounds("-32767", "smallint", 1), ("-32768", ">"))
        self.assertEqual(positions.window_bounds("2024-03-01", "date", 1, "d"), ("2024-02-29", ">="))
        self.assertEqual(positions.window_bounds("9", "bigint", 2), ("7", ">"))

    def test_lookback_validation_even_with_null_watermark(self):
        for size in (-1, True, "1", 1.0, 10**38):
            with self.assertRaises(ValueError):
                positions.window_bounds(None, "bigint", size)
        for kind, size, unit in (("bigint", 5, "d"), ("date", 1, None), ("date", 1, "h"),
                                 ("timestamp_local(0)", 1, None), ("timestamp_local(0)", 1, "m")):
            with self.assertRaises(ValueError):
                positions.window_bounds(None, kind, size, unit)

    def test_size_zero_ignores_unit_entirely(self):
        kinds = {"date": "2026-10-26", "timestamp_local(6)": "2026-10-26T12:00:00.000000",
                 "timestamp_instant(9)": "2026-10-26T12:00:00.000000000Z", "bigint": "-5"}
        for kind, position in kinds.items():
            for unit in (None, "h", "d", "nonsense"):
                with self.subTest(kind=kind, unit=unit):
                    self.assertEqual(positions.window_bounds(position, kind, 0, unit), (position, ">"))

    def test_numeric_lookback_keeps_strict_operator(self):
        self.assertEqual(positions.window_bounds("10", "bigint", 3), ("7", ">"))
        self.assertEqual(positions.window_bounds("2026-10-26", "date", 3, "d"), ("2026-10-23", ">="))

    def test_resolve_lookback_applies_kind_defaults(self):
        self.assertEqual(positions.resolve_lookback("bigint", 3, "d", False), (0, None))
        self.assertEqual(positions.resolve_lookback("decimal(18,0)", 3, "d", False), (0, None))
        self.assertEqual(positions.resolve_lookback("date", 3, "h", False), (3, "d"))
        self.assertEqual(positions.resolve_lookback("timestamp_local(6)", 3, "h", False), (3, "h"))
        self.assertEqual(positions.resolve_lookback("timestamp_instant(0)", 3, "d", False), (3, "d"))
        self.assertEqual(positions.resolve_lookback("bigint", 5, "d", True), (5, "d"))
        self.assertEqual(positions.resolve_lookback("timestamp_local(6)", 0, None, True), (0, None))


if __name__ == "__main__":
    unittest.main()
