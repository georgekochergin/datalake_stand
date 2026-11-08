"""Unit tests for the JDBC partition-column heuristic (jdbc_partitioning)."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.sources.rdbms import jdbc, jdbc_partitioning, registry
from ingestion.sources.rdbms.contracts import RdbmsDialect

MSSQL = registry.by_conn_type("mssql")
ORACLE = registry.by_conn_type("oracle")


def meta(name, type_name, precision=None, scale=None, not_null=True):
    """One column fact with defaults handy for the heuristic tests."""
    return jdbc.ColumnMeta(name=name, type_name=type_name, precision=precision,
                           scale=scale, not_null=not_null)


def keys(primary_key=(), unique_columns=(), columns=()):
    """KeyMetadata shorthand: columns may be infos or meta() argument tuples."""
    mapped = {}
    for item in columns:
        info = item if isinstance(item, jdbc.ColumnMeta) else meta(*item)
        mapped[info.name.lower()] = info
    return jdbc.KeyMetadata(primary_key=tuple(primary_key),
                            unique_columns=tuple(unique_columns), columns=mapped)


class StubKindDialect(RdbmsDialect):
    """Minimal adapter classifying only its own vendor-specific type name."""

    def position_kind(self, type_name, precision, scale):
        """Accept only INT8 as an integral position."""
        if type_name == "INT8":
            return "bigint"
        raise ValueError(f"Unsupported type: {type_name}")


class TestChoose(unittest.TestCase):
    """The four-step heuristic shared by every load mode."""

    def choose(self, dialect, key_metadata):
        return jdbc_partitioning.choose(dialect, key_metadata)

    def test_single_integer_pk_wins(self):
        choice = self.choose(MSSQL, keys(
            primary_key=("transaction_id",),
            columns=[("transaction_id", "bigint"), ("updated_at", "datetime2")],
        ))
        self.assertEqual((choice.column, choice.step, choice.derived),
                         ("transaction_id", jdbc_partitioning.STEP_PRIMARY_KEY, False))

    def test_composite_pk_falls_through(self):
        choice = self.choose(MSSQL, keys(
            primary_key=("code", "num"),
            columns=[("code", "nvarchar"), ("num", "bigint"),
                     ("updated_at", "datetime2", None, 7)],
        ))
        self.assertEqual((choice.column, choice.step, choice.derived),
                         ("updated_at", jdbc_partitioning.STEP_TIMESTAMP_DAY, True))

    def test_non_integer_pk_falls_through(self):
        choice = self.choose(MSSQL, keys(
            primary_key=("code",),
            columns=[("code", "nvarchar"), ("deal_date", "date")],
        ))
        self.assertEqual((choice.column, choice.step),
                         ("deal_date", jdbc_partitioning.STEP_DATE))

    def test_pk_missing_from_columns_ignored(self):
        self.assertIsNone(self.choose(MSSQL, keys(
            primary_key=("id",), columns=[("code", "nvarchar")],
        )))

    def test_unique_integer_requires_not_null(self):
        choice = self.choose(MSSQL, keys(
            unique_columns=("num",),
            columns=[("num", "int", None, None, False), ("deal_date", "date")],
        ))
        self.assertEqual((choice.column, choice.step),
                         ("deal_date", jdbc_partitioning.STEP_DATE))

    def test_unique_integer_picked_over_later_steps(self):
        choice = self.choose(MSSQL, keys(
            unique_columns=("num",),
            columns=[("num", "int"), ("deal_date", "date"), ("updated_at", "datetime2")],
        ))
        self.assertEqual((choice.column, choice.step),
                         ("num", jdbc_partitioning.STEP_UNIQUE_INTEGER))

    def test_unique_decimal_with_scale_not_integral(self):
        choice = self.choose(MSSQL, keys(
            unique_columns=("amount",),
            columns=[("amount", "decimal", 18, 2), ("deal_date", "date")],
        ))
        self.assertEqual((choice.column, choice.step),
                         ("deal_date", jdbc_partitioning.STEP_DATE))

    def test_unique_column_missing_from_columns_ignored(self):
        self.assertIsNone(self.choose(MSSQL, keys(unique_columns=("ghost",), columns=[("num", "int")])))

    def test_candidates_ordered_by_normalized_name(self):
        choice = self.choose(MSSQL, keys(
            columns=[("b_date", "date"), ("a_date", "date")],
        ))
        self.assertEqual((choice.column, choice.step), ("a_date", jdbc_partitioning.STEP_DATE))

    def test_mixed_case_names_ordered_by_lowercase(self):
        choice = self.choose(MSSQL, keys(
            columns=[("Billing_date", "date"), ("audit_date", "date")],
        ))
        self.assertEqual(choice.column, "audit_date")

    def test_original_case_preserved_in_choice(self):
        choice = self.choose(MSSQL, keys(columns=[("Posting_Date", "date")]))
        self.assertEqual(choice.column, "Posting_Date")

    def test_nullable_date_skipped(self):
        choice = self.choose(MSSQL, keys(
            columns=[("maybe_date", "date", None, None, False),
                     ("updated_at", "datetime2", None, 7)],
        ))
        self.assertEqual((choice.column, choice.step),
                         ("updated_at", jdbc_partitioning.STEP_TIMESTAMP_DAY))

    def test_timestamp_step_is_derived(self):
        choice = self.choose(ORACLE, keys(columns=[("updated_at", "TIMESTAMP(6) WITH TIME ZONE")]))
        self.assertEqual((choice.column, choice.step, choice.derived),
                         ("updated_at", jdbc_partitioning.STEP_TIMESTAMP_DAY, True))

    def test_oracle_date_is_timestamp_step(self):
        choice = self.choose(ORACLE, keys(columns=[("posting_date", "DATE")]))
        self.assertEqual((choice.column, choice.step, choice.derived),
                         ("posting_date", jdbc_partitioning.STEP_TIMESTAMP_DAY, True))

    def test_oracle_number_with_scale_zero_is_integral(self):
        choice = self.choose(ORACLE, keys(primary_key=("id",),
                                          columns=[("id", "NUMBER", 19, 0)]))
        self.assertEqual((choice.column, choice.step), ("id", jdbc_partitioning.STEP_PRIMARY_KEY))

    def test_oracle_unconstrained_number_rejected(self):
        self.assertIsNone(self.choose(ORACLE, keys(columns=[("value", "NUMBER", 0, -127)])))

    def test_identity_decoration_tolerated(self):
        choice = self.choose(MSSQL, keys(primary_key=("id",), columns=[("id", "INT IDENTITY")]))
        self.assertEqual((choice.column, choice.step), ("id", jdbc_partitioning.STEP_PRIMARY_KEY))

    def test_strings_and_floats_never_candidates(self):
        self.assertIsNone(self.choose(MSSQL, keys(
            columns=[("code", "nvarchar"), ("ratio", "float"), ("flag", "bit")],
        )))

    def test_stub_dialect_classifies_own_type_without_loader_changes(self):
        stub = StubKindDialect()
        choice = self.choose(stub, keys(unique_columns=("id",), columns=[("id", "INT8")]))
        self.assertEqual((choice.column, choice.step), ("id", jdbc_partitioning.STEP_UNIQUE_INTEGER))
        self.assertIsNone(self.choose(MSSQL, keys(unique_columns=("id",), columns=[("id", "INT8")])))

    def test_no_candidates_returns_none(self):
        self.assertIsNone(self.choose(MSSQL, keys(columns=[("code", "nvarchar")])))
        self.assertIsNone(self.choose(MSSQL, keys()))


class TestNoCandidateReason(unittest.TestCase):
    """Diagnostic rendering of the metadata behind an empty heuristic result."""

    def test_reason_lists_keys_types_families_and_nullability(self):
        reason = jdbc_partitioning.no_candidate_reason(MSSQL, keys(
            primary_key=("code",),
            unique_columns=("num",),
            columns=[("num", "int", None, None, False), ("code", "nvarchar"),
                     ("deal_date", "date", None, None, False)],
        ))
        self.assertEqual(
            reason,
            "primary_key=('code',) unique_columns=('num',) "
            "columns=[code nvarchar unsupported, deal_date date date null, "
            "num int integral null]",
        )

    def test_reason_marks_missing_keys_as_none(self):
        reason = jdbc_partitioning.no_candidate_reason(
            MSSQL, keys(columns=[("amount", "decimal", 18, 2)]))
        self.assertEqual(
            reason,
            "primary_key=none unique_columns=none columns=[amount decimal unsupported]",
        )


class TestDayExpression(unittest.TestCase):
    """Dialect rendering of the calendar-day derivative."""

    def test_mssql_datediff_on_plain_column(self):
        expression = jdbc_partitioning.day_expression(
            MSSQL, keys(columns=[("updated_at", "datetime2")]),
            jdbc_partitioning.PartitionChoice(
                "updated_at", "timestamp_local(7)",
                jdbc_partitioning.STEP_TIMESTAMP_DAY, True),
        )
        self.assertEqual(expression, "DATEDIFF(day, 0, extract_source.[updated_at])")

    def test_mssql_datetimeoffset_partitioned_by_column_itself(self):
        expression = jdbc_partitioning.day_expression(
            MSSQL, keys(columns=[("stamp", "datetimeoffset")]),
            jdbc_partitioning.PartitionChoice(
                "stamp", "timestamp_instant(7)",
                jdbc_partitioning.STEP_TIMESTAMP_DAY, True),
        )
        self.assertEqual(expression, "DATEDIFF(day, 0, extract_source.[stamp])")

    def test_oracle_julian_day_on_plain_column(self):
        expression = jdbc_partitioning.day_expression(
            ORACLE, keys(columns=[("updated_at", "TIMESTAMP(6)")]),
            jdbc_partitioning.PartitionChoice(
                "updated_at", "timestamp_local(6)",
                jdbc_partitioning.STEP_TIMESTAMP_DAY, True),
        )
        self.assertEqual(
            expression,
            'CAST(TO_NUMBER(TO_CHAR(extract_source."updated_at", \'J\')) AS NUMBER(10,0))',
        )

    def test_oracle_time_zone_derives_from_utc_projection(self):
        expression = jdbc_partitioning.day_expression(
            ORACLE, keys(columns=[("updated_at", "TIMESTAMP(6) WITH TIME ZONE")]),
            jdbc_partitioning.PartitionChoice(
                "updated_at", "timestamp_instant(6)",
                jdbc_partitioning.STEP_TIMESTAMP_DAY, True),
        )
        self.assertEqual(
            expression,
            'CAST(TO_NUMBER(TO_CHAR(SYS_EXTRACT_UTC(extract_source."updated_at"), \'J\'))'
            ' AS NUMBER(10,0))',
        )

    def test_partition_name_direct_and_derived(self):
        direct = jdbc_partitioning.PartitionChoice("id", "bigint", jdbc_partitioning.STEP_PRIMARY_KEY, False)
        derived = jdbc_partitioning.PartitionChoice("ts", "timestamp_local(0)",
                                                    jdbc_partitioning.STEP_TIMESTAMP_DAY, True)
        self.assertEqual(jdbc_partitioning.partition_name(direct), "id")
        self.assertEqual(jdbc_partitioning.partition_name(derived), jdbc_partitioning.DAY_ALIAS)


if __name__ == "__main__":
    unittest.main()
