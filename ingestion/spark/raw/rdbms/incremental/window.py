"""Window preparation for the incremental flow: frozen predicates and the exact position."""

from ingestion.sources.rdbms import positions
from ingestion.sources.rdbms import registry as dialects
from ingestion.spark.raw.rdbms import reader


def window_filters(dialect, batch, kind, incremental_column):
    """SQL predicates of the frozen batch window over the incremental column."""
    column = dialect.quote_identifier(incremental_column)
    filters = []
    for key, op in (("lower_bound", batch["lower_bound_op"]), ("upper_bound", "<=")):
        if batch[key] is not None:
            filters.append(
                f"{column} {op} {dialect.position_literal(positions.canonical(batch[key], kind), kind)}"
            )
    if batch["empty_at_plan"]:
        filters.append("1=0")
    return filters


def position_select(dialect, kind, incremental_column) -> str:
    """SELECT fragment with the exact typed extraction position of the window."""
    column = dialect.quote_identifier(incremental_column)
    return f', {dialect.position_expression(column, kind)} AS "__extract_position"'


def read_window_frame(spark, args, batch, kind):
    """Read the frozen batch window into a cached frame via the shared JDBC reader."""
    dialect = dialects.by_jdbc_url(args.jdbc_url)
    filters = window_filters(dialect, batch, kind, args.incremental_column)
    extra_select = position_select(dialect, kind, args.incremental_column)
    return reader.read_extract_frame(spark, args, filters=filters, extra_select=extra_select)
