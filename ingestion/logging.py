"""Phase headings and logging setup shared by ingestion entry points."""

import json
import logging
import os
import re

SQL_RELAY_ENV = "INGESTION_SQL_LOG_RELAY"
_SQL_FRAME = "INGESTION_SQL_LOG "


_SQL_PART = re.compile(
    r"--[^\n]*|/\*[\s\S]*?\*/|\$(?P<tag>[a-zA-Z_]\w*|)\$[\s\S]*?\$(?P=tag)\$"
    r"|(?:[eE]'(?:\\.|''|[^'\\])*'|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|\[(?:\]\]|[^\]])*\])"
    r"|\b(?:SELECT|FROM|WHERE|AND|OR|SET|VALUES|ON\s+CONFLICT|RETURNING|FOR\s+UPDATE|AS\s+SELECT)\b|[(),]",
    re.IGNORECASE,
)


def format_sql_for_log(sql):
    """Lay out generated SQL without splitting quoted values or function arguments."""
    depth = 0
    queries = {0}
    offset = 0
    indent = 0
    lines = []

    def flush(end, next_indent):
        """Keep the original SQL fragment and update its display indentation."""
        nonlocal offset, indent
        part = sql[offset:end].strip()
        if part:
            lines.append("  " * indent + part)
        offset, indent = end, next_indent

    for match in _SQL_PART.finditer(sql):
        token = match[0].upper()
        if token == "(":
            depth += 1
        elif token == ")":
            if depth in queries and depth:
                flush(match.start(), depth - 1)
                queries.remove(depth)
            depth -= 1
        elif token == "SELECT":
            queries.add(depth)
            flush(match.start(), depth)
        elif depth in queries and token == ",":
            flush(match.end(), depth + 1)
        elif depth in queries and token[0].isalpha() and not token.startswith("E'"):
            flush(match.start(), depth + (token in ("AND", "OR")))
    flush(len(sql), 0)
    return "\n".join(lines)


def log_sql(logger, label, sql):
    """Send one SQL record, framed only when the Airflow relay is enabled."""
    if logger.isEnabledFor(logging.INFO):
        block = f"{label}:\n" + "\n".join("  " + line for line in format_sql_for_log(sql).splitlines())
        logger.info("%s", _SQL_FRAME + json.dumps(block) if os.environ.get(SQL_RELAY_ENV) == "1" else block)


def restore_sql_blocks(lines):
    """Restore framed SQL records before the provider splits them into log events."""
    for line in lines:
        prefix, marker, payload = line.partition(_SQL_FRAME)
        if marker and (not prefix or re.fullmatch(r"INFO [\w.]+: ", prefix)):
            try:
                block = json.loads(payload)
            except ValueError:
                pass
            else:
                if isinstance(block, str):
                    yield block
                    continue
        yield line


def log_step(logger, title):
    """Emit a single record with a visually distinct phase heading."""
    heading = f"== {title} =="
    border = "=" * len(heading)
    logger.info("\n%s\n%s\n%s", border, heading, border)


def configure_driver_logging():
    """Enable application INFO records without overriding Spark JVM logging."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("ingestion").setLevel(logging.INFO)
    logging.getLogger("__main__").setLevel(logging.INFO)
