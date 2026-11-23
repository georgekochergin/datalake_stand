"""Phase headings and logging setup shared by ingestion entry points."""

import json
import logging
import os
import re
from collections.abc import Iterable, Iterator

SQL_RELAY_ENV = "INGESTION_SQL_LOG_RELAY"
_SQL_FRAME = "INGESTION_SQL_LOG "


def log_sql(logger: logging.Logger, label: str, sql: str) -> None:
    """Send one SQL record as emitted by its generator, framed only when the relay is enabled."""
    if logger.isEnabledFor(logging.INFO):
        block = f"{label}:\n" + "\n".join("  " + line for line in sql.splitlines())
        logger.info("%s", _SQL_FRAME + json.dumps(block) if os.environ.get(SQL_RELAY_ENV) == "1" else block)


def restore_sql_blocks(lines: Iterable[str]) -> Iterator[str]:
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
                    yield prefix + block
                    continue
        yield line


def log_step(logger: logging.Logger, title: str) -> None:
    """Emit a single record with a visually distinct phase heading."""
    heading = f"== {title} =="
    border = "=" * len(heading)
    logger.info("\n%s\n%s\n%s", border, heading, border)


def configure_driver_logging() -> None:
    """Enable application INFO records without overriding Spark JVM logging."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("ingestion").setLevel(logging.INFO)
    logging.getLogger("__main__").setLevel(logging.INFO)
