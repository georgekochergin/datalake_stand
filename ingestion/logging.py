"""Phase headings and logging setup shared by ingestion entry points."""

import logging


def log_step(logger, title, *, number=None):
    """Emit a single record with a visually distinct phase heading."""
    label = f"step №{number}: {title}" if number is not None else title
    heading = f"== {label} =="
    border = "=" * len(heading)
    logger.info("\n%s\n%s\n%s", border, heading, border)


def configure_driver_logging():
    """Enable application INFO records without overriding Spark JVM logging."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("ingestion").setLevel(logging.INFO)
    logging.getLogger("__main__").setLevel(logging.INFO)
