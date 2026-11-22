import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logger(log_level: str = "INFO") -> logging.Logger:
    """
    Configure and return the application logger.

    Console logging is always enabled. Set ``SALES_DATA_LOG_FILE`` to enable
    rotating file output. Rotation defaults to 5 MiB per file and five backups.

    :param log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
    :return: Configured application logger.
    """
    logger = logging.getLogger("sales_data")

    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(log_level.upper())
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    log_file = os.getenv("SALES_DATA_LOG_FILE")
    if log_file:
        log_path = Path(log_file).expanduser()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=int(os.getenv("SALES_DATA_LOG_MAX_BYTES", 5 * 1024 * 1024)),
            backupCount=int(os.getenv("SALES_DATA_LOG_BACKUP_COUNT", "5")),
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
