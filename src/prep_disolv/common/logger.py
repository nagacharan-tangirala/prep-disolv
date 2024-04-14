from __future__ import annotations

import logging.config
from datetime import datetime
from os.path import join
from pathlib import Path

LOG_FILE = "log_file"
LOG_LEVEL = "log_level"
LOG_OVERWRITE = "log_overwrite"

DEFAULT_LOG_FILE = "disolv_positions.log"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_OVERWRITE = False


def _setup_logger_config(
    log_file: str, log_level: str, log_overwrite: bool = False
) -> None:
    """
    Configure the logger.
    """
    # Check if the log file already exists
    if log_file and not log_overwrite:
        log_file = log_file.replace(
            ".log", "_%s.log" % datetime.now().strftime("%Y%m%d_%H%M%S")
        )

    with open(log_file, "w") as f:
        f.write("")

    simple_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "%(asctime)s - %(name)40s - %(levelname)6s - %(message)s"
            },
        },
        "handlers": {
            "stderr": {
                "class": "logging.StreamHandler",
                "level": "ERROR",
                "formatter": "simple",
                "stream": "ext://sys.stderr",
            },
            "local_file_handler": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": log_level.upper(),
                "formatter": "simple",
                "filename": log_file,
            },
        },
        "root": {"level": log_level.upper(), "handlers": ["local_file_handler"]},
    }

    logging.config.dictConfig(simple_config)


def setup_logging(config_path: Path, log_settings: dict) -> None:
    """
    Set up the logging.
    """
    log_file = join(config_path, DEFAULT_LOG_FILE)
    if LOG_FILE in log_settings:
        log_file = join(config_path, log_settings[LOG_FILE])

    log_level = DEFAULT_LOG_LEVEL
    if LOG_LEVEL in log_settings:
        log_level = log_settings[LOG_LEVEL]

    log_overwrite = DEFAULT_LOG_OVERWRITE
    if LOG_OVERWRITE in log_settings:
        log_overwrite = log_settings[LOG_OVERWRITE]

    _setup_logger_config(log_file, log_level, log_overwrite)
