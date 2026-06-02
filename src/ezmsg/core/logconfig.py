import logging
import sys
from typing import TextIO


EZMSG_LOG_FORMAT = (
    "%(asctime)s.%(msecs)03d - pid: %(process)d - %(threadName)s "
    "- %(levelname)s - %(funcName)s: %(message)s"
)
EZMSG_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
EZMSG_STDERR_LOG_LEVEL = logging.WARNING


class BelowLevelFilter(logging.Filter):
    def __init__(self, level: int):
        super().__init__()
        self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < self.level


class AtOrAboveLevelFilter(logging.Filter):
    def __init__(self, level: int):
        super().__init__()
        self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= self.level


def create_ezmsg_log_formatter() -> logging.Formatter:
    return logging.Formatter(EZMSG_LOG_FORMAT, datefmt=EZMSG_LOG_DATE_FORMAT)


def create_ezmsg_stdout_handler(stream: TextIO | None = None) -> logging.StreamHandler:
    handler = logging.StreamHandler(sys.stdout if stream is None else stream)
    handler.setFormatter(create_ezmsg_log_formatter())
    handler.addFilter(BelowLevelFilter(EZMSG_STDERR_LOG_LEVEL))
    return handler


def create_ezmsg_stderr_handler(stream: TextIO | None = None) -> logging.StreamHandler:
    handler = logging.StreamHandler(sys.stderr if stream is None else stream)
    handler.setFormatter(create_ezmsg_log_formatter())
    handler.addFilter(AtOrAboveLevelFilter(EZMSG_STDERR_LOG_LEVEL))
    return handler
