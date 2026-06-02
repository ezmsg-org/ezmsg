import logging


EZMSG_LOG_FORMAT = (
    "%(asctime)s.%(msecs)03d - pid: %(process)d - %(threadName)s "
    "- %(levelname)s - %(funcName)s: %(message)s"
)
EZMSG_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def create_ezmsg_log_formatter() -> logging.Formatter:
    return logging.Formatter(EZMSG_LOG_FORMAT, datefmt=EZMSG_LOG_DATE_FORMAT)
