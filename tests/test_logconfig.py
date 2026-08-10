import io
import logging

from ezmsg.core.logconfig import (
    create_ezmsg_stderr_handler,
    create_ezmsg_stdout_handler,
)


def test_ezmsg_console_handlers_split_logs_by_level():
    stdout = io.StringIO()
    stderr = io.StringIO()
    logger = logging.Logger("ezmsg-test")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(create_ezmsg_stdout_handler(stdout))
    logger.addHandler(create_ezmsg_stderr_handler(stderr))

    logger.debug("debug message")
    logger.info("info message")
    logger.warning("warning message")
    logger.error("error message")
    logger.critical("critical message")

    stdout_value = stdout.getvalue()
    stderr_value = stderr.getvalue()

    assert "debug message" in stdout_value
    assert "info message" in stdout_value
    assert "warning message" not in stdout_value
    assert "error message" not in stdout_value
    assert "critical message" not in stdout_value

    assert "debug message" not in stderr_value
    assert "info message" not in stderr_value
    assert "warning message" in stderr_value
    assert "error message" in stderr_value
    assert "critical message" in stderr_value
