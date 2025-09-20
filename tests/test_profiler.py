from datetime import datetime
import os
import pytest
import logging
from pathlib import Path
from unittest.mock import patch
from ezmsg.util.profiler import (
    _setup_logger,
    profile_method,
    profile_subpub,
    get_logger_path,
    HEADER,
)


def test_logger_creation():
    """Test that the ezprofiler logger is created when importing from ezmsg.util.profiler."""
    logger_name = "ezprofiler"
    assert logger_name in logging.Logger.manager.loggerDict
    logger = logging.getLogger(logger_name)
    assert logger.level == logging.INFO
    assert len(logger.handlers) == 1
    handler = logger.handlers[0]
    assert isinstance(handler, logging.FileHandler)
    assert handler.level == logging.DEBUG
    assert Path(handler.baseFilename) == get_logger_path()

    # Remove and close all handlers
    logger.removeHandler(handler)
    handler.close()


@pytest.fixture
def mock_env():
    """Fixture to mock environment variables."""
    with patch.dict(
        os.environ, {"EZMSG_PROFILER": "test_profiler.log", "EZMSG_LOGLEVEL": "DEBUG"}
    ):
        yield


@pytest.fixture
def mock_logger_path(mock_env):
    """Fixture to mock the logger path."""
    logpath = get_logger_path()
    yield logpath


def test_logger_not_append(mock_logger_path):
    """Test that the logger creates a new file if append==False."""
    some_text = "Some,Previous,Logger,Text"
    correct_header = HEADER

    # Create a file with some text
    test_logpath = mock_logger_path
    with open(test_logpath, "w") as f:
        f.truncate(0)
        f.write(some_text + "\n")

    logger = _setup_logger(append=False)
    assert mock_logger_path.exists() and mock_logger_path.is_file()
    assert logger.name == "ezprofiler"
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) == 1
    handler = logger.handlers[0]
    assert isinstance(handler, logging.FileHandler)
    assert handler.level == logging.DEBUG
    assert Path(handler.baseFilename) == test_logpath

    with open(mock_logger_path, "r") as f:
        first_line = f.readline().strip()
    assert first_line == correct_header

    # Clean up the logger file handler
    logger.removeHandler(handler)
    handler.close()


def test_logger_append_header_mismatch(mock_logger_path):
    """Test that the logger deletes the file if the header mismatches when append=True."""
    incorrect_header = "Incorrect,Header,Format"
    correct_header = HEADER

    # Create a file with an incorrect header
    test_logpath = mock_logger_path
    with open(test_logpath, "w") as f:
        f.truncate(0)
        f.write(incorrect_header + "\n")

    logger = _setup_logger(append=True)

    # Assert that the file was deleted and recreated
    assert test_logpath.exists()
    with open(test_logpath, "r") as f:
        first_line = f.readline().strip()
    assert first_line == correct_header

    # Clean up the logger handlers
    handlers = logger.handlers
    for handler in handlers:
        logger.removeHandler(handler)
        handler.close()


def test_logger_append_no_header(mock_logger_path):
    """Test that the logger correctly appends to the logfile if append=True and the header is correct."""

    # Create a file with no header
    test_logpath = mock_logger_path
    with open(test_logpath, "w") as f:
        f.truncate(0)

    logger = _setup_logger(append=True)

    # Assert that the file had the correct header logged
    assert test_logpath.exists()
    with open(test_logpath, "r") as f:
        first_line = f.readline().strip()
    assert first_line == HEADER

    # Clean up the logger handlers
    handlers = logger.handlers
    for handler in handlers:
        if isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)
            handler.close()


def test_logger_append_header_match(mock_logger_path):
    """Test that the logger correctly appends to the logfile if append=True and the header is correct."""
    correct_header = HEADER
    next_line = "Some,New,Logger,Text"

    # Create a file with the correct header
    test_logpath = mock_logger_path
    with open(test_logpath, "w") as f:
        f.truncate(0)
        f.write(correct_header + "\n")
        f.write(next_line + "\n")

    logger = _setup_logger(append=True)
    logger.debug("Some,Added,Logger,Text")

    # Assert that the file was appended to
    assert test_logpath.exists()
    with open(test_logpath, "r") as f:
        first_line = f.readline().strip()
        second_line = f.readline().strip()
        third_line = f.readline().strip()
    assert first_line == correct_header
    assert second_line == next_line
    assert "Some,Added,Logger,Text" in third_line

    # Clean up the logger handlers
    handlers = logger.handlers
    for handler in handlers:
        if isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)
            handler.close()


def test_profile_method_decorator():
    """Test the profile_method decorator."""

    class DummyClass:
        address = "dummy_address"

        @profile_method(trace_oldest=True)
        def sample_method(self, x, y):
            return x + y

    instance = DummyClass()

    with patch("ezmsg.util.profiler.logger") as mock_logger:
        result = instance.sample_method(2, 3)
        assert result == 5

        # Assert the logger was called
        mock_logger.debug.assert_called()


@pytest.mark.asyncio
async def test_profile_subpub_decorator(mock_logger_path):
    """Test the profile_subpub decorator."""
    test_logpath = mock_logger_path
    _setup_logger()

    class DummyUnit:
        address = "dummy_address"

        @profile_subpub(trace_oldest=True)
        async def sample_subpub(self, msg):
            yield "stream", msg

    unit = DummyUnit()

    async for stream, obj in unit.sample_subpub("message"):
        # Assert the generator yields correctly
        assert stream == "stream"
        assert obj == "message"

    # Assert the logger was called and printed in the correct format
    with open(test_logpath, "r") as f:
        f.readline()
        second_line = f.readline().strip()
    log_text = second_line.split(",")
    assert len(log_text) == 6
    datetime.strptime(
        log_text[0], "%Y-%m-%dT%H:%M:%S%z"
    )  # Will throw if format is incorrect
    assert log_text[1].endswith("test_profiler.DummyUnit")
    assert log_text[2] == "dummy_address"
    assert log_text[3] == "None"
    float(log_text[4])  # Will throw if not float
    float(log_text[5])  # Will throw if not float
