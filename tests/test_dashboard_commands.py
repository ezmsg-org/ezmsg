import argparse
import logging
import sys
from types import SimpleNamespace

import pytest

from ezmsg.core.commands.dashboard import (
    DASHBOARD_ADDR_ENV,
    DashboardDependencyError,
    DASHBOARD_INSTALL_HINT,
    dashboard_address,
    require_dashboard_dependency,
    start_dashboard,
)
from ezmsg.core.commands.start import handle_start
from ezmsg.core.commands.serve import handle_serve
from ezmsg.core.commands.common import graph_address_from_args
from ezmsg.core.netprotocol import Address


def log_file_handlers_for(log_path):
    return [
        handler
        for handler in logging.getLogger("ezmsg").handlers
        if isinstance(handler, logging.FileHandler)
        and getattr(handler, "baseFilename", None) == str(log_path)
    ]


def test_dashboard_address_defaults_to_graph_port_plus_one():
    graph_address = Address("127.0.0.1", 25978)

    assert dashboard_address(graph_address) == Address("127.0.0.1", 25979)


def test_dashboard_address_uses_environment_override(monkeypatch):
    monkeypatch.setenv(DASHBOARD_ADDR_ENV, "0.0.0.0:4100")

    assert dashboard_address(Address("127.0.0.1", 25978)) == Address("0.0.0.0", 4100)


def test_dashboard_address_uses_explicit_port_with_graph_host():
    assert dashboard_address(Address("127.0.0.1", 30000), dashboard_port=30001) == Address(
        "127.0.0.1", 30001
    )


def test_dashboard_address_uses_explicit_port_with_env_host(monkeypatch):
    monkeypatch.setenv(DASHBOARD_ADDR_ENV, "0.0.0.0:4100")

    assert dashboard_address(Address("127.0.0.1", 25978), dashboard_port=4101) == Address(
        "0.0.0.0", 4101
    )


def test_graph_address_from_args_uses_environment_override(monkeypatch):
    monkeypatch.setenv("EZMSG_GRAPHSERVER_ADDR", "0.0.0.0:4101")

    assert graph_address_from_args(argparse.Namespace(address=None)) == Address(
        "0.0.0.0", 4101
    )


def test_require_dashboard_dependency_raises_helpful_error_when_package_missing(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "ezmsg.dashboard.server":
            raise ImportError("missing optional dashboard package")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(RuntimeError, match="pip install ezmsg-dashboard"):
        require_dashboard_dependency()


def test_start_dashboard_raises_helpful_error_when_package_missing(monkeypatch):
    monkeypatch.setattr(
        "ezmsg.core.commands.dashboard.require_dashboard_dependency",
        lambda: (_ for _ in ()).throw(DashboardDependencyError(DASHBOARD_INSTALL_HINT)),
    )

    with pytest.raises(RuntimeError, match="pip install ezmsg-dashboard"):
        start_dashboard(Address("127.0.0.1", 25978))


@pytest.mark.asyncio
async def test_handle_start_forwards_dashboard_flag(monkeypatch):
    popen_calls: list[list[str]] = []

    class DummyPopen:
        def __init__(self, cmd):
            popen_calls.append(cmd)
            self.pid = 1234

    async def fake_open_connection(self):
        return object(), SimpleNamespace()

    async def fake_close_stream_writer(writer):
        return None

    monkeypatch.setattr("ezmsg.core.commands.start.subprocess.Popen", DummyPopen)
    monkeypatch.setattr(
        "ezmsg.core.commands.start.require_dashboard_dependency",
        lambda: object(),
    )
    monkeypatch.setattr(
        "ezmsg.core.commands.start.GraphService.open_connection", fake_open_connection
    )
    monkeypatch.setattr(
        "ezmsg.core.commands.start.close_stream_writer", fake_close_stream_writer
    )

    args = argparse.Namespace(address="127.0.0.1:25978", dashboard=True, log_file=None)
    await handle_start(args)

    assert popen_calls == [
        [
            sys.executable,
            "-m",
            "ezmsg.core",
            "serve",
            "--address=127.0.0.1:25978",
            "--dashboard",
        ]
    ]


@pytest.mark.asyncio
async def test_handle_start_forwards_dashboard_port(monkeypatch):
    popen_calls: list[list[str]] = []

    class DummyPopen:
        def __init__(self, cmd):
            popen_calls.append(cmd)
            self.pid = 1234

    async def fake_open_connection(self):
        return object(), SimpleNamespace()

    async def fake_close_stream_writer(writer):
        return None

    monkeypatch.setattr("ezmsg.core.commands.start.subprocess.Popen", DummyPopen)
    monkeypatch.setattr(
        "ezmsg.core.commands.start.require_dashboard_dependency",
        lambda: object(),
    )
    monkeypatch.setattr(
        "ezmsg.core.commands.start.GraphService.open_connection", fake_open_connection
    )
    monkeypatch.setattr(
        "ezmsg.core.commands.start.close_stream_writer", fake_close_stream_writer
    )

    args = argparse.Namespace(address="127.0.0.1:25978", dashboard=28123, log_file=None)
    await handle_start(args)

    assert popen_calls == [
        [
            sys.executable,
            "-m",
            "ezmsg.core",
            "serve",
            "--address=127.0.0.1:25978",
            "--dashboard",
            "28123",
        ]
    ]


@pytest.mark.asyncio
async def test_handle_start_warns_when_dashboard_dependency_missing(monkeypatch, caplog):
    monkeypatch.setattr(
        "ezmsg.core.commands.start.require_dashboard_dependency",
        lambda: (_ for _ in ()).throw(DashboardDependencyError(DASHBOARD_INSTALL_HINT)),
    )

    args = argparse.Namespace(address="127.0.0.1:25978", dashboard=True, log_file=None)

    with caplog.at_level("WARNING"):
        await handle_start(args)

    assert "pip install ezmsg-dashboard" in caplog.text


@pytest.mark.asyncio
async def test_handle_serve_warns_when_dashboard_dependency_missing(monkeypatch, caplog):
    class DummyGraphServer:
        def join(self):
            return None

        def stop(self):
            return None

    monkeypatch.setattr(
        "ezmsg.core.commands.serve.GraphService.create_server",
        lambda self: DummyGraphServer(),
    )
    monkeypatch.setattr(
        "ezmsg.core.commands.serve.start_dashboard",
        lambda graph_address, dashboard_port=None: (_ for _ in ()).throw(
            DashboardDependencyError(DASHBOARD_INSTALL_HINT)
        ),
    )
    monkeypatch.setenv("EZMSG_LOG_FILE", "/tmp/ezmsg-serve-test.log")

    args = argparse.Namespace(address="127.0.0.1:25978", dashboard=True, log_file=None)

    with caplog.at_level("WARNING"):
        await handle_serve(args)

    assert "pip install ezmsg-dashboard" in caplog.text


@pytest.mark.asyncio
async def test_handle_serve_closes_created_log_file_handler(monkeypatch, tmp_path):
    log_path = (tmp_path / "ezmsg-serve.log").resolve()
    handlers_during_join = []

    class DummyGraphServer:
        def join(self):
            handlers_during_join.extend(log_file_handlers_for(log_path))

        def stop(self):
            return None

    monkeypatch.setattr(
        "ezmsg.core.commands.serve.GraphService.create_server",
        lambda self: DummyGraphServer(),
    )

    args = argparse.Namespace(
        address="127.0.0.1:25978",
        dashboard=None,
        log_file=str(log_path),
    )

    await handle_serve(args)

    assert len(handlers_during_join) == 1
    assert handlers_during_join[0] not in logging.getLogger("ezmsg").handlers
    assert handlers_during_join[0].stream is None
    assert log_file_handlers_for(log_path) == []


@pytest.mark.asyncio
async def test_handle_serve_leaves_existing_log_file_handler(monkeypatch, tmp_path):
    log_path = (tmp_path / "ezmsg-serve.log").resolve()
    logger = logging.getLogger("ezmsg")
    existing_handler = logging.FileHandler(log_path, encoding="utf-8")
    handlers_during_join = []

    class DummyGraphServer:
        def join(self):
            handlers_during_join.extend(log_file_handlers_for(log_path))

        def stop(self):
            return None

    monkeypatch.setattr(
        "ezmsg.core.commands.serve.GraphService.create_server",
        lambda self: DummyGraphServer(),
    )

    logger.addHandler(existing_handler)
    try:
        args = argparse.Namespace(
            address="127.0.0.1:25978",
            dashboard=None,
            log_file=str(log_path),
        )

        await handle_serve(args)

        assert handlers_during_join == [existing_handler]
        assert existing_handler in logger.handlers
        assert existing_handler.stream is not None
    finally:
        if existing_handler in logger.handlers:
            logger.removeHandler(existing_handler)
        existing_handler.close()
