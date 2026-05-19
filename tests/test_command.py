import pytest
import argparse
import asyncio
import sys
from pathlib import Path

from ezmsg.core.command import build_parser, cmdline
from ezmsg.core.commands.start import handle_start


def test_mermaid_subparser_accepts_mermaid_specific_args():
    parser = build_parser()

    args = parser.parse_args(
        [
            "mermaid",
            "--address",
            "127.0.0.1:4000",
            "--target",
            "ink",
            "-cc",
            "--nobrowser",
        ]
    )

    assert args.command == "mermaid"
    assert args.address == "127.0.0.1:4000"
    assert args.target == "ink"
    assert args.compact == 2
    assert args.nobrowser is True


def test_perf_subparser_accepts_nested_perf_args():
    parser = build_parser()

    args = parser.parse_args(
        [
            "perf",
            "benchmark",
            "--name",
            "smoke",
            "--num-msgs",
            "2",
            "--repeats",
            "1",
            "--no-browser",
        ]
    )

    assert args.command == "perf"
    assert args.perf_command == "benchmark"
    assert args.name == "smoke"
    assert args.num_msgs == 2
    assert args.repeats == 1
    assert args.no_browser is True


def test_perf_compare_subparser_accepts_baseline_args():
    parser = build_parser()

    args = parser.parse_args(
        [
            "perf",
            "compare",
            "candidate.txt",
            "--baseline",
            "baseline.txt",
            "--output",
            "diff.html",
            "--no-browser",
        ]
    )

    assert args.command == "perf"
    assert args.perf_command == "compare"
    assert str(args.perf) == "candidate.txt"
    assert str(args.baseline) == "baseline.txt"
    assert str(args.output) == "diff.html"
    assert args.no_browser is True


def test_perf_ab_subparser_accepts_manual_env_args():
    parser = build_parser()

    args = parser.parse_args(
        [
            "perf",
            "ab",
            "--dir-a",
            "/tmp/a",
            "--dir-b",
            "/tmp/b",
            "--env-mode",
            "existing",
            "--env",
            "FOO=bar",
            "--env-a",
            "ONLY_A=1",
            "--python-b",
            "/tmp/b/.venv/bin/python",
        ]
    )

    assert args.command == "perf"
    assert args.perf_command == "ab"
    assert args.dir_a == Path("/tmp/a")
    assert args.dir_b == Path("/tmp/b")
    assert args.env_mode == "existing"
    assert args.env == ["FOO=bar"]
    assert args.env_a == ["ONLY_A=1"]
    assert args.python_b == "/tmp/b/.venv/bin/python"


def test_graphviz_subparser_rejects_mermaid_only_args():
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["graphviz", "--nobrowser"])


def test_serve_subparser_rejects_visualization_args():
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["serve", "--target", "play"])


def test_serve_subparser_accepts_dashboard_flag():
    parser = build_parser()

    args = parser.parse_args(["serve", "--dashboard"])

    assert args.command == "serve"
    assert args.dashboard is True


def test_start_subparser_accepts_dashboard_flag():
    parser = build_parser()

    args = parser.parse_args(["start", "--dashboard"])

    assert args.command == "start"
    assert args.dashboard is True


def test_serve_subparser_accepts_log_file():
    parser = build_parser()

    args = parser.parse_args(["serve", "--log-file", "/tmp/ezmsg.log"])

    assert args.command == "serve"
    assert args.log_file == "/tmp/ezmsg.log"


def test_serve_subparser_accepts_dashboard_port():
    parser = build_parser()

    args = parser.parse_args(["serve", "--dashboard", "28000"])

    assert args.command == "serve"
    assert args.dashboard == 28000


def test_dashboard_subparser_accepts_dashboard_args():
    parser = build_parser()

    args = parser.parse_args(
        [
            "dashboard",
            "--graph-address",
            "127.0.0.1:4000",
            "--host",
            "0.0.0.0",
            "--port",
            "28000",
            "--open-browser",
            "--log-level",
            "debug",
        ]
    )

    assert args.command == "dashboard"
    assert args.graph_address == "127.0.0.1:4000"
    assert args.host == "0.0.0.0"
    assert args.port == 28000
    assert args.open_browser is True
    assert args.log_level == "debug"


def test_perf_subparser_rejects_core_only_args():
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["perf", "benchmark", "--address", "127.0.0.1:4000"])


def test_cmdline_suppresses_keyboard_interrupt_from_asyncio_run(monkeypatch):
    class DummyParser:
        def parse_args(self, args=None):
            return argparse.Namespace(_handler=lambda parsed_args: object())

    monkeypatch.setattr("ezmsg.core.command.build_parser", lambda: DummyParser())
    monkeypatch.setattr("ezmsg.core.command.inspect.isawaitable", lambda result: True)

    def raise_keyboard_interrupt(result):
        raise KeyboardInterrupt

    monkeypatch.setattr("ezmsg.core.command.asyncio.run", raise_keyboard_interrupt)

    cmdline([])


def test_dashboard_subcommand_warns_when_optional_dependency_missing(monkeypatch, caplog):
    real_import = __import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "ezmsg.dashboard.server":
            raise ImportError("missing optional dashboard package")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr("builtins.__import__", fake_import)
    monkeypatch.delitem(__import__("sys").modules, "ezmsg.dashboard.server", raising=False)
    monkeypatch.delitem(__import__("sys").modules, "ezmsg.core.commands.dashboard_cmd", raising=False)

    from ezmsg.core.commands.dashboard_cmd import setup_dashboard_cmdline

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    setup_dashboard_cmdline(subparsers)

    args = parser.parse_args(["dashboard"])

    with caplog.at_level("WARNING"):
        args._handler(args)

    assert "pip install ezmsg-dashboard" in caplog.text


def test_start_passes_log_file_to_serve(monkeypatch):
    commands = []

    class DummyPopen:
        pid = 123

        def __init__(self, cmd):
            commands.append(cmd)

    class DummyWriter:
        def close(self):
            pass

        async def wait_closed(self):
            pass

    class DummyGraphService:
        async def open_connection(self):
            return object(), DummyWriter()

    async def noop_close_stream_writer(writer):
        return None

    monkeypatch.setattr("ezmsg.core.commands.start.subprocess.Popen", DummyPopen)
    monkeypatch.setattr(
        "ezmsg.core.commands.start.GraphService", lambda address: DummyGraphService()
    )
    monkeypatch.setattr(
        "ezmsg.core.commands.start.close_stream_writer", noop_close_stream_writer
    )

    args = argparse.Namespace(
        address="127.0.0.1:25978",
        dashboard=None,
        log_file="/tmp/ezmsg.log",
    )

    asyncio.run(handle_start(args))

    assert commands == [
        [
            sys.executable,
            "-m",
            "ezmsg.core",
            "serve",
            "--address=127.0.0.1:25978",
            "--log-file=/tmp/ezmsg.log",
        ]
    ]
