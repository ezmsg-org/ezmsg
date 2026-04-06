import pytest

from ezmsg.core.command import build_parser


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
            "hotpath",
            "--count",
            "10",
            "--samples",
            "2",
            "--quiet",
        ]
    )

    assert args.command == "perf"
    assert args.perf_command == "hotpath"
    assert args.count == 10
    assert args.samples == 2
    assert args.quiet is True


def test_graphviz_subparser_rejects_mermaid_only_args():
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["graphviz", "--nobrowser"])


def test_serve_subparser_rejects_visualization_args():
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["serve", "--target", "play"])


def test_perf_subparser_rejects_core_only_args():
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["perf", "hotpath", "--address", "127.0.0.1:4000"])
