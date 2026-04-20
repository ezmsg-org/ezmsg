from __future__ import annotations

import argparse
import asyncio
import contextlib
import inspect
import json
import logging
import random
import statistics
import sys
import time

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal
from uuid import uuid4

import ezmsg.core as ez

from ezmsg.core.graphserver import GraphServer

from .util import coef_var, median_of_means, stable_perf

ApiName = Literal["async", "sync"]
TransportName = Literal["local", "shm", "tcp"]

DEFAULT_APIS: tuple[ApiName, ...] = ("async",)
DEFAULT_TRANSPORTS: tuple[TransportName, ...] = ("local", "shm", "tcp")
DEFAULT_PAYLOAD_SIZES = (64, 4096)


def _supports_same_process_transport_selection() -> bool:
    return "allow_local" in inspect.signature(ez.Publisher.create).parameters


def _validate_transport_support(case: "HotPathCase") -> None:
    if case.transport == "local":
        return
    if _supports_same_process_transport_selection():
        return
    raise RuntimeError(
        "This ref does not support bypassing the same-process local fast path, "
        f"so '{case.case_id}' cannot be benchmarked here. Compare against a ref "
        "that includes the allow_local transport-selection support, or limit the "
        "run to '--transports local'."
    )


def _publisher_transport_kwargs(case: "HotPathCase", host: str) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "host": host,
        "num_buffers": case.num_buffers,
        "force_tcp": (case.transport == "tcp"),
    }
    if _supports_same_process_transport_selection():
        kwargs["allow_local"] = case.transport == "local"
    return kwargs


@dataclass(frozen=True)
class HotPathCase:
    api: ApiName
    transport: TransportName
    payload_size: int
    num_buffers: int

    @property
    def case_id(self) -> str:
        return (
            f"{self.api}/{self.transport}/payload={self.payload_size}"
            f"/buffers={self.num_buffers}"
        )


@dataclass(frozen=True)
class HotPathSummary:
    samples: int
    seconds_median: float
    seconds_mean: float
    seconds_min: float
    seconds_max: float
    seconds_median_of_means: float
    seconds_cv: float
    us_per_message_median: float
    us_per_message_mean: float
    messages_per_second_median: float
    messages_per_second_mean: float


@dataclass(frozen=True)
class HotPathCaseResult:
    case: HotPathCase
    count: int
    warmup: int
    samples_seconds: list[float]
    summary: HotPathSummary


@dataclass(frozen=True)
class HotPathSuiteResult:
    seed: int
    count: int
    warmup: int
    results: list[HotPathCaseResult]


@contextlib.contextmanager
def _quiet_ezmsg_logs(enabled: bool):
    if not enabled:
        yield
        return

    old_level = ez.logger.level
    ez.logger.setLevel(logging.WARNING)
    try:
        yield
    finally:
        ez.logger.setLevel(old_level)


def build_cases(
    apis: list[str],
    transports: list[str],
    payload_sizes: list[int],
    num_buffers: int,
) -> list[HotPathCase]:
    cases = [
        HotPathCase(
            api=api,  # type: ignore[arg-type]
            transport=transport,  # type: ignore[arg-type]
            payload_size=payload_size,
            num_buffers=num_buffers,
        )
        for api in apis
        for transport in transports
        for payload_size in payload_sizes
        if api in ("async", "sync") and transport in ("local", "shm", "tcp")
    ]
    return sorted(cases, key=lambda case: case.case_id)


def summarize_samples(samples: list[float], count: int) -> HotPathSummary:
    us_per_message = [(sample / count) * 1e6 for sample in samples]
    rates = [count / sample for sample in samples]
    return HotPathSummary(
        samples=len(samples),
        seconds_median=statistics.median(samples),
        seconds_mean=statistics.fmean(samples),
        seconds_min=min(samples),
        seconds_max=max(samples),
        seconds_median_of_means=median_of_means(samples),
        seconds_cv=coef_var(samples),
        us_per_message_median=statistics.median(us_per_message),
        us_per_message_mean=statistics.fmean(us_per_message),
        messages_per_second_median=statistics.median(rates),
        messages_per_second_mean=statistics.fmean(rates),
    )


async def _async_roundtrip(
    case: HotPathCase,
    count: int,
    warmup: int,
    graph_address: tuple[str, int],
) -> float:
    _validate_transport_support(case)
    topic = f"/EZMSG/PERF/HOTPATH/{uuid4().hex}"
    payload = bytes(case.payload_size)

    async with ez.GraphContext(graph_address, auto_start=False) as ctx:
        pub = await ctx.publisher(topic, **_publisher_transport_kwargs(case, graph_address[0]))
        sub = await ctx.subscriber(topic)

        for _ in range(warmup):
            await pub.broadcast(payload)
            await sub.recv()

        start = time.perf_counter()
        for _ in range(count):
            await pub.broadcast(payload)
            await sub.recv()
        return time.perf_counter() - start


def _sync_roundtrip(
    case: HotPathCase,
    count: int,
    warmup: int,
    graph_address: tuple[str, int],
) -> float:
    _validate_transport_support(case)
    topic = f"/EZMSG/PERF/HOTPATH/{uuid4().hex}"
    payload = bytes(case.payload_size)

    with ez.sync.init(graph_address, auto_start=False) as ctx:
        pub = ctx.create_publisher(topic, **_publisher_transport_kwargs(case, graph_address[0]))
        sub = ctx.create_subscription(topic)

        for _ in range(warmup):
            pub.publish(payload)
            sub.recv()

        start = time.perf_counter()
        for _ in range(count):
            pub.publish(payload)
            sub.recv()
        return time.perf_counter() - start


def run_hotpath_case(
    case: HotPathCase,
    count: int,
    warmup: int,
    samples: int,
    graph_address: tuple[str, int],
) -> HotPathCaseResult:
    results: list[float] = []

    for _ in range(samples):
        with stable_perf():
            if case.api == "async":
                elapsed = asyncio.run(
                    _async_roundtrip(case, count, warmup, graph_address)
                )
            else:
                elapsed = _sync_roundtrip(case, count, warmup, graph_address)
        results.append(elapsed)

    return HotPathCaseResult(
        case=case,
        count=count,
        warmup=warmup,
        samples_seconds=results,
        summary=summarize_samples(results, count),
    )


def _format_case_result(result: HotPathCaseResult) -> str:
    summary = result.summary
    return (
        f"{result.case.case_id:<36} "
        f"{summary.us_per_message_median:>10.2f} us/msg "
        f"{summary.messages_per_second_median:>12,.0f} msg/s "
        f"cv={summary.seconds_cv:>5.3f}"
    )


def run_hotpath_suite(
    count: int,
    warmup: int,
    samples: int,
    apis: list[str],
    transports: list[str],
    payload_sizes: list[int],
    num_buffers: int,
    seed: int,
    quiet: bool,
) -> HotPathSuiteResult:
    rng = random.Random(seed)
    cases = build_cases(apis, transports, payload_sizes, num_buffers)
    rng.shuffle(cases)

    graph_server = GraphServer()
    graph_server.start(("127.0.0.1", 0))
    try:
        with _quiet_ezmsg_logs(quiet):
            results = [
                run_hotpath_case(
                    case,
                    count=count,
                    warmup=warmup,
                    samples=samples,
                    graph_address=graph_server.address,
                )
                for case in cases
            ]
    finally:
        graph_server.stop()

    results.sort(key=lambda result: result.case.case_id)
    return HotPathSuiteResult(seed=seed, count=count, warmup=warmup, results=results)


def dump_suite_json(result: HotPathSuiteResult, path: Path) -> None:
    payload = {
        "suite": "hotpath",
        "seed": result.seed,
        "count": result.count,
        "warmup": result.warmup,
        "results": [
            {
                "case": asdict(case_result.case),
                "case_id": case_result.case.case_id,
                "count": case_result.count,
                "warmup": case_result.warmup,
                "samples_seconds": case_result.samples_seconds,
                "summary": asdict(case_result.summary),
            }
            for case_result in result.results
        ],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def perf_hotpath(
    count: int,
    warmup: int,
    samples: int,
    apis: list[str],
    transports: list[str],
    payload_sizes: list[int],
    num_buffers: int,
    seed: int,
    json_out: Path | None,
    quiet: bool,
) -> None:
    result = run_hotpath_suite(
        count=count,
        warmup=warmup,
        samples=samples,
        apis=apis,
        transports=transports,
        payload_sizes=payload_sizes,
        num_buffers=num_buffers,
        seed=seed,
        quiet=quiet,
    )

    print("Hot-path roundtrip benchmark")
    print(
        f"count={count}, warmup={warmup}, samples={samples}, "
        f"payload_sizes={payload_sizes}, transports={transports}, apis={apis}"
    )
    for case_result in result.results:
        print(_format_case_result(case_result))

    if json_out is not None:
        dump_suite_json(result, json_out)
        print(f"Wrote JSON results to {json_out}")


def setup_hotpath_cmdline(subparsers: argparse._SubParsersAction) -> None:
    p_hotpath = subparsers.add_parser(
        "hotpath",
        help="run fast, focused hot-path roundtrip benchmarks",
    )
    p_hotpath.add_argument(
        "--count",
        type=int,
        default=5_000,
        help="messages per sample (default = 5000)",
    )
    p_hotpath.add_argument(
        "--warmup",
        type=int,
        default=500,
        help="warmup messages per sample (default = 500)",
    )
    p_hotpath.add_argument(
        "--samples",
        type=int,
        default=5,
        help="timed samples per case (default = 5)",
    )
    p_hotpath.add_argument(
        "--apis",
        nargs="*",
        choices=DEFAULT_APIS + ("sync",),
        default=list(DEFAULT_APIS),
        help=f"apis to benchmark (default = {list(DEFAULT_APIS)})",
    )
    p_hotpath.add_argument(
        "--transports",
        nargs="*",
        choices=DEFAULT_TRANSPORTS,
        default=list(DEFAULT_TRANSPORTS),
        help=f"transports to benchmark (default = {list(DEFAULT_TRANSPORTS)})",
    )
    p_hotpath.add_argument(
        "--payload-sizes",
        nargs="*",
        type=int,
        default=list(DEFAULT_PAYLOAD_SIZES),
        help=f"payload sizes in bytes (default = {list(DEFAULT_PAYLOAD_SIZES)})",
    )
    p_hotpath.add_argument(
        "--num-buffers",
        type=int,
        default=1,
        help="publisher buffers (default = 1)",
    )
    p_hotpath.add_argument(
        "--seed",
        type=int,
        default=0,
        help="shuffle seed for case order (default = 0)",
    )
    p_hotpath.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="optional JSON output path",
    )
    p_hotpath.add_argument(
        "--quiet",
        action="store_true",
        help="suppress ezmsg runtime logs during the benchmark",
    )
    p_hotpath.set_defaults(
        _handler=lambda ns: perf_hotpath(
            count=ns.count,
            warmup=ns.warmup,
            samples=ns.samples,
            apis=ns.apis,
            transports=ns.transports,
            payload_sizes=ns.payload_sizes,
            num_buffers=ns.num_buffers,
            seed=ns.seed,
            json_out=ns.json_out,
            quiet=ns.quiet,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run ezmsg hot-path roundtrip benchmarks."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    setup_hotpath_cmdline(subparsers)
    ns = parser.parse_args(["hotpath", *sys.argv[1:]])
    ns._handler(ns)


if __name__ == "__main__":
    main()
