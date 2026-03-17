#!/usr/bin/env python3
"""
Simple live profiling TUI for ezmsg GraphServer.

Features:
- Periodic profiling snapshot view broken out by publisher/subscriber endpoints
- Live trace sample counts via GraphContext.subscribe_profiling_trace()
- Optional automatic trace enablement for discovered processes

Usage:
    .venv/bin/python examples/profiling_tui.py --host 127.0.0.1 --port 25978
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import time
from dataclasses import dataclass
from uuid import UUID

from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.graphmeta import (
    ProcessProfilingSnapshot,
    ProfilingStreamControl,
    ProfilingTraceControl,
)
from ezmsg.core.netprotocol import DEFAULT_HOST, GRAPHSERVER_PORT_DEFAULT


def _truncate(text: object, width: int) -> str:
    text = str(text)
    if width <= 3:
        return text[:width]
    if len(text) <= width:
        return text
    return text[: width - 3] + "..."


def _fmt_float(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


@dataclass
class PublisherView:
    process_id: UUID
    topic: str
    endpoint_id: str
    published_total: int
    published_window: int
    publish_rate_hz: float
    publish_delta_ms_avg: float
    inflight_current: int
    inflight_peak: int
    trace_samples_seen: int
    trace_last_age_s: float | None
    backpressure_wait_ms_window: float


@dataclass
class SubscriberView:
    process_id: UUID
    topic: str
    endpoint_id: str
    channel_kind: str
    received_total: int
    received_window: int
    lease_time_ms_avg: float
    user_span_ms_avg: float
    attributable_backpressure_ms_window: float
    attributable_backpressure_events_total: int
    trace_samples_seen: int
    trace_last_age_s: float | None


class ProfilingTUI:
    def __init__(
        self,
        ctx: GraphContext,
        *,
        snapshot_interval: float,
        trace_interval: float,
        trace_max_samples: int,
        auto_trace: bool,
        trace_sample_mod: int,
        max_rows: int,
    ) -> None:
        self.ctx = ctx
        self.snapshot_interval = max(0.2, snapshot_interval)
        self.trace_interval = max(0.01, trace_interval)
        self.trace_max_samples = max(1, trace_max_samples)
        self.auto_trace = auto_trace
        self.trace_sample_mod = max(1, trace_sample_mod)
        self.max_rows = max(5, max_rows)

        self.snapshots: dict[UUID, ProcessProfilingSnapshot] = {}
        self.route_units: dict[UUID, str] = {}
        self.trace_enabled_processes: set[UUID] = set()
        self.trace_errors: dict[UUID, str] = {}
        self.trace_samples_seen_by_endpoint: dict[str, int] = {}
        self.trace_last_timestamp_by_endpoint: dict[str, float] = {}
        self.last_snapshot_time: float | None = None

        self._snapshot_task: asyncio.Task[None] | None = None
        self._trace_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        await self._refresh_snapshot()
        self._snapshot_task = asyncio.create_task(self._snapshot_loop())
        self._trace_task = asyncio.create_task(self._trace_loop())

    async def close(self) -> None:
        for task in (self._snapshot_task, self._trace_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        if not self.auto_trace:
            return

        for process_id, route_unit in self.route_units.items():
            if process_id not in self.trace_enabled_processes:
                continue
            with contextlib.suppress(Exception):
                await self.ctx.process_set_profiling_trace(
                    route_unit,
                    ProfilingTraceControl(enabled=False),
                    timeout=0.5,
                )

    async def _snapshot_loop(self) -> None:
        while True:
            await self._refresh_snapshot()
            await asyncio.sleep(self.snapshot_interval)

    async def _refresh_snapshot(self) -> None:
        graph_snapshot = await self.ctx.snapshot()
        route_units: dict[UUID, str] = {}
        for process in graph_snapshot.processes.values():
            if process.units:
                route_units[process.process_id] = process.units[0]
        self.route_units = route_units

        if self.auto_trace:
            for process_id, route_unit in route_units.items():
                if process_id in self.trace_enabled_processes:
                    continue
                try:
                    response = await self.ctx.process_set_profiling_trace(
                        route_unit,
                        ProfilingTraceControl(
                            enabled=True,
                            sample_mod=self.trace_sample_mod,
                        ),
                        timeout=0.5,
                    )
                    if response.ok:
                        self.trace_enabled_processes.add(process_id)
                        self.trace_errors.pop(process_id, None)
                    else:
                        self.trace_errors[process_id] = str(
                            response.error or "unknown error"
                        )
                except Exception as exc:
                    self.trace_errors[process_id] = str(exc)

        self.snapshots = await self.ctx.profiling_snapshot_all(
            timeout_per_process=max(0.1, self.snapshot_interval * 0.8)
        )
        self.last_snapshot_time = time.time()

    async def _trace_loop(self) -> None:
        async for batch in self.ctx.subscribe_profiling_trace(
            ProfilingStreamControl(
                interval=self.trace_interval,
                max_samples=self.trace_max_samples,
            )
        ):
            for process_batch in batch.batches.values():
                for sample in process_batch.samples:
                    endpoint_id = sample.endpoint_id
                    self.trace_samples_seen_by_endpoint[endpoint_id] = (
                        self.trace_samples_seen_by_endpoint.get(endpoint_id, 0) + 1
                    )
                    self.trace_last_timestamp_by_endpoint[endpoint_id] = batch.timestamp

    def _trace_for_endpoint(self, endpoint_id: str) -> tuple[int, float | None]:
        now = time.time()
        count = self.trace_samples_seen_by_endpoint.get(endpoint_id, 0)
        ts = self.trace_last_timestamp_by_endpoint.get(endpoint_id)
        age = None if ts is None else max(0.0, now - ts)
        return count, age

    def _publisher_rows(self) -> list[PublisherView]:
        rows: list[PublisherView] = []
        for process_id, snapshot in self.snapshots.items():
            for pub in snapshot.publishers.values():
                trace_count, trace_age = self._trace_for_endpoint(pub.endpoint_id)
                rows.append(
                    PublisherView(
                        process_id=process_id,
                        topic=pub.topic,
                        endpoint_id=pub.endpoint_id,
                        published_total=pub.messages_published_total,
                        published_window=pub.messages_published_window,
                        publish_rate_hz=pub.publish_rate_hz_window,
                        publish_delta_ms_avg=pub.publish_delta_ns_avg_window / 1_000_000.0,
                        inflight_current=pub.inflight_messages_current,
                        inflight_peak=pub.inflight_messages_peak_window,
                        trace_samples_seen=trace_count,
                        trace_last_age_s=trace_age,
                        backpressure_wait_ms_window=(
                            pub.backpressure_wait_ns_window / 1_000_000.0
                        ),
                    )
                )
        rows.sort(
            key=lambda row: (
                -row.publish_rate_hz,
                -row.published_total,
                row.process_id,
                row.topic,
            )
        )
        return rows

    def _subscriber_rows(self) -> list[SubscriberView]:
        rows: list[SubscriberView] = []
        for process_id, snapshot in self.snapshots.items():
            for sub in snapshot.subscribers.values():
                trace_count, trace_age = self._trace_for_endpoint(sub.endpoint_id)
                channel_kind = (
                    sub.channel_kind_last.value
                    if hasattr(sub.channel_kind_last, "value")
                    else str(sub.channel_kind_last)
                )
                rows.append(
                    SubscriberView(
                        process_id=process_id,
                        topic=sub.topic,
                        endpoint_id=sub.endpoint_id,
                        channel_kind=channel_kind,
                        received_total=sub.messages_received_total,
                        received_window=sub.messages_received_window,
                        lease_time_ms_avg=sub.lease_time_ns_avg_window / 1_000_000.0,
                        user_span_ms_avg=sub.user_span_ns_avg_window / 1_000_000.0,
                        attributable_backpressure_ms_window=(
                            sub.attributable_backpressure_ns_window / 1_000_000.0
                        ),
                        attributable_backpressure_events_total=(
                            sub.attributable_backpressure_events_total
                        ),
                        trace_samples_seen=trace_count,
                        trace_last_age_s=trace_age,
                    )
                )
        rows.sort(
            key=lambda row: (
                -row.lease_time_ms_avg,
                -row.received_total,
                row.process_id,
                row.topic,
            )
        )
        return rows

    def render(self) -> None:
        print("\x1bc", end="")
        print("ezmsg profiling tui")
        print("Ctrl-C to quit")
        print(
            "snapshot interval="
            f"{self.snapshot_interval:.2f}s, trace interval={self.trace_interval:.2f}s, "
            f"trace max_samples={self.trace_max_samples}, auto_trace={self.auto_trace}"
        )
        if self.last_snapshot_time is not None:
            print(
                "last snapshot age: "
                f"{_fmt_float(max(0.0, time.time() - self.last_snapshot_time), 2)}s"
            )
        print(
            f"processes discovered={len(self.route_units)} "
            f"publishers={sum(len(s.publishers) for s in self.snapshots.values())} "
            f"subscribers={sum(len(s.subscribers) for s in self.snapshots.values())}"
        )

        publisher_rows = self._publisher_rows()
        subscriber_rows = self._subscriber_rows()

        print("\nPublishers")
        pub_header = (
            f"{'Process':<20} {'Topic':<26} {'Endpoint':<24} "
            f"{'Total':>8} {'Win':>6} {'RateHz':>8} {'DeltaMs':>8} "
            f"{'InFl':>5} {'InPk':>5} {'BPmsW':>8} {'Trace':>7} {'TAge':>6}"
        )
        print(pub_header)
        print("-" * len(pub_header))
        if not publisher_rows:
            print("<none>")
        else:
            for row in publisher_rows[: self.max_rows]:
                trace_age = (
                    "-" if row.trace_last_age_s is None else _fmt_float(row.trace_last_age_s, 2)
                )
                print(
                    f"{_truncate(row.process_id, 20):<20} "
                    f"{_truncate(row.topic, 26):<26} "
                    f"{_truncate(row.endpoint_id, 24):<24} "
                    f"{row.published_total:>8} "
                    f"{row.published_window:>6} "
                    f"{_fmt_float(row.publish_rate_hz, 2):>8} "
                    f"{_fmt_float(row.publish_delta_ms_avg, 2):>8} "
                    f"{row.inflight_current:>5} "
                    f"{row.inflight_peak:>5} "
                    f"{_fmt_float(row.backpressure_wait_ms_window, 2):>8} "
                    f"{row.trace_samples_seen:>7} "
                    f"{trace_age:>6}"
                )

        print("\nSubscribers")
        sub_header = (
            f"{'Process':<20} {'Topic':<26} {'Endpoint':<24} {'Kind':<6} "
            f"{'Total':>8} {'Win':>6} {'LeaseMs':>8} {'UserMs':>8} "
            f"{'BPmsW':>8} {'BPev':>6} {'Trace':>7} {'TAge':>6}"
        )
        print(sub_header)
        print("-" * len(sub_header))
        if not subscriber_rows:
            print("<none>")
        else:
            for row in subscriber_rows[: self.max_rows]:
                trace_age = (
                    "-" if row.trace_last_age_s is None else _fmt_float(row.trace_last_age_s, 2)
                )
                print(
                    f"{_truncate(row.process_id, 20):<20} "
                    f"{_truncate(row.topic, 26):<26} "
                    f"{_truncate(row.endpoint_id, 24):<24} "
                    f"{_truncate(row.channel_kind, 6):<6} "
                    f"{row.received_total:>8} "
                    f"{row.received_window:>6} "
                    f"{_fmt_float(row.lease_time_ms_avg, 2):>8} "
                    f"{_fmt_float(row.user_span_ms_avg, 2):>8} "
                    f"{_fmt_float(row.attributable_backpressure_ms_window, 2):>8} "
                    f"{row.attributable_backpressure_events_total:>6} "
                    f"{row.trace_samples_seen:>7} "
                    f"{trace_age:>6}"
                )

        if self.trace_errors:
            print("\ntrace errors:")
            for process_id, err in sorted(self.trace_errors.items(), key=lambda item: str(item[0])):
                print(f"  {_truncate(str(process_id), 30)}: {_truncate(err, 120)}")


def _parse_address(host: str, port: int) -> tuple[str, int]:
    return (host, port)


async def _run_tui(args: argparse.Namespace) -> None:
    async with GraphContext(
        _parse_address(args.host, args.port), auto_start=args.auto_start
    ) as ctx:
        tui = ProfilingTUI(
            ctx,
            snapshot_interval=args.snapshot_interval,
            trace_interval=args.trace_interval,
            trace_max_samples=args.max_samples,
            auto_trace=args.auto_trace,
            trace_sample_mod=args.sample_mod,
            max_rows=args.max_rows,
        )
        await tui.start()
        try:
            while True:
                tui.render()
                await asyncio.sleep(max(0.1, args.render_interval))
        finally:
            await tui.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ezmsg profiling TUI")
    parser.add_argument("--host", default=DEFAULT_HOST, help="GraphServer host")
    parser.add_argument(
        "--port",
        type=int,
        default=GRAPHSERVER_PORT_DEFAULT,
        help="GraphServer port",
    )
    parser.add_argument(
        "--auto-start",
        action="store_true",
        help="Allow GraphContext to auto-start GraphServer if unavailable",
    )
    parser.add_argument(
        "--snapshot-interval",
        type=float,
        default=1.0,
        help="Seconds between snapshot refreshes",
    )
    parser.add_argument(
        "--trace-interval",
        type=float,
        default=0.05,
        help="Seconds between GraphServer trace stream batches",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=512,
        help="Max samples per process per streamed batch",
    )
    parser.add_argument(
        "--sample-mod",
        type=int,
        default=1,
        help="Trace sampling divisor when auto-enabling trace",
    )
    parser.add_argument(
        "--render-interval",
        type=float,
        default=0.5,
        help="Seconds between TUI redraws",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=30,
        help="Max publisher/subscriber rows to render per table",
    )
    parser.add_argument(
        "--no-auto-trace",
        action="store_true",
        help="Do not auto-enable trace mode on discovered processes",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.auto_trace = not args.no_auto_trace
    asyncio.run(_run_tui(args))


if __name__ == "__main__":
    main()
