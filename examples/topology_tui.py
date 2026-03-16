#!/usr/bin/env python3
"""
Simple live topology TUI for ezmsg GraphServer.

Features:
- Push-based topology event subscription
- Live graph summary (nodes/edges/sessions/processes)
- Process ownership view
- Current edge list
- Recent topology event log

Usage:
    PYTHONPATH=src .venv/bin/python examples/topology_tui.py --host 127.0.0.1 --port 25978
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import time
from collections import deque

from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.graphmeta import GraphSnapshot, TopologyChangedEvent
from ezmsg.core.netprotocol import DEFAULT_HOST, GRAPHSERVER_PORT_DEFAULT


def _truncate(text: str, width: int) -> str:
    if width <= 3:
        return text[:width]
    if len(text) <= width:
        return text
    return text[: width - 3] + "..."


def _fmt_age(age_s: float) -> str:
    return f"{age_s:0.2f}s"


def _flatten_edges(snapshot: GraphSnapshot) -> list[tuple[str, str]]:
    edges: list[tuple[str, str]] = []
    for src, destinations in snapshot.graph.items():
        for dst in destinations:
            edges.append((src, dst))
    edges.sort(key=lambda edge: (edge[0], edge[1]))
    return edges


class TopologyTUI:
    def __init__(
        self,
        ctx: GraphContext,
        *,
        snapshot_interval: float,
        render_interval: float,
        max_edges: int,
        max_events: int,
        max_processes: int,
    ) -> None:
        self.ctx = ctx
        self.snapshot_interval = max(0.2, snapshot_interval)
        self.render_interval = max(0.1, render_interval)
        self.max_edges = max(10, max_edges)
        self.max_events = max(10, max_events)
        self.max_processes = max(5, max_processes)

        self.snapshot: GraphSnapshot | None = None
        self.last_snapshot_time: float | None = None
        self._events: deque[TopologyChangedEvent] = deque(maxlen=self.max_events)
        self._event_queue: asyncio.Queue[TopologyChangedEvent] = asyncio.Queue()

        self._watch_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        await self._refresh_snapshot()
        self._watch_task = asyncio.create_task(self._watch_topology_events())

    async def close(self) -> None:
        if self._watch_task is not None:
            self._watch_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._watch_task

    async def _watch_topology_events(self) -> None:
        after_seq = 0
        async for event in self.ctx.subscribe_topology_events(after_seq=after_seq):
            after_seq = event.seq
            await self._event_queue.put(event)

    async def _refresh_snapshot(self) -> None:
        self.snapshot = await self.ctx.snapshot()
        self.last_snapshot_time = time.time()

    async def update(self) -> int:
        """
        Drain queued topology events and refresh snapshot if needed.

        Returns:
            Number of drained events.
        """
        drained = 0
        refresh_requested = False

        while True:
            try:
                event = self._event_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._events.append(event)
            refresh_requested = True
            drained += 1

        if self.last_snapshot_time is None:
            await self._refresh_snapshot()
        elif refresh_requested or (time.time() - self.last_snapshot_time) >= self.snapshot_interval:
            await self._refresh_snapshot()

        return drained

    def render(self, drained_events: int) -> None:
        print("\x1bc", end="")
        print("ezmsg topology tui")
        print("Ctrl-C to quit")
        print(
            f"snapshot_interval={self.snapshot_interval:.2f}s "
            f"render_interval={self.render_interval:.2f}s"
        )
        if self.last_snapshot_time is not None:
            print(f"snapshot_age={_fmt_age(max(0.0, time.time() - self.last_snapshot_time))}")
        if drained_events > 0:
            print(f"applied_events={drained_events}")

        snapshot = self.snapshot
        if snapshot is None:
            print("\n<no snapshot available yet>")
            return

        edges = _flatten_edges(snapshot)
        node_names = set(snapshot.graph.keys())
        for _, dst in edges:
            node_names.add(dst)

        print(
            "\nsummary: "
            f"nodes={len(node_names)} edges={len(edges)} "
            f"sessions={len(snapshot.sessions)} processes={len(snapshot.processes)}"
        )

        print("\nprocesses")
        proc_header = f"{'Process':<30} {'PID':>8} {'Host':<24} {'Units':<80}"
        print(proc_header)
        print("-" * len(proc_header))
        if not snapshot.processes:
            print("<none>")
        else:
            process_items = sorted(snapshot.processes.values(), key=lambda p: p.process_id)
            for proc in process_items[: self.max_processes]:
                units = ", ".join(proc.units) if proc.units else "-"
                print(
                    f"{_truncate(proc.process_id, 30):<30} "
                    f"{str(proc.pid) if proc.pid is not None else '-':>8} "
                    f"{_truncate(proc.host if proc.host is not None else '-', 24):<24} "
                    f"{_truncate(units, 80):<80}"
                )
            if len(process_items) > self.max_processes:
                print(f"... {len(process_items) - self.max_processes} more process rows")

        print("\nedges")
        edge_header = f"{'From':<48} {'To':<48}"
        print(edge_header)
        print("-" * len(edge_header))
        if not edges:
            print("<none>")
        else:
            for src, dst in edges[: self.max_edges]:
                print(f"{_truncate(src, 48):<48} {_truncate(dst, 48):<48}")
            if len(edges) > self.max_edges:
                print(f"... {len(edges) - self.max_edges} more edges")

        print("\nrecent topology events")
        event_header = (
            f"{'Seq':>6} {'Type':<15} {'Age':>8} {'Topics':<44} "
            f"{'Source Session':<38} {'Source Process':<30}"
        )
        print(event_header)
        print("-" * len(event_header))
        if not self._events:
            print("<none>")
        else:
            now = time.time()
            for event in reversed(self._events):
                topics = ", ".join(event.changed_topics) if event.changed_topics else "-"
                print(
                    f"{event.seq:>6} "
                    f"{_truncate(event.event_type.value, 15):<15} "
                    f"{_fmt_age(max(0.0, now - event.timestamp)):>8} "
                    f"{_truncate(topics, 44):<44} "
                    f"{_truncate(event.source_session_id or '-', 38):<38} "
                    f"{_truncate(event.source_process_id or '-', 30):<30}"
                )


def _parse_address(host: str, port: int) -> tuple[str, int]:
    return (host, port)


async def _run_tui(args: argparse.Namespace) -> None:
    async with GraphContext(
        _parse_address(args.host, args.port), auto_start=args.auto_start
    ) as ctx:
        tui = TopologyTUI(
            ctx,
            snapshot_interval=args.snapshot_interval,
            render_interval=args.render_interval,
            max_edges=args.max_edges,
            max_events=args.max_events,
            max_processes=args.max_processes,
        )
        await tui.start()
        try:
            while True:
                drained = await tui.update()
                tui.render(drained_events=drained)
                await asyncio.sleep(tui.render_interval)
        finally:
            await tui.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ezmsg topology TUI")
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
        help="Seconds between forced snapshot refreshes",
    )
    parser.add_argument(
        "--render-interval",
        type=float,
        default=0.5,
        help="Seconds between screen redraws",
    )
    parser.add_argument(
        "--max-edges",
        type=int,
        default=50,
        help="Max edge rows to render",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=25,
        help="Max recent topology events to retain/render",
    )
    parser.add_argument(
        "--max-processes",
        type=int,
        default=20,
        help="Max process rows to render",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run_tui(args))


if __name__ == "__main__":
    main()
