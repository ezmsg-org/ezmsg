import time
from collections import defaultdict, deque
from typing import Any
from uuid import UUID

# Sliding window used for profiling summaries. Keep a small window to bound memory.
PROFILE_WINDOW_S = 10.0


class SlidingWindow:
    """
    Maintain a rolling window of timestamped values and provide simple summaries.
    """

    def __init__(self, max_window_s: float = PROFILE_WINDOW_S) -> None:
        self.max_window_s = max_window_s
        self._events: deque[tuple[float, float]] = deque()
        self._total = 0.0

    def add(self, value: float, now: float | None = None) -> None:
        now = time.perf_counter() if now is None else now
        self._events.append((now, value))
        self._total += value
        self._prune(now)

    def _prune(self, now: float, window_s: float | None = None) -> None:
        window = self.max_window_s if window_s is None else min(
            window_s, self.max_window_s
        )
        cutoff = now - window
        while self._events and self._events[0][0] < cutoff:
            _, val = self._events.popleft()
            self._total -= val

    def summary(
        self, window_s: float | None = None, now: float | None = None
    ) -> dict[str, float]:
        now = time.perf_counter() if now is None else now
        self._prune(now)

        if window_s is None or window_s > self.max_window_s:
            window_s = self.max_window_s

        cutoff = now - window_s
        total = 0.0
        count = 0
        max_val = 0.0
        for ts, val in self._events:
            if ts >= cutoff:
                total += val
                count += 1
                if val > max_val:
                    max_val = val

        avg = total / count if count else 0.0

        return {
            "count": count,
            "total": total,
            "avg": avg,
            "max": max_val,
            "window_s": window_s,
        }


class LeaseDurationTelemetry:
    """
    Track lease/free durations from Backpressure to estimate processing latency.
    """

    def __init__(self, max_window_s: float = PROFILE_WINDOW_S, per_client: bool = False):
        self.max_window_s = max_window_s
        self.per_client = per_client
        self._starts: dict[tuple[UUID, int], float] = {}

        if per_client:
            self._windows: dict[UUID, SlidingWindow] = defaultdict(
                lambda: SlidingWindow(max_window_s)
            )
        else:
            self._window = SlidingWindow(max_window_s)

    def on_lease(self, client_id: UUID, buf_idx: int, now: float | None = None) -> None:
        self._starts[(client_id, buf_idx)] = time.perf_counter() if now is None else now

    def on_free(
        self, client_id: UUID, buf_idx: int | None, now: float | None = None
    ) -> None:
        now = time.perf_counter() if now is None else now

        if buf_idx is None:
            # Client disconnected; drop any inflight timers.
            for key in [k for k in self._starts if k[0] == client_id]:
                self._starts.pop(key, None)
            return

        start = self._starts.pop((client_id, buf_idx), None)
        if start is None:
            return

        duration = now - start
        if self.per_client:
            self._windows[client_id].add(duration, now)
        else:
            self._window.add(duration, now)

    def summary(
        self, window_s: float | None = None, now: float | None = None
    ) -> dict[Any, dict[str, float]]:
        now = time.perf_counter() if now is None else now
        if self.per_client:
            return {
                client_id: window.summary(window_s=window_s, now=now)
                for client_id, window in self._windows.items()
            }
        else:
            return {None: self._window.summary(window_s=window_s, now=now)}

    def in_flight(self, client_id: UUID | None = None) -> int:
        if client_id is None:
            return len(self._starts)
        return sum(1 for cid, _ in self._starts if cid == client_id)


class PublisherTelemetry:
    """
    Record publish throughput and backpressure latency for a Publisher.
    """

    def __init__(self, max_window_s: float = PROFILE_WINDOW_S) -> None:
        self.max_window_s = max_window_s
        self.msg_window = SlidingWindow(max_window_s)
        self.byte_window = SlidingWindow(max_window_s)
        self.backpressure = LeaseDurationTelemetry(max_window_s)
        self.total_messages = 0
        self.total_bytes = 0

    def record_message(self, num_bytes: int | None, now: float | None = None) -> None:
        now = time.perf_counter() if now is None else now
        self.msg_window.add(1.0, now)
        self.total_messages += 1

        if num_bytes is not None:
            self.byte_window.add(float(num_bytes), now)
            self.total_bytes += num_bytes

    def snapshot(self, window_s: float | None = None) -> dict[str, Any]:
        now = time.perf_counter()
        msg_stats = self.msg_window.summary(window_s=window_s, now=now)
        byte_stats = self.byte_window.summary(window_s=window_s, now=now)
        bp_stats = list(self.backpressure.summary(window_s=window_s, now=now).values())[
            0
        ]

        window = msg_stats["window_s"]
        message_rate = (
            msg_stats["count"] / window if window > 0 else 0.0
        )
        byte_rate = (
            byte_stats["total"] / window if window > 0 else 0.0
        )

        return {
            "window_s": window,
            "message_rate_hz": message_rate,
            "byte_rate_per_s": byte_rate,
            "messages": {
                "total": self.total_messages,
                "window": msg_stats["count"],
            },
            "bytes": {
                "total": self.total_bytes,
                "window": int(byte_stats["total"]),
            },
            "backpressure": {
                "avg_ms": bp_stats["avg"] * 1000.0 if bp_stats["count"] else 0.0,
                "max_ms": bp_stats["max"] * 1000.0 if bp_stats["count"] else 0.0,
                "samples": bp_stats["count"],
                "in_flight": self.backpressure.in_flight(),
            },
        }


class ChannelTelemetry:
    """
    Track per-subscriber processing time on a Channel.
    """

    def __init__(self, max_window_s: float = PROFILE_WINDOW_S) -> None:
        self.max_window_s = max_window_s
        self.leases = LeaseDurationTelemetry(max_window_s, per_client=True)

    def snapshot(
        self,
        window_s: float | None = None,
        handles: dict[UUID, str] | None = None,
    ) -> dict[str, Any]:
        now = time.perf_counter()
        lease_stats = self.leases.summary(window_s=window_s, now=now)

        subscribers: list[dict[str, Any]] = []
        for client_id, stats in lease_stats.items():
            window = stats["window_s"]
            subscribers.append(
                {
                    "subscriber_id": str(client_id),
                    "handle": handles.get(client_id, None) if handles else None,
                    "avg_ms": stats["avg"] * 1000.0 if stats["count"] else 0.0,
                    "max_ms": stats["max"] * 1000.0 if stats["count"] else 0.0,
                    "samples": stats["count"],
                    "message_rate_hz": stats["count"] / window if window > 0 else 0.0,
                    "in_flight": self.leases.in_flight(client_id),
                }
            )

        return {
            "window_s": window_s if window_s is not None else self.max_window_s,
            "subscribers": subscribers,
            "in_flight": self.leases.in_flight(),
        }
