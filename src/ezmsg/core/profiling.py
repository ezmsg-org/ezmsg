import os
import socket
import time
import heapq

from collections import deque
from dataclasses import dataclass, field
from typing import Callable, TypeAlias
from uuid import UUID

from .graphmeta import (
    ProcessProfilingSnapshot,
    ProcessProfilingTraceBatch,
    ProfileChannelType,
    ProfilingTraceControl,
    ProfilingTraceSample,
    PublisherProfileSnapshot,
    SubscriberProfileSnapshot,
)


TRACE_MAX_SAMPLES = int(os.environ.get("EZMSG_PROFILE_TRACE_MAX_SAMPLES", "10000"))
# Must return monotonic nanoseconds so *_ns metrics remain unit-consistent.
PROFILE_TIME_TYPE: TypeAlias = Callable[[], int]
PROFILE_TIME: PROFILE_TIME_TYPE = time.perf_counter_ns


def _endpoint_id(topic: str, id: UUID) -> str:
    return f"{topic}:{id}"


@dataclass
class _PublisherMetrics:
    topic: str
    endpoint_id: str
    num_buffers: int
    messages_published_total: int = 0
    inflight_messages_current: int = 0
    _last_snapshot_total: int = 0
    _last_publish_ts_ns: int | None = None
    trace_enabled: bool = False
    trace_sample_mod: int = 1
    trace_metrics: set[str] | None = None
    _trace_counter: int = 0
    _trace_publish_delta_enabled: bool = False
    _trace_backpressure_wait_enabled: bool = False
    trace_samples: deque[ProfilingTraceSample] = field(
        default_factory=lambda: deque(maxlen=TRACE_MAX_SAMPLES)
    )

    def record_publish(self, inflight: int, msg_seq: int | None = None) -> None:
        self.messages_published_total += 1
        self.inflight_messages_current = inflight

        if not self._trace_publish_delta_enabled:
            return
        self._trace_counter += 1
        if self._trace_counter % max(1, self.trace_sample_mod) != 0:
            return

        now_ns = PROFILE_TIME()
        publish_delta_ns = (
            0 if self._last_publish_ts_ns is None else now_ns - self._last_publish_ts_ns
        )
        self._last_publish_ts_ns = now_ns
        self.trace_samples.append(
            ProfilingTraceSample(
                timestamp=float(now_ns),
                endpoint_id=self.endpoint_id,
                topic=self.topic,
                metric="publish_delta_ns",
                value=float(publish_delta_ns),
                sample_seq=msg_seq,
            )
        )

    def record_backpressure_wait(self, wait_ns: int, msg_seq: int | None = None) -> None:
        if not self._trace_backpressure_wait_enabled:
            return

        now_ns = PROFILE_TIME()
        self.trace_samples.append(
            ProfilingTraceSample(
                timestamp=float(now_ns),
                endpoint_id=self.endpoint_id,
                topic=self.topic,
                metric="backpressure_wait_ns",
                value=float(wait_ns),
                sample_seq=msg_seq,
            )
        )

    def sample_inflight(self, inflight: int) -> None:
        self.inflight_messages_current = inflight

    def snapshot(
        self,
        now_ns: int,
        window_seconds: float,
        *,
        has_previous_snapshot: bool,
    ) -> PublisherProfileSnapshot:
        window_count = (
            self.messages_published_total - self._last_snapshot_total
            if has_previous_snapshot
            else 0
        )
        self._last_snapshot_total = self.messages_published_total
        return PublisherProfileSnapshot(
            endpoint_id=self.endpoint_id,
            topic=self.topic,
            messages_published_total=self.messages_published_total,
            messages_published_window=window_count,
            publish_rate_hz_window=(
                float(window_count) / max(window_seconds, 1e-9)
                if has_previous_snapshot and window_seconds > 0.0
                else 0.0
            ),
            inflight_messages_current=self.inflight_messages_current,
            num_buffers=self.num_buffers,
            timestamp=float(now_ns),
        )


@dataclass
class _SubscriberMetrics:
    topic: str
    endpoint_id: str
    messages_received_total: int = 0
    channel_kind_last: ProfileChannelType = ProfileChannelType.UNKNOWN
    _last_snapshot_total: int = 0
    trace_enabled: bool = False
    trace_sample_mod: int = 1
    trace_metrics: set[str] | None = None
    _trace_counter: int = 0
    _trace_lease_time_enabled: bool = False
    _trace_user_span_enabled: bool = False
    trace_samples: deque[ProfilingTraceSample] = field(
        default_factory=lambda: deque(maxlen=TRACE_MAX_SAMPLES)
    )

    def begin_message(self, channel_kind: ProfileChannelType) -> bool:
        self.messages_received_total += 1
        self.channel_kind_last = channel_kind

        if not (self._trace_lease_time_enabled or self._trace_user_span_enabled):
            return False

        self._trace_counter += 1
        return self._trace_counter % max(1, self.trace_sample_mod) == 0

    def record_receive(
        self,
        channel_kind: ProfileChannelType,
        lease_ns: int | None = None,
        msg_seq: int | None = None,
    ) -> None:
        sampled = self.begin_message(channel_kind)
        self.record_lease_time(
            channel_kind,
            lease_ns,
            msg_seq=msg_seq,
            sampled=sampled,
        )

    def record_lease_time(
        self,
        channel_kind: ProfileChannelType,
        lease_ns: int | None,
        msg_seq: int | None = None,
        *,
        sampled: bool,
    ) -> None:
        if lease_ns is None or not self._trace_lease_time_enabled or not sampled:
            return

        now_ns = PROFILE_TIME()
        self.trace_samples.append(
            ProfilingTraceSample(
                timestamp=float(now_ns),
                endpoint_id=self.endpoint_id,
                topic=self.topic,
                metric="lease_time_ns",
                value=float(lease_ns),
                channel_kind=channel_kind,
                sample_seq=msg_seq,
            )
        )

    def record_user_span(
        self,
        span_ns: int,
        label: str | None,
        msg_seq: int | None = None,
        *,
        sampled: bool,
    ) -> None:
        if not self._trace_user_span_enabled or not sampled:
            return

        now_ns = PROFILE_TIME()
        self.trace_samples.append(
            ProfilingTraceSample(
                timestamp=float(now_ns),
                endpoint_id=self.endpoint_id,
                topic=self.topic if label is None else f"{self.topic}:{label}",
                metric="user_span_ns",
                value=float(span_ns),
                channel_kind=self.channel_kind_last,
                sample_seq=msg_seq,
            )
        )

    def snapshot(
        self,
        now_ns: int,
        *,
        has_previous_snapshot: bool,
    ) -> SubscriberProfileSnapshot:
        window_count = (
            self.messages_received_total - self._last_snapshot_total
            if has_previous_snapshot
            else 0
        )
        self._last_snapshot_total = self.messages_received_total
        return SubscriberProfileSnapshot(
            endpoint_id=self.endpoint_id,
            topic=self.topic,
            messages_received_total=self.messages_received_total,
            messages_received_window=window_count,
            channel_kind_last=self.channel_kind_last,
            timestamp=float(now_ns),
        )


class ProfileRegistry:
    def __init__(self) -> None:
        self._process_id = UUID(int=0)
        self._pid = os.getpid()
        self._host = socket.gethostname()
        self._publishers: dict[UUID, _PublisherMetrics] = {}
        self._subscribers: dict[UUID, _SubscriberMetrics] = {}
        self._default_trace_control = ProfilingTraceControl(enabled=False)
        self._trace_control_expires_ns: int | None = None
        self._last_snapshot_ts_ns: int | None = None

    def set_process_id(self, process_id: UUID, *, reset: bool = False) -> None:
        if reset:
            self._publishers.clear()
            self._subscribers.clear()
            self._default_trace_control = ProfilingTraceControl(enabled=False)
            self._trace_control_expires_ns = None
        self._process_id = process_id
        self._last_snapshot_ts_ns = None

    def register_publisher(self, pub_id: UUID, topic: str, num_buffers: int) -> _PublisherMetrics:
        metric = _PublisherMetrics(
            topic=topic,
            endpoint_id=_endpoint_id(topic, pub_id),
            num_buffers=max(1, int(num_buffers)),
        )
        self._publishers[pub_id] = metric
        self._apply_trace_control_to_publisher(metric)
        return metric

    def unregister_publisher(self, pub_id: UUID) -> None:
        self._publishers.pop(pub_id, None)

    def register_subscriber(self, sub_id: UUID, topic: str) -> _SubscriberMetrics:
        metric = _SubscriberMetrics(
            topic=topic,
            endpoint_id=_endpoint_id(topic, sub_id),
        )
        self._subscribers[sub_id] = metric
        self._apply_trace_control_to_subscriber(metric)
        return metric

    def unregister_subscriber(self, sub_id: UUID) -> None:
        self._subscribers.pop(sub_id, None)

    def snapshot(self) -> ProcessProfilingSnapshot:
        now_ns = PROFILE_TIME()
        last_snapshot_ts_ns = self._last_snapshot_ts_ns
        has_previous_snapshot = last_snapshot_ts_ns is not None
        window_seconds = (
            float(now_ns - last_snapshot_ts_ns) / 1e9
            if has_previous_snapshot
            else 0.0
        )
        self._last_snapshot_ts_ns = now_ns
        return ProcessProfilingSnapshot(
            process_id=self._process_id,
            pid=self._pid,
            host=self._host,
            window_seconds=window_seconds,
            timestamp=float(now_ns),
            publishers={
                metric.endpoint_id: metric.snapshot(
                    now_ns,
                    window_seconds,
                    has_previous_snapshot=has_previous_snapshot,
                )
                for metric in self._publishers.values()
            },
            subscribers={
                metric.endpoint_id: metric.snapshot(
                    now_ns,
                    has_previous_snapshot=has_previous_snapshot,
                )
                for metric in self._subscribers.values()
            },
        )

    def set_trace_control(self, control: ProfilingTraceControl) -> None:
        # Changing filters/mode should start from a clean trace buffer so new
        # consumers do not receive stale samples from an old control scope.
        self._clear_trace_samples()
        self._default_trace_control = control
        if control.enabled and control.ttl_seconds is not None:
            self._trace_control_expires_ns = PROFILE_TIME() + max(
                0, int(control.ttl_seconds * 1e9)
            )
        else:
            self._trace_control_expires_ns = None

        for metric in self._publishers.values():
            self._apply_trace_control_to_publisher(metric)

        for metric in self._subscribers.values():
            self._apply_trace_control_to_subscriber(metric)

    def trace_batch(self, max_samples: int = 1000) -> ProcessProfilingTraceBatch:
        self._expire_trace_control_if_needed()
        samples: list[ProfilingTraceSample] = []
        limit = max(1, int(max_samples))

        queues: list[deque[ProfilingTraceSample]] = []
        for metric in self._publishers.values():
            if metric.trace_samples:
                queues.append(metric.trace_samples)
        for metric in self._subscribers.values():
            if metric.trace_samples:
                queues.append(metric.trace_samples)

        if len(queues) == 1:
            queue = queues[0]
            while queue and len(samples) < limit:
                samples.append(queue.popleft())
        elif len(queues) > 1:
            heap: list[tuple[float, int, int]] = []
            for idx, queue in enumerate(queues):
                sample = queue[0]
                seq = sample.sample_seq if sample.sample_seq is not None else -1
                heapq.heappush(heap, (sample.timestamp, seq, idx))

            while heap and len(samples) < limit:
                _timestamp, _seq, queue_idx = heapq.heappop(heap)
                queue = queues[queue_idx]
                if not queue:
                    continue
                sample = queue.popleft()
                samples.append(sample)
                if queue:
                    nxt = queue[0]
                    nxt_seq = nxt.sample_seq if nxt.sample_seq is not None else -1
                    heapq.heappush(heap, (nxt.timestamp, nxt_seq, queue_idx))

        return ProcessProfilingTraceBatch(
            process_id=self._process_id,
            pid=self._pid,
            host=self._host,
            timestamp=float(PROFILE_TIME()),
            samples=samples,
        )

    def trace_enabled(self) -> bool:
        self._expire_trace_control_if_needed()
        return self._default_trace_control.enabled

    def _expire_trace_control_if_needed(self, now_ns: int | None = None) -> None:
        expires_ns = self._trace_control_expires_ns
        if expires_ns is None:
            return
        ts_ns = now_ns if now_ns is not None else PROFILE_TIME()
        if ts_ns < expires_ns:
            return
        self.set_trace_control(ProfilingTraceControl(enabled=False))

    def _apply_trace_control_to_publisher(self, metric: _PublisherMetrics) -> None:
        control = self._default_trace_control
        sample_mod = max(1, control.sample_mod)
        pub_topics = set(control.publisher_topics or [])
        pub_endpoint_ids = set(control.publisher_endpoint_ids or [])
        trace_metrics = (
            set(control.metrics) if control.metrics is not None else None
        )
        enabled = control.enabled
        if enabled and pub_topics and metric.topic not in pub_topics:
            enabled = False
        if enabled and pub_endpoint_ids and metric.endpoint_id not in pub_endpoint_ids:
            enabled = False
        metric.trace_enabled = enabled
        metric.trace_sample_mod = sample_mod
        metric.trace_metrics = trace_metrics
        metric._trace_counter = 0
        metric._last_publish_ts_ns = None
        metric._trace_publish_delta_enabled = enabled and (
            trace_metrics is None or "publish_delta_ns" in trace_metrics
        )
        metric._trace_backpressure_wait_enabled = enabled and (
            trace_metrics is None or "backpressure_wait_ns" in trace_metrics
        )

    def _apply_trace_control_to_subscriber(self, metric: _SubscriberMetrics) -> None:
        control = self._default_trace_control
        sample_mod = max(1, control.sample_mod)
        sub_topics = set(control.subscriber_topics or [])
        sub_endpoint_ids = set(control.subscriber_endpoint_ids or [])
        trace_metrics = (
            set(control.metrics) if control.metrics is not None else None
        )
        enabled = control.enabled
        if enabled and sub_topics and metric.topic not in sub_topics:
            enabled = False
        if enabled and sub_endpoint_ids and metric.endpoint_id not in sub_endpoint_ids:
            enabled = False
        metric.trace_enabled = enabled
        metric.trace_sample_mod = sample_mod
        metric.trace_metrics = trace_metrics
        metric._trace_counter = 0
        metric._trace_lease_time_enabled = enabled and (
            trace_metrics is None or "lease_time_ns" in trace_metrics
        )
        metric._trace_user_span_enabled = enabled and (
            trace_metrics is None or "user_span_ns" in trace_metrics
        )

    def _clear_trace_samples(self) -> None:
        for metric in self._publishers.values():
            metric.trace_samples.clear()
        for metric in self._subscribers.values():
            metric.trace_samples.clear()


PROFILES = ProfileRegistry()
