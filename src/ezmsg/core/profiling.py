import os
import socket
import time
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


WINDOW_SECONDS = float(os.environ.get("EZMSG_PROFILE_WINDOW_SECONDS", "10.0"))
BUCKET_SECONDS = float(os.environ.get("EZMSG_PROFILE_BUCKET_SECONDS", "0.1"))
TRACE_MAX_SAMPLES = int(os.environ.get("EZMSG_PROFILE_TRACE_MAX_SAMPLES", "10000"))
# Must return monotonic nanoseconds so *_ns metrics remain unit-consistent.
PROFILE_TIME_TYPE: TypeAlias = Callable[[], int]
PROFILE_TIME: PROFILE_TIME_TYPE = time.perf_counter_ns


def _endpoint_id(topic: str, id: UUID) -> str:
    return f"{topic}:{id}"


@dataclass
class _Rolling:
    window_seconds: float = WINDOW_SECONDS
    bucket_seconds: float = BUCKET_SECONDS
    count: list[int] = field(default_factory=list)
    value_sum: list[int] = field(default_factory=list)
    max_value: list[int] = field(default_factory=list)
    _num_buckets: int = 0
    _bucket_ns: int = 0
    _last_bucket: int | None = None

    def __post_init__(self) -> None:
        self._num_buckets = max(1, int(self.window_seconds / self.bucket_seconds))
        self._bucket_ns = max(1, int(self.bucket_seconds * 1e9))
        self.count = [0 for _ in range(self._num_buckets)]
        self.value_sum = [0 for _ in range(self._num_buckets)]
        self.max_value = [0 for _ in range(self._num_buckets)]

    def _bucket(self, ts_ns: int) -> int:
        return (ts_ns // self._bucket_ns) % self._num_buckets

    def _advance(self, ts_ns: int) -> int:
        bucket = self._bucket(ts_ns)
        if self._last_bucket is None:
            self._last_bucket = bucket
            return bucket
        if bucket == self._last_bucket:
            return bucket
        idx = (self._last_bucket + 1) % self._num_buckets
        while idx != bucket:
            self.count[idx] = 0
            self.value_sum[idx] = 0
            self.max_value[idx] = 0
            idx = (idx + 1) % self._num_buckets
        self.count[bucket] = 0
        self.value_sum[bucket] = 0
        self.max_value[bucket] = 0
        self._last_bucket = bucket
        return bucket

    def add(self, ts_ns: int, value: int) -> None:
        idx = self._advance(ts_ns)
        self.count[idx] += 1
        self.value_sum[idx] += value
        if value > self.max_value[idx]:
            self.max_value[idx] = value

    def count_total(self) -> int:
        return sum(self.count)

    def sum_total(self) -> int:
        return sum(self.value_sum)

    def max_total(self) -> int:
        return max(self.max_value) if self.max_value else 0

    def avg(self) -> float:
        c = self.count_total()
        if c == 0:
            return 0.0
        return float(self.sum_total()) / float(c)


@dataclass
class _PublisherMetrics:
    topic: str
    endpoint_id: str
    messages_published_total: int = 0
    backpressure_wait_ns_total: int = 0
    inflight_messages_current: int = 0
    _last_publish_ts_ns: int | None = None
    _publish_delta: _Rolling = field(default_factory=_Rolling)
    _publish_count: _Rolling = field(default_factory=lambda: _Rolling())
    _backpressure_wait: _Rolling = field(default_factory=_Rolling)
    _inflight: _Rolling = field(default_factory=_Rolling)
    trace_enabled: bool = False
    trace_sample_mod: int = 1
    _trace_counter: int = 0
    trace_samples: deque[ProfilingTraceSample] = field(
        default_factory=lambda: deque(maxlen=TRACE_MAX_SAMPLES)
    )

    def record_publish(self, ts_ns: int, inflight: int) -> None:
        self.messages_published_total += 1
        self._publish_count.add(ts_ns, 1)
        publish_delta_ns = 0
        if self._last_publish_ts_ns is not None:
            publish_delta_ns = ts_ns - self._last_publish_ts_ns
            self._publish_delta.add(ts_ns, publish_delta_ns)
        self._last_publish_ts_ns = ts_ns
        self.sample_inflight(ts_ns, inflight)
        self._trace_counter += 1
        if self.trace_enabled and (self._trace_counter % max(1, self.trace_sample_mod) == 0):
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(PROFILE_TIME()),
                    endpoint_id=self.endpoint_id,
                    topic=self.topic,
                    metric="publish_delta_ns",
                    value=float(publish_delta_ns),
                )
            )

    def record_backpressure_wait(self, ts_ns: int, wait_ns: int) -> None:
        self.backpressure_wait_ns_total += wait_ns
        self._backpressure_wait.add(ts_ns, wait_ns)
        if self.trace_enabled:
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(PROFILE_TIME()),
                    endpoint_id=self.endpoint_id,
                    topic=self.topic,
                    metric="backpressure_wait_ns",
                    value=float(wait_ns),
                )
            )

    def sample_inflight(self, ts_ns: int, inflight: int) -> None:
        self.inflight_messages_current = inflight
        self._inflight.add(ts_ns, inflight)

    def snapshot(self) -> PublisherProfileSnapshot:
        window_msgs = self._publish_count.count_total()
        return PublisherProfileSnapshot(
            endpoint_id=self.endpoint_id,
            topic=self.topic,
            messages_published_total=self.messages_published_total,
            messages_published_window=window_msgs,
            publish_delta_ns_avg_window=self._publish_delta.avg(),
            publish_rate_hz_window=float(window_msgs) / max(WINDOW_SECONDS, 1e-9),
            inflight_messages_current=self.inflight_messages_current,
            inflight_messages_peak_window=self._inflight.max_total(),
            backpressure_wait_ns_total=self.backpressure_wait_ns_total,
            backpressure_wait_ns_window=self._backpressure_wait.sum_total(),
            timestamp=float(PROFILE_TIME()),
        )


@dataclass
class _SubscriberMetrics:
    topic: str
    endpoint_id: str
    messages_received_total: int = 0
    lease_time_ns_total: int = 0
    user_span_ns_total: int = 0
    attributable_backpressure_ns_total: int = 0
    attributable_backpressure_events_total: int = 0
    channel_kind_last: ProfileChannelType = ProfileChannelType.UNKNOWN
    _recv_count: _Rolling = field(default_factory=lambda: _Rolling())
    _lease_time: _Rolling = field(default_factory=_Rolling)
    _user_span: _Rolling = field(default_factory=_Rolling)
    _attrib_bp: _Rolling = field(default_factory=_Rolling)
    trace_enabled: bool = False
    trace_sample_mod: int = 1
    _trace_counter: int = 0
    trace_samples: deque[ProfilingTraceSample] = field(
        default_factory=lambda: deque(maxlen=TRACE_MAX_SAMPLES)
    )

    def record_receive(self, ts_ns: int, lease_ns: int, channel_kind: ProfileChannelType) -> None:
        self.messages_received_total += 1
        self.lease_time_ns_total += lease_ns
        self.channel_kind_last = channel_kind
        self._recv_count.add(ts_ns, 1)
        self._lease_time.add(ts_ns, lease_ns)
        self._trace_counter += 1
        if self.trace_enabled and (self._trace_counter % max(1, self.trace_sample_mod) == 0):
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(PROFILE_TIME()),
                    endpoint_id=self.endpoint_id,
                    topic=self.topic,
                    metric="lease_time_ns",
                    value=float(lease_ns),
                    channel_kind=channel_kind,
                )
            )

    def record_user_span(self, ts_ns: int, span_ns: int, label: str | None) -> None:
        self.user_span_ns_total += span_ns
        self._user_span.add(ts_ns, span_ns)
        if self.trace_enabled:
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(PROFILE_TIME()),
                    endpoint_id=self.endpoint_id,
                    topic=self.topic if label is None else f"{self.topic}:{label}",
                    metric="user_span_ns",
                    value=float(span_ns),
                    channel_kind=self.channel_kind_last,
                )
            )

    def record_attributed_backpressure(
        self, ts_ns: int, duration_ns: int, channel_kind: ProfileChannelType
    ) -> None:
        self.attributable_backpressure_ns_total += duration_ns
        self.attributable_backpressure_events_total += 1
        self.channel_kind_last = channel_kind
        self._attrib_bp.add(ts_ns, duration_ns)

    def snapshot(self) -> SubscriberProfileSnapshot:
        recv_count = self._recv_count.count_total()
        user_count = self._user_span.count_total()
        return SubscriberProfileSnapshot(
            endpoint_id=self.endpoint_id,
            topic=self.topic,
            messages_received_total=self.messages_received_total,
            messages_received_window=recv_count,
            lease_time_ns_total=self.lease_time_ns_total,
            lease_time_ns_avg_window=self._lease_time.avg(),
            user_span_ns_total=self.user_span_ns_total,
            user_span_ns_avg_window=(
                float(self._user_span.sum_total()) / float(user_count)
                if user_count > 0
                else 0.0
            ),
            attributable_backpressure_ns_total=self.attributable_backpressure_ns_total,
            attributable_backpressure_ns_window=self._attrib_bp.sum_total(),
            attributable_backpressure_events_total=self.attributable_backpressure_events_total,
            channel_kind_last=self.channel_kind_last,
            timestamp=float(PROFILE_TIME()),
        )


class ProfileRegistry:
    def __init__(self) -> None:
        self._process_id = ""
        self._pid = os.getpid()
        self._host = socket.gethostname()
        self._publishers: dict[UUID, _PublisherMetrics] = {}
        self._subscribers: dict[UUID, _SubscriberMetrics] = {}
        self._default_trace_control = ProfilingTraceControl(enabled=False)

    def set_process_id(self, process_id: str, *, reset: bool = False) -> None:
        if reset or (self._process_id and self._process_id != process_id):
            self._publishers.clear()
            self._subscribers.clear()
            self._default_trace_control = ProfilingTraceControl(enabled=False)
        self._process_id = process_id

    def register_publisher(self, pub_id: UUID, topic: str) -> None:
        self._publishers[pub_id] = _PublisherMetrics(
            topic=topic,
            endpoint_id=_endpoint_id(topic, pub_id),
        )

    def unregister_publisher(self, pub_id: UUID) -> None:
        self._publishers.pop(pub_id, None)

    def register_subscriber(self, sub_id: UUID, topic: str) -> None:
        self._subscribers[sub_id] = _SubscriberMetrics(
            topic=topic,
            endpoint_id=_endpoint_id(topic, sub_id),
        )

    def unregister_subscriber(self, sub_id: UUID) -> None:
        self._subscribers.pop(sub_id, None)

    def publisher_publish(self, pub_id: UUID, ts_ns: int, inflight: int) -> None:
        metric = self._publishers.get(pub_id)
        if metric is not None:
            metric.record_publish(ts_ns, inflight)

    def publisher_backpressure_wait(self, pub_id: UUID, ts_ns: int, wait_ns: int) -> None:
        metric = self._publishers.get(pub_id)
        if metric is not None:
            metric.record_backpressure_wait(ts_ns, wait_ns)

    def publisher_sample_inflight(self, pub_id: UUID, ts_ns: int, inflight: int) -> None:
        metric = self._publishers.get(pub_id)
        if metric is not None:
            metric.sample_inflight(ts_ns, inflight)

    def subscriber_receive(
        self,
        sub_id: UUID,
        ts_ns: int,
        lease_ns: int,
        channel_kind: ProfileChannelType,
    ) -> None:
        metric = self._subscribers.get(sub_id)
        if metric is not None:
            metric.record_receive(ts_ns, lease_ns, channel_kind)

    def subscriber_user_span(
        self, sub_id: UUID, ts_ns: int, span_ns: int, label: str | None
    ) -> None:
        metric = self._subscribers.get(sub_id)
        if metric is not None:
            metric.record_user_span(ts_ns, span_ns, label)

    def subscriber_attributed_backpressure(
        self,
        sub_id: UUID,
        ts_ns: int,
        duration_ns: int,
        channel_kind: ProfileChannelType,
    ) -> None:
        metric = self._subscribers.get(sub_id)
        if metric is not None:
            metric.record_attributed_backpressure(ts_ns, duration_ns, channel_kind)

    def snapshot(self) -> ProcessProfilingSnapshot:
        return ProcessProfilingSnapshot(
            process_id=self._process_id,
            pid=self._pid,
            host=self._host,
            window_seconds=WINDOW_SECONDS,
            timestamp=float(PROFILE_TIME()),
            publishers={
                metric.endpoint_id: metric.snapshot()
                for metric in self._publishers.values()
            },
            subscribers={
                metric.endpoint_id: metric.snapshot()
                for metric in self._subscribers.values()
            },
        )

    def set_trace_control(self, control: ProfilingTraceControl) -> None:
        self._default_trace_control = control
        sample_mod = max(1, control.sample_mod)
        pub_topics = set(control.publisher_topics or [])
        sub_topics = set(control.subscriber_topics or [])

        for metric in self._publishers.values():
            enabled = control.enabled and (
                not pub_topics or metric.topic in pub_topics
            )
            metric.trace_enabled = enabled
            metric.trace_sample_mod = sample_mod

        for metric in self._subscribers.values():
            enabled = control.enabled and (
                not sub_topics or metric.topic in sub_topics
            )
            metric.trace_enabled = enabled
            metric.trace_sample_mod = sample_mod

    def trace_batch(self, max_samples: int = 1000) -> ProcessProfilingTraceBatch:
        samples: list[ProfilingTraceSample] = []
        for metric in self._publishers.values():
            while metric.trace_samples and len(samples) < max_samples:
                samples.append(metric.trace_samples.popleft())
            if len(samples) >= max_samples:
                break
        if len(samples) < max_samples:
            for metric in self._subscribers.values():
                while metric.trace_samples and len(samples) < max_samples:
                    samples.append(metric.trace_samples.popleft())
                if len(samples) >= max_samples:
                    break

        return ProcessProfilingTraceBatch(
            process_id=self._process_id,
            pid=self._pid,
            host=self._host,
            timestamp=float(PROFILE_TIME()),
            samples=samples,
        )


PROFILES = ProfileRegistry()
