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
    _last_bucket_tick: int | None = None

    def __post_init__(self) -> None:
        self._num_buckets = max(1, int(self.window_seconds / self.bucket_seconds))
        self._bucket_ns = max(1, int(self.bucket_seconds * 1e9))
        self.count = [0 for _ in range(self._num_buckets)]
        self.value_sum = [0 for _ in range(self._num_buckets)]
        self.max_value = [0 for _ in range(self._num_buckets)]

    def _bucket_tick(self, ts_ns: int) -> int:
        return ts_ns // self._bucket_ns

    def _bucket(self, ts_ns: int) -> int:
        return self._bucket_tick(ts_ns) % self._num_buckets

    def _reset_bucket(self, idx: int) -> None:
        self.count[idx] = 0
        self.value_sum[idx] = 0
        self.max_value[idx] = 0

    def _advance(self, ts_ns: int) -> int:
        bucket_tick = self._bucket_tick(ts_ns)
        bucket = bucket_tick % self._num_buckets
        if self._last_bucket_tick is None:
            self._last_bucket_tick = bucket_tick
            return bucket
        if bucket_tick <= self._last_bucket_tick:
            return bucket
        elapsed_buckets = bucket_tick - self._last_bucket_tick
        if elapsed_buckets >= self._num_buckets:
            for idx in range(self._num_buckets):
                self._reset_bucket(idx)
        else:
            previous_bucket = self._last_bucket_tick % self._num_buckets
            for step in range(1, elapsed_buckets + 1):
                self._reset_bucket((previous_bucket + step) % self._num_buckets)
        self._last_bucket_tick = bucket_tick
        return bucket

    def advance_to(self, ts_ns: int) -> None:
        self._advance(ts_ns)

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
    num_buffers: int
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
    trace_metrics: set[str] | None = None
    _trace_counter: int = 0
    trace_samples: deque[ProfilingTraceSample] = field(
        default_factory=lambda: deque(maxlen=TRACE_MAX_SAMPLES)
    )

    def _trace_metric_enabled(self, metric: str) -> bool:
        return self.trace_metrics is None or metric in self.trace_metrics

    def record_publish(
        self, ts_ns: int, inflight: int, msg_seq: int | None = None
    ) -> None:
        self.messages_published_total += 1
        self._publish_count.add(ts_ns, 1)
        publish_delta_ns = 0
        if self._last_publish_ts_ns is not None:
            publish_delta_ns = ts_ns - self._last_publish_ts_ns
            self._publish_delta.add(ts_ns, publish_delta_ns)
        self._last_publish_ts_ns = ts_ns
        self.sample_inflight(ts_ns, inflight)
        self._trace_counter += 1
        if (
            self.trace_enabled
            and self._trace_metric_enabled("publish_delta_ns")
            and (self._trace_counter % max(1, self.trace_sample_mod) == 0)
        ):
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(PROFILE_TIME()),
                    endpoint_id=self.endpoint_id,
                    topic=self.topic,
                    metric="publish_delta_ns",
                    value=float(publish_delta_ns),
                    sample_seq=msg_seq,
                )
            )

    def record_backpressure_wait(
        self, ts_ns: int, wait_ns: int, msg_seq: int | None = None
    ) -> None:
        self.backpressure_wait_ns_total += wait_ns
        self._backpressure_wait.add(ts_ns, wait_ns)
        if self.trace_enabled and self._trace_metric_enabled("backpressure_wait_ns"):
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(PROFILE_TIME()),
                    endpoint_id=self.endpoint_id,
                    topic=self.topic,
                    metric="backpressure_wait_ns",
                    value=float(wait_ns),
                    sample_seq=msg_seq,
                )
            )

    def sample_inflight(self, ts_ns: int, inflight: int) -> None:
        self.inflight_messages_current = inflight
        self._inflight.add(ts_ns, inflight)

    def snapshot(self) -> PublisherProfileSnapshot:
        now_ns = PROFILE_TIME()
        self._publish_delta.advance_to(now_ns)
        self._publish_count.advance_to(now_ns)
        self._backpressure_wait.advance_to(now_ns)
        self._inflight.advance_to(now_ns)
        window_msgs = self._publish_count.count_total()
        return PublisherProfileSnapshot(
            endpoint_id=self.endpoint_id,
            topic=self.topic,
            messages_published_total=self.messages_published_total,
            messages_published_window=window_msgs,
            publish_delta_ns_avg_window=self._publish_delta.avg(),
            publish_rate_hz_window=float(window_msgs) / max(WINDOW_SECONDS, 1e-9),
            inflight_messages_current=self.inflight_messages_current,
            num_buffers=self.num_buffers,
            inflight_messages_peak_window=self._inflight.max_total(),
            backpressure_wait_ns_total=self.backpressure_wait_ns_total,
            backpressure_wait_ns_window=self._backpressure_wait.sum_total(),
            timestamp=float(now_ns),
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
    trace_metrics: set[str] | None = None
    _trace_counter: int = 0
    trace_samples: deque[ProfilingTraceSample] = field(
        default_factory=lambda: deque(maxlen=TRACE_MAX_SAMPLES)
    )

    def _trace_metric_enabled(self, metric: str) -> bool:
        return self.trace_metrics is None or metric in self.trace_metrics

    def record_receive(
        self,
        ts_ns: int,
        lease_ns: int,
        channel_kind: ProfileChannelType,
        msg_seq: int | None = None,
    ) -> None:
        self.messages_received_total += 1
        self.lease_time_ns_total += lease_ns
        self.channel_kind_last = channel_kind
        self._recv_count.add(ts_ns, 1)
        self._lease_time.add(ts_ns, lease_ns)
        self._trace_counter += 1
        if (
            self.trace_enabled
            and self._trace_metric_enabled("lease_time_ns")
            and (self._trace_counter % max(1, self.trace_sample_mod) == 0)
        ):
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(PROFILE_TIME()),
                    endpoint_id=self.endpoint_id,
                    topic=self.topic,
                    metric="lease_time_ns",
                    value=float(lease_ns),
                    channel_kind=channel_kind,
                    sample_seq=msg_seq,
                )
            )

    def record_user_span(
        self, ts_ns: int, span_ns: int, label: str | None, msg_seq: int | None = None
    ) -> None:
        self.user_span_ns_total += span_ns
        self._user_span.add(ts_ns, span_ns)
        if self.trace_enabled and self._trace_metric_enabled("user_span_ns"):
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(PROFILE_TIME()),
                    endpoint_id=self.endpoint_id,
                    topic=self.topic if label is None else f"{self.topic}:{label}",
                    metric="user_span_ns",
                    value=float(span_ns),
                    channel_kind=self.channel_kind_last,
                    sample_seq=msg_seq,
                )
            )

    def record_attributed_backpressure(
        self,
        ts_ns: int,
        duration_ns: int,
        channel_kind: ProfileChannelType,
        msg_seq: int | None = None,
    ) -> None:
        self.attributable_backpressure_ns_total += duration_ns
        self.attributable_backpressure_events_total += 1
        self.channel_kind_last = channel_kind
        self._attrib_bp.add(ts_ns, duration_ns)
        if self.trace_enabled and self._trace_metric_enabled("attributable_backpressure_ns"):
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(PROFILE_TIME()),
                    endpoint_id=self.endpoint_id,
                    topic=self.topic,
                    metric="attributable_backpressure_ns",
                    value=float(duration_ns),
                    channel_kind=channel_kind,
                    sample_seq=msg_seq,
                )
            )

    def snapshot(self) -> SubscriberProfileSnapshot:
        now_ns = PROFILE_TIME()
        self._recv_count.advance_to(now_ns)
        self._lease_time.advance_to(now_ns)
        self._user_span.advance_to(now_ns)
        self._attrib_bp.advance_to(now_ns)
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

    def set_process_id(self, process_id: UUID, *, reset: bool = False) -> None:
        if reset:
            self._publishers.clear()
            self._subscribers.clear()
            self._default_trace_control = ProfilingTraceControl(enabled=False)
            self._trace_control_expires_ns = None
        self._process_id = process_id

    def register_publisher(self, pub_id: UUID, topic: str, num_buffers: int) -> None:
        metric = _PublisherMetrics(
            topic=topic,
            endpoint_id=_endpoint_id(topic, pub_id),
            num_buffers=max(1, int(num_buffers)),
        )
        self._publishers[pub_id] = metric
        self._apply_trace_control_to_publisher(metric)

    def unregister_publisher(self, pub_id: UUID) -> None:
        self._publishers.pop(pub_id, None)

    def register_subscriber(self, sub_id: UUID, topic: str) -> None:
        metric = _SubscriberMetrics(
            topic=topic,
            endpoint_id=_endpoint_id(topic, sub_id),
        )
        self._subscribers[sub_id] = metric
        self._apply_trace_control_to_subscriber(metric)

    def unregister_subscriber(self, sub_id: UUID) -> None:
        self._subscribers.pop(sub_id, None)

    def publisher_publish(
        self, pub_id: UUID, ts_ns: int, inflight: int, msg_seq: int | None = None
    ) -> None:
        self._expire_trace_control_if_needed(ts_ns)
        metric = self._publishers.get(pub_id)
        if metric is not None:
            metric.record_publish(ts_ns, inflight, msg_seq)

    def publisher_backpressure_wait(
        self, pub_id: UUID, ts_ns: int, wait_ns: int, msg_seq: int | None = None
    ) -> None:
        self._expire_trace_control_if_needed(ts_ns)
        metric = self._publishers.get(pub_id)
        if metric is not None:
            metric.record_backpressure_wait(ts_ns, wait_ns, msg_seq)

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
        msg_seq: int | None = None,
    ) -> None:
        self._expire_trace_control_if_needed(ts_ns)
        metric = self._subscribers.get(sub_id)
        if metric is not None:
            metric.record_receive(ts_ns, lease_ns, channel_kind, msg_seq)

    def subscriber_user_span(
        self,
        sub_id: UUID,
        ts_ns: int,
        span_ns: int,
        label: str | None,
        msg_seq: int | None = None,
    ) -> None:
        self._expire_trace_control_if_needed(ts_ns)
        metric = self._subscribers.get(sub_id)
        if metric is not None:
            metric.record_user_span(ts_ns, span_ns, label, msg_seq)

    def subscriber_attributed_backpressure(
        self,
        sub_id: UUID,
        ts_ns: int,
        duration_ns: int,
        channel_kind: ProfileChannelType,
        msg_seq: int | None = None,
    ) -> None:
        self._expire_trace_control_if_needed(ts_ns)
        metric = self._subscribers.get(sub_id)
        if metric is not None:
            metric.record_attributed_backpressure(
                ts_ns, duration_ns, channel_kind, msg_seq
            )

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
                # Include sample_seq to keep deterministic ordering when timestamps tie.
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

    def _clear_trace_samples(self) -> None:
        for metric in self._publishers.values():
            metric.trace_samples.clear()
        for metric in self._subscribers.values():
            metric.trace_samples.clear()


PROFILES = ProfileRegistry()
