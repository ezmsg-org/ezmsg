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
class _PublisherBucket:
    publish_count: int = 0
    publish_delta_sum: int = 0
    publish_delta_count: int = 0
    backpressure_wait_sum: int = 0
    inflight_peak: int = 0

    def reset(self) -> None:
        self.publish_count = 0
        self.publish_delta_sum = 0
        self.publish_delta_count = 0
        self.backpressure_wait_sum = 0
        self.inflight_peak = 0


@dataclass
class _SubscriberBucket:
    recv_count: int = 0
    lease_time_sum: int = 0
    user_span_sum: int = 0
    user_span_count: int = 0
    attributable_backpressure_sum: int = 0
    attributable_backpressure_count: int = 0

    def reset(self) -> None:
        self.recv_count = 0
        self.lease_time_sum = 0
        self.user_span_sum = 0
        self.user_span_count = 0
        self.attributable_backpressure_sum = 0
        self.attributable_backpressure_count = 0


class _BucketWindow:
    def __init__(self, bucket_type: type[_PublisherBucket | _SubscriberBucket]) -> None:
        self._num_buckets = max(1, int(WINDOW_SECONDS / BUCKET_SECONDS))
        self._bucket_ns = max(1, int(BUCKET_SECONDS * 1e9))
        self._bucket_type = bucket_type
        self._ticks = [-1 for _ in range(self._num_buckets)]
        self._buckets = [bucket_type() for _ in range(self._num_buckets)]
        self._last_tick: int | None = None

    def _bucket_tick(self, ts_ns: int) -> int:
        return ts_ns // self._bucket_ns

    def _clear_bucket(self, idx: int) -> None:
        self._ticks[idx] = -1
        self._buckets[idx].reset()

    def _advance(self, ts_ns: int) -> int:
        tick = self._bucket_tick(ts_ns)
        if self._last_tick is None:
            self._last_tick = tick
            return tick
        if tick <= self._last_tick:
            return tick

        elapsed = tick - self._last_tick
        if elapsed >= self._num_buckets:
            for idx in range(self._num_buckets):
                self._clear_bucket(idx)
        else:
            previous_idx = self._last_tick % self._num_buckets
            for step in range(1, elapsed + 1):
                self._clear_bucket((previous_idx + step) % self._num_buckets)

        self._last_tick = tick
        return tick

    def bucket(self, ts_ns: int) -> _PublisherBucket | _SubscriberBucket:
        tick = self._advance(ts_ns)
        idx = tick % self._num_buckets
        if self._ticks[idx] != tick:
            self._ticks[idx] = tick
            self._buckets[idx].reset()
        return self._buckets[idx]

    def buckets(self, ts_ns: int) -> list[_PublisherBucket | _SubscriberBucket]:
        tick = self._advance(ts_ns)
        min_tick = tick - self._num_buckets + 1
        return [
            bucket
            for bucket_tick, bucket in zip(self._ticks, self._buckets)
            if bucket_tick >= min_tick
        ]


@dataclass
class _PublisherMetrics:
    topic: str
    endpoint_id: str
    num_buffers: int
    messages_published_total: int = 0
    backpressure_wait_ns_total: int = 0
    inflight_messages_current: int = 0
    _last_publish_ts_ns: int | None = None
    _window: _BucketWindow = field(
        default_factory=lambda: _BucketWindow(_PublisherBucket)
    )
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
        bucket = self._window.bucket(ts_ns)
        assert isinstance(bucket, _PublisherBucket)
        bucket.publish_count += 1
        publish_delta_ns = 0
        if self._last_publish_ts_ns is not None:
            publish_delta_ns = ts_ns - self._last_publish_ts_ns
            bucket.publish_delta_sum += publish_delta_ns
            bucket.publish_delta_count += 1
        self._last_publish_ts_ns = ts_ns
        self.inflight_messages_current = inflight
        if inflight > bucket.inflight_peak:
            bucket.inflight_peak = inflight
        self._trace_counter += 1
        if (
            self.trace_enabled
            and self._trace_metric_enabled("publish_delta_ns")
            and (self._trace_counter % max(1, self.trace_sample_mod) == 0)
        ):
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(ts_ns),
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
        bucket = self._window.bucket(ts_ns)
        assert isinstance(bucket, _PublisherBucket)
        bucket.backpressure_wait_sum += wait_ns
        if self.trace_enabled and self._trace_metric_enabled("backpressure_wait_ns"):
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(ts_ns),
                    endpoint_id=self.endpoint_id,
                    topic=self.topic,
                    metric="backpressure_wait_ns",
                    value=float(wait_ns),
                    sample_seq=msg_seq,
                )
            )

    def sample_inflight(self, ts_ns: int, inflight: int) -> None:
        self.inflight_messages_current = inflight
        bucket = self._window.bucket(ts_ns)
        assert isinstance(bucket, _PublisherBucket)
        if inflight > bucket.inflight_peak:
            bucket.inflight_peak = inflight

    def snapshot(self) -> PublisherProfileSnapshot:
        now_ns = PROFILE_TIME()
        buckets = self._window.buckets(now_ns)
        window_msgs = 0
        publish_delta_sum = 0
        publish_delta_count = 0
        backpressure_wait_sum = 0
        inflight_peak = 0
        for bucket in buckets:
            assert isinstance(bucket, _PublisherBucket)
            window_msgs += bucket.publish_count
            publish_delta_sum += bucket.publish_delta_sum
            publish_delta_count += bucket.publish_delta_count
            backpressure_wait_sum += bucket.backpressure_wait_sum
            if bucket.inflight_peak > inflight_peak:
                inflight_peak = bucket.inflight_peak
        return PublisherProfileSnapshot(
            endpoint_id=self.endpoint_id,
            topic=self.topic,
            messages_published_total=self.messages_published_total,
            messages_published_window=window_msgs,
            publish_delta_ns_avg_window=(
                float(publish_delta_sum) / float(publish_delta_count)
                if publish_delta_count > 0
                else 0.0
            ),
            publish_rate_hz_window=float(window_msgs) / max(WINDOW_SECONDS, 1e-9),
            inflight_messages_current=self.inflight_messages_current,
            num_buffers=self.num_buffers,
            inflight_messages_peak_window=inflight_peak,
            backpressure_wait_ns_total=self.backpressure_wait_ns_total,
            backpressure_wait_ns_window=backpressure_wait_sum,
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
    _window: _BucketWindow = field(
        default_factory=lambda: _BucketWindow(_SubscriberBucket)
    )
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
        bucket = self._window.bucket(ts_ns)
        assert isinstance(bucket, _SubscriberBucket)
        bucket.recv_count += 1
        bucket.lease_time_sum += lease_ns
        self._trace_counter += 1
        if (
            self.trace_enabled
            and self._trace_metric_enabled("lease_time_ns")
            and (self._trace_counter % max(1, self.trace_sample_mod) == 0)
        ):
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(ts_ns),
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
        bucket = self._window.bucket(ts_ns)
        assert isinstance(bucket, _SubscriberBucket)
        bucket.user_span_sum += span_ns
        bucket.user_span_count += 1
        if self.trace_enabled and self._trace_metric_enabled("user_span_ns"):
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(ts_ns),
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
        bucket = self._window.bucket(ts_ns)
        assert isinstance(bucket, _SubscriberBucket)
        bucket.attributable_backpressure_sum += duration_ns
        bucket.attributable_backpressure_count += 1
        if self.trace_enabled and self._trace_metric_enabled("attributable_backpressure_ns"):
            self.trace_samples.append(
                ProfilingTraceSample(
                    timestamp=float(ts_ns),
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
        buckets = self._window.buckets(now_ns)
        recv_count = 0
        lease_time_sum = 0
        user_span_sum = 0
        user_count = 0
        attributable_backpressure_sum = 0
        for bucket in buckets:
            assert isinstance(bucket, _SubscriberBucket)
            recv_count += bucket.recv_count
            lease_time_sum += bucket.lease_time_sum
            user_span_sum += bucket.user_span_sum
            user_count += bucket.user_span_count
            attributable_backpressure_sum += bucket.attributable_backpressure_sum
        return SubscriberProfileSnapshot(
            endpoint_id=self.endpoint_id,
            topic=self.topic,
            messages_received_total=self.messages_received_total,
            messages_received_window=recv_count,
            lease_time_ns_total=self.lease_time_ns_total,
            lease_time_ns_avg_window=(
                float(lease_time_sum) / float(recv_count) if recv_count > 0 else 0.0
            ),
            user_span_ns_total=self.user_span_ns_total,
            user_span_ns_avg_window=(
                float(user_span_sum) / float(user_count)
                if user_count > 0
                else 0.0
            ),
            attributable_backpressure_ns_total=self.attributable_backpressure_ns_total,
            attributable_backpressure_ns_window=attributable_backpressure_sum,
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
