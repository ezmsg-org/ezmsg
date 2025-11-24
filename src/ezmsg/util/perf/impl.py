import asyncio
import dataclasses
import os
import time
import typing
import enum
import sys

import ezmsg.core as ez

from ezmsg.util.messages.util import replace
from ezmsg.core.netprotocol import Address

try:
    import numpy as np
except ImportError:
    ez.logger.error("ezmsg perf requires numpy")
    raise

from .util import stable_perf

try:
    StrEnum = enum.StrEnum  # type: ignore[attr-defined]
except AttributeError:
    class StrEnum(str, enum.Enum):
        """Fallback for Python < 3.11 where enum.StrEnum is unavailable."""

        pass

TIME = time.monotonic
if sys.platform.startswith('win'):
    TIME = time.perf_counter

def collect(
    components: typing.Optional[typing.Mapping[str, ez.Component]] = None,
    network: ez.NetworkDefinition = (),
    process_components: typing.Collection[ez.Component] | None = None,
    **components_kwargs: ez.Component,
) -> ez.Collection:
    """collect a grouping of pre-configured components into a new "Collection" """
    from ezmsg.core.util import either_dict_or_kwargs

    components = either_dict_or_kwargs(components, components_kwargs, "collect")
    if components is None:
        raise ValueError("Must supply at least one component to run")

    out = ez.Collection()
    for name, comp in components.items():
        comp._set_name(name)
    out._components = (
        components  # FIXME: Component._components should be typehinted as a Mapping
    )
    out.network = lambda: network
    out.process_components = (
        lambda: (out,) if process_components is None else process_components
    )
    return out


@dataclasses.dataclass
class Metrics:
    num_msgs: int
    sample_rate_mean: float
    sample_rate_median: float
    latency_mean: float
    latency_median: float
    latency_total: float
    data_rate: float


class LoadTestSettings(ez.Settings):
    max_duration: float
    num_msgs: int
    dynamic_size: int
    buffers: int
    force_tcp: bool


@dataclasses.dataclass
class LoadTestSample:
    _timestamp: float
    counter: int
    dynamic_data: np.ndarray
    key: str


class LoadTestSourceState(ez.State):
    counter: int = 0


class LoadTestSource(ez.Unit):
    OUTPUT = ez.OutputStream(LoadTestSample)
    SETTINGS = LoadTestSettings
    STATE = LoadTestSourceState

    async def initialize(self) -> None:
        self.OUTPUT.num_buffers = self.SETTINGS.buffers
        self.OUTPUT.force_tcp = self.SETTINGS.force_tcp

    @ez.publisher(OUTPUT)
    async def publish(self) -> typing.AsyncGenerator:
        ez.logger.info(f"Load test publisher started. (PID: {os.getpid()})")
        start_time = TIME()
        for _ in range(self.SETTINGS.num_msgs):
            current_time = TIME()
            if current_time - start_time >= self.SETTINGS.max_duration:
                break

            yield (
                self.OUTPUT,
                LoadTestSample(
                    _timestamp=TIME(),
                    counter=self.STATE.counter,
                    dynamic_data=np.zeros(
                        int(self.SETTINGS.dynamic_size // 4), dtype=np.float32
                    ),
                    key=self.name,
                ),
            )
            self.STATE.counter += 1

        ez.logger.info("Exiting publish")
        raise ez.Complete

    async def shutdown(self) -> None:
        ez.logger.info(f"Samples sent: {self.STATE.counter}")


class LoadTestRelay(ez.Unit):
    INPUT = ez.InputStream(LoadTestSample)
    OUTPUT = ez.OutputStream(LoadTestSample)

    @ez.subscriber(INPUT, zero_copy=True)
    @ez.publisher(OUTPUT)
    async def on_msg(self, msg: LoadTestSample) -> typing.AsyncGenerator:
        yield self.OUTPUT, msg


class LoadTestReceiverState(ez.State):
    # Tuples of sent timestamp, received timestamp, counter, dynamic size
    received_data: list[tuple[float, float, int]] = dataclasses.field(
        default_factory=list
    )
    counters: dict[str, int] = dataclasses.field(default_factory=dict)


class LoadTestReceiver(ez.Unit):
    INPUT = ez.InputStream(LoadTestSample)
    SETTINGS = LoadTestSettings
    STATE = LoadTestReceiverState

    async def initialize(self) -> None:
        ez.logger.info(f"Load test subscriber started. (PID: {os.getpid()})")

    @ez.subscriber(INPUT, zero_copy=True)
    async def receive(self, sample: LoadTestSample) -> None:
        counter = self.STATE.counters.get(sample.key, -1)
        if sample.counter != counter + 1:
            ez.logger.warning(f"{sample.counter - counter - 1} samples skipped!")
        self.STATE.received_data.append(
            (sample._timestamp, TIME(), sample.counter)
        )
        self.STATE.counters[sample.key] = sample.counter


class LoadTestSink(LoadTestReceiver):
    INPUT = ez.InputStream(LoadTestSample)

    @ez.subscriber(INPUT, zero_copy=True)
    async def receive(self, sample: LoadTestSample) -> None:
        await super().receive(sample)
        if len(self.STATE.received_data) == self.SETTINGS.num_msgs:
            raise ez.NormalTermination

    @ez.task
    async def terminate(self) -> None:
        # Wait for the max duration of the load test
        await asyncio.sleep(self.SETTINGS.max_duration)
        ez.logger.warning("TIMEOUT -- terminating test.")
        raise ez.NormalTermination


### TEST CONFIGURATIONS


@dataclasses.dataclass
class ConfigSettings:
    n_clients: int
    settings: LoadTestSettings
    source: LoadTestSource
    sink: LoadTestSink


Configuration = typing.Tuple[typing.Iterable[ez.Component], ez.NetworkDefinition]
Configurator = typing.Callable[[ConfigSettings], Configuration]


def fanout(config: ConfigSettings) -> Configuration:
    """one pub to many subs"""
    connections: ez.NetworkDefinition = [(config.source.OUTPUT, config.sink.INPUT)]
    subs = [LoadTestReceiver(config.settings) for _ in range(config.n_clients)]
    for sub in subs:
        connections.append((config.source.OUTPUT, sub.INPUT))

    return subs, connections


def fanin(config: ConfigSettings) -> Configuration:
    """many pubs to one sub"""
    connections: ez.NetworkDefinition = [(config.source.OUTPUT, config.sink.INPUT)]
    pubs = [LoadTestSource(config.settings) for _ in range(config.n_clients)]
    expected_num_msgs = config.sink.SETTINGS.num_msgs * len(pubs)
    config.sink.SETTINGS = replace(config.sink.SETTINGS, num_msgs=expected_num_msgs)  # type: ignore
    for pub in pubs:
        connections.append((pub.OUTPUT, config.sink.INPUT))
    return pubs, connections


def relay(config: ConfigSettings) -> Configuration:
    """one pub to one sub through many relays"""
    connections: ez.NetworkDefinition = []

    relays = [LoadTestRelay(config.settings) for _ in range(config.n_clients)]
    if len(relays):
        connections.append((config.source.OUTPUT, relays[0].INPUT))
        for from_relay, to_relay in zip(relays[:-1], relays[1:]):
            connections.append((from_relay.OUTPUT, to_relay.INPUT))
        connections.append((relays[-1].OUTPUT, config.sink.INPUT))
    else:
        connections.append((config.source.OUTPUT, config.sink.INPUT))

    return relays, connections


CONFIGS: typing.Mapping[str, Configurator] = {
    c.__name__: c for c in [fanin, fanout, relay]
}


class Communication(StrEnum):
    LOCAL = "local"
    SHM = "shm"
    SHM_SPREAD = "shm_spread"
    TCP = "tcp"
    TCP_SPREAD = "tcp_spread"


def perform_test(
    n_clients: int,
    max_duration: float,
    num_msgs: int,
    msg_size: int,
    buffers: int,
    comms: Communication,
    config: Configurator,
    graph_address: Address,
) -> Metrics:
    settings = LoadTestSettings(
        dynamic_size=int(msg_size),
        num_msgs=num_msgs,
        max_duration=max_duration,
        buffers=buffers,
        force_tcp=(comms in (Communication.TCP, Communication.TCP_SPREAD)),
    )

    source = LoadTestSource(settings)
    sink = LoadTestSink(settings)

    components: typing.Mapping[str, ez.Component] = dict(
        SINK=sink,
    )

    clients, connections = config(ConfigSettings(n_clients, settings, source, sink))

    # The 'sink' MUST remain in this process for us to pull its state.
    process_components: typing.Iterable[ez.Component] = []
    if comms == Communication.LOCAL:
        # Every component in the same process (this one)
        components["SOURCE"] = source
        for i, client in enumerate(clients):
            components[f"CLIENT_{i + 1}"] = client

    else:
        if comms in (Communication.SHM_SPREAD, Communication.TCP_SPREAD):
            # Every component in its own process.
            components["SOURCE"] = source
            process_components.append(source)
            for i, client in enumerate(clients):
                components[f"CLIENT_{i + 1}"] = client
                process_components.append(client)

        else:
            # All clients and the source in ONE other process.
            collect_comps: typing.Mapping[str, ez.Component] = dict()
            collect_comps["SOURCE"] = source
            for i, client in enumerate(clients):
                collect_comps[f"CLIENT_{i + 1}"] = client
            proc_collection = collect(components=collect_comps)
            components["PROC"] = proc_collection
            process_components = [proc_collection]

    with stable_perf():
        ez.run(
            components=components,
            connections=connections,
            process_components=process_components,
            graph_address=graph_address,
        )

    return calculate_metrics(sink)


def calculate_metrics(sink: LoadTestSink) -> Metrics:
    # Log some useful summary statistics
    min_timestamp = min(timestamp for timestamp, _, _ in sink.STATE.received_data)
    max_timestamp = max(timestamp for timestamp, _, _ in sink.STATE.received_data)
    latency = [
        receive_timestamp - send_timestamp
        for send_timestamp, receive_timestamp, _ in sink.STATE.received_data
    ]
    total_latency = abs(sum(latency))

    counters = list(sorted(t[2] for t in sink.STATE.received_data))
    dropped_samples = sum(
        [max((x1 - x0) - 1, 0) for x1, x0 in zip(counters[1:], counters[:-1])]
    )

    rx_timestamps = np.array([rx_ts for _, rx_ts, _ in sink.STATE.received_data])
    rx_timestamps.sort()
    runtime = max_timestamp - min_timestamp
    num_samples = len(sink.STATE.received_data)
    samplerate_mean = num_samples / runtime
    diff_timestamps = np.diff(rx_timestamps)
    diff_timestamps = diff_timestamps[np.nonzero(diff_timestamps)]
    samplerate_median = 1.0 / float(np.median(diff_timestamps))
    latency_mean = total_latency / num_samples
    latency_median = list(sorted(latency))[len(latency) // 2]
    total_data = num_samples * sink.SETTINGS.dynamic_size
    data_rate = total_data / runtime

    ez.logger.info(f"Samples received: {num_samples}")
    ez.logger.info(f"Mean sample rate: {samplerate_mean} Hz")
    ez.logger.info(f"Median sample rate: {samplerate_median} Hz")
    ez.logger.info(f"Mean latency: {latency_mean} s")
    ez.logger.info(f"Median latency: {latency_median} s")
    ez.logger.info(f"Total latency: {total_latency} s")
    ez.logger.info(f"Data rate: {data_rate * 1e-6} MB/s")

    if dropped_samples:
        ez.logger.error(
            f"Dropped samples: {dropped_samples} ({dropped_samples / (dropped_samples + num_samples)}%)",
        )

    return Metrics(
        num_msgs=num_samples,
        sample_rate_mean=samplerate_mean,
        sample_rate_median=samplerate_median,
        latency_mean=latency_mean,
        latency_median=latency_median,
        latency_total=total_latency,
        data_rate=data_rate,
    )


@dataclasses.dataclass(unsafe_hash=True)
class TestParameters:
    msg_size: int
    num_msgs: int
    n_clients: int
    config: str
    comms: str
    max_duration: float
    num_buffers: int


@dataclasses.dataclass
class TestLogEntry:
    params: TestParameters
    results: Metrics
