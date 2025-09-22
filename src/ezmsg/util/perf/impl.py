import asyncio
import dataclasses
import os
import time
import typing
import enum

import ezmsg.core as ez

try:
    import numpy as np
except ImportError:
    ez.logger.error("ezmsg perf requires numpy")
    raise


def collect(
    components: typing.Optional[typing.Mapping[str, ez.Component]] = None,
    network: ez.NetworkDefinition = (),
    process_components: typing.Collection[ez.Component] | None = None,
    **components_kwargs: ez.Component,
) -> ez.Collection:
    """ collect a grouping of pre-configured components into a new "Collection" """
    from ezmsg.core.util import either_dict_or_kwargs
    
    components = either_dict_or_kwargs(components, components_kwargs, "collect")
    if components is None:
        raise ValueError("Must supply at least one component to run")
    
    out = ez.Collection()
    for name, comp in components.items():
        comp._set_name(name)
    out._components = components # FIXME: Component._components should be typehinted as a Mapping
    out.network = lambda: network
    out.process_components = lambda: (out,) if process_components is None else process_components
    return out


@dataclasses.dataclass
class Metrics:
    num_msgs: int
    sample_rate: float
    latency_mean: float
    latency_total: float
    data_rate: float


class LoadTestSettings(ez.Settings):
    duration: float
    dynamic_size: int
    buffers: int
    force_tcp: bool


@dataclasses.dataclass
class LoadTestSample:
    _timestamp: float
    counter: int
    dynamic_data: np.ndarray
    key: str


class LoadTestSender(ez.Unit):
    OUTPUT = ez.OutputStream(LoadTestSample)
    SETTINGS = LoadTestSettings

    async def initialize(self) -> None:
        self.running = True
        self.counter = 0

        self.OUTPUT.num_buffers = self.SETTINGS.buffers
        self.OUTPUT.force_tcp = self.SETTINGS.force_tcp

    @ez.publisher(OUTPUT)
    async def publish(self) -> typing.AsyncGenerator:
        ez.logger.info(f"Load test publisher started. (PID: {os.getpid()})")
        start_time = time.perf_counter()
        while self.running:
            current_time = time.perf_counter()
            if current_time - start_time >= self.SETTINGS.duration:
                break

            yield (
                self.OUTPUT,
                LoadTestSample(
                    _timestamp=time.perf_counter(),
                    counter=self.counter,
                    dynamic_data=np.zeros(
                        int(self.SETTINGS.dynamic_size // 8), dtype=np.float32
                    ),
                    key = self.name,
                ),
            )
            self.counter += 1
        ez.logger.info("Exiting publish")
        raise ez.Complete

class LoadTestSource(LoadTestSender):
    async def shutdown(self) -> None:
        self.running = False
        ez.logger.info(f"Samples sent: {self.counter}")


class LoadTestRelay(ez.Unit):
    INPUT = ez.InputStream(LoadTestSample)
    OUTPUT = ez.OutputStream(LoadTestSample)

    @ez.subscriber(INPUT, zero_copy = True)
    @ez.publisher(OUTPUT)
    async def on_msg(self, msg: LoadTestSample) -> typing.AsyncGenerator:
        yield self.OUTPUT, msg


class LoadTestReceiverState(ez.State):
    # Tuples of sent timestamp, received timestamp, counter, dynamic size
    received_data: typing.List[typing.Tuple[float, float, int]] = dataclasses.field(
        default_factory=list
    )
    counters: typing.Dict[str, int] = dataclasses.field(default_factory=dict)


class LoadTestReceiver(ez.Unit):
    INPUT = ez.InputStream(LoadTestSample)
    SETTINGS = LoadTestSettings
    STATE = LoadTestReceiverState

    @ez.subscriber(INPUT, zero_copy=True)
    async def receive(self, sample: LoadTestSample) -> None:
        counter = self.STATE.counters.get(sample.key, -1)
        if sample.counter != counter + 1:
            ez.logger.warning(
                f"{sample.counter - counter - 1} samples skipped!"
            )
        self.STATE.received_data.append(
            (sample._timestamp, time.perf_counter(), sample.counter)
        )
        self.STATE.counters[sample.key] = sample.counter


class LoadTestSink(LoadTestReceiver):

    @ez.task
    async def terminate(self) -> None:
        ez.logger.info(f"Load test subscriber started. (PID: {os.getpid()})")

        # Wait for the duration of the load test
        await asyncio.sleep(self.SETTINGS.duration)
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
    """ one pub to many subs """
    connections: ez.NetworkDefinition = [(config.source.OUTPUT, config.sink.INPUT)]
    subs = [LoadTestReceiver(config.settings) for _ in range(config.n_clients)]
    for sub in subs:
        connections.append((config.source.OUTPUT, sub.INPUT))

    return subs, connections

def fanin(config: ConfigSettings) -> Configuration:
    """ many pubs to one sub """
    connections: ez.NetworkDefinition = [(config.source.OUTPUT, config.sink.INPUT)]
    pubs = [LoadTestSender(config.settings) for _ in range(config.n_clients)]
    for pub in pubs:
        connections.append((pub.OUTPUT, config.sink.INPUT))
    return pubs, connections


def relay(config: ConfigSettings) -> Configuration:
    """ one pub to one sub through many relays """
    connections: ez.NetworkDefinition = []

    relays = [LoadTestRelay(config.settings) for _ in range(config.n_clients)]
    if len(relays):
        connections.append((config.source.OUTPUT, relays[0].INPUT))
        for from_relay, to_relay in zip(relays[:-1], relays[1:]):
            connections.append((from_relay.OUTPUT, to_relay.INPUT))
        connections.append((relays[-1].OUTPUT, config.sink.INPUT))
    else: connections.append((config.source.OUTPUT, config.sink.INPUT))

    return relays, connections

CONFIGS: typing.Mapping[str, Configurator] = {
    c.__name__: c for c in [
        fanin, 
        fanout, 
        relay
    ]
}

class Communication(enum.StrEnum):
    LOCAL = "local"
    SHM = "shm"
    SHM_SPREAD = "shm_spread"
    TCP = "tcp"
    TCP_SPREAD = "tcp_spread"
    
def perform_test(
    n_clients: int,
    duration: float, 
    msg_size: int, 
    buffers: int,
    comms: Communication,
    config: Configurator
) -> Metrics:
    
    settings = LoadTestSettings(
        dynamic_size = int(msg_size), 
        duration = duration, 
        buffers = buffers, 
        force_tcp = (comms == Communication.TCP),
    )

    source = LoadTestSource(settings)
    sink = LoadTestSink(settings)
    
    components: typing.Mapping[str, ez.Component] = dict(
        SINK = sink,
    )

    clients, connections = config(ConfigSettings(n_clients, settings, source, sink))

    # The 'sink' MUST remain in this process for us to pull its state.
    process_components: typing.Iterable[ez.Component] = []
    if comms == Communication.LOCAL:
        # Every component in the same process (this one)
        components["SOURCE"] = source
        for i, client in enumerate(clients):
            components[f"CLIENT_{i+1}"] = client

    else:

        if comms in (Communication.SHM_SPREAD, Communication.TCP_SPREAD):
            # Every component in its own process.
            components["SOURCE"] = source
            process_components.append(source)
            for i, client in enumerate(clients):
                components[f'CLIENT_{i+1}'] = client
                process_components.append(client)

        else:
            # All clients and the source in ONE other process.
            collect_comps: typing.Mapping[str, ez.Component] = dict()
            collect_comps["SOURCE"] = source
            for i, client in enumerate(clients):
                collect_comps[f"CLIENT_{i+1}"] = client
            proc_collection = collect(components = collect_comps)
            components["PROC"] = proc_collection
            process_components = [proc_collection]

    ez.run(
        components = components,
        connections = connections,
        process_components = process_components,
    )

    return calculate_metrics(sink, duration)


def calculate_metrics(sink: LoadTestSink, duration: float) -> Metrics:

    # Log some useful summary statistics
    min_timestamp = min(timestamp for timestamp, _, _ in sink.STATE.received_data)
    max_timestamp = max(timestamp for timestamp, _, _ in sink.STATE.received_data)
    total_latency = abs(
        sum(
            receive_timestamp - send_timestamp
            for send_timestamp, receive_timestamp, _ in sink.STATE.received_data
        )
    )

    counters = list(sorted(t[2] for t in sink.STATE.received_data))
    dropped_samples = sum(
        [max((x1 - x0) - 1, 0) for x1, x0 in zip(counters[1:], counters[:-1])]
    )

    num_samples = len(sink.STATE.received_data)
    ez.logger.info(f"Samples received: {num_samples}")
    sample_rate = num_samples / duration
    ez.logger.info(f"Sample rate: {sample_rate} Hz")
    latency_mean = total_latency / num_samples
    ez.logger.info(f"Mean latency: {latency_mean} s")
    ez.logger.info(f"Total latency: {total_latency} s")

    total_data = num_samples * sink.SETTINGS.dynamic_size
    data_rate = total_data / (max_timestamp - min_timestamp)
    ez.logger.info(f"Data rate: {data_rate * 1e-6} MB/s")

    if dropped_samples:
        ez.logger.error(
            f"Dropped samples: {dropped_samples} ({dropped_samples / (dropped_samples + num_samples)}%)",
        )

    return Metrics(
        num_msgs = num_samples,
        sample_rate = sample_rate,
        latency_mean = latency_mean,
        latency_total = total_latency,
        data_rate = data_rate
    )


@dataclasses.dataclass
class TestParameters:
    msg_size: int
    n_clients: int
    config: str
    comms: str
    duration: float
    num_buffers: int


@dataclasses.dataclass
class TestLogEntry:
    params: TestParameters
    results: typing.List[Metrics]