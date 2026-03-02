import pytest

import ezmsg.core as ez

from ezmsg.core.backend import ExecutionContext
from ezmsg.core.graphmeta import (
    CollectionMetadata,
    InputRelayMetadata,
    InputStreamMetadata,
    OutputStreamMetadata,
    OutputTopicMetadata,
    UnitMetadata,
)


@pytest.mark.parametrize(
    "endpoint_factory",
    [
        lambda: ez.Topic(int),
        lambda: ez.InputTopic(int),
        lambda: ez.OutputTopic(int),
        lambda: ez.InputRelay(int),
        lambda: ez.OutputRelay(int),
    ],
)
def test_unit_rejects_topic_endpoints(endpoint_factory):
    with pytest.raises(TypeError, match="Units may only declare InputStream"):

        class BadUnit(ez.Unit):
            ENDPOINT = endpoint_factory()


def test_collection_stream_endpoint_warns_futurewarning():
    with pytest.warns(FutureWarning, match="deprecated"):

        class LegacyCollection(ez.Collection):
            INPUT = ez.InputStream(int)


class _Source(ez.Unit):
    OUTPUT = ez.OutputStream(int)


class _Sink(ez.Unit):
    INPUT = ez.InputStream(int)


class _TopicPassthrough(ez.Collection):
    IN = ez.InputTopic(int)
    OUT = ez.OutputTopic(int)

    def network(self) -> ez.NetworkDefinition:
        return ((self.IN, self.OUT),)


class _RelayInputPassthrough(ez.Collection):
    IN = ez.InputRelay(int, leaky=False, max_queue=None, copy_on_forward=True)
    OUT = ez.OutputTopic(int)

    def configure(self) -> None:
        self.IN.leaky = True
        self.IN.max_queue = 7

    def network(self) -> ez.NetworkDefinition:
        return ((self.IN, self.OUT),)


class _RelayOutputPassthrough(ez.Collection):
    IN = ez.InputTopic(int)
    OUT = ez.OutputRelay(int, num_buffers=16, force_tcp=True, copy_on_forward=False)

    def configure(self) -> None:
        self.OUT.num_buffers = 8

    def network(self) -> ez.NetworkDefinition:
        return ((self.IN, self.OUT),)


class _TopicSystem(ez.Collection):
    SOURCE = _Source()
    PASSTHROUGH = _TopicPassthrough()
    SINK = _Sink()

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.SOURCE.OUTPUT, self.PASSTHROUGH.IN),
            (self.PASSTHROUGH.OUT, self.SINK.INPUT),
        )


class _InputRelaySystem(ez.Collection):
    SOURCE = _Source()
    PASSTHROUGH = _RelayInputPassthrough()
    SINK = _Sink()

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.SOURCE.OUTPUT, self.PASSTHROUGH.IN),
            (self.PASSTHROUGH.OUT, self.SINK.INPUT),
        )


class _OutputRelaySystem(ez.Collection):
    SOURCE = _Source()
    PASSTHROUGH = _RelayOutputPassthrough()
    SINK = _Sink()

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.SOURCE.OUTPUT, self.PASSTHROUGH.IN),
            (self.PASSTHROUGH.OUT, self.SINK.INPUT),
        )


def test_input_output_topics_behave_as_shortcuts():
    system = _TopicSystem()
    ctx = ExecutionContext.setup({"SYSTEM": system})
    assert ctx is not None
    assert (system.SOURCE.OUTPUT.address, system.PASSTHROUGH.IN.address) in ctx.connections
    assert (system.PASSTHROUGH.IN.address, system.PASSTHROUGH.OUT.address) in ctx.connections
    assert (system.PASSTHROUGH.OUT.address, system.SINK.INPUT.address) in ctx.connections


def test_input_relay_rewrites_edges_and_syncs_settings():
    system = _InputRelaySystem()
    ctx = ExecutionContext.setup({"SYSTEM": system})
    assert ctx is not None

    relay = system.PASSTHROUGH.components["__relay_in_IN"]
    source = system.SOURCE.OUTPUT.address
    endpoint_in = system.PASSTHROUGH.IN.address
    endpoint_out = system.PASSTHROUGH.OUT.address
    sink = system.SINK.INPUT.address

    assert (source, endpoint_in) in ctx.connections
    assert (endpoint_in, relay.INPUT.address) in ctx.connections
    assert (relay.OUTPUT.address, endpoint_out) in ctx.connections
    assert (endpoint_out, sink) in ctx.connections
    assert (endpoint_in, endpoint_out) not in ctx.connections

    assert relay.SETTINGS.leaky is True
    assert relay.SETTINGS.max_queue == 7
    assert relay.SETTINGS.copy_on_forward is True


def test_output_relay_rewrites_edges_and_syncs_settings():
    system = _OutputRelaySystem()
    ctx = ExecutionContext.setup({"SYSTEM": system})
    assert ctx is not None

    relay = system.PASSTHROUGH.components["__relay_out_OUT"]
    source = system.SOURCE.OUTPUT.address
    endpoint_in = system.PASSTHROUGH.IN.address
    endpoint_out = system.PASSTHROUGH.OUT.address
    sink = system.SINK.INPUT.address

    assert (source, endpoint_in) in ctx.connections
    assert (endpoint_in, relay.INPUT.address) in ctx.connections
    assert (relay.OUTPUT.address, endpoint_out) in ctx.connections
    assert (endpoint_out, sink) in ctx.connections

    assert relay.SETTINGS.num_buffers == 8
    assert relay.SETTINGS.force_tcp is True
    assert relay.SETTINGS.copy_on_forward is False


def test_metadata_separates_collection_topics_relays_and_unit_streams():
    system = _InputRelaySystem()
    ctx = ExecutionContext.setup({"SYSTEM": system})
    assert ctx is not None

    runner = ez.GraphRunner(components={"SYSTEM": system})
    metadata = runner._component_metadata()

    passthrough_meta = metadata.components[system.PASSTHROUGH.address]
    assert isinstance(passthrough_meta, CollectionMetadata)
    assert "IN" in passthrough_meta.relays
    assert isinstance(passthrough_meta.relays["IN"], InputRelayMetadata)
    assert passthrough_meta.relays["IN"].leaky is True
    assert passthrough_meta.relays["IN"].max_queue == 7
    assert "OUT" in passthrough_meta.topics
    assert isinstance(passthrough_meta.topics["OUT"], OutputTopicMetadata)

    source_meta = metadata.components[system.SOURCE.address]
    sink_meta = metadata.components[system.SINK.address]
    assert isinstance(source_meta, UnitMetadata)
    assert isinstance(source_meta.streams["OUTPUT"], OutputStreamMetadata)
    assert isinstance(sink_meta, UnitMetadata)
    assert isinstance(sink_meta.streams["INPUT"], InputStreamMetadata)
