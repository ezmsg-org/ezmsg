import pytest

import ezmsg.core as ez

from ezmsg.core.backend import ExecutionContext
from ezmsg.core.graphmeta import (
    CollectionMetadata,
    InputRelayMetadata,
    InputStreamMetadata,
    OutputRelayMetadata,
    OutputStreamMetadata,
    OutputTopicMetadata,
    UnitMetadata,
)
from ez_test_utils import (
    MessageGenerator,
    MessageGeneratorSettings,
    MessageReceiver,
    MessageReceiverSettings,
    get_test_fn,
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


class _RuntimeInputRelaySystem(ez.Collection):
    SOURCE = MessageGenerator()
    PASSTHROUGH = _RelayInputPassthrough()
    SINK = MessageReceiver()

    def configure(self) -> None:
        self.SOURCE.apply_settings(MessageGeneratorSettings(num_msgs=3))
        self.SINK.apply_settings(MessageReceiverSettings(num_msgs=3, output_fn=str(self.SETTINGS.output_fn)))

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.SOURCE.OUTPUT, self.PASSTHROUGH.IN),
            (self.PASSTHROUGH.OUT, self.SINK.INPUT),
        )


class _RuntimeOutputRelaySystem(ez.Collection):
    SOURCE = MessageGenerator()
    PASSTHROUGH = _RelayOutputPassthrough()
    SINK = MessageReceiver()

    def configure(self) -> None:
        self.SOURCE.apply_settings(MessageGeneratorSettings(num_msgs=3))
        self.SINK.apply_settings(MessageReceiverSettings(num_msgs=3, output_fn=str(self.SETTINGS.output_fn)))

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

    source = system.SOURCE.OUTPUT.address
    endpoint_in = system.PASSTHROUGH.IN.address
    endpoint_out = system.PASSTHROUGH.OUT.address
    sink = system.SINK.INPUT.address
    relay_input = f"{system.PASSTHROUGH.address}/__relays__/IN/INPUT"
    relay_output = f"{system.PASSTHROUGH.address}/__relays__/IN/OUTPUT"

    assert (source, endpoint_in) in ctx.connections
    assert (endpoint_in, relay_input) in ctx.connections
    assert (relay_output, endpoint_out) in ctx.connections
    assert (endpoint_out, sink) in ctx.connections
    assert (endpoint_in, endpoint_out) not in ctx.connections

    assert "__relay_in_IN" not in system.PASSTHROUGH.components
    relay = ctx._process_specs[0].relays[0]
    assert relay.kind == "input"
    assert relay.leaky is True
    assert relay.max_queue == 7
    assert relay.copy_on_forward is True


def test_output_relay_rewrites_edges_and_syncs_settings():
    system = _OutputRelaySystem()
    ctx = ExecutionContext.setup({"SYSTEM": system})
    assert ctx is not None

    source = system.SOURCE.OUTPUT.address
    endpoint_in = system.PASSTHROUGH.IN.address
    endpoint_out = system.PASSTHROUGH.OUT.address
    sink = system.SINK.INPUT.address
    relay_input = f"{system.PASSTHROUGH.address}/__relays__/OUT/INPUT"
    relay_output = f"{system.PASSTHROUGH.address}/__relays__/OUT/OUTPUT"

    assert (source, endpoint_in) in ctx.connections
    assert (endpoint_in, relay_input) in ctx.connections
    assert (relay_output, endpoint_out) in ctx.connections
    assert (endpoint_out, sink) in ctx.connections

    assert "__relay_out_OUT" not in system.PASSTHROUGH.components
    relay = ctx._process_specs[0].relays[0]
    assert relay.kind == "output"
    assert relay.num_buffers == 8
    assert relay.force_tcp is True
    assert relay.copy_on_forward is False


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
    assert passthrough_meta.relays["IN"].relay_group == "SYSTEM/PASSTHROUGH/__relays__/IN"
    assert passthrough_meta.relays["IN"].relay_input_topic == "SYSTEM/PASSTHROUGH/__relays__/IN/INPUT"
    assert passthrough_meta.relays["IN"].relay_output_topic == "SYSTEM/PASSTHROUGH/__relays__/IN/OUTPUT"
    assert "OUT" in passthrough_meta.topics
    assert isinstance(passthrough_meta.topics["OUT"], OutputTopicMetadata)
    assert passthrough_meta.children == []

    output_system = _OutputRelaySystem()
    assert ExecutionContext.setup({"SYSTEM": output_system}) is not None
    output_metadata = ez.GraphRunner(components={"SYSTEM": output_system})._component_metadata()
    output_passthrough_meta = output_metadata.components[output_system.PASSTHROUGH.address]
    assert isinstance(output_passthrough_meta, CollectionMetadata)
    assert isinstance(output_passthrough_meta.relays["OUT"], OutputRelayMetadata)
    assert output_passthrough_meta.relays["OUT"].relay_group == "SYSTEM/PASSTHROUGH/__relays__/OUT"

    source_meta = metadata.components[system.SOURCE.address]
    sink_meta = metadata.components[system.SINK.address]
    assert isinstance(source_meta, UnitMetadata)
    assert isinstance(source_meta.streams["OUTPUT"], OutputStreamMetadata)
    assert isinstance(sink_meta, UnitMetadata)
    assert isinstance(sink_meta.streams["INPUT"], InputStreamMetadata)


@pytest.mark.parametrize(
    "system_type",
    [_RuntimeInputRelaySystem, _RuntimeOutputRelaySystem],
)
def test_relays_forward_messages_at_runtime(system_type):
    with get_test_fn() as output_fn:
        class RuntimeRelaySettings(ez.Settings):
            output_fn: str

        class RuntimeRelaySystem(system_type):
            SETTINGS = RuntimeRelaySettings

        ez.run(
            SYSTEM=RuntimeRelaySystem(
                RuntimeRelaySettings(output_fn=str(output_fn))
            )
        )

        with open(output_fn, "r") as stream:
            assert len(stream.readlines()) == 3
