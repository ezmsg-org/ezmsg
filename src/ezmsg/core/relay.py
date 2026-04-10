from dataclasses import dataclass
from typing import Literal

from .netprotocol import DEFAULT_SHM_SIZE
from .stream import InputRelay, OutputRelay

_RELAY_GROUP = "__relays__"


@dataclass(frozen=True, slots=True)
class _RelayRuntimeInfo:
    group: str
    input_topic: str
    output_topic: str


@dataclass(frozen=True, slots=True)
class _RelayRuntime:
    kind: Literal["input", "output"]
    endpoint_topic: str
    collection_address: str
    relay_group: str
    relay_input_topic: str
    relay_output_topic: str
    leaky: bool
    max_queue: int | None
    host: str | None
    port: int | None
    num_buffers: int
    buf_size: int
    force_tcp: bool
    copy_on_forward: bool


def _relay_runtime_info(endpoint: InputRelay | OutputRelay) -> _RelayRuntimeInfo:
    group = "/".join(endpoint.location + [_RELAY_GROUP, endpoint.name])
    return _RelayRuntimeInfo(
        group=group,
        input_topic=f"{group}/INPUT",
        output_topic=f"{group}/OUTPUT",
    )


def _relay_runtime(endpoint: InputRelay | OutputRelay) -> _RelayRuntime:
    runtime = _relay_runtime_info(endpoint)

    if isinstance(endpoint, InputRelay):
        return _RelayRuntime(
            kind="input",
            endpoint_topic=endpoint.address,
            collection_address="/".join(endpoint.location),
            relay_group=runtime.group,
            relay_input_topic=runtime.input_topic,
            relay_output_topic=runtime.output_topic,
            leaky=endpoint.leaky,
            max_queue=endpoint.max_queue,
            host=None,
            port=None,
            num_buffers=32,
            buf_size=DEFAULT_SHM_SIZE,
            force_tcp=False,
            copy_on_forward=endpoint.copy_on_forward,
        )

    return _RelayRuntime(
        kind="output",
        endpoint_topic=endpoint.address,
        collection_address="/".join(endpoint.location),
        relay_group=runtime.group,
        relay_input_topic=runtime.input_topic,
        relay_output_topic=runtime.output_topic,
        leaky=False,
        max_queue=None,
        host=endpoint.host,
        port=endpoint.port,
        num_buffers=endpoint.num_buffers,
        buf_size=endpoint.buf_size,
        force_tcp=endpoint.force_tcp,
        copy_on_forward=endpoint.copy_on_forward,
    )
