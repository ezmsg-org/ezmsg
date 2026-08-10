"""Cross-process SHM grow / reattach coverage.

When a published message exceeds the publisher's shared-memory ``buf_size``,
``PublisherClient.broadcast`` grows the segment (allocate ``total_size*2``, copy
buffers, swap) and hands subscribers a new SHM name. Each subscriber must detach
from the old segment and reattach to the new one mid-stream
(``MessageChannel.monitor``). Historically that reattach path had bare
``assert``s that, on any transient skew, escaped the receive loop and silently
killed the channel task -> the subscriber stalled forever.

These tests drive a real cross-process publisher/subscriber with a deliberately
small ``buf_size`` and a mid-stream oversized message, and assert every message
is still delivered (the grow does not wedge the stream). Small messages before
the grow populate the cache so the reattach repopulation loop is exercised too.
"""

from dataclasses import dataclass
from collections.abc import AsyncGenerator

import json

import pytest

import ezmsg.core as ez

from ez_test_utils import get_test_fn


# A payload whose serialized size we can control via the bytes field.
@dataclass
class BlobMessage:
    seq: int
    payload: bytes


class BlobGeneratorSettings(ez.Settings):
    sizes: tuple[int, ...]
    """Per-message payload byte counts; a value above ``buf_size`` forces a grow."""

    buf_size: int
    num_buffers: int


class BlobGenerator(ez.Unit):
    SETTINGS = BlobGeneratorSettings

    # buf_size is intentionally small so a large payload triggers a grow.
    OUTPUT = ez.OutputStream(
        BlobMessage,
        num_buffers=4,
        buf_size=4096,
        allow_local=False,  # keep cross-process on the SHM path, not local fast path
    )

    async def initialize(self) -> None:
        # Apply the parametrized transport sizing on the stream instance.
        self.OUTPUT.buf_size = self.SETTINGS.buf_size
        self.OUTPUT.num_buffers = self.SETTINGS.num_buffers

    @ez.publisher(OUTPUT)
    async def spawn(self) -> AsyncGenerator:
        for seq, size in enumerate(self.SETTINGS.sizes):
            yield self.OUTPUT, BlobMessage(seq=seq, payload=b"x" * size)
        raise ez.Complete


class BlobReceiverSettings(ez.Settings):
    num_msgs: int
    output_fn: str


class BlobReceiverState(ez.State):
    num_received: int = 0


class BlobReceiver(ez.Unit):
    STATE = BlobReceiverState
    SETTINGS = BlobReceiverSettings

    INPUT = ez.InputStream(BlobMessage)

    @ez.subscriber(INPUT)
    async def on_message(self, msg: BlobMessage) -> None:
        self.STATE.num_received += 1
        with open(self.SETTINGS.output_fn, "a") as output_file:
            output_file.write(
                json.dumps({"seq": msg.seq, "len": len(msg.payload)}) + "\n"
            )
        if self.STATE.num_received == self.SETTINGS.num_msgs:
            raise ez.Complete


class GrowSystemSettings(ez.Settings):
    sizes: tuple[int, ...]
    buf_size: int
    num_buffers: int
    output_fn: str


class GrowSystem(ez.Collection):
    SETTINGS = GrowSystemSettings

    PUB = BlobGenerator()
    SUB = BlobReceiver()

    def configure(self) -> None:
        self.PUB.apply_settings(
            BlobGeneratorSettings(
                sizes=self.SETTINGS.sizes,
                buf_size=self.SETTINGS.buf_size,
                num_buffers=self.SETTINGS.num_buffers,
            )
        )
        self.SUB.apply_settings(
            BlobReceiverSettings(
                num_msgs=len(self.SETTINGS.sizes),
                output_fn=self.SETTINGS.output_fn,
            )
        )

    def network(self) -> ez.NetworkDefinition:
        return ((self.PUB.OUTPUT, self.SUB.INPUT),)

    def process_components(self):
        # Force PUB and SUB into separate processes so the boundary uses the
        # cross-process SHM transport (and thus the grow/reattach path).
        return (self.PUB, self.SUB)


@pytest.mark.parametrize(
    "sizes",
    [
        # small, small, GROW (exceeds buf_size), small, small
        (8, 8, 16384, 8, 8),
        # two successive grows of increasing size -> two reattaches
        (8, 16384, 8, 65536, 8),
        # grow on the very first message (empty cache to repopulate)
        (16384, 8, 8),
    ],
)
def test_cross_process_grow_delivers_all(sizes):
    with get_test_fn() as test_filename:
        system = GrowSystem(
            GrowSystemSettings(
                sizes=sizes,
                buf_size=4096,
                num_buffers=4,
                output_fn=str(test_filename),
            )
        )
        ez.run(SYSTEM=system)

        results = []
        with open(test_filename, "r") as file:
            for line in file:
                results.append(json.loads(line))

        # Every message survived the mid-stream grow: none dropped, in order.
        assert [r["seq"] for r in results] == list(range(len(sizes)))
        assert [r["len"] for r in results] == list(sizes)
