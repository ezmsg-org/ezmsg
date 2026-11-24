from collections.abc import AsyncGenerator
from dataclasses import dataclass

import json
import pickle

import ezmsg.core as ez


@dataclass
class BlobMessage:
    seq: int
    payload: bytes


class BlobGeneratorSettings(ez.Settings):
    sizes: tuple[int, ...]
    buf_size: int
    num_buffers: int


class BlobGenerator(ez.Unit):
    SETTINGS = BlobGeneratorSettings

    OUTPUT = ez.OutputStream(
        BlobMessage,
        num_buffers=4,
        buf_size=4096,
        allow_local=False,
    )

    async def initialize(self) -> None:
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
        return (self.PUB, self.SUB)


class ViewPayload:
    """
    Pickles its data out-of-band, so on receipt ``data`` is a memoryview into
    the channel's SHM (like a zero-copy numpy array) rather than a copy.
    """

    def __init__(self, data) -> None:
        # memoryview() registers a fresh export on the underlying buffer, so a
        # retained ViewPayload pins the SHM mapping just like an ndarray does.
        self.data = memoryview(data)

    def __reduce_ex__(self, protocol):
        return (ViewPayload, (pickle.PickleBuffer(self.data),))


class ViewGenerator(ez.Unit):
    SETTINGS = BlobGeneratorSettings

    OUTPUT = ez.OutputStream(
        ViewPayload,
        num_buffers=4,
        buf_size=4096,
        allow_local=False,
    )

    async def initialize(self) -> None:
        self.OUTPUT.buf_size = self.SETTINGS.buf_size
        self.OUTPUT.num_buffers = self.SETTINGS.num_buffers

    @ez.publisher(OUTPUT)
    async def spawn(self) -> AsyncGenerator:
        for seq, size in enumerate(self.SETTINGS.sizes):
            yield self.OUTPUT, ViewPayload(bytes([seq]) * size)
        raise ez.Complete


class RetainingReceiverState(ez.State):
    num_received: int = 0
    retained: list | None = None


class RetainingReceiver(ez.Unit):
    """Keeps every received message past its callback (issue #272)."""

    STATE = RetainingReceiverState
    SETTINGS = BlobReceiverSettings

    INPUT = ez.InputStream(ViewPayload)

    async def initialize(self) -> None:
        self.STATE.retained = []

    @ez.subscriber(INPUT)
    async def on_message(self, msg: ViewPayload) -> None:
        self.STATE.num_received += 1
        self.STATE.retained.append(msg)
        with open(self.SETTINGS.output_fn, "a") as output_file:
            output_file.write(
                json.dumps(
                    {
                        "seq": msg.data[0],
                        "len": len(msg.data),
                        "readonly": msg.data.readonly,
                    }
                )
                + "\n"
            )
        if self.STATE.num_received == self.SETTINGS.num_msgs:
            raise ez.Complete


class RetainingGrowSystem(GrowSystem):
    PUB = ViewGenerator()
    SUB = RetainingReceiver()
