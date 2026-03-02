from copy import deepcopy
from collections.abc import AsyncGenerator
from typing import Any

from .settings import Settings
from .netprotocol import DEFAULT_SHM_SIZE
from .stream import InputStream, OutputStream
from .unit import Unit, publisher, subscriber


class _RelaySettings(Settings):
    leaky: bool = False
    max_queue: int | None = None
    host: str | None = None
    port: int | None = None
    num_buffers: int = 32
    buf_size: int = DEFAULT_SHM_SIZE
    force_tcp: bool = False
    copy_on_forward: bool = True


class _CollectionRelayUnit(Unit):
    SETTINGS = _RelaySettings

    INPUT = InputStream(Any)
    OUTPUT = OutputStream(Any)

    async def initialize(self) -> None:
        self.INPUT.leaky = self.SETTINGS.leaky
        self.INPUT.max_queue = self.SETTINGS.max_queue
        self.OUTPUT.host = self.SETTINGS.host
        self.OUTPUT.port = self.SETTINGS.port
        self.OUTPUT.num_buffers = self.SETTINGS.num_buffers
        self.OUTPUT.buf_size = self.SETTINGS.buf_size
        self.OUTPUT.force_tcp = self.SETTINGS.force_tcp

    @subscriber(INPUT)
    @publisher(OUTPUT)
    async def relay(self, msg: Any) -> AsyncGenerator:
        if self.SETTINGS.copy_on_forward:
            msg = deepcopy(msg)
        yield self.OUTPUT, msg
