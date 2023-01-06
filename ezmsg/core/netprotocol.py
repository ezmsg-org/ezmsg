import asyncio
import enum
import os

from uuid import UUID
from dataclasses import field, dataclass
from contextlib import asynccontextmanager

from typing import Tuple, NamedTuple, Union, AsyncGenerator, Optional

VERSION = b'1'
UINT64_SIZE = 8
DEFAULT_SHM_SIZE = 2 ** 16
BYTEORDER = 'little'

GRAPHSERVER_PORT_ENV = 'EZMSG_GRAPHSERVER_PORT'
GRAPHSERVER_PORT_DEFAULT = 25978
GRAPHSERVER_PORT = int(os.getenv(GRAPHSERVER_PORT_ENV, GRAPHSERVER_PORT_DEFAULT))
GRAPHSERVER_ADDR = ('127.0.0.1', GRAPHSERVER_PORT)

# SHMServer must reside on localhost because it manages shared memory
# for local processes.  GraphServer may live elsewhere
SHMSERVER_PORT_ENV = 'EZMSG_SHMSERVER_PORT'
SHMSERVER_PORT_DEFAULT = 25979
SHMSERVER_PORT = int(os.getenv(SHMSERVER_PORT_ENV, SHMSERVER_PORT_DEFAULT))
SHMSERVER_ADDR = ('127.0.0.1', SHMSERVER_PORT)

PUBLISHER_START_PORT_ENV = 'EZMSG_PUBLISHER_PORT_START'
PUBLISHER_START_PORT_DEFAULT = 25980
PUBLISHER_START_PORT = int(os.getenv(PUBLISHER_START_PORT_ENV, PUBLISHER_START_PORT_DEFAULT))


class Address(NamedTuple):
    host: str
    port: int

    @classmethod
    async def from_stream(cls, reader: asyncio.StreamReader) -> "Address":
        address = await read_str(reader)
        return cls.from_string(address)

    @classmethod
    def from_string(cls, address: str) -> "Address":
        host, port = address.split(':')
        return cls(host, int(port))

    def to_stream(self, writer: asyncio.StreamWriter) -> None:
        writer.write(encode_str(str(self)))

    def __str__(self):
        return f'{self.host}:{self.port}'


AddressType = Union[Tuple[str, int], Address]


@dataclass
class ClientInfo:
    id: UUID
    writer: asyncio.StreamWriter
    pid: int
    topic: str

    _pending: asyncio.Event = field(default_factory=asyncio.Event, init = False)

    def __post_init__(self) -> None:
        self.set_sync()

    def set_sync(self) -> None:
        self._pending.set()

    @asynccontextmanager
    async def sync_writer(self) -> AsyncGenerator[asyncio.StreamWriter, None]:
        await self._pending.wait()
        try:
            yield self.writer
            await self.writer.drain()
            self._pending.clear()
            await self._pending.wait()
        finally:
            self._pending.set()
        

@dataclass
class PublisherInfo(ClientInfo):
    address: Address


@dataclass
class SubscriberInfo(ClientInfo):
    ...

def uint64_to_bytes(i: int) -> bytes:
    return i.to_bytes(UINT64_SIZE, BYTEORDER, signed=False)


def bytes_to_uint(b: bytes) -> int:
    return int.from_bytes(b, BYTEORDER, signed=False)


def encode_str(string: str) -> bytes:
    str_bytes = string.encode('utf-8')
    str_len_bytes = uint64_to_bytes(len(str_bytes))
    return str_len_bytes + str_bytes


async def read_int(reader: asyncio.StreamReader) -> int:
    raw = await reader.readexactly(UINT64_SIZE)
    return bytes_to_uint(raw)


async def read_str(reader: asyncio.StreamReader) -> str:
    str_size = await read_int(reader)
    str_bytes = await reader.readexactly(str_size)
    return str_bytes.decode('utf-8')


class Command(enum.Enum):
    def _generate_next_value_(name, start, count, last_values) -> bytes:
        return count.to_bytes(1, BYTEORDER, signed=False)

    COMPLETE = enum.auto()

    # GraphConnection Commands
    PUBLISH = enum.auto()
    SUBSCRIBE = enum.auto()
    CONNECT = enum.auto()
    DISCONNECT = enum.auto()
    CYCLIC = enum.auto()
    PAUSE = enum.auto()
    SYNC = enum.auto()
    RESUME = enum.auto()
    UPDATE = enum.auto()

    # Pub<->Sub Commands
    TX_LOCAL = enum.auto()
    TX_SHM = enum.auto()
    TX_TCP = enum.auto()
    RX_ACK = enum.auto()

    # SHMServer Commands
    SHM_CREATE = enum.auto()
    SHM_ATTACH = enum.auto()

    SHUTDOWN = enum.auto()

