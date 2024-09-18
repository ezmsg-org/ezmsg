import asyncio
import socket
import typing
import enum

from uuid import UUID
from dataclasses import field, dataclass
from contextlib import asynccontextmanager
from asyncio.base_events import Server

VERSION = b"1"
UINT64_SIZE = 8
DEFAULT_SHM_SIZE = 2**16
BYTEORDER = "little"

DEFAULT_HOST = "127.0.0.1"

GRAPHSERVER_ADDR_ENV = "EZMSG_GRAPHSERVER_ADDR"
GRAPHSERVER_PORT_DEFAULT = 25978

SHMSERVER_ADDR_ENV = "EZMSG_SHMSERVER_ADDR"
SHMSERVER_PORT_DEFAULT = 25979

RESERVED_PORTS = [GRAPHSERVER_PORT_DEFAULT, SHMSERVER_PORT_DEFAULT]

SERVER_PORT_START_ENV = "EZMSG_SERVER_PORT_START"
SERVER_PORT_START_DEFAULT = 10000

PUBLISHER_START_PORT_ENV = "EZMSG_PUBLISHER_PORT_START"
PUBLISHER_START_PORT_DEFAULT = 25980


class Address(typing.NamedTuple):
    host: str
    port: int

    @classmethod
    async def from_stream(cls, reader: asyncio.StreamReader) -> "Address":
        address = await read_str(reader)
        return cls.from_string(address)

    @classmethod
    def from_string(cls, address: str) -> "Address":
        host, port = address.split(":")
        return cls(host, int(port))

    def to_stream(self, writer: asyncio.StreamWriter) -> None:
        writer.write(encode_str(str(self)))

    def bind_socket(self) -> socket.socket:
        return create_socket(self.host, self.port)

    def __str__(self):
        return f"{self.host}:{self.port}"


AddressType = typing.Union[typing.Tuple[str, int], Address]


@dataclass
class ClientInfo:
    id: UUID
    writer: asyncio.StreamWriter
    pid: int
    topic: str

    _pending: asyncio.Event = field(default_factory=asyncio.Event, init=False)

    def __post_init__(self) -> None:
        self.set_sync()

    def set_sync(self) -> None:
        self._pending.set()

    @asynccontextmanager
    async def sync_writer(self) -> typing.AsyncGenerator[asyncio.StreamWriter, None]:
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
class SubscriberInfo(ClientInfo): ...


def uint64_to_bytes(i: int) -> bytes:
    return i.to_bytes(UINT64_SIZE, BYTEORDER, signed=False)


def bytes_to_uint(b: bytes) -> int:
    return int.from_bytes(b, BYTEORDER, signed=False)


def encode_str(string: str) -> bytes:
    str_bytes = string.encode("utf-8")
    str_len_bytes = uint64_to_bytes(len(str_bytes))
    return str_len_bytes + str_bytes


async def read_int(reader: asyncio.StreamReader) -> int:
    raw = await reader.readexactly(UINT64_SIZE)
    return bytes_to_uint(raw)


async def read_str(reader: asyncio.StreamReader) -> str:
    str_size = await read_int(reader)
    str_bytes = await reader.readexactly(str_size)
    return str_bytes.decode("utf-8")


async def close_stream_writer(writer: asyncio.StreamWriter):
    writer.close()
    # ConnectionResetError can be raised on wait_closed.
    # See: https://github.com/python/cpython/issues/83037
    try:
        await writer.wait_closed()
    except (ConnectionResetError, BrokenPipeError):
        pass


async def close_server(server: Server):
    server.close()
    # ConnectionResetError can be raised on wait_closed.
    # See: https://github.com/python/cpython/issues/83037
    try:
        await server.wait_closed()
    except (ConnectionResetError, BrokenPipeError):
        pass


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
    DAG = enum.auto()

    # Pub<->Sub Commands
    TX_LOCAL = enum.auto()
    TX_SHM = enum.auto()
    TX_TCP = enum.auto()
    RX_ACK = enum.auto()

    # SHMServer Commands
    SHM_CREATE = enum.auto()
    SHM_ATTACH = enum.auto()

    SHUTDOWN = enum.auto()


def create_socket(
    host: typing.Optional[str] = None,
    port: typing.Optional[int] = None,
    start_port: int = 0,
    max_port: int = 65535,
    ignore_ports: typing.List[int] = RESERVED_PORTS,
) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    if host is None:
        host = DEFAULT_HOST

    if port is not None:
        sock.bind((host, port))
        return sock

    port = start_port
    while port <= max_port:
        if port not in ignore_ports:
            try:
                sock.bind((host, port))
                return sock
            except OSError:
                pass
        port += 1

    raise IOError("Failed to bind socket; no free ports")
