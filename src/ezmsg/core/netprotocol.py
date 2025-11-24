import asyncio
from collections.abc import AsyncGenerator
import socket
import typing
import enum
import os

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

RESERVED_PORTS = [GRAPHSERVER_PORT_DEFAULT]

SERVER_PORT_START_ENV = "EZMSG_SERVER_PORT_START"
SERVER_PORT_START_DEFAULT = 10000

PUBLISHER_START_PORT_ENV = "EZMSG_PUBLISHER_PORT_START"
PUBLISHER_START_PORT_DEFAULT = 25980

GRAPHSERVER_ADDR = os.environ.get(
    GRAPHSERVER_ADDR_ENV, f"{DEFAULT_HOST}:{GRAPHSERVER_PORT_DEFAULT}"
)


class Address(typing.NamedTuple):
    """
    Network address representation with host and port.

    Provides utility methods for address parsing, serialization,
    and socket binding operations.
    """

    host: str
    port: int

    @classmethod
    async def from_stream(cls, reader: asyncio.StreamReader) -> "Address":
        """
        Read an Address from an async stream.

        :param reader: Stream reader to read address string from.
        :type reader: asyncio.StreamReader
        :return: Parsed Address instance.
        :rtype: Address
        """
        address = await read_str(reader)
        return cls.from_string(address)

    @classmethod
    def from_string(cls, address: str) -> "Address":
        """
        Parse an Address from a string representation.

        :param address: Address string in "host:port" format.
        :type address: str
        :return: Parsed Address instance.
        :rtype: Address
        """
        host, port = address.split(":")
        return cls(host, int(port))

    def to_stream(self, writer: asyncio.StreamWriter) -> None:
        """
        Write this address to an async stream.

        :param writer: Stream writer to send address string to.
        :type writer: asyncio.StreamWriter
        """
        writer.write(encode_str(str(self)))

    def bind_socket(self) -> socket.socket:
        """
        Create and bind a socket to this address.

        :return: Socket bound to this address.
        :rtype: socket.socket
        :raises IOError: If no free ports are available.
        """
        return create_socket(self.host, self.port)

    def __str__(self):
        return f"{self.host}:{self.port}"


AddressType = tuple[str, int] | Address


@dataclass
class ClientInfo:
    """
    Base information for client connections.

    Tracks client identification, communication writer, and provides
    synchronized access to the writer for thread-safe operations.
    """

    id: UUID
    writer: asyncio.StreamWriter

    _pending: asyncio.Event = field(default_factory=asyncio.Event, init=False)

    def __post_init__(self) -> None:
        self.set_sync()

    def set_sync(self) -> None:
        self._pending.set()

    @asynccontextmanager
    async def sync_writer(self) -> AsyncGenerator[asyncio.StreamWriter, None]:
        """
        Get synchronized access to the writer.

        Ensures thread-safe access to the stream writer by coordinating
        access through an asyncio Event mechanism.

        :return: Context manager yielding the synchronized writer.
        :rtype: collections.abc.AsyncGenerator[asyncio.StreamWriter, None]
        """
        await self._pending.wait()
        try:
            yield self.writer
            self._pending.clear()
            await self._pending.wait()
        finally:
            self._pending.set()


@dataclass
class PublisherInfo(ClientInfo):
    """
    Publisher-specific client information.
    Extends ClientInfo with the publisher's network address.
    """

    topic: str
    address: Address


@dataclass
class SubscriberInfo(ClientInfo):
    """
    Subscriber-specific client information.
    """

    topic: str


@dataclass
class ChannelInfo(ClientInfo):
    """
    Channel-specific client information.
    """

    pub_id: UUID


def uint64_to_bytes(i: int) -> bytes:
    """
    Convert a 64-bit unsigned integer to bytes.

    :param i: Integer value to convert.
    :type i: int
    :return: Byte representation in little-endian format.
    :rtype: bytes
    """
    return i.to_bytes(UINT64_SIZE, BYTEORDER, signed=False)


def bytes_to_uint(b: bytes) -> int:
    """
    Convert bytes to a 64-bit unsigned integer.

    :param b: Byte data to convert.
    :type b: bytes
    :return: Integer value decoded from little-endian bytes.
    :rtype: int
    """
    return int.from_bytes(b, BYTEORDER, signed=False)


def encode_str(string: str) -> bytes:
    """
    Encode a string with length prefix for network transmission.

    :param string: String to encode.
    :type string: str
    :return: Length-prefixed UTF-8 encoded bytes.
    :rtype: bytes
    """
    str_bytes = string.encode("utf-8")
    str_len_bytes = uint64_to_bytes(len(str_bytes))
    return str_len_bytes + str_bytes


async def read_int(reader: asyncio.StreamReader) -> int:
    """
    Read a 64-bit unsigned integer from an async stream.

    :param reader: Stream reader to read from.
    :type reader: asyncio.StreamReader
    :return: Integer value read from stream.
    :rtype: int
    :raises asyncio.IncompleteReadError: If stream ends before reading complete integer.
    """
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
    """
    Enumeration of protocol commands for ezmsg network communication.

    Defines all command types used in the ezmsg protocol for graph management,
    publisher-subscriber communication, and shared memory operations.
    """

    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> bytes:
        """
        Generate byte values for enum members.

        :param name: Name of the enum member.
        :type name: str
        :param start: Starting value (unused).
        :param count: Current count for automatic value generation.
        :type count: int
        :param last_values: Previously generated values (unused).
        :return: Byte representation of the command.
        :rtype: bytes
        """
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

    # SHM Commands
    SHM_CREATE = enum.auto()
    SHM_ATTACH = enum.auto()

    SHUTDOWN = enum.auto()

    CHANNEL = enum.auto()
    SHM_OK = enum.auto()
    SHM_ATTACH_FAILED = enum.auto()


def create_socket(
    host: str | None = None,
    port: int | None = None,
    start_port: int = 0,
    max_port: int = 65535,
    ignore_ports: list[int] = RESERVED_PORTS,
) -> socket.socket:
    """
    Create a socket bound to an available port.

    Attempts to bind to the specified port, or searches for an available
    port within the given range if no specific port is provided.

    :param host: Host address to bind to (defaults to DEFAULT_HOST).
    :type host: str | None
    :param port: Specific port to bind to (if None, searches for available port).
    :type port: int | None
    :param start_port: Starting port for search range.
    :type start_port: int
    :param max_port: Maximum port for search range.
    :type max_port: int
    :param ignore_ports: List of ports to skip during search.
    :type ignore_ports: list[int]
    :return: Bound socket ready for use.
    :rtype: socket.socket
    :raises IOError: If no available ports can be found in the specified range.
    """

    if host is None:
        host = DEFAULT_HOST

    if port is not None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # set REUSEADDR if a port is explicitly requested; leads to quick server restart on same port
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.bind((host, port))
        return sock

    port = start_port
    while port <= max_port:
        if port not in ignore_ports:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                # setting REUSEADDR during portscan can lead to race conditions during bind on Linux
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.bind((host, port))
                return sock
            except OSError:
                sock.close()
                pass

        port += 1

    raise IOError("Failed to bind socket; no free ports")
