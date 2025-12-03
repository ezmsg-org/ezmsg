import asyncio
import typing
import logging
import enum
import os

from uuid import UUID

from .netprotocol import (
    Command,
    uint64_to_bytes,
    encode_str,
)

logger = logging.getLogger("ezmsg")


class TCPMessage(typing.NamedTuple):
    msg_id: int
    tcp_data: bytes


class SHMMessage(typing.NamedTuple):
    msg_id: int
    shm_name: str


class TransmitMode(enum.Enum):
    AUTO = enum.auto()
    LOCAL = enum.auto()
    SHM = enum.auto()
    TCP = enum.auto()


class PublisherProtocolState(enum.Enum):
    # Handshake states
    HANDSHAKE_SHM_NAME_LEN = enum.auto()
    HANDSHAKE_SHM_NAME_DATA = enum.auto()
    HANDSHAKE_COMPLETE = enum.auto()
    HANDSHAKE_NUM_BUFFERS = enum.auto()
    HANDSHAKE_MODE = enum.auto()
    # Message states
    COMMAND = enum.auto()
    MSG_ID = enum.auto()
    MSG_SIZE = enum.auto()
    MSG_DATA = enum.auto()

PublisherMessage = SHMMessage | TCPMessage
T = typing.TypeVar('T', SHMMessage, TCPMessage)

class PublisherClientProtocol(typing.Generic[T], asyncio.Protocol):
    """High-performance protocol for publisher message stream"""

    channel_id: str
    message_queue: asyncio.Queue[PublisherMessage]
    current_msg_id: int
    mode: TransmitMode

    def __init__(
        self,
        uuid: UUID,
        message_queue: asyncio.Queue[T],
    ):
        self.channel_id = str(uuid)
        self.message_queue = message_queue
        self.transport: asyncio.Transport | None = None

        # Pre-allocate buffer with reasonable size (1MB)
        # This avoids repeated reallocations during growth
        self.buffer = bytearray(1024 * 1024)
        self.buffer_size = 0  # Actual data in buffer
        self.buffer_offset = 0  # Read position

        # Handshake state
        self.handshake_complete = asyncio.Future()
        self.handshake_shm_name = asyncio.Future()
        self.num_buffers: int | None = None

        # Parser state machine - start in handshake mode
        self.state = PublisherProtocolState.HANDSHAKE_SHM_NAME_LEN
        self.expected_bytes = 8  # Expecting shm_name length

        # Message parsing state
        self.current_msg_id: int = -1
        self.mode: TransmitMode = TransmitMode.AUTO

    def connection_made(self, transport: asyncio.Transport) -> None:  # type: ignore
        self.transport = transport
        logger.debug("PublisherClientProtocol connected")
        # Send initial handshake
        self.transport.write(Command.CHANNEL.value)
        self.transport.write(encode_str(self.channel_id))

    def send_handshake_response(self, shm_ok: bool, pid: int) -> None:
        """Send SHM attachment response after Channel attaches"""
        assert self.transport is not None
        if shm_ok:
            self.transport.write(Command.SHM_OK.value)
        else:
            self.transport.write(Command.SHM_ATTACH_FAILED.value)
        self.transport.write(uint64_to_bytes(pid))
        # Resume parsing - might have buffered data
        while self._try_parse():
            pass

    def data_received(self, data: bytes) -> None:
        """Hot path - called directly by event loop"""
        data_len = len(data)

        # Ensure buffer has space
        if self.buffer_size + data_len > len(self.buffer):
            # Compact buffer if offset is large enough
            if self.buffer_offset > len(self.buffer) // 2:
                remaining = self.buffer_size - self.buffer_offset
                self.buffer[0:remaining] = self.buffer[
                    self.buffer_offset : self.buffer_size
                ]
                self.buffer_offset = 0
                self.buffer_size = remaining
            else:
                # Grow buffer
                new_size = max(len(self.buffer) * 2, self.buffer_size + data_len)
                self.buffer.extend(bytearray(new_size - len(self.buffer)))

        # Copy data into buffer
        self.buffer[self.buffer_size : self.buffer_size + data_len] = data
        self.buffer_size += data_len

        # Process as many complete messages as possible
        while self._try_parse():
            pass

    def _try_parse(self) -> bool:
        """State machine parser - returns True if made progress"""
        available = self.buffer_size - self.buffer_offset
        if available < self.expected_bytes:
            return False

        # Handshake states
        if self.state == PublisherProtocolState.HANDSHAKE_SHM_NAME_LEN:
            name_len = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.expected_bytes = name_len
            self.state = PublisherProtocolState.HANDSHAKE_SHM_NAME_DATA
            return True

        elif self.state == PublisherProtocolState.HANDSHAKE_SHM_NAME_DATA:
            shm_name = self.buffer[
                self.buffer_offset : self.buffer_offset + self.expected_bytes
            ].decode("utf-8")
            self.buffer_offset += self.expected_bytes
            self.handshake_shm_name.set_result(shm_name)
            self.state = PublisherProtocolState.HANDSHAKE_COMPLETE
            self.expected_bytes = 1
            return False  # Pause until handshake response is sent

        elif self.state == PublisherProtocolState.HANDSHAKE_COMPLETE:
            complete_byte = self.buffer[self.buffer_offset]
            self.buffer_offset += 1
            if complete_byte != Command.COMPLETE.value[0]:
                logger.error("Handshake failed: did not receive COMPLETE")
                if self.transport:
                    self.transport.close()
                return False
            self.state = PublisherProtocolState.HANDSHAKE_NUM_BUFFERS
            self.expected_bytes = 8
            return True

        elif self.state == PublisherProtocolState.HANDSHAKE_NUM_BUFFERS:
            num_buffers = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.num_buffers = num_buffers
            self.state = PublisherProtocolState.HANDSHAKE_MODE
            self.expected_bytes = 1
            return True

        elif self.state == PublisherProtocolState.HANDSHAKE_MODE:
            self.mode = TransmitMode(self.buffer[self.buffer_offset])
            self.buffer_offset += 1
            self.state = PublisherProtocolState.MSG_ID
            self.expected_bytes = 8  # uint64
            self.handshake_complete.set_result(self.num_buffers)
            return True

        # Message states
        elif self.state == PublisherProtocolState.MSG_ID:
            msg_id = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.current_msg_id = msg_id
            self.state = PublisherProtocolState.MSG_SIZE
            self.expected_bytes = 8
            return True

        elif self.state == PublisherProtocolState.MSG_SIZE:
            msg_size = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.expected_bytes = msg_size
            self.state = PublisherProtocolState.MSG_DATA
            return True

        elif self.state == PublisherProtocolState.MSG_DATA:
            msg_data = self.buffer[
                self.buffer_offset : self.buffer_offset + self.expected_bytes
            ]

            self.buffer_offset += self.expected_bytes

            if self.mode == TransmitMode.SHM:
                self.message_queue.put_nowait(
                    SHMMessage(
                        msg_id=self.current_msg_id,
                        shm_name=msg_data.decode("utf-8"),
                    )
                )

            elif self.mode == TransmitMode.TCP:
                self.message_queue.put_nowait(
                    TCPMessage(
                        msg_id=self.current_msg_id,
                        tcp_data=msg_data
                    )
                )

            else:
                logger.warning(
                    f"{self.channel_id}:{self.current_msg_id} dropped "
                    f"unknown TransmitMode: {self.mode}"
                )

            self.state = PublisherProtocolState.MSG_ID
            self.expected_bytes = 8  # uint64
            return True

        return False


    def connection_lost(self, exc: Exception | None) -> None:
        if exc:
            logger.debug(f"PublisherClientProtocol connection lost: {exc}")
        else:
            logger.debug("PublisherClientProtocol disconnected")


class PublisherServerProtocolState(enum.Enum):
    WAIT_COMMAND = enum.auto()
    CHANNEL_ID_LEN = enum.auto()
    CHANNEL_ID_DATA = enum.auto()
    SHM_RESPONSE = enum.auto()
    PID = enum.auto()
    COMMAND = enum.auto()
    ACK_MSG_ID = enum.auto()


class PublisherServerProtocol(asyncio.Protocol):
    """
    Server-side protocol for publisher<->channel link.

    Handles handshake (CHANNEL command, SHM negotiation, COMPLETE + num_buffers)
    and ACK telemetry from channels. Publisher injects callbacks to integrate
    with its state/backpressure tracking.
    """

    def __init__(
        self,
        get_shm_info: typing.Callable[[], tuple[str, int]],
        on_handshake: typing.Callable[[UUID, "PublisherServerProtocol"], None],
        on_ack: typing.Callable[[UUID, int], None],
        on_disconnect: typing.Callable[[UUID], None] | None = None,
        mode: TransmitMode = TransmitMode.AUTO,
    ):
        self._get_shm_info = get_shm_info
        self._on_handshake = on_handshake
        self._on_ack = on_ack
        self._on_disconnect = on_disconnect

        self.transport: asyncio.Transport | None = None

        self.buffer = bytearray(1024 * 1024)
        self.buffer_size = 0
        self.buffer_offset = 0

        self.state = PublisherServerProtocolState.WAIT_COMMAND
        self.expected_bytes = 1

        self.channel_id: str | None = None
        self._channel_uuid: UUID | None = None
        self.num_buffers: int | None = None
        self._shm_ok: bool | None = None
        self._channel_pid: int | None = None

        self.handshake_complete: asyncio.Future[
            tuple[UUID, bool, int]
        ] = asyncio.get_running_loop().create_future()

        self._paused = False
        self._drain_waiter: asyncio.Future[None] | None = None

        self.mode: TransmitMode = mode

    @property
    def uuid(self) -> UUID:
        if self._channel_uuid is None:
            raise RuntimeError("Channel UUID accessed before it was received")
        return self._channel_uuid

    def connection_made(self, transport: asyncio.Transport) -> None:  # type: ignore
        self.transport = transport
        logger.debug("PublisherServerProtocol connected")

    def data_received(self, data: bytes) -> None:
        data_len = len(data)

        if self.buffer_size + data_len > len(self.buffer):
            if self.buffer_offset > len(self.buffer) // 2:
                remaining = self.buffer_size - self.buffer_offset
                self.buffer[0:remaining] = self.buffer[
                    self.buffer_offset : self.buffer_size
                ]
                self.buffer_offset = 0
                self.buffer_size = remaining
            else:
                new_size = max(len(self.buffer) * 2, self.buffer_size + data_len)
                self.buffer.extend(bytearray(new_size - len(self.buffer)))

        self.buffer[self.buffer_size : self.buffer_size + data_len] = data
        self.buffer_size += data_len

        while self._try_parse():
            pass

    def _try_parse(self) -> bool:
        available = self.buffer_size - self.buffer_offset
        if available < self.expected_bytes:
            return False

        if self.state == PublisherServerProtocolState.WAIT_COMMAND:
            cmd = self.buffer[self.buffer_offset]
            self.buffer_offset += 1
            if cmd != Command.CHANNEL.value[0]:
                logger.error(f"Unexpected command from channel: {cmd}")
                if self.transport:
                    self.transport.close()
                return False
            self.state = PublisherServerProtocolState.CHANNEL_ID_LEN
            self.expected_bytes = 8
            return True

        elif self.state == PublisherServerProtocolState.CHANNEL_ID_LEN:
            chan_len = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.expected_bytes = chan_len
            self.state = PublisherServerProtocolState.CHANNEL_ID_DATA
            return True

        elif self.state == PublisherServerProtocolState.CHANNEL_ID_DATA:
            chan_id = self.buffer[
                self.buffer_offset : self.buffer_offset + self.expected_bytes
            ].decode("utf-8")
            self.buffer_offset += self.expected_bytes
            self.channel_id = chan_id            
            self._channel_uuid = UUID(chan_id)

            shm_name, num_buffers = self._get_shm_info()
            self.num_buffers = num_buffers

            if self.transport is None:
                return False

            # Send SHM name (length + data) as handshake step
            self.transport.write(encode_str(shm_name))

            self.state = PublisherServerProtocolState.SHM_RESPONSE
            self.expected_bytes = 1
            return True

        elif self.state == PublisherServerProtocolState.SHM_RESPONSE:
            resp = self.buffer[self.buffer_offset]
            self.buffer_offset += 1
            if resp == Command.SHM_OK.value[0]:
                self.shm_ok = True
            elif resp == Command.SHM_ATTACH_FAILED.value[0]:
                self.shm_ok = False
            else:
                logger.error(f"Unexpected SHM response: {resp}")
                if self.transport:
                    self.transport.close()
                return False

            self.state = PublisherServerProtocolState.PID
            self.expected_bytes = 8
            return True

        elif self.state == PublisherServerProtocolState.PID:
            pid = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.pid = pid

            if self.mode == TransmitMode.AUTO:
                self.mode = TransmitMode.TCP
                if self.shm_ok:
                    self.mode = TransmitMode.LOCAL if pid == os.getpid() else TransmitMode.SHM

            if self.transport is not None and self.num_buffers is not None:
                self.transport.write(Command.COMPLETE.value)
                self.transport.write(uint64_to_bytes(self.num_buffers))
                self.transport.write(self.mode.value.to_bytes(length=1))

            if (self.shm_ok is not None and self.pid is not None):
                self._on_handshake(self.uuid, self)
                if not self.handshake_complete.done():
                    self.handshake_complete.set_result(
                        (self.uuid, self.shm_ok, self.pid)
                    )

            self.state = PublisherServerProtocolState.COMMAND
            self.expected_bytes = 1
            return True

        elif self.state == PublisherServerProtocolState.COMMAND:
            cmd = self.buffer[self.buffer_offset]
            self.buffer_offset += 1
            if cmd == Command.RX_ACK.value[0]:
                self.state = PublisherServerProtocolState.ACK_MSG_ID
                self.expected_bytes = 8
            else:
                logger.error(f"Unknown command from channel: {cmd}")
                if self.transport:
                    self.transport.close()
                return False
            return True

        elif self.state == PublisherServerProtocolState.ACK_MSG_ID:
            msg_id = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self._on_ack(self.uuid, msg_id)
            self.state = PublisherServerProtocolState.COMMAND
            self.expected_bytes = 1
            return True

        return False

    def close(self) -> None:
        if self.transport is not None:
            self.transport.close()

    def transmit(self, msg_id: int, shm_name: str, tcp_payload: bytes | None) -> None:
        if self.transport is None:
            return
        
        if self.mode == TransmitMode.SHM:
            self.transport.write(
                uint64_to_bytes(msg_id) + 
                encode_str(shm_name)
            )

        elif self.mode == TransmitMode.TCP:
            assert tcp_payload is not None, \
                "must assemble tcp payload to transmit it"
            self.transport.write(
                uint64_to_bytes(msg_id) +
                uint64_to_bytes(len(tcp_payload)) +
                tcp_payload
            )

        else:
            raise ValueError(f"cannot transmit with {self.mode=}")

    async def drain(self) -> None:
        if self._paused and self._drain_waiter is not None:
            await self._drain_waiter

    def pause_writing(self) -> None:
        self._paused = True
        loop = asyncio.get_running_loop()
        if self._drain_waiter is None or self._drain_waiter.done():
            self._drain_waiter = loop.create_future()

    def resume_writing(self) -> None:
        self._paused = False
        if self._drain_waiter is not None and not self._drain_waiter.done():
            self._drain_waiter.set_result(None)
        self._drain_waiter = None

    def connection_lost(self, exc: Exception | None) -> None:
        if exc:
            logger.debug(f"PublisherServerProtocol connection lost: {exc}")
        else:
            logger.debug("PublisherServerProtocol disconnected")

        if self._on_disconnect:
            self._on_disconnect(self.uuid)

        if not self.handshake_complete.done():
            self.handshake_complete.cancel()
