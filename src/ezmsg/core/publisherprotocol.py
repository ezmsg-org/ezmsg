import asyncio
import typing
import logging
import enum

from .netprotocol import (
    Command,
    uint64_to_bytes,
    encode_str,
)

logger = logging.getLogger("ezmsg")

class PublisherProtocolState(enum.Enum):
    # Handshake states
    HANDSHAKE_SHM_NAME_LEN = enum.auto()
    HANDSHAKE_SHM_NAME_DATA = enum.auto()
    HANDSHAKE_COMPLETE = enum.auto()
    HANDSHAKE_NUM_BUFFERS = enum.auto()
    # Message states
    COMMAND = enum.auto()
    MSG_ID = enum.auto()
    SHM_NAME_LEN = enum.auto()
    SHM_NAME_DATA = enum.auto()
    TCP_SIZE = enum.auto()
    TCP_DATA = enum.auto()


class PublisherMessage(typing.NamedTuple):
    """Parsed message from publisher"""

    msg_id: int
    command: bytes
    shm_name: str | None = None
    tcp_data: bytes | None = None


class PublisherProtocol(asyncio.Protocol):
    """High-performance protocol for publisher message stream"""

    def __init__(
        self,
        channel_id: str,
        message_queue: asyncio.Queue[PublisherMessage],
    ):
        self.channel_id = channel_id
        self.message_queue = message_queue
        self.transport: asyncio.Transport | None = None

        # Pre-allocate buffer with reasonable size (1MB)
        # This avoids repeated reallocations during growth
        self.buffer = bytearray(1024 * 1024)
        self.buffer_size = 0  # Actual data in buffer
        self.buffer_offset = 0  # Read position

        # Handshake state
        self.handshake_complete = asyncio.Future()
        self.handshake_shm_received = asyncio.Event()
        self.handshake_shm_name: str | None = None
        self.num_buffers: int | None = None

        # Parser state machine - start in handshake mode
        self.state = PublisherProtocolState.HANDSHAKE_SHM_NAME_LEN
        self.expected_bytes = 8  # Expecting shm_name length

        # Message parsing state
        self.current_msg_id: int | None = None
        self.current_command: int | None = None  # Store as int, not bytes
        self.payload_size: int | None = None
        self.shm_name_len: int | None = None

    def connection_made(self, transport: asyncio.Transport) -> None:  # type: ignore
        self.transport = transport
        logger.debug("PublisherProtocol connected")
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
            self.handshake_shm_name = shm_name
            self.handshake_shm_received.set()
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
            self.state = PublisherProtocolState.COMMAND
            self.expected_bytes = 1
            self.handshake_complete.set_result(num_buffers)
            return True

        # Message states
        elif self.state == PublisherProtocolState.COMMAND:
            cmd = self.buffer[self.buffer_offset]
            self.buffer_offset += 1
            self.current_command = cmd
            self.state = PublisherProtocolState.MSG_ID
            self.expected_bytes = 8  # uint64
            return True

        elif self.state == PublisherProtocolState.MSG_ID:
            msg_id = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.current_msg_id = msg_id

            if self.current_command == Command.TX_SHM.value[0]:
                self.state = PublisherProtocolState.SHM_NAME_LEN
                self.expected_bytes = 8
            elif self.current_command == Command.TX_TCP.value[0]:
                self.state = PublisherProtocolState.TCP_SIZE
                self.expected_bytes = 8
            else:
                # Unknown command - queue it anyway
                self.message_queue.put_nowait(
                    PublisherMessage(
                        msg_id=msg_id, command=bytes([self.current_command or 0])
                    )
                )
                self._reset_state()
            return True

        elif self.state == PublisherProtocolState.SHM_NAME_LEN:
            name_len = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.expected_bytes = name_len
            self.state = PublisherProtocolState.SHM_NAME_DATA
            return True

        elif self.state == PublisherProtocolState.SHM_NAME_DATA:
            shm_name = self.buffer[
                self.buffer_offset : self.buffer_offset + self.expected_bytes
            ].decode("utf-8")
            self.buffer_offset += self.expected_bytes
            if self.current_msg_id is not None and self.current_command is not None:
                self.message_queue.put_nowait(
                    PublisherMessage(
                        msg_id=self.current_msg_id,
                        command=bytes([self.current_command]),
                        shm_name=shm_name,
                    )
                )
            self._reset_state()
            return True

        elif self.state == PublisherProtocolState.TCP_SIZE:
            buf_size = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.expected_bytes = buf_size
            self.state = PublisherProtocolState.TCP_DATA
            return True

        elif self.state == PublisherProtocolState.TCP_DATA:
            # Use bytes() here - memoryview causes issues with pickle/marshal
            obj_bytes = bytes(
                self.buffer[
                    self.buffer_offset : self.buffer_offset + self.expected_bytes
                ]
            )
            self.buffer_offset += self.expected_bytes
            if self.current_msg_id is not None and self.current_command is not None:
                self.message_queue.put_nowait(
                    PublisherMessage(
                        msg_id=self.current_msg_id,
                        command=bytes([self.current_command]),
                        tcp_data=obj_bytes,
                    )
                )
            self._reset_state()
            return True

        return False

    def _reset_state(self) -> None:
        self.state = PublisherProtocolState.COMMAND
        self.expected_bytes = 1
        self.current_msg_id = None
        self.current_command = None
        self.payload_size = None
        self.shm_name_len = None

    def connection_lost(self, exc: Exception | None) -> None:
        if exc:
            logger.debug(f"PublisherProtocol connection lost: {exc}")
        else:
            logger.debug("PublisherProtocol disconnected")