from typing import Any

from .netprotocol import DEFAULT_SHM_SIZE
from .addressable import Addressable


class Stream(Addressable):
    """
    Base class for all streams in the ezmsg framework.

    Streams define the communication channels between components, carrying
    messages of a specific type through the system.

    :param msg_type: The type of messages this stream will carry
    :type msg_type: Any
    """

    msg_type: Any

    def __init__(self, msg_type: Any):
        super().__init__()
        self.msg_type = msg_type

    def __repr__(self) -> str:
        _addr = self.address if self._location is not None else "unlocated"
        return f"Stream:{_addr}[{self.msg_type.__name__}]"


class InputStream(Stream):
    """
    Can be added to any Component as a member variable. Methods may subscribe to it.

    InputStream represents a channel that receives messages from other components.
    Units can subscribe to InputStreams to process incoming messages.

    :param msg_type: The type of messages this input stream will receive
    :type msg_type: Any
    """

    def __repr__(self) -> str:
        return f"Input{super().__repr__()}()"


class OutputStream(Stream):
    """
    Can be added to any Component as a member variable. Methods may publish to it.

    OutputStream represents a channel that sends messages to other components.
    Units can publish to OutputStreams to send messages through the system.

    :param msg_type: The type of messages this output stream will send
    :type msg_type: Any
    :param host: Optional host address for network publishing
    :type host: str | None
    :param port: Optional port number for network publishing
    :type port: int | None
    :param num_buffers: Number of message buffers to allocate (default: 32)
    :type num_buffers: int
    :param buf_size: Size of each message buffer in bytes
    :type buf_size: int
    :param force_tcp: Whether to force TCP transport instead of shared memory
    :type force_tcp: bool
    """

    host: str | None
    port: int | None
    num_buffers: int
    buf_size: int
    force_tcp: bool

    def __init__(
        self,
        msg_type: Any,
        host: str | None = None,
        port: int | None = None,
        num_buffers: int = 32,
        buf_size: int = DEFAULT_SHM_SIZE,
        force_tcp: bool = False,
    ) -> None:
        super().__init__(msg_type)
        self.host = host
        self.port = port
        self.num_buffers = num_buffers
        self.buf_size = buf_size
        self.force_tcp = force_tcp

    def __repr__(self) -> str:
        preamble = f"Output{super().__repr__()}"
        return f"{preamble}({self.num_buffers=}, {self.force_tcp=})"
