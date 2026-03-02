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


class Topic(Stream):
    """
    Graph endpoint metadata for Collection boundaries and graph wiring.

    Topics represent named DAG nodes only. Unlike InputStream / OutputStream,
    they do not directly configure Subscriber / Publisher transport behavior.
    """

    def __repr__(self) -> str:
        return f"Topic{super().__repr__()}()"


class InputTopic(Topic):
    """
    Directional alias for a Collection input topic.
    """

    def __repr__(self) -> str:
        return f"Input{super().__repr__()}"


class OutputTopic(Topic):
    """
    Directional alias for a Collection output topic.
    """

    def __repr__(self) -> str:
        return f"Output{super().__repr__()}"


class InputRelay(InputTopic):
    """
    Collection input boundary that materializes an internal relay subscriber/publisher.

    This enables subscriber-side behavior (e.g., leaky reception) on the boundary.
    """

    leaky: bool
    max_queue: int | None
    copy_on_forward: bool

    def __init__(
        self,
        msg_type: Any,
        leaky: bool = False,
        max_queue: int | None = None,
        copy_on_forward: bool = True,
    ) -> None:
        super().__init__(msg_type)
        if max_queue is not None and max_queue <= 0:
            raise ValueError("max_queue must be positive")
        self.leaky = leaky
        self.max_queue = max_queue
        self.copy_on_forward = copy_on_forward

    def __repr__(self) -> str:
        base = f"InputRelay{Stream.__repr__(self)}"
        return (
            f"{base}(leaky={self.leaky}, max_queue={self.max_queue}, "
            f"copy_on_forward={self.copy_on_forward})"
        )


class OutputRelay(OutputTopic):
    """
    Collection output boundary that materializes an internal relay subscriber/publisher.

    This enables publisher-side behavior (e.g., custom transport buffer settings)
    on the boundary.
    """

    host: str | None
    port: int | None
    num_buffers: int
    buf_size: int
    force_tcp: bool
    copy_on_forward: bool

    def __init__(
        self,
        msg_type: Any,
        host: str | None = None,
        port: int | None = None,
        num_buffers: int = 32,
        buf_size: int = DEFAULT_SHM_SIZE,
        force_tcp: bool = False,
        copy_on_forward: bool = True,
    ) -> None:
        super().__init__(msg_type)
        self.host = host
        self.port = port
        self.num_buffers = num_buffers
        self.buf_size = buf_size
        self.force_tcp = force_tcp
        self.copy_on_forward = copy_on_forward

    def __repr__(self) -> str:
        base = f"OutputRelay{Stream.__repr__(self)}"
        return (
            f"{base}(num_buffers={self.num_buffers}, force_tcp={self.force_tcp}, "
            f"copy_on_forward={self.copy_on_forward})"
        )


class InputStream(Stream):
    """
    Can be added to any Component as a member variable. Methods may subscribe to it.

    InputStream represents a channel that receives messages from other components.
    Units can subscribe to InputStreams to process incoming messages.

    Leaky Subscribers
    -----------------

    By default, ezmsg uses backpressure to prevent fast publishers from overwhelming
    slow subscribers. When a subscriber can't keep up, the publisher blocks until
    the subscriber catches up. This guarantees no message loss but can cause latency
    buildup in real-time applications.

    Setting ``leaky=True`` creates a "leaky" subscriber that drops old messages
    instead of applying backpressure. This is useful when you need the most recent
    data rather than processing a growing backlog of stale messages.

    **Architecture**: The leaky behavior is implemented at the subscriber's
    notification queue, *after* message serialization and transport. This means:

    - Publishers still serialize and transmit every message (to shared memory or TCP)
    - The Channel still receives and caches every message
    - Dropping occurs when the subscriber's notification queue is full
    - Backpressure is properly released for dropped messages (ACKs sent to publisher)

    This design ensures that:

    1. One leaky subscriber doesn't affect other subscribers to the same topic
    2. The publisher's buffer management remains consistent
    3. Backpressure accounting stays correct (no resource leaks)

    **Trade-offs**: Leaky subscribers don't reduce serialization or network overhead;
    they prevent slow consumers from blocking fast producers. If you need to reduce
    data transfer, consider filtering or downsampling at the publisher level.

    **NOTE**: If a leaky subscriber has a max_queue size that is greater than or
    equal to any connected publisher's num_buffers, it can still cause backpressure
    to those publishers! You will receive a warning if configured as such.

    Example usage::

        # Leaky subscriber that keeps at most 3 pending messages
        INPUT = ez.InputStream(MyMessage, leaky=True, max_queue=3)

        @ez.subscriber(INPUT)
        async def process(self, msg: MyMessage) -> None:
            # Will only see recent messages; older ones dropped if queue fills
            await slow_processing(msg)

    :param msg_type: The type of messages this input stream will receive
    :type msg_type: Any
    :param leaky: If True, drop oldest messages when queue is full (default: False)
    :type leaky: bool
    :param max_queue: Maximum queue depth for leaky mode (ignored if leaky=False)
    :type max_queue: int | None
    """

    leaky: bool
    max_queue: int | None

    def __init__(
        self,
        msg_type: Any,
        leaky: bool = False,
        max_queue: int | None = None,
    ) -> None:
        super().__init__(msg_type)
        if max_queue is not None and max_queue <= 0:
            raise ValueError("max_queue must be positive")
        self.leaky = leaky
        self.max_queue = max_queue

    def __repr__(self) -> str:
        base = f"Input{super().__repr__()}"
        if self.leaky:
            return f"{base}(leaky=True, max_queue={self.max_queue})"
        return f"{base}()"


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
