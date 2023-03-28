import asyncio

from dataclasses import dataclass
from pickle import PickleBuffer

import zmq
import zmq.asyncio
from zmq.utils.monitor import parse_monitor_message

from typing import AsyncGenerator

import ezmsg.core as ez

POLL_TIME = 0.1
STARTUP_WAIT_TIME = 0.1


class ZeroCopyBytes(bytes):
    def __reduce_ex__(self, protocol):
        if protocol >= 5:
            return type(self)._reconstruct, (PickleBuffer(self),), None
        else:
            # PickleBuffer is forbidden with pickle protocols <= 4.
            return type(self)._reconstruct, (bytes(self),)

    @classmethod
    def _reconstruct(cls, obj):
        with memoryview(obj) as m:
            # Get a handle over the original buffer object
            obj = m.obj
            if isinstance(obj, cls):
                # Original buffer object is a ZeroCopyBytes, return it
                # as-is.
                return obj
            else:
                return cls(obj)


@dataclass
class ZMQMessage:
    data: bytes


class ZMQSenderSettings(ez.Settings):
    write_addr: str
    zmq_topic: str
    multipart: bool = False


class ZMQSenderUnit(ez.Unit):
    """
    Represents a node in a Labgraph graph that subscribes to messages in a
    Labgraph topic and forwards them by writing to a ZMQ socket.

    Args:
        write_addr: The address to which ZMQ data should be written.
        zmq_topic: The ZMQ topic being sent.
    """

    INPUT = ez.InputStream(ZMQMessage)

    SETTINGS: ZMQSenderSettings

    def setup(self) -> None:
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.monitor = self.socket.get_monitor_socket()
        ez.logger.debug(f"{self}:binding to {self.SETTINGS.write_addr}")
        self.socket.bind(self.SETTINGS.write_addr)
        self.has_subscribers = False

    def shutdown(self) -> None:
        self.socket.close()

    @ez.task
    async def _socket_monitor(self) -> None:
        while True:
            monitor_result = await self.monitor.poll(100, zmq.POLLIN)
            if monitor_result:
                data = await self.monitor.recv_multipart()
                evt = parse_monitor_message(data)

                event = evt["event"]

                if event == zmq.EVENT_ACCEPTED:
                    ez.logger.debug(f"{self}:subscriber joined")
                    self.has_subscribers = True
                elif event in (
                    zmq.EVENT_DISCONNECTED,
                    zmq.EVENT_MONITOR_STOPPED,
                    zmq.EVENT_CLOSED,
                ):
                    break

    @ez.subscriber(INPUT)
    async def zmq_subscriber(self, message: ZMQMessage) -> None:
        while not self.has_subscribers:
            await asyncio.sleep(STARTUP_WAIT_TIME)
        if self.SETTINGS.multipart is True:
            await self.socket.send_multipart(
                (bytes(self.SETTINGS.zmq_topic, "UTF-8"), message.data),
                flags=zmq.NOBLOCK,
            )
        else:
            await self.socket.send(
                b"".join((bytes(self.SETTINGS.zmq_topic, "UTF-8"), message.data)),
                flags=zmq.NOBLOCK,
            )


class ZMQPollerSettings(ez.Settings):
    read_addr: str
    zmq_topic: str
    poll_time: float = POLL_TIME
    multipart: bool = False


class ZMQPollerUnit(ez.Unit):
    """
    Represents a node in the graph which polls data from ZMQ.
    Data polled from ZMQ are subsequently pushed to the rest of the
    graph as a ZMQMessage.

    Args:
        read_addr: The address from which ZMQ data should be polled.
        zmq_topic: The ZMQ topic being polled.
        timeout:
            The maximum amount of time (in seconds) that should be
            spent polling a ZMQ socket each time.  Defaults to
            FOREVER_POLL_TIME if not specified.
        exit_condition:
            An optional ZMQ event code specifying the event which,
            if encountered by the monitor, should signal the termination
            of this particular node's activity.
    """

    OUTPUT = ez.OutputStream(ZMQMessage)
    SETTINGS: ZMQPollerSettings

    def setup(self) -> None:
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.monitor = self.socket.get_monitor_socket()
        self.socket.connect(self.SETTINGS.read_addr)
        self.socket.subscribe(self.SETTINGS.zmq_topic)

        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

        self.socket_open = False

    def shutdown(self) -> None:
        self.socket.close()

    @ez.task
    async def socket_monitor(self) -> None:
        while True:
            monitor_result = await self.monitor.poll(100, zmq.POLLIN)
            if monitor_result:
                data = await self.monitor.recv_multipart()
                evt = parse_monitor_message(data)

                event = evt["event"]

                if event == zmq.EVENT_CONNECTED:
                    self.socket_open = True
                elif event == zmq.EVENT_CLOSED:
                    was_open = self.socket_open
                    self.socket_open = False
                    # ZMQ seems to be sending spurious CLOSED event when we
                    # try to connect before the source is running. Only give up
                    # if we were previously connected. If we give up now, we
                    # will never unblock zmq_publisher.
                    if was_open:
                        break
                elif event in (
                    zmq.EVENT_DISCONNECTED,
                    zmq.EVENT_MONITOR_STOPPED,
                ):
                    self.socket_open = False
                    break

    @ez.publisher(OUTPUT)
    async def zmq_publisher(self) -> AsyncGenerator:
        # Wait for socket connection
        while not self.socket_open:
            await asyncio.sleep(POLL_TIME)

        while self.socket_open:
            poll_result = await self.socket.poll(
                self.SETTINGS.poll_time * 1000, zmq.POLLIN
            )
            if poll_result:
                if self.SETTINGS.multipart is True:
                    _, data = await self.socket.recv_multipart()
                else:
                    data = await self.socket.recv()
                yield self.OUTPUT, ZMQMessage(data)
