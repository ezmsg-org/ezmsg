import asyncio
import zmq
import zmq.asyncio
from zmq.utils.monitor import parse_monitor_message

import ezmsg.core as ez

from . import ZMQMessage

STARTUP_WAIT_TIME = 0.1

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
                (bytes(self.SETTINGS.zmq_topic, "UTF-8"), message.data), flags=zmq.NOBLOCK
            )
        else:
            await self.socket.send(
                b''.join((bytes(self.SETTINGS.zmq_topic, "UTF-8"), message.data)), flags=zmq.NOBLOCK
            )
