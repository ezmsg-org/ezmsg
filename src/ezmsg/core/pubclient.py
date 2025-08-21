import os
import asyncio
import logging
import time

from uuid import UUID
from contextlib import suppress
from dataclasses import dataclass

from .backpressure import Backpressure
from .shm import SHMContext
from .graphserver import GraphService
from .messagechannel import CHANNELS, _Channel
from .messagemarshal import MessageMarshal

from .netprotocol import (
    Address,
    AddressType,
    uint64_to_bytes,
    read_int,
    read_str,
    encode_str,
    close_stream_writer,
    close_server,
    Command,
    ChannelInfo,
    create_socket,
    DEFAULT_SHM_SIZE,
    PUBLISHER_START_PORT_ENV,
    PUBLISHER_START_PORT_DEFAULT,
)

from typing import Any, Dict, Optional

logger = logging.getLogger("ezmsg")

BACKPRESSURE_WARNING = "EZMSG_DISABLE_BACKPRESSURE_WARNING" not in os.environ
BACKPRESSURE_REFRACTORY = 5.0  # sec


# Publisher needs a bit more information about connected channels
@dataclass
class PubChannelInfo(ChannelInfo):
    pid: int
    shm_ok: bool = False


class Publisher:
    id: UUID
    pid: int
    topic: str

    _initialized: asyncio.Event
    _graph_task: "asyncio.Task[None]"
    _connection_task: "asyncio.Task[None]"
    _channels: Dict[UUID, PubChannelInfo]
    _channel_tasks: Dict[UUID, "asyncio.Task[None]"]
    _local_channel: _Channel
    _address: Address
    _backpressure: Backpressure
    _num_buffers: int
    _running: asyncio.Event
    _msg_id: int
    _shm: SHMContext
    _force_tcp: bool
    _last_backpressure_event: float

    _graph_address: AddressType | None

    @staticmethod
    def client_type() -> bytes:
        return Command.PUBLISH.value

    @classmethod
    async def create(
        cls,
        topic: str,
        graph_address: AddressType | None = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        buf_size: int = DEFAULT_SHM_SIZE,
        num_buffers: int = 32,
        **kwargs,
    ) -> "Publisher":
        # We have to fill in some parts of this class using async
        pub = cls(topic, graph_address, num_buffers, **kwargs)

        graph_service = GraphService(graph_address)
        reader, writer = await graph_service.open_connection()
        pub._shm = await graph_service.create_shm(num_buffers, buf_size)

        start_port = int(
            os.getenv(PUBLISHER_START_PORT_ENV, PUBLISHER_START_PORT_DEFAULT)
        )
        sock = create_socket(host, port, start_port=start_port)
        address = Address(*sock.getsockname())

        server = await asyncio.start_server(pub._channel_connect, sock=sock)
        
        writer.write(Command.PUBLISH.value)
        writer.write(encode_str(topic))
        address.to_stream(writer)

        result = await reader.read(1)
        if result != Command.COMPLETE.value:
            logger.warning(f'Could not create publisher {topic=}')

        pub.id = UUID(await read_str(reader))

        pub._graph_task = asyncio.create_task(pub._graph_connection(reader, writer))

        async def serve() -> None:
            try:
                await server.serve_forever()
            except asyncio.CancelledError:
                logger.debug("{pub.log_name} cancelled")
            finally:
                await close_server(server)

        pub._connection_task = asyncio.create_task(serve(), name=pub.log_name)

        def on_done(_: asyncio.Future) -> None:
            logger.debug("{pub.log_name} done")

        pub._connection_task.add_done_callback(on_done)

        # Create the local Channel (it shouldn't already exist)
        pub._local_channel = await CHANNELS.get(pub.id, graph_address, create = True)

        return pub

    def __init__(
        self,
        topic: str,
        graph_address: AddressType | None = None,
        num_buffers: int = 32,
        start_paused: bool = False,
        force_tcp: bool = False,
    ) -> None:
        """DO NOT USE this constructor to make a Publisher; use `create` instead"""
        self.pid = os.getpid()
        self.topic = topic

        self._msg_id = 0
        self._channels = dict()
        self._channel_tasks = dict()
        self._running = asyncio.Event()
        if not start_paused:
            self._running.set()
        self._num_buffers = num_buffers
        self._backpressure = Backpressure(num_buffers)
        self._force_tcp = force_tcp
        self._last_backpressure_event = -1

        self._graph_address = graph_address

    @property
    def log_name(self) -> str:
        return f"pub_{self.topic}{str(self.id)}"

    def close(self) -> None:
        self._graph_task.cancel()
        self._shm.close()
        self._connection_task.cancel()
        for task in self._channel_tasks.values():
            task.cancel()

    async def wait_closed(self) -> None:
        await self._shm.wait_closed()
        with suppress(asyncio.CancelledError):
            await self._graph_task
        with suppress(asyncio.CancelledError):
            await self._connection_task
        for task in self._channel_tasks.values():
            with suppress(asyncio.CancelledError):
                await task

    async def _graph_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                cmd = await reader.read(1)
                if not cmd:
                    break

                elif cmd == Command.PAUSE.value:
                    self._running.clear()

                elif cmd == Command.RESUME.value:
                    self._running.set()

                elif cmd == Command.SYNC.value:
                    await self.sync()
                    writer.write(Command.COMPLETE.value)

                else:
                    logger.warning(
                        f"Publisher {self.id} rx unknown command from GraphServer {cmd}"
                    )

                await writer.drain()

        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"Publisher {self.id} lost connection to graph server")

        finally:
            await close_stream_writer(writer)

    async def _channel_connect(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        cmd = await reader.read(1)

        if len(cmd) == 0:
            return
        
        if cmd == Command.CHANNEL.value:
            channel_id_str = await read_str(reader)
            channel_id = UUID(channel_id_str)
            writer.write(encode_str(self._shm.name))
            shm_ok = await reader.read(1) == Command.SHM_OK.value
            pid = await read_int(reader)
            info = PubChannelInfo(channel_id, writer, self.id, pid, shm_ok)
            coro = self._handle_channel(info, reader)
            self._channel_tasks[channel_id] = asyncio.create_task(coro)
            writer.write(Command.COMPLETE.value + uint64_to_bytes(self._num_buffers))

        await writer.drain()

    async def _handle_channel(
        self, info: PubChannelInfo, reader: asyncio.StreamReader
    ) -> None:
        self._channels[info.id] = info

        try:
            while True:
                msg = await reader.read(1)

                if len(msg) == 0:
                    break

                elif msg == Command.RX_ACK.value:
                    msg_id = await read_int(reader)
                    self._backpressure.free(info.id, msg_id % self._num_buffers)

        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"Publisher {self.id}: Channel {info.id} connection fail")

        finally:
            self._backpressure.free(info.id)
            await close_stream_writer(self._channels[info.id].writer)
            del self._channels[info.id]

    async def sync(self) -> None:
        """Pause and drain backpressure"""
        self._running.clear()
        await self._backpressure.sync()

    @property
    def running(self) -> bool:
        return self._running.is_set()

    def pause(self) -> None:
        self._running.clear()

    def resume(self) -> None:
        self._running.set()

    async def broadcast(self, obj: Any) -> None:
        await self._running.wait()

        buf_idx = self._msg_id % self._num_buffers
        msg_id_bytes = uint64_to_bytes(self._msg_id)

        if not self._backpressure.available(buf_idx):
            delta = time.time() - self._last_backpressure_event
            if BACKPRESSURE_WARNING and (delta > BACKPRESSURE_REFRACTORY):
                logger.warning(f"{self.topic} under subscriber backpressure!")
            self._last_backpressure_event = time.time()
            await self._backpressure.wait(buf_idx)

        # Get local channel and put variable there for local tx
        if not self._force_tcp:
            self._local_channel.put_local(self._msg_id, obj)
            self._backpressure.lease(self._local_channel.id, buf_idx)

        if self._force_tcp or any(ch.pid != self.pid or not ch.shm_ok for ch in self._channels.values()):
            with MessageMarshal.serialize(self._msg_id, obj) as (total_size, header, buffers):
                total_size_bytes = uint64_to_bytes(total_size)

                if not self._force_tcp and any(ch.pid != self.pid and ch.shm_ok for ch in self._channels.values()):
                    if self._shm.buf_size < total_size:
                        new_shm = await GraphService(self._graph_address).create_shm(self._num_buffers, total_size * 2)
                        
                        for i in range(self._num_buffers):
                            with self._shm.buffer(i, readonly=True) as from_buf:
                                with new_shm.buffer(i) as to_buf:
                                    MessageMarshal.copy_obj(from_buf, to_buf)
                        
                        self._shm.close()
                        self._shm = new_shm

                    with self._shm.buffer(buf_idx) as mem:
                        MessageMarshal._write(mem, header, buffers)

                for channel in self._channels.values():

                    if (not self._force_tcp) and self.pid == channel.pid and channel.shm_ok:
                        continue # Local transmission handled by channel.put

                    elif (not self._force_tcp) and self.pid != channel.pid and channel.shm_ok:
                        channel.writer.write(
                            Command.TX_SHM.value + \
                            msg_id_bytes + \
                            encode_str(self._shm.name)
                        )

                    else:
                        channel.writer.write(
                            Command.TX_TCP.value + \
                            msg_id_bytes + \
                            total_size_bytes + \
                            header + \
                            b''.join([buffer for buffer in buffers])
                        )
                    
                    try:
                        await channel.writer.drain()
                        self._backpressure.lease(channel.id, buf_idx)

                    except (ConnectionResetError, BrokenPipeError):
                        logger.debug(
                            f"Publisher {self.id}: Channel {channel.id} connection fail"
                        )
                        continue

        self._msg_id += 1
