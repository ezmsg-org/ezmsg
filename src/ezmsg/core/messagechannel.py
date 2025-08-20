import os
import asyncio
import typing
import logging

from uuid import UUID
from contextlib import contextmanager

from .shm import SHMContext
from .messagemarshal import MessageMarshal
from .backpressure import Backpressure

from .graphserver import GraphService
from .netprotocol import (
    Command,
    Address,
    AddressType, 
    read_str, 
    read_int, 
    uint64_to_bytes,
    encode_str,
    close_stream_writer,
    GRAPHSERVER_ADDR
)

logger = logging.getLogger("ezmsg")


class CacheMiss(Exception): ...


class _Channel:
    """cache-backed message channel for a particular publisher"""

    id: UUID
    pub_id: UUID
    pid: int
    topic: str

    num_buffers: int
    cache: typing.List[typing.Any]
    cache_id: typing.List[int | None]
    shm: SHMContext | None
    sub_queues: typing.Dict[UUID, asyncio.Queue[typing.Tuple[UUID, int]]]
    backpressure: Backpressure

    _graph_task: asyncio.Task[None]
    _pub_task: asyncio.Task[None]
    _pub_writer: asyncio.StreamWriter
    _graph_address: AddressType | None

    def __init__(
        self, 
        id: UUID, 
        pub_id: UUID, 
        num_buffers: int, 
        shm: SHMContext | None,
        graph_address: AddressType | None = None
    ) -> None:
        self.id = id
        self.pub_id = pub_id
        self.num_buffers = num_buffers
        self.shm = shm

        self.cache_id = [None] * self.num_buffers
        self.cache = [None] * self.num_buffers
        self.backpressure = Backpressure(self.num_buffers)
        self.sub_queues = dict()
        self._graph_address = graph_address

    @classmethod
    async def create(
        cls,
        pub_id: UUID,
        graph_address: AddressType,
    ) -> "_Channel":
        graph_service = GraphService(graph_address)

        graph_reader, graph_writer = await graph_service.open_connection()
        graph_writer.write(Command.CHANNEL.value)
        graph_writer.write(encode_str(str(pub_id)))

        response = await graph_reader.read(1)
        if response != Command.COMPLETE.value:
            raise ValueError(f'failed to create channel {pub_id=}')
        
        id_str = await read_str(graph_reader)
        pub_address = await Address.from_stream(graph_reader)

        reader, writer = await asyncio.open_connection(*pub_address)

        writer.write(Command.CHANNEL.value)
        writer.write(encode_str(id_str))

        shm = None
        shm_name = await read_str(reader)
        try:
            shm = await graph_service.attach_shm(shm_name)
            writer.write(Command.SHM_OK.value)
        except (ValueError, OSError):
            writer.write(Command.SHM_ATTACH_FAILED.value)
        writer.write(uint64_to_bytes(os.getpid()))

        result = await reader.readexactly(1)
        if result != Command.COMPLETE.value:
            raise ValueError(f'failed to create channel {pub_id=}')
        
        num_buffers = await read_int(reader)
        
        chan = cls(UUID(id_str), pub_id, num_buffers, shm)

        chan._graph_task = asyncio.create_task(
            chan._graph_connection(graph_reader, graph_writer)
        )

        chan._pub_writer = writer
        chan._pub_task = asyncio.create_task(
            chan._publisher_connection(reader)
        )

        return chan
    
    async def _graph_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                cmd = await reader.read(1)
                
                if not cmd:
                    break

                else:
                    logger.warning(
                        f"Channel {self.id} rx unknown command from GraphServer: {cmd}"
                    )
        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"Channel {self.id} lost connection to graph server")

        finally:
            await close_stream_writer(writer)

    async def _publisher_connection(self, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                msg = await reader.read(1)

                if not msg:
                    break

                msg_id = await read_int(reader)
                buf_idx = msg_id % self.num_buffers

                if msg == Command.TX_SHM.value:
                    shm_name = await read_str(reader)

                    if self.shm is not None and self.shm.name != shm_name:
                        self.shm.close()
                        try:
                            self.shm = await GraphService(self._graph_address).attach_shm(shm_name)
                        except ValueError:
                            logger.info(
                                "Invalid SHM received from publisher; may be dead"
                            )
                            raise

                elif msg == Command.TX_TCP.value:
                    buf_size = await read_int(reader)
                    obj_bytes = await reader.readexactly(buf_size)

                    with MessageMarshal.obj_from_mem(memoryview(obj_bytes)) as obj:
                        self.cache[buf_idx] = obj
                    self.cache_id[buf_idx] = msg_id

                self._notify_subs(msg_id)

        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"connection fail: channel:{self.id} - pub:{self.pub_id}")

        finally:
            await close_stream_writer(self._pub_writer)
            # TODO: Remove this channel from CHANNELS ... ? maybe?
            logger.debug(f"disconnected: channel:{self.id} -> pub:{id}")

    def _notify_subs(self, msg_id: int) -> None:
        for sub_id, queue in self.sub_queues.items():
            self.backpressure.lease(sub_id, msg_id % self.num_buffers)
            queue.put_nowait((self.pub_id, msg_id))

    def put(self, msg_id: int, msg: typing.Any) -> None:
        """put an object into cache (should only be used by Publishers)"""
        buf_idx = msg_id % self.num_buffers
        self.cache_id[buf_idx] = msg_id
        self.cache[buf_idx] = msg
        self._notify_subs(msg_id)

    @contextmanager
    def get(self, msg_id: int, sub_id: UUID) -> typing.Generator[typing.Any, None, None]:
        """get object from cache; if not in cache and shm provided -- get from shm"""

        buf_idx = msg_id % self.num_buffers
        if self.cache_id[buf_idx] == msg_id:
            yield self.cache[buf_idx]

        else:
            if self.shm is None:
                raise CacheMiss

            with self.shm.buffer(buf_idx, readonly=True) as mem:
                if MessageMarshal.msg_id(mem) != msg_id:
                    raise CacheMiss

                with MessageMarshal.obj_from_mem(mem) as obj:
                    # Could deepcopy and put in cache here, but 
                    # profiling indicates its faster to repeatedly
                    # reconstruct from memory for fanout <= 4 subs
                    # which I suspect will be majority of cases
                    yield obj

        self.backpressure.free(sub_id, buf_idx)
        if self.backpressure.buffers[buf_idx].is_empty:
            try:
                ack = Command.RX_ACK.value + uint64_to_bytes(msg_id)
                self._pub_writer.write(ack)
            except (BrokenPipeError, ConnectionResetError):
                logger.debug(f"ack fail: channel:{self.id} -> pub:{self.pub_id}")

    def subscribe(self, sub_id: UUID, sub_queue: asyncio.Queue[typing.Tuple[UUID, int]]) -> None:
        self.sub_queues[sub_id] = sub_queue 

    def unsubscribe(self, sub_id: UUID) -> None:
        queue = self.sub_queues.get(sub_id, None) 
        
        if queue is None:
            return

        del self.sub_queues[sub_id]

        for _ in range(queue.qsize()):
            ch_id, msg_id = queue.get_nowait()
            if ch_id == self.id:
                continue
            queue.put_nowait((ch_id, msg_id))

        self.backpressure.free(sub_id)

    def clear_cache(self):
        self.cache_id = [None] * self.num_buffers
        self.cache = [None] * self.num_buffers


class _ChannelManager:
    
    _registry: typing.Dict[Address, typing.Dict[UUID, _Channel]]

    def __init__(self):
        default_address = Address.from_string(GRAPHSERVER_ADDR)
        self._registry = {default_address: dict()}

    async def get(self, id: UUID, graph_address: AddressType | None = None) -> _Channel:
        
        if graph_address is None:
            graph_address = Address.from_string(GRAPHSERVER_ADDR)

        elif not isinstance(graph_address, Address):
            graph_address = Address(*graph_address)

        channels = self._registry.get(graph_address, dict())
        channel = channels.get(id, None)
        if channel is None:
            channel = await _Channel.create(id, graph_address)
            channels[id] = channel
            self._registry[graph_address] = channels

        return channel
    
    def unsubscribe_all(self, sub_id: UUID, graph_address: AddressType | None = None) -> None:
        
        if graph_address is None:
            graph_address = Address.from_string(GRAPHSERVER_ADDR)

        elif not isinstance(graph_address, Address):
            graph_address = Address(*graph_address)

        channels = self._registry.get(graph_address, None)
        if channels is None:
            return
        
        for channel in channels.values():
            channel.unsubscribe(sub_id)


CHANNELS = _ChannelManager()