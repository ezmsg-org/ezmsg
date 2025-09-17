import os
import asyncio
import typing
import logging

from uuid import UUID
from contextlib import contextmanager, suppress

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


NotificationQueue = asyncio.Queue[typing.Tuple[UUID, int]]


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
    clients: typing.Dict[UUID, NotificationQueue | None]
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
        graph_address: AddressType | None
    ) -> None:
        self.id = id
        self.pub_id = pub_id
        self.num_buffers = num_buffers
        self.shm = shm

        self.cache_id = [None] * self.num_buffers
        self.cache = [None] * self.num_buffers
        self.backpressure = Backpressure(self.num_buffers)
        self.clients = dict()
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
            # FIXME: This will happen if the channel requested connection
            # to a non-existent (or non-publisher) UUID.  Ideally GraphServer
            # would tell us what happened rather than drop connection
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

        result = await reader.read(1)
        if result != Command.COMPLETE.value:
            # NOTE: The only reason this would happen is if the 
            # publisher's writer is closed due to a crash or shutdown
            raise ValueError(f'failed to create channel {pub_id=}')
        
        num_buffers = await read_int(reader)
        
        chan = cls(UUID(id_str), pub_id, num_buffers, shm, graph_address)

        chan._graph_task = asyncio.create_task(
            chan._graph_connection(graph_reader, graph_writer),
            name = f'chan-{chan.id}: _graph_connection'
        )

        chan._pub_writer = writer
        chan._pub_task = asyncio.create_task(
            chan._publisher_connection(reader),
            name = f'chan-{chan.id}: _publisher_connection'
        )

        logger.debug(f'created channel {chan.id=} {pub_id=} {pub_address=}')

        return chan
    
    def close(self) -> None:
        if self.shm is not None:
            self.shm.close()
        self._graph_task.cancel()
        self._pub_task.cancel()

    async def wait_closed(self) -> None:
        if self.shm is not None:
            await self.shm.wait_closed()
        with suppress(asyncio.CancelledError):
            await self._graph_task
        with suppress(asyncio.CancelledError):
            await self._pub_task
    
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
                        await self.shm.wait_closed()
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

                self._notify_clients(msg_id)

        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"connection fail: channel:{self.id} - pub:{self.pub_id}")

        finally:
            await close_stream_writer(self._pub_writer)
            logger.debug(f"disconnected: channel:{self.id} -> pub:{id}")

    def _notify_clients(self, msg_id: int) -> None:
        for client_id, queue in self.clients.items():
            if queue is None: continue # queue is none if this is the pub
            self.backpressure.lease(client_id, msg_id % self.num_buffers)
            queue.put_nowait((self.pub_id, msg_id))

    def put_local(self, msg_id: int, msg: typing.Any) -> bool:
        """
        put an object into cache (should only be used by Publishers)
        returns true if any clients were notified
        """
        self._notify_clients(msg_id)

        # if buffer is still available after notify, no subs were notified here
        buf_idx = msg_id % self.num_buffers
        if self.backpressure.available(buf_idx):
            return False
        
        self.cache_id[buf_idx] = msg_id
        self.cache[buf_idx] = msg
        return True

    @contextmanager
    def get(self, msg_id: int, client_id: UUID) -> typing.Generator[typing.Any, None, None]:
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
                    # Could deepcopy and put in cache here...
                    # Profiling indicates its faster to repeatedly
                    # reconstruct <=512kB msgs from memory for up to
                    # about 4 subs -- deepcopy is about 4x slower
                    # which I suspect will be majority of cases
                    # With more than 4 subs, one deepcopy may be faster
                    # for <=512kB msgs, but becomes remarkably time
                    # consuming for >= 512kB msgs.

                    # TODO: Implement a tuning method that runs on
                    # first ezmsg run that determines this tradeoff
                    # and decides fastest behavior; then decide 
                    # to cache or not cache depending on this profiling
                    # for this particular machine.
                    # NOTE: We have information about the message size and
                    # the current fanout at time of RX, so we could
                    # intelligently copy here!
                    # self.shm.buf_size # Buffer size
                    # len(self.clients) # Channel fanout
                    yield obj

        self.backpressure.free(client_id, buf_idx)
        if self.backpressure.buffers[buf_idx].is_empty:
            try:
                ack = Command.RX_ACK.value + uint64_to_bytes(msg_id)
                self._pub_writer.write(ack)
            except (BrokenPipeError, ConnectionResetError):
                logger.info(f"ack fail: channel:{self.id} -> pub:{self.pub_id}")

    def register_client(self, client_id: UUID, queue: NotificationQueue | None = None) -> None:
        self.clients[client_id] = queue 

    def unregister_client(self, client_id: UUID) -> None:
        queue = self.clients[client_id]

        # queue is only 'None' if this client is a local publisher
        if queue is not None:
            for _ in range(queue.qsize()):
                pub_id, msg_id = queue.get_nowait()
                if pub_id != self.pub_id:
                    queue.put_nowait((pub_id, msg_id))

            self.backpressure.free(client_id)

        del self.clients[client_id]

    def clear_cache(self):
        self.cache_id = [None] * self.num_buffers
        self.cache = [None] * self.num_buffers


def _ensure_address(address: AddressType | None) -> Address:
    if address is None:
        return Address.from_string(GRAPHSERVER_ADDR)

    elif not isinstance(address, Address):
        return Address(*address)
    
    return address


class _ChannelManager:
    
    _registry: typing.Dict[Address, typing.Dict[UUID, _Channel]]

    def __init__(self):
        default_address = Address.from_string(GRAPHSERVER_ADDR)
        self._registry = {default_address: dict()}
        self._lock = asyncio.Lock()

    async def get(
        self,
        pub_id: UUID,
        graph_address: AddressType | None = None,
        create: bool = False
    ) -> _Channel:
        graph_address = _ensure_address(graph_address)
        channel = self._registry.get(graph_address, dict()).get(pub_id, None)
        if create and channel is None:
            channel = await _Channel.create(pub_id, graph_address)
            channels = self._registry.get(graph_address, dict())
            channels[pub_id] = channel
            self._registry[graph_address] = channels
        if channel is None:
            raise KeyError(f"channel {pub_id=} {graph_address=} does not exist")
        return channel

    async def register(
        self, 
        pub_id: UUID, 
        client_id: UUID, 
        queue: NotificationQueue | None = None, 
        graph_address: AddressType | None = None
    ) -> _Channel:
        channel = await self.get(pub_id, graph_address, create = True)
        channel.register_client(client_id, queue)
        return channel

    async def unregister(
        self, 
        pub_id: UUID, 
        client_id: UUID, 
        graph_address: AddressType | None = None
    ) -> None:
        channel = await self.get(pub_id, graph_address)
        channel.unregister_client(client_id)

        if len(channel.clients) == 0:
            channel.close()
            await channel.wait_closed()
            graph_address = _ensure_address(graph_address)
            registry = self._registry[graph_address]
            del registry[pub_id]
            logger.debug(f'closed channel {pub_id}: no clients')


CHANNELS = _ChannelManager()