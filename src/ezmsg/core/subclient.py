import asyncio
import logging
import typing

from uuid import UUID
from contextlib import asynccontextmanager, suppress
from copy import deepcopy

from .graphserver import GraphService
from .messagechannel import CHANNELS, NotificationQueue, _Channel

from .netprotocol import (
    AddressType,
    read_str,
    encode_str,
    close_stream_writer,
    Command,
)


logger = logging.getLogger("ezmsg")


class Subscriber:
    id: UUID
    topic: str

    _graph_address: AddressType | None
    _graph_task: asyncio.Task[None]
    _cur_pubs: typing.Set[UUID]
    _incoming: NotificationQueue

    # This is an optimization to retain a local handle to channels
    # so that dict lookup and wrapper contextmanager aren't in 
    # the hotpath - Griff
    _channels: typing.Dict[UUID, _Channel]

    @classmethod
    async def create(
        cls, 
        topic: str, 
        graph_address: AddressType | None, 
        **kwargs
    ) -> "Subscriber":
        logger.info(f'attempting to create sub {topic=}')
        reader, writer = await GraphService(graph_address).open_connection()
        writer.write(Command.SUBSCRIBE.value)
        writer.write(encode_str(topic))

        result = await reader.read(1)
        if result != Command.COMPLETE.value:
            logger.warning(f'Could not create subscriber {topic=}')

        id_str = await read_str(reader)

        sub = cls(UUID(id_str), topic, graph_address, **kwargs)
        sub._graph_task = asyncio.create_task(
            sub._graph_connection(reader, writer),
            name = f'sub-{sub.id}: _graph_connection'
        )

        logger.info(f'created sub {topic=} {sub.id=}')

        return sub

    def __init__(self, 
        id: UUID, 
        topic: str, 
        graph_address: AddressType | None,
        **kwargs
    ) -> None:
        """DO NOT USE this constructor, use Subscriber.create instead"""
        self.id = id
        self.topic = topic
        self._graph_address = graph_address

        self._cur_pubs = set()
        self._incoming = asyncio.Queue()
        self._channels = dict()

    def close(self) -> None:
        self._graph_task.cancel()

    async def wait_closed(self) -> None:
        with suppress(asyncio.CancelledError):
            await self._graph_task

    async def _graph_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                cmd = await reader.read(1)

                if not cmd:
                    break

                elif cmd == Command.UPDATE.value:
                    update = await read_str(reader)
                    pub_ids = set([UUID(id) for id in update.split(',')]) if update else set()
                    logger.info(f'{pub_ids=}')

                    for pub_id in set(pub_ids - self._cur_pubs):
                        channel = await CHANNELS.register(pub_id, self.id, self._incoming, self._graph_address)
                        self._channels[pub_id] = channel

                    for pub_id in set(self._cur_pubs - pub_ids):
                        await CHANNELS.unregister(pub_id, self.id, self._graph_address)
                        del self._channels[pub_id]

                    writer.write(Command.COMPLETE.value)
                    await writer.drain()

                else:
                    logger.warning(
                        f"Subscriber {self.id} rx unknown command from GraphServer: {cmd}"
                    )

        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"Subscriber {self.id} lost connection to graph server")

        finally:
            for pub_id in self._channels:
                await CHANNELS.unregister(pub_id, self.id, self._graph_address)
            await close_stream_writer(writer)

    async def recv(self) -> typing.Any:
        out_msg = None
        async with self.recv_zero_copy() as msg:
            out_msg = deepcopy(msg)
        return out_msg

    @asynccontextmanager
    async def recv_zero_copy(self) -> typing.AsyncGenerator[typing.Any, None]:
        pub_id, msg_id = await self._incoming.get()

        with self._channels[pub_id].get(msg_id, self.id) as msg:
            yield msg

  
