import asyncio
import logging
import typing

from uuid import UUID
from contextlib import asynccontextmanager, suppress
from copy import deepcopy

from .graphserver import GraphService
from .messagechannel import CHANNELS

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
    _graph_task: "asyncio.Task[None]"
    _cur_pubs: typing.Set[UUID]
    _incoming: "asyncio.Queue[typing.Tuple[UUID, int]]"

    @classmethod
    async def create(
        cls, 
        topic: str, 
        graph_address: AddressType | None, 
        **kwargs
    ) -> "Subscriber":
        reader, writer = await GraphService(graph_address).open_connection()
        writer.write(Command.SUBSCRIBE.value)
        writer.write(encode_str(topic))

        result = await reader.readexactly(1)
        if result != Command.COMPLETE.value:
            logger.warning(f'Could not create subscriber {topic=}')

        id_str = await read_str(reader)

        sub = cls(UUID(id_str), topic, graph_address, **kwargs)
        sub._graph_task = asyncio.create_task(sub._graph_connection(reader, writer))

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
                    pub_ids = set([UUID(id) for id in update.split(',')])

                    for pub_id in set(pub_ids - self._cur_pubs):
                        channel = await CHANNELS.get(pub_id, self._graph_address)
                        channel.subscribe(self.id, self._incoming)

                    for pub_id in set(self._cur_pubs - pub_ids):
                        channel = await CHANNELS.get(pub_id, self._graph_address)
                        channel.unsubscribe(self.id)

                    writer.write(Command.COMPLETE.value)
                    await writer.drain()

                else:
                    logger.warning(
                        f"Subscriber {self.id} rx unknown command from GraphServer: {cmd}"
                    )

        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"Subscriber {self.id} lost connection to graph server")

        finally:
            CHANNELS.unsubscribe_all(self.id, self._graph_address) # good idea?
            await close_stream_writer(writer)

    async def recv(self) -> typing.Any:
        out_msg = None
        async with self.recv_zero_copy() as msg:
            out_msg = deepcopy(msg)
        return out_msg

    @asynccontextmanager
    async def recv_zero_copy(self) -> typing.AsyncGenerator[typing.Any, None]:
        id, msg_id = await self._incoming.get()

        channel = await CHANNELS.get(id, self._graph_address)
        with channel.get(msg_id, self.id) as msg:
            yield msg

  
