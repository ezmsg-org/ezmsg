import os
import asyncio
import logging

from uuid import UUID
from contextlib import suppress

from .graphserver import GraphServer

from .netprotocol import (
    Address,
    encode_str,
    uint64_to_bytes,
    Command,
)

from typing import Optional

logger = logging.getLogger('ezmsg')


class GraphClient:
    id: UUID
    pid: int
    topic: str

    _graph_server_task: Optional[asyncio.Task] = None

    def __init__(self, id: UUID, topic: str) -> None:
        self.id = id
        self.pid = os.getpid()
        self.topic = topic

    @staticmethod
    def client_type() -> bytes:
        return Command.CLIENT.value

    async def connect_to_servers(self, graph_address: Address) -> None:
        graph_reader, graph_writer = await GraphServer.open(graph_address)
        graph_writer.write(Command.CLIENT.value)
        graph_writer.write(self.client_type())
        graph_writer.write(encode_str(str(self.id)))
        graph_writer.write(uint64_to_bytes(self.pid))
        graph_writer.write(encode_str(self.topic))
        await graph_writer.drain()
        server_connection = self._graph_server_connection(graph_reader, graph_writer)
        self._graph_server_task = asyncio.create_task(server_connection)

    async def _graph_server_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        graph_lock = asyncio.Lock()
        try:
            while True:
                cmd = await reader.read(1)
                if len(cmd) == 0:
                    break
                async with graph_lock:
                    await self._handle_graph_command(cmd, reader, writer)

        except asyncio.CancelledError:
            ...

        except ConnectionResetError:
            logger.debug(f'Client {self.id} lost connection to graph server')

        finally:
            writer.close()
            ...

    async def _handle_graph_command(self, cmd: bytes, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        logger.warn(f'Client {self.id} rx unknown command from GraphServer {cmd}')

    def close(self) -> None:
        if self._graph_server_task is not None:
            self._graph_server_task.cancel()

    async def wait_closed(self) -> None:
        if self._graph_server_task is not None:
            with suppress(asyncio.CancelledError):
                await self._graph_server_task
