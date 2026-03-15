import asyncio
import logging
import os
import pickle
import socket
import time

from uuid import UUID, uuid1

from .graphmeta import (
    ProcessRegistration,
    ProcessOwnershipUpdate,
    ProcessSettingsUpdate,
    SettingsSnapshotValue,
)
from .graphserver import GraphService
from .netprotocol import (
    AddressType,
    Command,
    close_stream_writer,
    read_str,
    uint64_to_bytes,
)

logger = logging.getLogger("ezmsg")


class ProcessControlClient:
    _graph_address: AddressType | None
    _process_id: str
    _client_id: UUID | None
    _reader: asyncio.StreamReader | None
    _writer: asyncio.StreamWriter | None
    _lock: asyncio.Lock

    def __init__(
        self, graph_address: AddressType | None = None, process_id: str | None = None
    ) -> None:
        self._graph_address = graph_address
        self._process_id = process_id if process_id is not None else str(uuid1())
        self._client_id = None
        self._reader = None
        self._writer = None
        self._lock = asyncio.Lock()

    @property
    def process_id(self) -> str:
        return self._process_id

    @property
    def client_id(self) -> UUID | None:
        return self._client_id

    async def connect(self) -> None:
        if self._writer is not None:
            return

        reader, writer = await GraphService(self._graph_address).open_connection()
        writer.write(Command.PROCESS.value)
        await writer.drain()

        client_id = UUID(await read_str(reader))
        response = await reader.read(1)
        if response != Command.COMPLETE.value:
            await close_stream_writer(writer)
            raise RuntimeError("Failed to create process control connection")

        self._client_id = client_id
        self._reader = reader
        self._writer = writer

    async def register(self, units: list[str]) -> None:
        await self.connect()
        payload = ProcessRegistration(
            process_id=self._process_id,
            pid=os.getpid(),
            host=socket.gethostname(),
            units=sorted(set(units)),
        )
        await self._payload_command(Command.PROCESS_REGISTER, payload)

    async def update_ownership(
        self,
        added_units: list[str] | None = None,
        removed_units: list[str] | None = None,
    ) -> None:
        await self.connect()
        payload = ProcessOwnershipUpdate(
            process_id=self._process_id,
            added_units=sorted(set(added_units or [])),
            removed_units=sorted(set(removed_units or [])),
        )
        await self._payload_command(Command.PROCESS_UPDATE_OWNERSHIP, payload)

    async def report_settings_update(
        self,
        component_address: str,
        value: SettingsSnapshotValue,
        timestamp: float | None = None,
    ) -> None:
        await self.connect()
        payload = ProcessSettingsUpdate(
            process_id=self._process_id,
            component_address=component_address,
            value=value,
            timestamp=timestamp if timestamp is not None else time.time(),
        )
        await self._payload_command(Command.PROCESS_SETTINGS_UPDATE, payload)

    async def close(self) -> None:
        writer = self._writer
        if writer is None:
            return

        self._reader = None
        self._writer = None
        self._client_id = None
        await close_stream_writer(writer)

    async def _payload_command(self, command: Command, payload_obj: object) -> None:
        reader = self._reader
        writer = self._writer
        if reader is None or writer is None:
            raise RuntimeError("Process control connection is not active")

        payload = pickle.dumps(payload_obj)
        async with self._lock:
            writer.write(command.value)
            writer.write(uint64_to_bytes(len(payload)))
            writer.write(payload)
            await writer.drain()

            response = await reader.read(1)
            if response != Command.COMPLETE.value:
                raise RuntimeError(
                    f"Unexpected response to process control command: {command.name}"
                )
