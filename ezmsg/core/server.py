import os
import asyncio
import logging
import socket
import typing

from contextlib import suppress
from threading import Thread, Event

from .netprotocol import (
    Address,
    AddressType,
    close_server,
    close_stream_writer,
    create_socket,
    SERVER_PORT_START_ENV,
    SERVER_PORT_START_DEFAULT,
)

logger = logging.getLogger("ezmsg")


class ThreadedAsyncServer(Thread):
    """An asyncio server that runs in a dedicated loop in a separate thread"""

    _server_up: Event
    _shutdown: Event

    _sock: socket.socket
    _loop: asyncio.AbstractEventLoop

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self._server_up = Event()
        self._shutdown = Event()

    @property
    def address(self) -> Address:
        return Address(*self._sock.getsockname())

    def start(self, address: typing.Optional[AddressType] = None) -> None:
        if address is not None:
            self._sock = create_socket(*address)
        else:
            start_port = int(
                os.environ.get(SERVER_PORT_START_ENV, SERVER_PORT_START_DEFAULT)
            )
            self._sock = create_socket(start_port=start_port)

        self._loop = asyncio.new_event_loop()
        super().start()
        self._server_up.wait()

    def stop(self) -> None:
        self._shutdown.set()
        self.join()

    def run(self) -> None:
        try:
            asyncio.set_event_loop(self._loop)
            with suppress(asyncio.CancelledError):
                self._loop.run_until_complete(self._serve())
        finally:
            self._loop.stop()
            self._loop.close()

    async def _serve(self) -> None:
        await self.setup()

        server = await asyncio.start_server(self.api, sock=self._sock)

        async def monitor_shutdown() -> None:
            await self._loop.run_in_executor(None, self._shutdown.wait)
            await close_server(server)

        monitor_task = self._loop.create_task(monitor_shutdown())

        self._server_up.set()

        try:
            await server.serve_forever()

        finally:
            await self.shutdown()
            monitor_task.cancel()
            with suppress(asyncio.CancelledError):
                await monitor_task

    async def setup(self) -> None:
        ...

    async def shutdown(self) -> None:
        ...

    async def api(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        raise NotImplementedError


T = typing.TypeVar("T", bound=ThreadedAsyncServer)


class ServiceManager(typing.Generic[T]):
    _address: typing.Optional[Address] = None
    _factory: typing.Callable[[], T]

    ADDR_ENV: str
    PORT_DEFAULT: int

    def __init__(
        self,
        factory: typing.Callable[[], T],
        address: typing.Optional[AddressType] = None,
    ) -> None:
        self._factory = factory
        if address is not None:
            self._address = Address(*address)

    async def ensure(self) -> typing.Optional[T]:
        server = None
        ensure_server = False
        if self._address is None:
            ensure_server = self.ADDR_ENV not in os.environ

        try:
            reader, writer = await self.open_connection()
            await close_stream_writer(writer)

        except ConnectionRefusedError as ref_e:
            if not ensure_server:
                raise ref_e

            server = self.create_server()

        return server

    @property
    def address(self) -> Address:
        return self._address if self._address is not None else self.default_address()

    @classmethod
    def default_address(cls) -> Address:
        address_str = os.environ.get(cls.ADDR_ENV, f"127.0.0.1:{cls.PORT_DEFAULT}")
        return Address.from_string(address_str)

    def create_server(self) -> T:
        server = self._factory()
        server.start(self._address)
        self._address = server.address
        return server

    async def open_connection(
        self,
    ) -> typing.Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_connection(
            *(self.default_address() if self._address is None else self._address)
        )
