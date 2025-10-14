import os
from collections.abc import Callable
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
    """
    An asyncio server that runs in a dedicated loop in a separate thread.
    
    This class provides a foundation for running asyncio-based servers in their own
    threads, allowing for concurrent operation with other parts of the application.

    :obj:`GraphServer` inherits from this class to implement specific server functionality.
    """

    _server_up: Event
    _shutdown: Event

    _sock: socket.socket
    _loop: asyncio.AbstractEventLoop

    def __init__(self):
        """
        Initialize the threaded async server.
        """
        super().__init__(daemon=True)
        self._server_up = Event()
        self._shutdown = Event()

    @property
    def address(self) -> Address:
        return Address(*self._sock.getsockname())

    def start(self, address: AddressType | None = None) -> None:
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
        """
        Run the async server in its own thread.
        
        This method starts a new asyncio event loop and runs the server's main
        execution logic within that loop.
        """
        try:
            asyncio.set_event_loop(self._loop)
            with suppress(asyncio.CancelledError):
                self._loop.run_until_complete(self.amain())
        finally:
            self._loop.stop()
            self._loop.close()

    async def amain(self) -> None:
        """
        Main asynchronous execution method for the server.
        
        This abstract method should be implemented by subclasses to define
        the specific server behavior and async operations.
        """
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
        """
        Setup method called before server starts serving.
        
        This method can be overridden by subclasses to perform any
        initialization needed before the server begins accepting connections.
        """
        ...

    async def shutdown(self) -> None: 
        """
        Shutdown method called when server is stopping.
        
        This method can be overridden by subclasses to perform any
        cleanup needed when the server is shutting down.
        """
        ...

    async def api(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """
        API handler for client connections.
        
        This abstract method must be implemented by subclasses to handle
        the server's communication protocol with connected clients.
        
        :param reader: Stream reader for receiving data from clients.
        :type reader: asyncio.StreamReader
        :param writer: Stream writer for sending data to clients.
        :type writer: asyncio.StreamWriter
        :raises NotImplementedError: Must be implemented by subclasses.
        """
        raise NotImplementedError


T = typing.TypeVar("T", bound=ThreadedAsyncServer)


class ServiceManager(typing.Generic[T]):
    """
    Manages the lifecycle and connection of threaded async servers.
    
    This generic class provides utilities for ensuring servers are running,
    managing connections, and handling server creation with proper addressing.
    
    :type T: A ThreadedAsyncServer subclass type.
    """
    _address: Address | None = None
    _factory: Callable[[], T]

    ADDR_ENV: str
    PORT_DEFAULT: int

    def __init__(
        self,
        factory: Callable[[], T],
        address: AddressType | None = None,
    ) -> None:
        """
        Initialize the service manager.
        
        :param factory: Factory function that creates server instances.
        :type factory: collections.abc.Callable[[], T]
        :param address: Optional address tuple (host, port) for the server.
        :type address: AddressType | None
        """
        self._factory = factory
        if address is not None:
            self._address = Address(*address)

    async def ensure(self) -> T | None:
        """
        Ensure a server is running and accessible.
        
        Attempts to connect to an existing server. If connection fails and
        we should ensure a server exists, creates a new server instance.
        
        :return: The server instance if one was created, None if using existing.
        :rtype: T | None
        :raises OSError: If connection fails and no server should be created.
        """
        server = None
        ensure_server = False
        if self._address is None:
            ensure_server = self.ADDR_ENV not in os.environ

        try:
            reader, writer = await self.open_connection()
            await close_stream_writer(writer)

        except OSError as ref_e:
            if not ensure_server:
                raise ref_e

            server = self.create_server()

        return server

    @property
    def address(self) -> Address:
        """
        Get the server address.
        
        :return: The configured address or default address if none set.
        :rtype: Address
        """
        return self._address if self._address is not None else self.default_address()

    @classmethod
    def default_address(cls) -> Address:
        """
        Get the default server address from environment or class constants.
        
        :return: Address parsed from environment variable or default host:port.
        :rtype: Address
        """
        address_str = os.environ.get(cls.ADDR_ENV, f"127.0.0.1:{cls.PORT_DEFAULT}")
        return Address.from_string(address_str)

    def create_server(self) -> T:
        """
        Create and start a new server instance.
        
        :return: The newly created and started server instance.
        :rtype: T
        """
        server = self._factory()
        server.start(self._address)
        self._address = server.address
        return server

    async def open_connection(
        self,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """
        Open a connection to the managed server.
        
        :return: Tuple of (reader, writer) for communicating with the server.
        :rtype: tuple[asyncio.StreamReader, asyncio.StreamWriter]
        :raises OSError: If connection cannot be established.
        """
        return await asyncio.open_connection(
            *(self.default_address() if self._address is None else self._address)
        )
