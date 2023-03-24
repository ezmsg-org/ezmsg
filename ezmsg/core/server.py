import os
import asyncio
import logging
import signal
import socket
import typing

from contextlib import suppress
from threading import Thread
from multiprocessing import Process, Event
from multiprocessing.synchronize import Event as EventType
from concurrent.futures import Future

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

class ThreadedAsyncServer(Process):
    """ 
    A primitive for an asyncio server that runs in a dedicated loop
    either in a separate thread or as a separate process.
    Mix all of the parallelism together! YAY!
    """

    _server_up: EventType
    _shutdown: EventType

    _thread: Thread
    _loop: asyncio.AbstractEventLoop
    _serve_future: Future[None]

    ADDR_ENV: str
    PORT_DEFAULT: int

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self._server_up = Event()
        self._shutdown = Event()

    @classmethod
    def address(cls) -> Address:
        address_str = os.environ.get(cls.ADDR_ENV, f'127.0.0.1:{cls.PORT_DEFAULT}')
        return Address.from_string(address_str)

    @classmethod
    async def connect(cls, address: typing.Optional[AddressType] = None):
        f""" 
        If address is None, check $cls.ADDR_ENV for an address
        - If $cls.ADDR_ENV also not defined, check the default 
          Server Address cls.ADDR_DEFAULT for a running server
          and start a Server on a random (non-default) port if there isn't 
          a Server running there.  We also set $cls.ADDR_ENV to the
          new server's address
        - If $cls.ADDR_ENV is defined; we force a connection to that server
          and raise ConnectionRefusedError if that address can't be connected to.
        """
        server = None
        ensure_server = False
        if address is not None:
            address = Address(*address)
        else:
            ensure_server = cls.ADDR_ENV not in os.environ
            address = cls.address()
        
        try:
            _, writer = await asyncio.open_connection(*address)
            await close_stream_writer(writer)

        except ConnectionRefusedError as ref_e:
            if not ensure_server:
                raise ref_e

            try:
                start_port = int(os.environ.get(SERVER_PORT_START_ENV, SERVER_PORT_START_DEFAULT))
                sock = create_socket(start_port = start_port)
                os.environ[cls.ADDR_ENV] = str(Address(*sock.getsockname()))

                # Start server in a new threaded event loop
                server = cls()
                server.start_server(sock)

            except IOError:
                logger.error(f"Could not connect to {cls.__name__} or find an open port to host it on")
                raise ref_e
            
        return server
    

    ## MULTIPROCESSING INTERFACE
    def start(self, address: AddressType) -> None:
        self._address = Address(*address)
        super().start()
        self._server_up.wait()

    def stop(self) -> None:
        self.stop_server()
        self.join()

    def run(self) -> None:
        handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        self.start_server(self._address)
        self.join_server()
        signal.signal(signal.SIGINT, handler)


    ## THREADING INTERFACE
    def start_server(self, endpoint: typing.Union[Address, socket.socket]) -> None:
        if not isinstance(endpoint, socket.socket):
            endpoint = endpoint.bind_socket()
        self._loop = asyncio.new_event_loop()
        self._thread = Thread(target = self._loop_thread, daemon = True)
        self._thread.start()
        self._serve_future = asyncio.run_coroutine_threadsafe(self._serve(endpoint), self._loop)
        self._server_up.wait()

    def stop_server(self) -> None:
        self._shutdown.set()

    def join_server(self) -> None:
        with suppress(asyncio.CancelledError):
            self._serve_future.result()
        self._loop.stop()
        
    def _loop_thread(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()


    ## SERVER INTERNALS
    async def _serve(self, sock: socket.socket) -> None:
        await self.setup()

        server = await asyncio.start_server(self.api, sock = sock)

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
    
