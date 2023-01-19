import asyncio
from dataclasses import field
import websockets
import websockets.server
from typing import Optional
import ssl

import ezmsg.core as ez

from .__version__ import __version__

from typing import AsyncGenerator, ByteString


class WebsocketSettings(ez.Settings):
    host: str
    port: int
    cert_path: Optional[str] = None


class WebsocketState(ez.State):
    incoming_queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    outgoing_queue: asyncio.Queue = field(default_factory=asyncio.Queue)


class WebsocketServer(ez.Unit):
    """
    Receives arbitrary content from outside world
    and injects it into system in a DataArray
    """

    SETTINGS: WebsocketSettings
    STATE: WebsocketState

    INPUT = ez.InputStream(ByteString)
    OUTPUT = ez.OutputStream(ByteString)

    @ez.task
    async def start_server(self):
        # print( f'Starting WS Input Server @ ws://{self.SETTINGS.host}:{self.SETTINGS.port}' )

        async def connection(websocket: websockets.WebSocketServerProtocol, path):
            # print( 'Client Connected to Websocket Input' )

            async def loop(mode):
                try:
                    if mode == 'rx':
                        while True:
                            data = await websocket.recv()
                            self.STATE.incoming_queue.put_nowait(data)
                    elif mode == 'tx':
                        while True:
                            data = await self.STATE.outgoing_queue.get()
                            await websocket.send(data)
                except websockets.ConnectionClosedOK:
                    pass
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    print('Error in websocket server:', e)
                    pass
                finally:
                    # print( 'Websocket Receiver Task Cancelled' )
                    ...

            await asyncio.wait([
                loop(mode='tx'),
                loop(mode='rx')
            ])

        try:

            if self.SETTINGS.cert_path:
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                ssl_context.load_cert_chain(self.SETTINGS.cert_path)
            else:
                ssl_context = None

            server = await websockets.server.serve(
                connection,
                self.SETTINGS.host,
                self.SETTINGS.port,
                ssl=ssl_context
            )

            await server.wait_closed()

        finally:
            # print( 'Closing Websocket' )
            ...

    @ez.publisher(OUTPUT)
    async def publish_incoming(self):
        while True:
            data = await self.STATE.incoming_queue.get()
            yield self.OUTPUT, data

    @ez.subscriber(INPUT)
    async def transmit_outgoing(self, message: ByteString):
        self.STATE.outgoing_queue.put_nowait(message)


class WebsocketClient(ez.Unit):

    SETTINGS: WebsocketSettings
    STATE: WebsocketState

    INPUT = ez.InputStream(ByteString)
    OUTPUT = ez.OutputStream(ByteString)

    async def receive_from_websocket(self,
                                     websocket: websockets.WebSocketClientProtocol
                                     ):
        # await incoming data from websocket and post them
        # to incoming queue for publication within ezmsg
        async for message in websocket:
            self.STATE.incoming_queue.put_nowait(message)

    async def transmit_to_websocket(self,
                                    websocket: websockets.WebSocketClientProtocol
                                    ):
        # await messages from subscription within ezmsg
        # and post them to outgoing websocket
        while True:
            message = await self.STATE.outgoing_queue.get()
            await websocket.send(message)

    @ez.task
    async def connection(self):
        if self.SETTINGS.cert_path:
            prefix = "wss"
        else:
            prefix = "ws"
        uri = f'{prefix}://{self.SETTINGS.host}:{self.SETTINGS.port}'
        websocket = None
        for attempt in range(10):
            try:
                websocket = await websockets.connect(uri)
                break
            except BaseException:
                await asyncio.sleep(0.5)

        if websocket is None:
            raise Exception(f"Could not connect to {uri}")

        receive_task = asyncio.ensure_future(
            self.receive_from_websocket(websocket))
        transmit_task = asyncio.ensure_future(
            self.transmit_to_websocket(websocket))
        done, pending = await asyncio.wait(
            [receive_task, transmit_task],
            return_when=asyncio.FIRST_COMPLETED
        )

        for task in pending:
            task.cancel()

        websocket.close()

    @ez.publisher(OUTPUT)
    async def receive(self) -> AsyncGenerator:
        while True:
            message = await self.STATE.incoming_queue.get()
            yield self.OUTPUT, message

    @ez.subscriber(INPUT)
    async def transmit(self, message: ByteString) -> None:
        self.STATE.outgoing_queue.put_nowait(message)
