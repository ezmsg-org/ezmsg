import asyncio
import ssl

from dataclasses import field

import websockets.server
import websockets.exceptions
from websockets.legacy.client import connect, WebSocketClientProtocol

import ezmsg.core as ez

from typing import Optional, Union, AsyncGenerator


class WebsocketSettings(ez.Settings):
    host: str
    port: int
    cert_path: Optional[str] = None


class WebsocketState(ez.State):
    incoming_queue: "asyncio.Queue[Union[str,bytes]]" = field(
        default_factory=asyncio.Queue
    )
    outgoing_queue: "asyncio.Queue[Union[str,bytes]]" = field(
        default_factory=asyncio.Queue
    )


class WebsocketServer(ez.Unit):

    """
    Receives arbitrary content from outside world
    and injects it into system in a DataArray
    """

    SETTINGS: WebsocketSettings
    STATE: WebsocketState

    INPUT = ez.InputStream(bytes)
    OUTPUT = ez.OutputStream(bytes)

    @ez.task
    async def start_server(self):
        ez.logger.info(
            f"Starting WS Input Server @ ws://{self.SETTINGS.host}:{self.SETTINGS.port}"
        )

        async def connection(
            websocket: websockets.server.WebSocketServerProtocol, path
        ):
            async def loop(mode):
                try:
                    if mode == "rx":
                        while True:
                            data = await websocket.recv()
                            self.STATE.incoming_queue.put_nowait(data)
                    elif mode == "tx":
                        while True:
                            data = await self.STATE.outgoing_queue.get()
                            await websocket.send(data)
                except websockets.exceptions.ConnectionClosedOK:
                    pass
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    print("Error in websocket server:", e)
                    pass
                finally:
                    ...

            await asyncio.wait([loop(mode="tx"), loop(mode="rx")])

        try:
            if self.SETTINGS.cert_path:
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                ssl_context.load_cert_chain(self.SETTINGS.cert_path)
            else:
                ssl_context = None

            server = await websockets.server.serve(
                connection, self.SETTINGS.host, self.SETTINGS.port, ssl=ssl_context
            )

            await server.wait_closed()

        finally:
            ...

    @ez.publisher(OUTPUT)
    async def publish_incoming(self):
        while True:
            data = await self.STATE.incoming_queue.get()
            yield self.OUTPUT, data

    @ez.subscriber(INPUT)
    async def transmit_outgoing(self, message: bytes):
        self.STATE.outgoing_queue.put_nowait(message)


class WebsocketClient(ez.Unit):
    SETTINGS: WebsocketSettings
    STATE: WebsocketState

    INPUT = ez.InputStream(bytes)
    OUTPUT = ez.OutputStream(bytes)

    async def rx_from(self, websocket: WebSocketClientProtocol):
        # await incoming data from websocket and post them
        # to incoming queue for publication within ezmsg
        async for message in websocket:
            self.STATE.incoming_queue.put_nowait(message)

    async def tx_to(self, websocket: WebSocketClientProtocol):
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
        uri = f"{prefix}://{self.SETTINGS.host}:{self.SETTINGS.port}"
        websocket = None
        for attempt in range(10):
            try:
                websocket = await connect(uri)
                break
            except:
                await asyncio.sleep(0.5)

        if websocket is None:
            raise Exception(f"Could not connect to {uri}")

        receive_task = asyncio.ensure_future(self.rx_from(websocket))
        transmit_task = asyncio.ensure_future(self.tx_to(websocket))
        done, pending = await asyncio.wait(
            [receive_task, transmit_task], return_when=asyncio.FIRST_COMPLETED
        )

        for task in pending:
            task.cancel()

        await websocket.close()

    @ez.publisher(OUTPUT)
    async def receive(self) -> AsyncGenerator:
        while True:
            message = await self.STATE.incoming_queue.get()
            yield self.OUTPUT, message

    @ez.subscriber(INPUT)
    async def transmit(self, message: bytes) -> None:
        self.STATE.outgoing_queue.put_nowait(message)
