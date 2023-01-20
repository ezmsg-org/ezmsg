import asyncio

import ezmsg.core as ez

from websockets.legacy.client import connect, WebSocketClientProtocol

from . import WebsocketSettings, WebsocketState

from typing import AsyncGenerator

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

    async def tx_to(self,websocket: WebSocketClientProtocol):
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
                websocket = await connect(uri)
                break
            except:
                await asyncio.sleep(0.5)

        if websocket is None:
            raise Exception(f"Could not connect to {uri}")

        receive_task = asyncio.ensure_future(self.rx_from(websocket))
        transmit_task = asyncio.ensure_future(self.tx_to(websocket))
        done, pending = await asyncio.wait(
            [receive_task, transmit_task],
            return_when=asyncio.FIRST_COMPLETED
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