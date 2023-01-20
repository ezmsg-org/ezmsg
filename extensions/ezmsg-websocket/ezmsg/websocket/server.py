import asyncio
import ssl

import websockets.server
import websockets.exceptions

import ezmsg.core as ez

from . import WebsocketSettings, WebsocketState

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
        ez.logger.info(f'Starting WS Input Server @ ws://{self.SETTINGS.host}:{self.SETTINGS.port}')

        async def connection(websocket: websockets.server.WebSocketServerProtocol, path):
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
                except websockets.exceptions.ConnectionClosedOK:
                    pass
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    print('Error in websocket server:', e)
                    pass
                finally:
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
            ...

    @ez.publisher(OUTPUT)
    async def publish_incoming(self):
        while True:
            data = await self.STATE.incoming_queue.get()
            yield self.OUTPUT, data

    @ez.subscriber(INPUT)
    async def transmit_outgoing(self, message: bytes):
        self.STATE.outgoing_queue.put_nowait(message)
