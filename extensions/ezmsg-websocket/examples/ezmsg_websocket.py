import json
import ezmsg.core as ez

from ezmsg.testing.lfo import LFO, LFOSettings
from ezmsg.websocket import (
    WebsocketServer, WebsocketClient, WebsocketSettings
)

from typing import Any, AsyncGenerator, Dict


class JSONAdapter(ez.Unit):
    DICT_INPUT = ez.InputStream(Dict[str, Any])
    JSON_OUTPUT = ez.OutputStream(str)

    @ez.subscriber(DICT_INPUT)
    @ez.publisher(JSON_OUTPUT)
    async def dict_to_json(self, message: Dict[str, Any]) -> AsyncGenerator:
        yield self.JSON_OUTPUT, json.dumps(message)

    JSON_INPUT = ez.InputStream(str)
    DICT_OUTPUT = ez.OutputStream(Dict[str, Any])

    @ez.subscriber(JSON_INPUT)
    @ez.publisher(DICT_OUTPUT)
    async def json_to_dict(self, message: str) -> AsyncGenerator:
        yield self.DICT_OUTPUT, json.loads(message)


class DebugOutput(ez.Unit):
    INPUT = ez.InputStream(str)

    @ez.subscriber(INPUT)
    async def print(self, message: str) -> None:
        print('DEBUG:', message)


class WebsocketSystemSettings(ez.Settings):
    host: str
    port: int


class WebsocketSystem(ez.System):

    SETTINGS: WebsocketSystemSettings

    OSC = LFO()
    SERVER = WebsocketServer()
    JSON = JSONAdapter()
    OUT = DebugOutput()
    CLIENT = WebsocketClient()

    def configure(self) -> None:
        self.OSC.apply_settings(
            LFOSettings(
                freq=0.2,
                update_rate=1.0
            ))

        self.SERVER.apply_settings(
            WebsocketSettings(
                host=self.SETTINGS.host,
                port=self.SETTINGS.port
            ))

        self.CLIENT.apply_settings(
            WebsocketSettings(
                host=self.SETTINGS.host,
                port=self.SETTINGS.port
            )
        )

    # Define Connections
    def network(self) -> ez.NetworkDefinition:
        return (
            (self.OSC.OUTPUT, self.JSON.DICT_INPUT),
            (self.JSON.JSON_OUTPUT, self.SERVER.INPUT),

            (self.CLIENT.OUTPUT, self.CLIENT.INPUT),  # Relay

            (self.SERVER.OUTPUT, self.JSON.JSON_INPUT),
            (self.JSON.DICT_OUTPUT, self.OUT.INPUT)
        )


if __name__ == '__main__':

    host = '127.0.0.1'
    port = 5038

    # Run the websocket system
    system = WebsocketSystem()
    system.apply_settings(WebsocketSystemSettings(host=host, port=port))
    ez.run_system(system)
