import asyncio
from typing import AsyncGenerator
import ezmsg.core as ez

from ezmsg.testing.debuglog import DebugLog

class Pong(ez.Unit):
    OUTPUT = ez.OutputStream(str)

    @ez.publisher(OUTPUT)
    async def spawn_message(self) -> AsyncGenerator:
        while True:
            yield self.OUTPUT, "PONG from attach!"
            await asyncio.sleep(2.0)

class AttachedCollection(ez.Collection):
    LOG_IN = ez.InputStream(str)
    PONG_OUT = ez.OutputStream(str)

    LOG = DebugLog()
    PONG = Pong()

    def network(self):
        return (
                (self.LOG_IN, self.LOG.INPUT),
                (self.PONG.OUTPUT, self.PONG_OUT),
                )

if __name__ == '__main__':
    print( 'This example attaches to the system created/run by ezmsg_toy.py.' )
    attach = AttachedCollection()
    ez.run(
            attach,
            connections = (( 'PING_OUTPUT', attach.LOG_IN), (attach.PONG_OUT, 'FOO_SUB'))
            )
