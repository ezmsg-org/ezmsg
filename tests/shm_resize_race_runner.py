import asyncio
import os
import time
from dataclasses import dataclass

import ezmsg.core as ez

INITIAL_SHM_SIZE = 64
NUM_MSGS = 4
SUBSCRIBER_DELAY_S = 0.001
READY_TOKEN = "READY"
DONE_TOKEN = "DONE"


@dataclass
class BurstMessage:
    seq: int
    payload: bytes
    created_at: float


class BurstyPublisher(ez.Unit):
    OUTPUT = ez.OutputStream(
        BurstMessage,
        num_buffers=2,
        buf_size=INITIAL_SHM_SIZE,
        force_tcp=False,
        allow_local=False,
    )

    @ez.publisher(OUTPUT)
    async def pump(self):
        cur_size = INITIAL_SHM_SIZE
        print(READY_TOKEN, flush=True)
        for itr in range(NUM_MSGS):
            cur_size *= 3
            yield self.OUTPUT, BurstMessage(
                seq=itr,
                payload=bytes(cur_size),
                created_at=time.time(),
            )


class SubscriberState(ez.State):
    cur_msg: int = 0


class Subscriber(ez.Unit):
    INPUT = ez.InputStream(BurstMessage)
    STATE = SubscriberState

    @ez.subscriber(INPUT)
    async def on_message(self, msg: BurstMessage) -> None:
        await asyncio.sleep(SUBSCRIBER_DELAY_S)
        self.STATE.cur_msg += 1
        if self.STATE.cur_msg == NUM_MSGS:
            print(DONE_TOKEN, flush=True)
            raise ez.NormalTermination


class ReproSystem(ez.Collection):
    PUB = BurstyPublisher()
    SUB = Subscriber()

    def network(self) -> ez.NetworkDefinition:
        return ((self.PUB.OUTPUT, self.SUB.INPUT),)

    def process_components(self) -> list[ez.Component]:
        return [self.PUB, self.SUB]


if __name__ == "__main__":
    ez.run(SYSTEM=ReproSystem())
