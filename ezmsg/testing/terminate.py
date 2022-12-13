
import asyncio
import time

import ezmsg.core as ez

from typing import Optional, Any


class TerminateTestSettings(ez.Settings):
    time: float = 2.0  # Terminate if no message has been received in this time (sec)
    poll_rate: float = 4.0  # Probably no good reason to mess with this (Hz)


class TerminateTestState(ez.State):
    last_msg_timestamp: Optional[float] = None


class TerminateTest(ez.Unit):

    SETTINGS: TerminateTestSettings
    STATE: TerminateTestState

    INPUT = ez.InputStream(Any)

    @ez.subscriber(INPUT)
    async def keepalive(self, _: Any) -> None:
        cur_timestamp = time.time()
        self.STATE.last_msg_timestamp = cur_timestamp

    @ez.task
    async def poll_terminate(self) -> None:
        while True:
            await asyncio.sleep(1.0 / self.SETTINGS.poll_rate)
            if self.STATE.last_msg_timestamp is not None:
                age = time.time() - self.STATE.last_msg_timestamp
                if age >= self.SETTINGS.time:
                    raise ez.NormalTermination
