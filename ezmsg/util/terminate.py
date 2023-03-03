import asyncio
import time

from dataclasses import dataclass

import ezmsg.core as ez

from typing import Optional, Any

@dataclass(frozen = True)
class TerminateOnTimeoutSettingsMessage:
    time: float = 2.0  # Terminate if no message has been received in this time (sec)
    poll_rate: float = 4.0  # Probably no good reason to mess with this (Hz)


class TerminateOnTimeoutSettings(ez.Settings, TerminateOnTimeoutSettingsMessage):
    ...


class TerminateOnTimeoutState(ez.State):
    last_msg_timestamp: Optional[float] = None


class TerminateOnTimeout(ez.Unit):

    SETTINGS: TerminateOnTimeoutSettings
    STATE: TerminateOnTimeoutState

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
                    ez.logger.info("Raising NormalTermination in TerminateOnTimeout!")
                    raise ez.NormalTermination
                
                
@dataclass(frozen = True)
class TerminateOnTotalSettingsMessage:
    total: Optional[int] = None


class TerminateOnTotalSettings(ez.Settings, TerminateOnTotalSettingsMessage):
    ...


class TerminateOnTotalState(ez.State):
    total: Optional[int]
    n_messages: int = 0


class TerminateOnTotal(ez.Unit):
    SETTINGS: TerminateOnTotalSettings
    STATE: TerminateOnTotalState

    INPUT = ez.InputStream(Any)
    INPUT_TOTAL = ez.InputStream(int)

    def initialize(self) -> None:
        self.STATE.total = self.SETTINGS.total

    @ez.subscriber(INPUT_TOTAL)
    async def on_total(self, msg: int) -> None:
        self.STATE.total = msg
        self.maybe_terminate()

    @ez.subscriber(INPUT)
    async def on_message(self, _: Any) -> None:
        self.STATE.n_messages += 1
        self.maybe_terminate()

    def maybe_terminate(self):
        if self.STATE.total is not None and self.STATE.n_messages >= self.STATE.total:
            raise ez.NormalTermination
