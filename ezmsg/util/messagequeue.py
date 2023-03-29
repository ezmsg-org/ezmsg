import asyncio
from typing import Any, AsyncGenerator
import ezmsg.core as ez


class MessageQueueSettings(ez.Settings):
    maxsize: int = 0
    leaky: bool = False


class MessageQueueState(ez.State):
    msg_queue: asyncio.Queue
    leaky: bool


class MessageQueue(ez.Unit):
    SETTINGS: MessageQueueSettings
    STATE: MessageQueueState

    INPUT = ez.InputStream(Any)
    OUTPUT = ez.OutputStream(Any)

    def initialize(self):
        self.STATE.leaky = self.SETTINGS.leaky
        if self.SETTINGS.leaky is True and self.SETTINGS.maxsize <= 0:
            ez.logger.warning(
                "MessageQueue specified as leaky, but maxsize is not greater than 0. Queue will not leak."
            )
            self.STATE.leaky = False
        self.STATE.msg_queue = asyncio.Queue(self.SETTINGS.maxsize)

    @ez.task
    async def monitor_queue_size(self) -> None:
        while True:
            ez.logger.debug(
                f"MessageQueue has {self.STATE.msg_queue.qsize()} messages queued."
            )
            await asyncio.sleep(1.0)

    @ez.subscriber(INPUT)
    async def on_message(self, message: Any) -> None:
        if self.STATE.leaky is False:
            await self.STATE.msg_queue.put(message)
        else:
            try:
                self.STATE.msg_queue.put_nowait(message)
            except asyncio.QueueFull:
                self.STATE.msg_queue.get_nowait()
                self.STATE.msg_queue.put_nowait(message)

    @ez.publisher(OUTPUT)
    async def send_message(self) -> AsyncGenerator:
        while True:
            msg = await self.STATE.msg_queue.get()
            yield self.OUTPUT, msg
