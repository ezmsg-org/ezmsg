import logging

import ezmsg.core as ez

from typing import AsyncGenerator, Optional, Any


class DebugLogSettings(ez.Settings):
    name: str = 'DEBUG'  # Useful name for logger
    max_length: Optional[int] = 400  # No limit if `None``


class DebugLogState(ez.State):
    logger: logging.Logger


class DebugLog(ez.Unit):

    SETTINGS: DebugLogSettings
    STATE: DebugLogState

    INPUT = ez.InputStream(Any)
    OUTPUT = ez.OutputStream(Any)

    def initialize(self) -> None:
        self.STATE.logger = logging.getLogger('ezmsg')

    @ez.subscriber(INPUT, zero_copy=True)
    @ez.publisher(OUTPUT)
    async def log(self, msg: Any) -> AsyncGenerator:
        logstr = f'{self.SETTINGS.name} : {msg=}'
        if self.SETTINGS.max_length is not None:
            if len(logstr) > self.SETTINGS.max_length:
                logstr = logstr[:self.SETTINGS.max_length]
                logstr = logstr + ' ... [truncated]'
        self.STATE.logger.info(logstr)
        yield self.OUTPUT, msg
