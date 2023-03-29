import ezmsg.core as ez

from typing import AsyncGenerator, Optional, Any


class DebugLogSettings(ez.Settings):
    name: str = "DEBUG"  # Useful name for logger
    max_length: Optional[int] = 400  # No limit if `None``


class DebugLog(ez.Unit):
    SETTINGS: DebugLogSettings

    INPUT = ez.InputStream(Any)
    OUTPUT = ez.OutputStream(Any)

    @ez.subscriber(INPUT, zero_copy=True)
    @ez.publisher(OUTPUT)
    async def log(self, msg: Any) -> AsyncGenerator:
        logstr = f"{self.SETTINGS.name} - {msg=}"
        if self.SETTINGS.max_length is not None:
            if len(logstr) > self.SETTINGS.max_length:
                logstr = logstr[: self.SETTINGS.max_length]
                logstr = logstr + " ... [truncated]"
        ez.logger.info(logstr)
        yield self.OUTPUT, msg
