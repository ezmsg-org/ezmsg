import math
import time
import asyncio

import ezmsg.core as ez

from typing import AsyncGenerator


class LFOSettings(ez.Settings):
    freq: float = 0.2  # Hz, sinus frequency
    update_rate: float = 2.0  # Hz, update rate


class LFO(ez.Unit):
    SETTINGS: LFOSettings

    OUTPUT = ez.OutputStream(float)

    def initialize(self) -> None:
        self.start_time = time.time()

    @ez.publisher(OUTPUT)
    async def generate(self) -> AsyncGenerator:
        while True:
            t = time.time() - self.start_time
            yield self.OUTPUT, math.sin(2.0 * math.pi * self.SETTINGS.freq * t)
            await asyncio.sleep(1.0 / self.SETTINGS.update_rate)
