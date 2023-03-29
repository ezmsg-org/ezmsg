import ezmsg.core as ez

from .lfo import LFO

from typing import AsyncGenerator


class MultiplierSettings(ez.Settings):
    multiplier: float = 2.0


class Multiplier(ez.Unit):
    SETTINGS: MultiplierSettings

    INPUT = ez.InputStream(float)
    OUTPUT = ez.OutputStream(float)

    @ez.subscriber(INPUT)
    @ez.publisher(OUTPUT)
    async def modify(self, message: float) -> AsyncGenerator:
        yield self.OUTPUT, message * self.SETTINGS.multiplier


class MultiplierCollection(ez.Collection):
    SETTINGS: MultiplierSettings

    INPUT = ez.InputStream(float)
    OUTPUT = ez.OutputStream(float)
    GENERATOR_OUTPUT = ez.OutputStream(float)

    GENERATOR = LFO()

    MODIFIER1 = Multiplier()
    MODIFIER2 = Multiplier()

    def network(self) -> ez.NetworkDefinition:
        return (
            # Input to Output
            (self.INPUT, self.MODIFIER1.INPUT),
            (self.MODIFIER1.OUTPUT, self.OUTPUT),
            # Internally generated output
            (self.GENERATOR.OUTPUT, self.MODIFIER2.INPUT),
            (self.MODIFIER2.OUTPUT, self.GENERATOR_OUTPUT),
        )
