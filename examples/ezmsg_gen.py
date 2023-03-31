import ezmsg.core as ez
import time
from ezmsg.core.experimental import gen_to_unit
from typing import Generator


def spawn_int(duration: float = 5) -> Generator[int, None, None]:
    i = 0
    stop = time.time() + duration
    ez.logger.info("hello from spawn_int")
    while True:
        yield i
        i += 1
        if time.time() >= stop:
            return


def square_plus_n(n: int = 100) -> Generator[float, int, None]:
    out = float()
    ez.logger.info("hello from square_plus_n")
    while True:
        x = yield out
        out = x * x + n


def print_msg() -> Generator[None, float, None]:
    ez.logger.info("hello from print_msg")
    while True:
        x = yield
        ez.logger.info(x)


if __name__ == "__main__":
    SpawnIntSettings, SpawnIntUnit = gen_to_unit(spawn_int, sleep_time=0.5)
    SquarePlusSettings, SquarePlusUnit = gen_to_unit(square_plus_n)
    PrintMsgSettings, PrintMsgUnit = gen_to_unit(print_msg)

    class GenTest(ez.Collection):
        SPAWN = SpawnIntUnit()
        SQUARE = SquarePlusUnit()
        PRINT = PrintMsgUnit()

        def configure(self):
            self.SPAWN.apply_settings(SpawnIntSettings(duration=3))
            self.SQUARE.apply_settings(SquarePlusSettings(1))

        def network(self):
            return (
                (self.SPAWN.OUTPUT, self.SQUARE.INPUT),
                (self.SQUARE.OUTPUT, self.PRINT.INPUT),
            )

    ez.run(GenTest())
