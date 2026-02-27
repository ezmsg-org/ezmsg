import asyncio
import os
import time
import multiprocessing as mp
import signal
import sys
import threading

import ezmsg.core as ez


class CountSettings(ez.Settings):
    num_msgs: int = 3


class CountPublisher(ez.Unit):
    SETTINGS = CountSettings
    OUTPUT = ez.OutputStream(int)

    @ez.publisher(OUTPUT)
    async def publish(self):
        for i in range(self.SETTINGS.num_msgs):
            yield self.OUTPUT, i
            await asyncio.sleep(0.01)
        raise ez.Complete


class CompleteSubscriber(ez.Unit):
    SETTINGS = CountSettings
    INPUT = ez.InputStream(int)

    async def initialize(self) -> None:
        self._count = 0

    @ez.subscriber(INPUT)
    async def on_message(self, _msg: int) -> None:
        self._count += 1
        if self._count >= self.SETTINGS.num_msgs:
            raise ez.Complete


class NormalTerminationSubscriber(ez.Unit):
    SETTINGS = CountSettings
    INPUT = ez.InputStream(int)

    async def initialize(self) -> None:
        self._count = 0

    @ez.subscriber(INPUT)
    async def on_message(self, _msg: int) -> None:
        self._count += 1
        if self._count >= self.SETTINGS.num_msgs:
            raise ez.NormalTermination


class NormalTerminationSubscriberWithThread(ez.Unit):
    SETTINGS = CountSettings
    INPUT = ez.InputStream(int)

    async def initialize(self) -> None:
        self._count = 0
        self._stop_thread = False

    @ez.thread
    def background(self) -> None:
        while not self._stop_thread:
            time.sleep(0.05)

    @ez.subscriber(INPUT)
    async def on_message(self, _msg: int) -> None:
        self._count += 1
        if self._count >= self.SETTINGS.num_msgs:
            self._stop_thread = True
            raise ez.NormalTermination


class InfiniteTask(ez.Unit):
    @ez.task
    async def run(self) -> None:
        while True:
            await asyncio.sleep(0.2)


class BaseSystem(ez.Collection):
    PUB = CountPublisher()
    SUB = CompleteSubscriber()

    def configure(self) -> None:
        self.PUB.apply_settings(CountSettings())
        self.SUB.apply_settings(CountSettings())

    def network(self) -> ez.NetworkDefinition:
        return ((self.PUB.OUTPUT, self.SUB.INPUT),)

    def process_components(self):
        return (self.PUB, self.SUB)


class CompleteSystem(BaseSystem):
    SUB = CompleteSubscriber()


class NormalTerminationSystem(BaseSystem):
    SUB = NormalTerminationSubscriber()


class NormalTerminationThreadSystem(BaseSystem):
    SUB = NormalTerminationSubscriberWithThread()


class InfiniteSystem(ez.Collection):
    TASK = InfiniteTask()

    def process_components(self):
        return (self.TASK,)


SYSTEMS = {
    "complete": CompleteSystem,
    "normalterm": NormalTerminationSystem,
    "normalterm_thread": NormalTerminationThreadSystem,
    "infinite": InfiniteSystem,
}


def main() -> None:
    if os.environ.get("EZMSG_INBAND_SIGINT"):
        def _listen() -> None:
            for line in sys.stdin:
                if line.strip().upper() == "SIGINT":
                    signal.raise_signal(signal.SIGINT)

        threading.Thread(target=_listen, daemon=True).start()

    start_method = os.environ.get("EZMSG_MP_START")
    if start_method:
        mp.set_start_method(start_method, force=True)
    case = os.environ.get("EZMSG_SHUTDOWN_EXAMPLE")
    if case not in SYSTEMS:
        raise SystemExit(
            "EZMSG_SHUTDOWN_EXAMPLE must be one of: "
            + ", ".join(sorted(SYSTEMS))
        )
    system = SYSTEMS[case]()
    runner = ez.GraphRunner(SYSTEM=system)
    ready_emitted = threading.Event()
    done = threading.Event()

    def _emit_ready() -> None:
        if not ready_emitted.is_set():
            print("READY", flush=True)
            ready_emitted.set()

    def _watch_ready() -> None:
        while not done.is_set():
            if runner.running:
                _emit_ready()
                return
            time.sleep(0.01)
        _emit_ready()

    threading.Thread(target=_watch_ready, daemon=True).start()
    try:
        runner.run_blocking()
    finally:
        done.set()
        _emit_ready()


if __name__ == "__main__":
    main()
