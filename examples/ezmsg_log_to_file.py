import asyncio
import logging
import logging.handlers
import ezmsg.core as ez
import multiprocessing as mp
from pathlib import Path
from ezmsg.core.backendprocess import DefaultBackendProcess
from ezmsg_toy import TestSystem, TestSystemSettings
from multiprocessing import Queue
from multiprocessing.managers import BaseManager

LOG_QUEUE = Queue()


def log_queue():
    return LOG_QUEUE


class QueueManager(BaseManager):
    pass


class LoggingBackend(DefaultBackendProcess):
    def process(self, loop: asyncio.AbstractEventLoop) -> None:
        m = QueueManager(
            address=("", 50000),
        )
        m.connect()
        queue = m.get_queue()  # type: ignore
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger()
        root.addHandler(handler)
        super().process(loop)


QueueManager.register("get_queue")

if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)

    QueueManager.register("get_queue", callable=log_queue)

    m = QueueManager(
        address=("", 50000),
    )
    m.start()

    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d - pid: %(process)d - %(threadName)s "
        + "- %(levelname)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log_path = str(Path(__file__).parent / "ezmsg_log_to_file.log")
    file_handler = logging.FileHandler(log_path, "w")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    queue = m.get_queue()  # type: ignore
    recv_handler = logging.handlers.QueueListener(queue, file_handler)
    recv_handler.start()

    system = TestSystem(TestSystemSettings(name="A"))

    ez.run(
        SYSTEM=system,
        backend_process=LoggingBackend,
    )

    recv_handler.stop()
    m.shutdown()
