import asyncio
import os
import socket
import threading
import time
import signal
import sys

import ezmsg.core as ez


class BlockingDiskIO(ez.Unit):
    @ez.task
    async def blocked_read(self) -> None:
        # Cross-platform "hung disk I/O" simulation.
        event = threading.Event()
        self._event = event
        await asyncio.shield(asyncio.to_thread(event.wait))


class BlockingSocket(ez.Unit):
    @ez.task
    async def blocked_recv(self) -> None:
        sock_r, sock_w = socket.socketpair()
        sock_r.setblocking(True)
        sock_w.setblocking(True)
        # Keep references so sockets stay open.
        self._sock_r = sock_r
        self._sock_w = sock_w
        await asyncio.shield(asyncio.to_thread(sock_r.recv, 1))


class ExplodeOnCancel(ez.Unit):
    @ez.task
    async def explode(self) -> None:
        try:
            while True:
                await asyncio.sleep(1.0)
        except asyncio.CancelledError as exc:
            raise RuntimeError("Simulated cleanup failure during cancellation") from exc


class StubbornTask(ez.Unit):
    @ez.task
    async def ignore_cancel(self) -> None:
        while True:
            try:
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                # Bug: swallow cancellation and keep running forever.
                continue


UNITS = {
    "blocking_disk": BlockingDiskIO,
    "blocking_socket": BlockingSocket,
    "exception_on_cancel": ExplodeOnCancel,
    "ignore_cancel": StubbornTask,
}


def main() -> None:
    if os.environ.get("EZMSG_INBAND_SIGINT"):
        def _listen() -> None:
            for line in sys.stdin:
                if line.strip().upper() == "SIGINT":
                    signal.raise_signal(signal.SIGINT)

        threading.Thread(target=_listen, daemon=True).start()

    target = os.environ.get("EZMSG_SHUTDOWN_TEST")
    if target not in UNITS:
        raise SystemExit(
            "EZMSG_SHUTDOWN_TEST must be one of: " + ", ".join(sorted(UNITS))
        )
    runner = ez.GraphRunner(SYSTEM=UNITS[target]())
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
