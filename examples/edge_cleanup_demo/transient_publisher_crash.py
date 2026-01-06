#!/usr/bin/env python3
"""
Transient publisher that simulates a crash/kill scenario.

This demonstrates what happens when a process is killed externally
(SIGKILL, system crash, `kill -9`, etc.) or when cleanup cannot complete.

The script intercepts Ctrl+C and calls os._exit() to simulate a hard crash,
bypassing Python's cleanup mechanisms including ez.run()'s finally block.

Real-world scenarios where this occurs:
- Process killed with `kill -9 <pid>`
- System runs out of memory (OOM killer)
- Hardware failure / power loss
- Segfault in native code
- Cleanup code itself hangs or deadlocks

Usage:
    # Make sure GraphServer is running and long_running_receiver.py is running
    python transient_publisher_crash.py
    # Wait for 5 messages, then press Ctrl+C
    # The edge will remain in the graph!
"""

import os
import signal
import ezmsg.core as ez

from typing import AsyncGenerator


class PeriodicPublisher(ez.Unit):
    """Publishes a message every second."""

    OUTPUT = ez.OutputStream(str)

    def initialize(self) -> None:
        self.count = 0

    @ez.task
    async def publish(self) -> AsyncGenerator:
        import asyncio

        while True:
            msg = f"Message {self.count} from transient publisher"
            ez.logger.info(f"Publishing: {msg}")
            yield self.OUTPUT, msg
            self.count += 1

            if self.count == 5:
                print()
                print("=" * 60)
                print("Sent 5 messages. Press Ctrl+C to simulate crash...")
                print("=" * 60)

            await asyncio.sleep(1.0)


def crash_handler(signum, frame):
    """Simulate a hard crash by calling os._exit()."""
    print()
    print("=" * 60)
    print("SIMULATING CRASH: calling os._exit(1)")
    print()
    print("This bypasses Python's cleanup, including ez.run()'s finally block.")
    print("The edge will remain in the GraphServer's DAG.")
    print()
    print("Check with: python check_graph.py")
    print("=" * 60)
    os._exit(1)


if __name__ == "__main__":
    # Intercept Ctrl+C to simulate a hard crash
    signal.signal(signal.SIGINT, crash_handler)

    publisher = PeriodicPublisher()

    print("Starting transient publisher (CRASH simulation)...")
    print("This will connect: PUBLISHER/OUTPUT -> RECEIVER/INPUT")
    print()
    print("Press Ctrl+C after a few messages to simulate a crash.")
    print("(os._exit bypasses cleanup, leaving a stale edge)")
    print()

    ez.run(
        PUBLISHER=publisher,
        connections=(
            (publisher.OUTPUT, "RECEIVER/INPUT"),
        ),
        graph_address=("127.0.0.1", 25978),
    )
