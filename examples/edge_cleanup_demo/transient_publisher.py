#!/usr/bin/env python3
"""
Transient publisher with proper cleanup.

This is a typical ezmsg script that creates a publisher and connects
to the long-running receiver. When you press Ctrl+C, it exits normally
and ez.run()'s finally block cleans up the edges properly.

Usage:
    # Make sure GraphServer is running and long_running_receiver.py is running
    python transient_publisher.py
    # Press Ctrl+C to exit - the edge will be properly cleaned up!
"""

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
            await asyncio.sleep(1.0)


if __name__ == "__main__":
    publisher = PeriodicPublisher()

    print("Starting transient publisher (PROPER cleanup)...")
    print("This will connect: PUBLISHER/OUTPUT -> RECEIVER/INPUT")
    print()
    print("Press Ctrl+C to exit gracefully")
    print("The edge WILL be cleaned up by ez.run()'s finally block!")
    print()

    ez.run(
        PUBLISHER=publisher,
        connections=(
            # Connect to the long-running receiver's INPUT topic
            (publisher.OUTPUT, "RECEIVER/INPUT"),
        ),
        graph_address=("127.0.0.1", 25978),
    )
