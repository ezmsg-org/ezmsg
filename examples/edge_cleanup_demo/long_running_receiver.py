#!/usr/bin/env python3
"""
Long-running receiver script that runs a DebugLog unit connected to a sink.

This script connects to an existing GraphServer (start with `ezmsg start`)
and runs a DebugLog unit that waits for messages on its INPUT topic,
connected to a Sink unit so there's an initial edge in the graph.

See accompanying README.md for more details.
"""

import ezmsg.core as ez
from ezmsg.util.debuglog import DebugLog


class Sink(ez.Unit):
    """Simple sink that receives messages and does nothing."""

    INPUT = ez.InputStream(object)

    @ez.subscriber(INPUT)
    async def on_message(self, msg: object) -> None:
        pass  # Just consume the message


if __name__ == "__main__":
    # Connect to existing GraphServer at default address
    debug = DebugLog()
    sink = Sink()

    print("Starting long-running receiver pipeline...")
    print("  RECEIVER/INPUT  -> RECEIVER/OUTPUT -> SINK/INPUT")
    print()
    print("Topics:")
    print("  - RECEIVER/INPUT   (external input)")
    print("  - RECEIVER/OUTPUT  (connected to SINK)")
    print("  - SINK/INPUT       (receives from RECEIVER)")
    print()
    print("Press Ctrl+C to stop")
    print()

    ez.run(
        RECEIVER=debug,
        SINK=sink,
        connections=(
            (debug.OUTPUT, sink.INPUT),
        ),
        # Connect to existing GraphServer
        graph_address=("127.0.0.1", 25978),
    )
