"""
ezmsg utilities package.

This package provides a collection of utility components for the ezmsg framework,
including message processing, logging, replay, transformation, and system utilities.

Key modules:
- :mod:`ezmsg.util.debuglog`: Debug logging utilities
- :mod:`ezmsg.util.generator`: Generator-based message processing decorators (for newer code, use ``ezmsg.baseproc`` instead)
- :mod:`ezmsg.util.messagecodec`: JSON encoding/decoding for message logging
- :mod:`ezmsg.util.messagegate`: Message flow control and gating
- :mod:`ezmsg.util.messagelogger`: File-based message logging
- :mod:`ezmsg.util.messagequeue`: Asynchronous message queuing with backpressure
- :mod:`ezmsg.util.messagereplay`: Message replay from logged files
- :mod:`ezmsg.util.rate`: Rate limiting and timing utilities
- :mod:`ezmsg.util.terminate`: Pipeline termination conditions
- :mod:`ezmsg.util.messages`: AxisArray and message transformation utilities
"""
