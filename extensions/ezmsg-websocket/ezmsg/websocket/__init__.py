__all__ = [
    "__version__",
    "WebsocketSettings",
    "WebsocketState"
]

from .__version__ import __version__

import asyncio
from dataclasses import field
from typing import Optional, Union

import ezmsg.core as ez

class WebsocketSettings(ez.Settings):
    host: str
    port: int
    cert_path: Optional[str] = None


class WebsocketState(ez.State):
    incoming_queue: "asyncio.Queue[Union[str,bytes]]" = field(default_factory=asyncio.Queue)
    outgoing_queue: "asyncio.Queue[Union[str,bytes]]" = field(default_factory=asyncio.Queue)





