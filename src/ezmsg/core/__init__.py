import importlib.metadata

__version__ = importlib.metadata.version("ezmsg")

__all__ = [
    "task",
    "publisher",
    "subscriber",
    "main",
    "timeit",
    "process",
    "thread",
    "Component",
    "Settings",
    "Collection",
    "NetworkDefinition",
    "InputStream",
    "OutputStream",
    "Unit",
    "State",
    "run",
    "Complete",
    "NormalTermination",
    "GraphServer",
    "GraphContext",
    "run_command",
    # All following are deprecated
    "System",
    "run_system",
    "Message",
    "Flag",
]

from .component import Component
from .state import State
from .settings import Settings
from .collection import Collection, NetworkDefinition
from .unit import Unit, task, publisher, subscriber, main, timeit, process, thread
from .stream import InputStream, OutputStream
from .backend import run
from .backendprocess import Complete, NormalTermination
from .graphserver import GraphServer
from .graphcontext import GraphContext
from .command import run_command

# Following imports are deprecated
from .backend import run_system
from .message import Message, Flag  # deprecated
from .collection import Collection as System  # deprecated, backward compatibility

import os
import logging

logger = logging.getLogger("ezmsg")
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d - pid: %(process)d - %(threadName)s "
    + "- %(levelname)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

handler.setFormatter(formatter)
logger.addHandler(handler)

LOGLEVEL = os.environ.get("EZMSG_LOGLEVEL", "INFO").upper()
logger.setLevel(LOGLEVEL)
logger.debug(f"Log level set to {LOGLEVEL}")
