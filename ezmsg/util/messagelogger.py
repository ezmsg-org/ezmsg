import json
import base64
import logging

from io import TextIOWrapper
from dataclasses import is_dataclass, field, asdict

from pathlib import Path

import ezmsg.core as ez

from typing import Optional, Any, Dict

logger = logging.getLogger('ezmsg')

try:
    import numpy as np
except ImportError:
    np = None

NDARRAY_TYPE: str = '_ndarray'
NDARRAY_DTYPE: str = 'dtype'
NDARRAY_SHAPE: str = 'shape'
NDARRAY_DATA: str = 'data'


class MessageEncoder(json.JSONEncoder):
    def default(self, obj: Any):
        if is_dataclass(obj):
            return dict(
                _type=obj.__class__.__name__,
                **asdict(obj)
            )
        elif np and isinstance(obj, np.ndarray):
            return {
                '_type': NDARRAY_TYPE,
                NDARRAY_DTYPE: str(obj.dtype),
                NDARRAY_DATA: base64.b64encode(np.ascontiguousarray(obj)).decode('ascii'),
                NDARRAY_SHAPE: obj.shape
            }
        return json.JSONEncoder.default(self, obj)


class MessageDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj: Dict[str, Any]) -> Any:
        if '_type' not in obj:
            return obj

        obj_type = obj.get('_type')
        if np and obj_type == NDARRAY_TYPE:
            data_bytes: bytes = obj.get(NDARRAY_DATA).encode('ascii')
            arr = np.frombuffer(
                base64.b64decode(data_bytes),
                dtype=obj.get(NDARRAY_DTYPE)
            )
            return arr.reshape(obj.get(NDARRAY_SHAPE))
        else:
            ...

        return obj


class MessageLoggerSettings(ez.Settings):
    output: Optional[Path] = None


class MessageLoggerState(ez.State):
    output_files: Dict[Path, TextIOWrapper] = field(default_factory=dict)


class MessageLogger(ez.Unit):

    SETTINGS: MessageLoggerSettings
    STATE: MessageLoggerState

    INPUT_START = ez.InputStream(Path)
    INPUT_STOP = ez.InputStream(Path)
    INPUT_MESSAGE = ez.InputStream(Any)
    OUTPUT_MESSAGE = ez.OutputStream(Any)
    OUTPUT_START = ez.OutputStream(Path)
    OUTPUT_STOP = ez.OutputStream(Path)

    def open_file(self, filepath: Path) -> Optional[Path]:
        """ Returns file path if file successfully opened, otherwise None """
        if filepath in self.STATE.output_files:
            # If the file is already open, we return None
            return None

        if not filepath.parent.exists():
            filepath.parent.mkdir(parents=True, exist_ok=True)
        self.STATE.output_files[filepath] = open(filepath, mode='w')

        return filepath

    def close_file(self, filepath: Path) -> Optional[Path]:
        """ Returns file path if file successfully closed, otherwise None """
        if filepath not in self.STATE.output_files:
            # We haven't opened this file
            return None

        self.STATE.output_files[filepath].close()
        del self.STATE.output_files[filepath]

        return filepath

    def initialize(self) -> None:
        """ Note that files defined at startup are not published to outputs"""
        if self.SETTINGS.output is not None:
            self.open_file(self.SETTINGS.output)

    @ez.subscriber(INPUT_START)
    @ez.publisher(OUTPUT_START)
    async def start_file(self, message: Path):
        out = self.open_file(message)
        if out is not None:
            yield (self.OUTPUT_START, out)

    @ez.subscriber(INPUT_STOP)
    @ez.publisher(OUTPUT_STOP)
    async def stop_file(self, message: Path):
        out = self.close_file(message)
        if out is not None:
            yield (self.OUTPUT_STOP, out)

    @ez.subscriber(INPUT_MESSAGE)
    @ez.publisher(OUTPUT_MESSAGE)
    async def on_message(self, message: Any) -> None:
        strmessage: str = json.dumps(message, cls=MessageEncoder)
        for output_f in self.STATE.output_files.values():
            output_f.write(f'{strmessage}\n')
            output_f.flush()
        yield (self.OUTPUT_MESSAGE, message)

    def shutdown(self) -> None:
        """ Note that files that are closed at shutdown don't publish messages """
        for filepath in list(self.STATE.output_files):
            self.close_file(filepath)
