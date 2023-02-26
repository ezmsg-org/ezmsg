import json
import base64
import importlib

from io import TextIOWrapper
from dataclasses import dataclass, is_dataclass, field, fields
from functools import reduce

from pathlib import Path

import ezmsg.core as ez

from typing import Optional, Any, Dict, Iterable, AsyncGenerator, List, Any, Generator

try:
    import numpy as np
    import numpy.typing as npt
except ImportError:
    np = None

TYPE = '_type'
NDARRAY_TYPE = '_ndarray'
NDARRAY_DTYPE = 'dtype'
NDARRAY_SHAPE = 'shape'
NDARRAY_DATA = 'data'

def type_str(obj: Any) -> str:
    t = type(obj)
    name = getattr(t, '__qualname__', t.__name__)
    return f'{t.__module__}:{name}'

def import_type(typestr: str) -> type:
    module, name = typestr.split(':')
    module = importlib.import_module(module)
    ty = reduce(lambda t, n: getattr(t, n), [module] + name.split('.'))

    if not isinstance(ty, type):
        raise ImportError(f"{typestr} does not resolve to type")

    return ty

class MessageEncoder(json.JSONEncoder):
    def default(self, obj: Any):
        if is_dataclass(obj):
            return { 
                **{f.name: getattr(obj, f.name) for f in fields(obj)}, 
                **{TYPE: type_str(obj)}
            }

        elif np and isinstance(obj, np.ndarray):
            contig_obj = np.ascontiguousarray(obj)
            buf = base64.b64encode(contig_obj.data)
            return {
                TYPE: NDARRAY_TYPE,
                NDARRAY_DTYPE: str(obj.dtype),
                NDARRAY_DATA: buf.decode('ascii'),
                NDARRAY_SHAPE: obj.shape
            }
        
        return json.JSONEncoder.default(self, obj)


class MessageDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj: Dict[str, Any]) -> Any:
        obj_type: Optional[str] = obj.get(TYPE)

        if obj_type is None:
            return obj

        if np and obj_type == NDARRAY_TYPE:
            data_bytes: Optional[str] = obj.get(NDARRAY_DATA)
            data_shape: Optional[Iterable[int]] = obj.get(NDARRAY_SHAPE)
            data_dtype: Optional[npt.DTypeLike] = obj.get(NDARRAY_DTYPE)

            if (
                isinstance(data_bytes, str) and \
                data_shape is not None and \
                data_dtype is not None
            ):
                buf = base64.b64decode(data_bytes.encode('ascii'))
                return np.frombuffer(buf, dtype = data_dtype).reshape(data_shape)
            
        else:
            cls = import_type(obj_type)
            del obj[TYPE]
            return cls(**obj)

        return obj


@dataclass(frozen = True)
class MessageLoggerSettingsMessage:
    output: Optional[Path] = None


class MessageLoggerSettings(ez.Settings, MessageLoggerSettingsMessage):
    ...


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
    async def start_file(self, message: Path) -> AsyncGenerator:
        out = self.open_file(message)
        if out is not None:
            yield (self.OUTPUT_START, out)

    @ez.subscriber(INPUT_STOP)
    @ez.publisher(OUTPUT_STOP)
    async def stop_file(self, message: Path) -> AsyncGenerator:
        out = self.close_file(message)
        if out is not None:
            yield (self.OUTPUT_STOP, out)

    @ez.subscriber(INPUT_MESSAGE)
    @ez.publisher(OUTPUT_MESSAGE)
    async def on_message(self, message: Any) -> AsyncGenerator:
        strmessage: str = json.dumps(message, cls=MessageEncoder)
        for output_f in self.STATE.output_files.values():
            output_f.write(f'{strmessage}\n')
            output_f.flush()
        yield (self.OUTPUT_MESSAGE, message)

    def shutdown(self) -> None:
        """ Note that files that are closed at shutdown don't publish messages """
        for filepath in list(self.STATE.output_files):
            self.close_file(filepath)


@dataclass(frozen = True)
class MessageReplaySettingsMessage:
    filename: Path
    message_types: List[type] = field(default_factory = list)


class MessageReplaySettings(ez.Settings, MessageReplaySettingsMessage):
    ...


class MessageReplay(ez.Unit):
    SETTINGS: MessageReplaySettings

    OUTPUT_MESSAGE = ez.OutputStream(Any)
    OUTPUT_TOTAL = ez.OutputStream(int)

    @ez.publisher(OUTPUT_MESSAGE)
    @ez.publisher(OUTPUT_TOTAL)
    async def pub_messages(self) -> AsyncGenerator:
        total_msgs = 0

        type_dict = {
            ty.__name__: ty 
            for ty in self.SETTINGS.message_types
        }

        with open(self.SETTINGS.filename, 'r') as f:
            for msg_idx, line in enumerate(f):

                try:
                    obj = json.loads(line, cls = MessageDecoder)
                except json.JSONDecodeError:
                    ez.logger.warning(f'Could not load message {msg_idx + 1} from {self.SETTINGS.filename}')
                    continue

                if not isinstance(obj, dict):
                    total_msgs += 1
                    yield self.OUTPUT_MESSAGE, obj
                    continue

                obj_type_str = obj.get(TYPE)
                if obj_type_str is None:
                    total_msgs += 1
                    yield self.OUTPUT_MESSAGE, obj
                    continue

                obj_type = type_dict.get(obj_type_str)

                if obj_type is None:
                    ez.logger.warning(f'Message {TYPE}: {obj_type_str} not in message_types')
                    ez.logger.info(f'{type_dict}')
                    continue

                del obj[TYPE]
                obj = obj_type(**obj)
                total_msgs += 1
                yield self.OUTPUT_MESSAGE, obj

        
        ez.logger.info(f'Replayed {total_msgs} messages from {self.SETTINGS.filename}')
        yield self.OUTPUT_TOTAL, total_msgs
        raise ez.Complete
    

@dataclass(frozen = True)
class MessageCollectorSettingsMessage:
    term_on_total: bool = False


class MessageCollectorSettings(ez.Settings, MessageCollectorSettingsMessage):
    ...


class MessageCollectorState(ez.State):
    messages: List[Any] = field(default_factory = list)
    expected_batches: Optional[int] = None


class MessageCollector(ez.Unit):
    SETTINGS: MessageCollectorSettings
    STATE: MessageCollectorState

    INPUT = ez.InputStream(Any)
    INPUT_TOTAL = ez.InputStream(int)

    @ez.subscriber(INPUT_TOTAL)
    async def on_total(self, msg: int) -> None:
        self.STATE.expected_batches = msg
        self.maybe_terminate()

    @ez.subscriber(INPUT)
    async def on_message(self, msg: Any) -> None:
        self.STATE.messages.append(msg)
        self.maybe_terminate()

    def maybe_terminate(self):
        if len(self.STATE.messages) == self.STATE.expected_batches:
            raise ez.NormalTermination if self.SETTINGS.term_on_total else ez.Complete
        
    @property
    def messages(self) -> List[Any]:
        return self.STATE.messages
    

MessageLoaderSettings = MessageReplaySettings

class MessageLoader(ez.Collection):
    SETTINGS: MessageLoaderSettings

    REPLAY = MessageReplay()
    COLLECTOR = MessageCollector()

    def configure(self) -> None:
        self.REPLAY.apply_settings(self.SETTINGS)
        self.COLLECTOR.apply_settings(
            MessageCollectorSettings(
                term_on_total = True
            )
        )

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.REPLAY.OUTPUT_MESSAGE, self.COLLECTOR.INPUT),
            (self.REPLAY.OUTPUT_TOTAL, self.COLLECTOR.INPUT_TOTAL)
        )
    
    @property
    def messages(self) -> List[Any]:
        return self.COLLECTOR.messages
    
def message_log(fname: Path) -> Generator[Any, None, None]:
    with open(fname, 'r') as f:
        for l in f:
            obj = json.loads(l, cls = MessageDecoder)
            yield obj