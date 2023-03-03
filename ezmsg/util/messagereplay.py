import json

from dataclasses import dataclass, field

from pathlib import Path

import ezmsg.core as ez

from typing import Any, AsyncGenerator, Any, List

from .messagecodec import MessageDecoder
from .terminate import TerminateOnTotal

@dataclass(frozen = True)
class MessageReplaySettingsMessage:
    filename: Path


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

        with open(self.SETTINGS.filename, 'r') as f:
            for msg_idx, line in enumerate(f):

                try:
                    obj = json.loads(line, cls = MessageDecoder)
                except json.JSONDecodeError:
                    ez.logger.warning(f'Could not load message {msg_idx + 1} from {self.SETTINGS.filename}')
                    continue

                total_msgs += 1
                yield self.OUTPUT_MESSAGE, obj

        ez.logger.info(f'Replayed {total_msgs} messages from {self.SETTINGS.filename}')
        yield self.OUTPUT_TOTAL, total_msgs
        raise ez.Complete
    

class MessageCollectorState(ez.State):
    messages: List[Any] = field(default_factory = list)


class MessageCollector(ez.Collection):
    STATE: MessageCollectorState

    INPUT_MESSAGE = ez.InputStream(Any)
    OUTPUT_MESSAGE = ez.OutputStream(Any)

    @ez.subscriber(INPUT_MESSAGE)
    @ez.publisher(OUTPUT_MESSAGE)
    async def on_message(self, msg: Any) -> AsyncGenerator:
        self.STATE.messages.append(msg)
        yield self.OUTPUT_MESSAGE, msg
  
    @property
    def messages(self) -> List[Any]:
        return self.STATE.messages
    

MessageLoaderSettings = MessageReplaySettings

class MessageLoader(ez.Collection):
    SETTINGS: MessageLoaderSettings

    REPLAY = MessageReplay()
    COLLECTOR = MessageCollector()
    TERM = TerminateOnTotal()

    def configure(self) -> None:
        self.REPLAY.apply_settings(self.SETTINGS)

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.REPLAY.OUTPUT_MESSAGE, self.COLLECTOR.INPUT_MESSAGE),
            (self.COLLECTOR.OUTPUT_MESSAGE, self.TERM.INPUT_MESSAGE),
            (self.REPLAY.OUTPUT_TOTAL, self.TERM.INPUT_TOTAL)
        )
    
    @property
    def messages(self) -> List[Any]:
        return self.COLLECTOR.messages