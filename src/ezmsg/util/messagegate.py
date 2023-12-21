import typing
from dataclasses import dataclass

import ezmsg.core as ez


@dataclass
class GateMessage:
    open: bool


class MessageGateSettings(ez.Settings):
    start_open: bool = False
    default_open: bool = False

    # Automatically change back to default state after X messages
    default_after: typing.Optional[int] = None


class MessageGateState(ez.State):
    gate_open: typing.Optional[bool] = None
    msgs: int = 0  # Messages since last gate change


class MessageGate(ez.Unit):
    SETTINGS: MessageGateSettings
    STATE: MessageGateState

    INPUT_GATE = ez.InputStream(GateMessage)

    INPUT = ez.InputStream(typing.Any)
    OUTPUT = ez.OutputStream(typing.Any)

    def initialize(self) -> None:
        self.STATE.gate_open = self.SETTINGS.start_open

    def set_gate(self, set_open: bool) -> None:
        if self.STATE.gate_open != set_open:
            self.STATE.msgs = 0
            self.STATE.gate_open = set_open

    @ez.subscriber(INPUT_GATE)
    async def on_gate(self, msg: GateMessage) -> None:
        self.set_gate(msg.open)

    @ez.subscriber(INPUT)
    @ez.publisher(OUTPUT)
    async def on_input(self, msg: typing.Any) -> typing.AsyncGenerator:
        self.STATE.msgs += 1

        if self.STATE.gate_open:
            yield (self.OUTPUT, msg)

        if (  # Auto-revert to default state if necessary
            (self.SETTINGS.default_after is not None)
            and (self.STATE.gate_open != self.SETTINGS.default_open)
            and (self.STATE.msgs >= self.SETTINGS.default_after)
        ):
            self.set_gate(self.SETTINGS.default_open)