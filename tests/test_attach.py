import asyncio
from collections.abc import AsyncGenerator
from multiprocessing import Process

import pytest

import ezmsg.core as ez


class TransmitReceiveSettings(ez.Settings):
    message: str = "MSG"
    num_messages: int = 5


class TransmitReceiveState(ez.State):
    messages: int = 0


class TransmitReceive(ez.Unit):
    SETTINGS = TransmitReceiveSettings
    STATE = TransmitReceiveState

    OUTPUT = ez.OutputStream(str)
    INPUT = ez.InputStream(str)

    # Force TCP on ack messages to ensure delivery
    # and avoid SHM deallocation between processes
    ACK = ez.OutputStream(str, force_tcp=True)

    @ez.publisher(OUTPUT)
    async def send(self) -> AsyncGenerator:
        while True:
            yield (self.OUTPUT, "MSG")
            await asyncio.sleep(0.2)

    @ez.subscriber(INPUT)
    @ez.publisher(ACK)
    async def receive(self, msg: str) -> AsyncGenerator:
        assert msg == self.SETTINGS.message
        self.STATE.messages += 1
        ez.logger.info(f"RX {msg} {self.STATE.messages}")
        yield (self.ACK, msg)
        if self.STATE.messages == self.SETTINGS.num_messages:
            raise ez.NormalTermination


class Echo(ez.Unit):
    OUTPUT = ez.OutputStream(str)
    INPUT = ez.InputStream(str)

    ACK = ez.InputStream(str)

    @ez.subscriber(INPUT)
    @ez.publisher(OUTPUT)
    async def echo(self, msg: str) -> AsyncGenerator:
        ez.logger.info(f"ECHO {msg}")
        yield (self.OUTPUT, msg)
        raise ez.Complete

    @ez.subscriber(ACK)
    async def ack(self, _: str) -> None:
        raise ez.Complete


class AttachTestProcess(Process):
    settings: TransmitReceiveSettings

    def __init__(self, settings: TransmitReceiveSettings) -> None:
        super().__init__()
        self.settings = settings


TX_TOPIC = "TX"
RX_TOPIC = "RX"
ACK_TOPIC = "ACK"


class TransmitReceiveProcess(AttachTestProcess):
    def run(self) -> None:
        txrx = TransmitReceive(self.settings)
        ez.run(
            TXRX=txrx,
            connections=(
                (txrx.OUTPUT, TX_TOPIC),
                (RX_TOPIC, txrx.INPUT),
                (txrx.ACK, ACK_TOPIC),
            ),
        )


class AttachEchoProcess(AttachTestProcess):
    def run(self) -> None:
        for _ in range(self.settings.num_messages):
            echo = Echo()
            ez.run(
                ECHO=echo,
                connections=(
                    (TX_TOPIC, echo.INPUT),
                    (echo.OUTPUT, RX_TOPIC),
                    (ACK_TOPIC, echo.ACK),
                ),
            )


@pytest.mark.asyncio
async def test_attach():
    """Independent processes attach to one already-running default server.

    Previously skipped as "canonical port isn't always available": the test
    needed the shared default port, which anything on the machine could
    occupy. The hermetic conftest pins the default to a session-private
    address and runs a server there for every test, so attaching — from
    this process and from the spawned children, which inherit the pinned
    environment — is reliable. The conftest's server IS the attach target;
    the test no longer creates its own.
    """
    async with ez.GraphContext():
        settings = TransmitReceiveSettings()

        txrx_process = TransmitReceiveProcess(settings)
        txrx_process.start()

        echo_process = AttachEchoProcess(settings)
        echo_process.start()

        echo_process.join()
        txrx_process.join()
