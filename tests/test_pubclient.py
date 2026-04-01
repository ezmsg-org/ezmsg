from contextlib import contextmanager
from uuid import uuid4

import pytest

from ezmsg.core.netprotocol import Address, Command
from ezmsg.core.pubclient import (
    ALLOW_LOCAL_ENV,
    FORCE_TCP_ENV,
    PubChannelInfo,
    Publisher,
)


class DummyLocalChannel:
    def __init__(self) -> None:
        self.calls: list[tuple[int, object]] = []

    def put_local(self, msg_id: int, obj: object) -> None:
        self.calls.append((msg_id, obj))


class DummyWriter:
    def __init__(self) -> None:
        self.buffer: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.buffer.append(data)

    async def drain(self) -> None:
        return None


class DummyShm:
    def __init__(self, num_buffers: int, buf_size: int = 65536) -> None:
        self.name = "dummy-shm"
        self.buf_size = buf_size
        self._buffers = [bytearray(buf_size) for _ in range(num_buffers)]

    @contextmanager
    def buffer(self, idx: int, readonly: bool = False):
        del readonly
        yield memoryview(self._buffers[idx])


def _make_publisher(
    *,
    force_tcp: bool | None,
    allow_local: bool | None,
    channel_pid: int,
    shm_ok: bool = True,
) -> tuple[Publisher, DummyLocalChannel, DummyWriter]:
    pub = Publisher(
        id=uuid4(),
        topic="/TEST",
        shm=DummyShm(num_buffers=2),
        graph_address=Address("127.0.0.1", 25978),
        num_buffers=2,
        force_tcp=force_tcp,
        allow_local=allow_local,
        _guard=Publisher._SENTINEL,
    )
    pub._running.set()

    local_channel = DummyLocalChannel()
    writer = DummyWriter()
    channel = PubChannelInfo(
        id=uuid4(),
        writer=writer,
        pub_id=pub.id,
        pid=channel_pid,
        shm_ok=shm_ok,
    )
    pub._channels[channel.id] = channel
    pub._local_channel = local_channel  # type: ignore[assignment]
    return pub, local_channel, writer


@pytest.mark.asyncio
async def test_broadcast_same_process_prefers_local_fast_path():
    pub, local_channel, writer = _make_publisher(
        force_tcp=False,
        allow_local=True,
        channel_pid=0,
    )
    pub.pid = 0

    await pub.broadcast(b"payload")

    assert local_channel.calls == [(0, b"payload")]
    assert writer.buffer == []


@pytest.mark.asyncio
async def test_broadcast_same_process_can_force_shm_path():
    pub, local_channel, writer = _make_publisher(
        force_tcp=False,
        allow_local=False,
        channel_pid=0,
    )
    pub.pid = 0

    await pub.broadcast(b"payload")

    assert local_channel.calls == []
    assert writer.buffer
    assert writer.buffer[0].startswith(Command.TX_SHM.value)


@pytest.mark.asyncio
async def test_broadcast_same_process_can_force_tcp_path():
    pub, local_channel, writer = _make_publisher(
        force_tcp=True,
        allow_local=False,
        channel_pid=0,
    )
    pub.pid = 0

    await pub.broadcast(b"payload")

    assert local_channel.calls == []
    assert writer.buffer
    assert writer.buffer[0].startswith(Command.TX_TCP.value)


def test_force_tcp_disables_allow_local_from_env(monkeypatch, caplog):
    monkeypatch.setenv(ALLOW_LOCAL_ENV, "1")
    with caplog.at_level("INFO"):
        pub, _, _ = _make_publisher(
            force_tcp=True,
            allow_local=None,
            channel_pid=0,
        )

    assert pub._allow_local is False
    assert "force_tcp=True disables local delivery" in caplog.text


def test_force_tcp_disables_explicit_allow_local(caplog):
    with caplog.at_level("INFO"):
        pub, _, _ = _make_publisher(
            force_tcp=True,
            allow_local=True,
            channel_pid=0,
        )

    assert pub._allow_local is False
    assert "force_tcp=True disables local delivery" in caplog.text


def test_force_tcp_uses_env_default_when_none(monkeypatch):
    monkeypatch.setenv(FORCE_TCP_ENV, "1")
    pub, _, _ = _make_publisher(
        force_tcp=None,
        allow_local=False,
        channel_pid=0,
    )

    assert pub._force_tcp is True


def test_explicit_force_tcp_false_overrides_env(monkeypatch):
    monkeypatch.setenv(FORCE_TCP_ENV, "1")
    pub, _, _ = _make_publisher(
        force_tcp=False,
        allow_local=False,
        channel_pid=0,
    )

    assert pub._force_tcp is False


@pytest.mark.asyncio
async def test_broadcast_same_process_uses_env_default_when_allow_local_is_none(monkeypatch):
    monkeypatch.setenv(ALLOW_LOCAL_ENV, "0")
    pub, local_channel, writer = _make_publisher(
        force_tcp=False,
        allow_local=None,
        channel_pid=0,
    )
    pub.pid = 0

    await pub.broadcast(b"payload")

    assert local_channel.calls == []
    assert writer.buffer
    assert writer.buffer[0].startswith(Command.TX_SHM.value)


@pytest.mark.asyncio
async def test_broadcast_same_process_explicit_allow_local_overrides_env(monkeypatch):
    monkeypatch.setenv(ALLOW_LOCAL_ENV, "0")
    pub, local_channel, writer = _make_publisher(
        force_tcp=False,
        allow_local=True,
        channel_pid=0,
    )
    pub.pid = 0

    await pub.broadcast(b"payload")

    assert local_channel.calls == [(0, b"payload")]
    assert writer.buffer == []
