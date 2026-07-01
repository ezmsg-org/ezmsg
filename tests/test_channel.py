import asyncio
from uuid import uuid4
from unittest.mock import patch

import pytest

from ezmsg.core.messagechannel import Channel, ChannelError
from ezmsg.core.messagecache import CacheMiss
from ezmsg.core.netprotocol import Command, uint64_to_bytes
from ezmsg.core.backpressure import Backpressure
from ezmsg.core.messagemarshal import MessageMarshal


class DummyWriter:
    def __init__(self):
        self.buffer: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.buffer.append(data)


def _resolved_task():
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    fut.set_result(None)
    return fut


@pytest.mark.asyncio
async def test_channel_acknowledges_remote_messages():
    channel = Channel(uuid4(), uuid4(), 2, None, None, Channel._SENTINEL)
    channel._pub_writer = DummyWriter()
    channel._pub_task = _resolved_task()
    channel._graph_task = _resolved_task()

    client_id = uuid4()
    queue: asyncio.Queue = asyncio.Queue()
    channel.register_client(client_id, queue)

    msg_id = 5
    payload = {"value": 42}
    channel.cache.put_local(payload, msg_id)
    channel._notify_clients(msg_id)

    assert queue.qsize() == 1
    queued_pub, queued_msg = queue.get_nowait()
    assert queued_pub == channel.pub_id
    assert queued_msg == msg_id

    with channel.get(msg_id, client_id) as obj:
        assert obj == payload

    with pytest.raises(CacheMiss):
        _ = channel.cache[msg_id]

    buf_idx = msg_id % channel.num_buffers
    assert channel.backpressure.buffers[buf_idx].is_empty

    expected_ack = Command.RX_ACK.value + uint64_to_bytes(msg_id)
    assert channel._pub_writer.buffer[-1] == expected_ack


@pytest.mark.asyncio
async def test_channel_releases_local_backpressure(monkeypatch):
    channel = Channel(uuid4(), uuid4(), 2, None, None, Channel._SENTINEL)
    channel._pub_writer = DummyWriter()
    channel._pub_task = _resolved_task()
    channel._graph_task = _resolved_task()

    local_bp = Backpressure(channel.num_buffers)
    channel.register_client(channel.pub_id, None, local_bp)

    client_id = uuid4()
    queue: asyncio.Queue = asyncio.Queue()
    channel.register_client(client_id, queue)

    msg_id = 3
    payload = "local"
    channel.put_local(msg_id, payload)

    assert queue.qsize() == 1
    queue.get_nowait()

    with channel.get(msg_id, client_id) as obj:
        assert obj == payload

    buf_idx = msg_id % channel.num_buffers
    assert local_bp.buffers[buf_idx].is_empty
    assert channel._pub_writer.buffer == []


def test_channel_put_local_requires_local_backpressure():
    channel = Channel(uuid4(), uuid4(), 1, None, None, Channel._SENTINEL)
    with pytest.raises(ValueError):
        channel.put_local(1, "no pub")


class FakeSHM:
    def __init__(self, name: str, buffers: dict[int, memoryview] | None = None):
        self.name = name
        self._buffers = buffers or {}
        self.closed = False
        self.waited = False

    def __getitem__(self, idx: int) -> memoryview:
        return self._buffers[idx]

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        self.waited = True


def _marshal_message(msg_id: int, obj: object) -> memoryview:
    with MessageMarshal.serialize(msg_id, obj) as (size, header, buffers):
        raw = bytearray(size + 1)
        mem = memoryview(raw)
        MessageMarshal._write(mem, header, buffers)
        return mem.toreadonly()


@pytest.mark.asyncio
async def test_channel_reattach_shm_drops_stale_cached_messages():
    old_shm = FakeSHM("old")
    channel = Channel(uuid4(), uuid4(), 2, old_shm, ("127.0.0.1", 0), Channel._SENTINEL)

    channel.cache.put_local("cached", 1)
    new_shm = FakeSHM("new", {1: _marshal_message(3, "fresh")})

    class FakeGraphService:
        def __init__(self, address):
            self.address = address

        async def attach_shm(self, shm_name: str):
            assert shm_name == "new"
            return new_shm

    with patch("ezmsg.core.messagechannel.GraphService", FakeGraphService):
        await channel._reattach_shm("new")

    assert channel.shm is new_shm
    assert old_shm.closed is True
    assert old_shm.waited is True
    with pytest.raises(CacheMiss):
        _ = channel.cache[1]


@pytest.mark.asyncio
async def test_channel_reattach_shm_wraps_missing_segment():
    channel = Channel(uuid4(), uuid4(), 2, None, ("127.0.0.1", 0), Channel._SENTINEL)

    class FakeGraphService:
        def __init__(self, address):
            self.address = address

        async def attach_shm(self, shm_name: str):
            raise FileNotFoundError(shm_name)

    with patch("ezmsg.core.messagechannel.GraphService", FakeGraphService):
        with pytest.raises(ChannelError):
            await channel._reattach_shm("missing")
