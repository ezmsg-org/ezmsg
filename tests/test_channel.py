import asyncio
from uuid import uuid4

import pytest

from ezmsg.core.messagechannel import Channel
from ezmsg.core.messagecache import CacheMiss
from ezmsg.core.messagemarshal import MessageMarshal
from ezmsg.core.netprotocol import Command, uint64_to_bytes
from ezmsg.core.backpressure import Backpressure


class DummyWriter:
    def __init__(self):
        self.buffer: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.buffer.append(data)

    def close(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


def _resolved_task():
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    fut.set_result(None)
    return fut


class FakeSHM:
    def __init__(self, name: str, slots: list[memoryview], on_wait_closed=None):
        self.name = name
        self._slots = slots
        self._on_wait_closed = on_wait_closed

    def __getitem__(self, idx: int) -> memoryview:
        return self._slots[idx]

    def close(self) -> None:
        return None

    async def wait_closed(self) -> None:
        if self._on_wait_closed is not None:
            self._on_wait_closed()


def _raw_message(msg_id: int, payload) -> memoryview:
    with MessageMarshal.serialize(msg_id, payload) as (_, header, buffers):
        return memoryview(header + b"".join(buffers)).toreadonly()


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


@pytest.mark.asyncio
async def test_channel_preserves_cached_message_during_shm_reattach(monkeypatch):
    old_slots = [_raw_message(0, {"value": 0}), _raw_message(1, {"value": 1})]
    new_slots = [_raw_message(4, {"value": 4}), _raw_message(1, {"value": 1}), _raw_message(2, {"value": 2})]

    channel = Channel(uuid4(), uuid4(), 3, None, None, Channel._SENTINEL)
    channel._pub_writer = DummyWriter()
    channel._pub_task = _resolved_task()
    channel._graph_task = _resolved_task()
    preserved_during_reattach = False

    def assert_cached_before_close():
        nonlocal preserved_during_reattach
        assert channel.cache[1] == {"value": 1}
        preserved_during_reattach = True

    channel.shm = FakeSHM("old", old_slots, on_wait_closed=assert_cached_before_close)
    channel.cache.put_from_mem(old_slots[1])

    async def fake_attach_shm(self, shm_name):
        assert shm_name == "new"
        return FakeSHM("new", new_slots)

    monkeypatch.setattr("ezmsg.core.messagechannel.GraphService.attach_shm", fake_attach_shm)

    reader = asyncio.StreamReader()
    reader.feed_data(Command.TX_SHM.value + uint64_to_bytes(2) + uint64_to_bytes(3) + b"new")
    reader.feed_eof()

    await channel._publisher_connection(reader)

    assert preserved_during_reattach
