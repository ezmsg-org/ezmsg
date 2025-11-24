
import asyncio
from contextlib import suppress
from uuid import uuid4

import pytest

from ezmsg.core.messagechannel import Channel
from ezmsg.core.messagecache import CacheMiss
from ezmsg.core.netprotocol import Command, uint64_to_bytes
from ezmsg.core.backpressure import Backpressure
from ezmsg.core.publisherprotocol import PublisherMessage
from ezmsg.core.messagemarshal import MessageMarshal


class DummyTransport:
    def __init__(self):
        self.buffer: list[bytes] = []
        self.closed = False

    def write(self, data: bytes) -> None:
        self.buffer.append(data)

    def close(self) -> None:
        self.closed = True


def _serialize(msg_id: int, payload: object) -> bytes:
    with MessageMarshal.serialize(msg_id, payload) as (_, header, buffers):
        return header + b"".join(bytes(buf) for buf in buffers)


@pytest.mark.asyncio
async def test_channel_acknowledges_remote_messages():
    channel = Channel(uuid4(), uuid4(), 2, None, None)
    channel._pub_transport = DummyTransport()
    channel._pub_message_queue = asyncio.Queue()

    client_id = uuid4()
    queue: asyncio.Queue = asyncio.Queue()
    channel.register_client(client_id, queue)

    process_task = asyncio.create_task(channel._process_publisher_messages())

    msg_id = 5
    payload = {"value": 42}
    tcp_data = _serialize(msg_id, payload)
    await channel._pub_message_queue.put(
        PublisherMessage(msg_id=msg_id, command=Command.TX_TCP.value, tcp_data=tcp_data)
    )

    queued_pub, queued_msg = await asyncio.wait_for(queue.get(), timeout=1)
    assert queued_pub == channel.pub_id
    assert queued_msg == msg_id

    with channel.get(msg_id, client_id) as obj:
        assert obj == payload

    with pytest.raises(CacheMiss):
        _ = channel.cache[msg_id]

    buf_idx = msg_id % channel.num_buffers
    assert channel.backpressure.buffers[buf_idx].is_empty

    expected_ack = Command.RX_ACK.value + uint64_to_bytes(msg_id)
    assert channel._pub_transport.buffer[-1] == expected_ack

    process_task.cancel()
    with suppress(asyncio.CancelledError):
        await process_task


@pytest.mark.asyncio
async def test_channel_releases_local_backpressure():
    channel = Channel(uuid4(), uuid4(), 2, None, None)
    channel._pub_transport = DummyTransport()

    local_bp = Backpressure(channel.num_buffers)
    channel.register_client(channel.pub_id, None, local_bp)

    client_id = uuid4()
    queue: asyncio.Queue = asyncio.Queue()
    channel.register_client(client_id, queue)

    msg_id = 3
    payload = "local"
    channel.put_local(msg_id, payload)

    queued_pub, queued_msg = await asyncio.wait_for(queue.get(), timeout=1)
    assert queued_pub == channel.pub_id
    assert queued_msg == msg_id

    with channel.get(msg_id, client_id) as obj:
        assert obj == payload

    buf_idx = msg_id % channel.num_buffers
    assert local_bp.buffers[buf_idx].is_empty
    assert channel._pub_transport.buffer == []


def test_channel_put_local_requires_local_backpressure():
    channel = Channel(uuid4(), uuid4(), 1, None, None)
    with pytest.raises(ValueError):
        channel.put_local(1, "no pub")
