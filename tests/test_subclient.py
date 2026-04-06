import asyncio
from contextlib import contextmanager
from uuid import uuid4

import pytest

from ezmsg.core.subclient import Subscriber
from ezmsg.core.graphmeta import ProfileChannelType
from ezmsg.core.netprotocol import Command, encode_str
from ezmsg.core import channelmanager as channelmanager_module
from ezmsg.core import subclient as subclient_module


class DummyChannel:
    """Minimal Channel stand-in for Subscriber tests."""

    def __init__(self):
        self.clients = {}
        self.closed = False
        self.waited = False
        self.topic = "test"
        self.num_buffers = 8
        self.channel_kind = ProfileChannelType.LOCAL

    def register_client(self, client_id, queue, local_backpressure=None):
        self.clients[client_id] = queue

    def unregister_client(self, client_id):
        del self.clients[client_id]

    def close(self):
        self.closed = True

    async def wait_closed(self):
        self.waited = True

    @contextmanager
    def get(self, msg_id, client_id):
        yield f"msg-{msg_id}"


class DummyWriter:
    """Minimal asyncio.StreamWriter stand-in."""

    def __init__(self):
        self.buffer = []
        self._closed = False

    def write(self, data):
        self.buffer.append(data)

    async def drain(self):
        pass

    def close(self):
        self._closed = True

    async def wait_closed(self):
        pass


@pytest.mark.asyncio
async def test_subscriber_unregisters_removed_publisher(monkeypatch):
    """A graph UPDATE that drops a publisher must remove it from _channels.

    Before the fix (PR #218), Subscriber._cur_pubs was initialised to an
    empty set and never updated, so ``cur_pubs - pub_ids`` was always
    empty and stale publishers were never unregistered.
    """
    async def fake_create(pub_id, address):
        return DummyChannel()

    monkeypatch.setattr(channelmanager_module.Channel, "create", fake_create)
    monkeypatch.setattr(
        subclient_module, "CHANNELS", channelmanager_module.ChannelManager()
    )

    pub_a = uuid4()

    reader = asyncio.StreamReader()
    writer = DummyWriter()

    sub = Subscriber(
        id=uuid4(),
        topic="test/topic",
        graph_address=None,
        _guard=Subscriber._SENTINEL,
    )

    # Protocol sequence:
    #   UPDATE [pub_a]  ->  subscriber registers pub_a
    #   COMPLETE        ->  _initialized is set
    #   UPDATE []       ->  subscriber should unregister pub_a
    reader.feed_data(
        Command.UPDATE.value
        + encode_str(str(pub_a))
        + Command.COMPLETE.value
        + Command.UPDATE.value
        + encode_str("")
    )
    # No EOF yet — connection stays open after the second UPDATE.

    task = asyncio.create_task(sub._graph_connection(reader, writer))

    # Wait until the subscriber has written two COMPLETE responses
    # (one per UPDATE), meaning both UPDATEs have been processed.
    for _ in range(200):
        if len(writer.buffer) >= 2:
            break
        await asyncio.sleep(0)
    else:
        pytest.fail("Subscriber did not process both UPDATEs")

    # Connection is still open — this is mid-session state, not post-cleanup.
    # Before the fix, pub_a would still be in _channels here.
    assert pub_a not in sub._channels, (
        "Publisher should be removed from _channels "
        "when a graph UPDATE no longer includes it"
    )

    reader.feed_eof()
    await task


@pytest.mark.asyncio
async def test_recv_zero_copy_skips_stale_notification():
    """recv_zero_copy must skip notifications from unregistered publishers.

    Before PR #218 there was no guard; a stale notification would cause
    a KeyError on ``self._channels[pub_id]``.
    """
    sub = Subscriber(
        id=uuid4(),
        topic="test/topic",
        graph_address=None,
        _guard=Subscriber._SENTINEL,
    )

    stale_pub = uuid4()
    valid_pub = uuid4()

    sub._channels[valid_pub] = DummyChannel()

    # Stale notification first, then a valid one.
    await sub._incoming.put((stale_pub, 0))
    await sub._incoming.put((valid_pub, 1))

    # Before the fix this would raise KeyError for stale_pub.
    async with sub.recv_zero_copy() as msg:
        assert msg == "msg-1"
