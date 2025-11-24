import asyncio
from dataclasses import dataclass
from uuid import uuid4

import pytest

from ezmsg.core.channelmanager import ChannelManager
from ezmsg.core.backpressure import Backpressure
from ezmsg.core.netprotocol import Address
from ezmsg.core import channelmanager as channelmanager_module


@dataclass
class DummyChannel:
    clients: dict

    def __init__(self):
        self.clients = {}
        self.closed = False
        self.waited = False
        self.local_bp: dict = {}

    def register_client(self, client_id, queue, local_backpressure):
        self.clients[client_id] = queue
        if local_backpressure is not None:
            self.local_bp[client_id] = local_backpressure

    def unregister_client(self, client_id):
        del self.clients[client_id]

    def close(self):
        self.closed = True

    async def wait_closed(self):
        self.waited = True


@pytest.mark.asyncio
async def test_channel_manager_reuses_existing_channel(monkeypatch):
    dummy_channel = DummyChannel()

    async def fake_create(pub_id, address):
        return dummy_channel

    monkeypatch.setattr(channelmanager_module.Channel, "create", fake_create)

    manager = ChannelManager()
    pub_id = uuid4()
    client_one = uuid4()
    client_two = uuid4()

    queue_one: asyncio.Queue = asyncio.Queue()
    queue_two: asyncio.Queue = asyncio.Queue()

    channel_a = await manager.register(pub_id, client_one, queue_one)
    channel_b = await manager.register(pub_id, client_two, queue_two)

    assert channel_a is dummy_channel
    assert channel_b is dummy_channel
    assert dummy_channel.clients[client_one] is queue_one
    assert dummy_channel.clients[client_two] is queue_two


@pytest.mark.asyncio
async def test_channel_manager_unregister_closes_channel(monkeypatch):
    dummy_channel = DummyChannel()

    async def fake_create(pub_id, address):
        dummy_channel.address = address
        return dummy_channel

    monkeypatch.setattr(channelmanager_module.Channel, "create", fake_create)

    manager = ChannelManager()
    pub_id = uuid4()
    client_id = uuid4()

    queue: asyncio.Queue = asyncio.Queue()
    await manager.register(pub_id, client_id, queue)
    await manager.unregister(pub_id, client_id)

    default_address = Address.from_string(channelmanager_module.GRAPHSERVER_ADDR)
    assert pub_id not in manager._registry[default_address]
    assert dummy_channel.closed
    assert dummy_channel.waited


@pytest.mark.asyncio
async def test_channel_manager_registers_local_publisher(monkeypatch):
    dummy_channel = DummyChannel()

    async def fake_create(pub_id, address):
        return dummy_channel

    monkeypatch.setattr(channelmanager_module.Channel, "create", fake_create)

    manager = ChannelManager()
    pub_id = uuid4()
    local_bp = Backpressure(1)

    channel = await manager.register_local_pub(pub_id, local_bp)

    assert channel is dummy_channel
    assert dummy_channel.clients[pub_id] is None
    assert dummy_channel.local_bp[pub_id] is local_bp
