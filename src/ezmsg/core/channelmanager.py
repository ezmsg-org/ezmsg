import asyncio
import logging

from uuid import UUID

from .messagechannel import Channel
from .backpressure import Backpressure
from .netprotocol import Address, AddressType, GRAPHSERVER_ADDR

logger = logging.getLogger("ezmsg")


def _ensure_address(address: AddressType | None) -> Address:
    if address is None:
        return Address.from_string(GRAPHSERVER_ADDR)

    elif not isinstance(address, Address):
        return Address(*address)

    return address


class ChannelManager:
    """
    ChannelManager maintains a process-specific registry of Channels, tracks what clients
    need access to what channels, and creates/deallocates Channels accordingly
    """

    _registry: dict[Address, dict[UUID, Channel]]

    def __init__(self):
        default_address = Address.from_string(GRAPHSERVER_ADDR)
        self._registry = {default_address: dict()}

    async def register(
        self,
        pub_id: UUID,
        client_id: UUID | None = None,
        queue: asyncio.Queue[tuple[UUID, int]] | None = None,
        graph_address: AddressType | None = None,
    ) -> Channel:
        """
        Acquire the channel associated with a particular publisher, creating it if necessary

        :param pub_id: The UUID associated with the publisher to acquire a channel for
        :type pub_id: UUID
        :param client_id: The UUID associated with the client interested in this channel; if None; client_id = pub_id
        :type client_id: UUID | None
        :param queue: An optional asyncio Queue for message notifications
        :type queue: asyncio.Queue[tuple[UUID, int]] | None
        :param graph_address: The address to the GraphServer that the requested publisher is managed by
        :type graph_address: AddressType | None
        :return: A Channel for retreiving messages from the requested Publisher
        :rtype: Channel
        """
        graph_address = _ensure_address(graph_address)

        if client_id is None:
            client_id = pub_id
            
        try:
            channel = self._registry.get(graph_address, dict())[pub_id]
        except KeyError:
            channel = await Channel.create(pub_id, graph_address)
            channels = self._registry.get(graph_address, dict())
            channels[pub_id] = channel
            self._registry[graph_address] = channels
        channel.register(client_id, queue)
        return channel

    async def unregister(
        self, 
        pub_id: UUID, 
        client_id: UUID | None = None, 
        graph_address: AddressType | None = None
    ) -> None:
        """
        Indicate to the ChannelManager that the client referred to by client_id 
        no longer needs access to the Channel associated with the publisher 
        referred to by pub_id. If no clients need access to this channel, the 
        channel will be closed and removed from the ChannelManager.

        :param pub_id: The UUID associated with the publisher
        :type pub_id: UUID
        :param client_id: The UUID associated with the client to unregister. if None; client_id = pub_id
        :type client_id: UUID | None
        :param graph_address: The address to the GraphServer that the requested publisher is managed by
        :type graph_address: AddressType | None
        """
        graph_address = _ensure_address(graph_address)

        if client_id is None:
            client_id = pub_id
        
        channel = self._registry.get(graph_address, dict())[pub_id]
        channel.unregister(client_id)

        logger.debug(
            f"unregistered {client_id} from {pub_id}; {len(channel.clients)} left"
        )

        if len(channel.clients) == 0:
            registry = self._registry[graph_address]
            del registry[pub_id]

            channel.close()
            await channel.wait_closed()

            logger.debug(f"closed channel {pub_id}: no clients")


CHANNELS = ChannelManager()
