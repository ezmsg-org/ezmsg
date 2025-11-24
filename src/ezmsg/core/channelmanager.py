import logging

from uuid import UUID

from .messagechannel import Channel, NotificationQueue
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
        client_id: UUID,
        queue: NotificationQueue,
        graph_address: AddressType | None = None,
    ) -> Channel:
        """
        Acquire the channel associated with a particular publisher, creating it if necessary

        :param pub_id: The UUID associated with the publisher to acquire a channel for
        :type pub_id: UUID
        :param client_id: The UUID associated with the client interested in this channel, used for deallocation when the channel has no more registered clients
        :type client_id: UUID
        :param queue: An asyncio Queue for the interested client that will be populated with incoming message notifications
        :type queue: asyncio.Queue[tuple[UUID, int]]
        :param graph_address: The address to the GraphServer that the requested publisher is managed by
        :type graph_address: AddressType | None
        :return: A Channel for retreiving messages from the requested Publisher
        :rtype: Channel
        """
        return await self._register(pub_id, client_id, queue, graph_address, None)

    async def register_local_pub(
        self,
        pub_id: UUID,
        local_backpressure: Backpressure,
        graph_address: AddressType | None = None,
    ) -> Channel:
        """
        Register/create a channel for a publisher that is local to (in the same process as) the publisher in question.
        Because this message channel is local to the publisher, it will directly push messages into the channel and the channel will directly manage the publisher's Backpressure without any telemetry or serialization.
        .. note:: Since only a Publisher should be registering a channel this way, it will not need access to the messages it is publishing, hence it will not provide a queue.

        :param pub_id: The UUID associated with the publisher to acquire a channel for
        :type pub_id: UUID
        :param local_backpressure: The Backpressure object associated with the Publisher
        :type local_backpressure: Backpressure
        :param graph_address: The address to the GraphServer that the requested publisher is managed by
        :type graph_address: AddressType | None
        :return: A Channel that the Publisher can push messages to locally
        :rtype: Channel
        """
        return await self._register(
            pub_id, pub_id, None, graph_address, local_backpressure
        )

    async def _register(
        self,
        pub_id: UUID,
        client_id: UUID,
        queue: NotificationQueue | None = None,
        graph_address: AddressType | None = None,
        local_backpressure: Backpressure | None = None,
    ) -> Channel:
        graph_address = _ensure_address(graph_address)
        try:
            channel = self._registry.get(graph_address, dict())[pub_id]
        except KeyError:
            channel = await Channel.create(pub_id, graph_address)
            channels = self._registry.get(graph_address, dict())
            channels[pub_id] = channel
            self._registry[graph_address] = channels
        channel.register_client(client_id, queue, local_backpressure)
        return channel

    async def unregister(
        self, pub_id: UUID, client_id: UUID, graph_address: AddressType | None = None
    ) -> None:
        """
        Indicate to the ChannelManager that the client referred to by client_id no longer needs access to the Channel associated with the publisher referred to by pub_id.
        If no clients need access to this channel, the channel will be closed and removed from the ChannelManager.

        :param pub_id: The UUID associated with the publisher to acquire a channel for
        :type pub_id: UUID
        :param client_id: The UUID associated with the client interested in this channel, used for deallocation when the channel has no more registered clients
        :type client_id: UUID
        :param graph_address: The address to the GraphServer that the requested publisher is managed by
        :type graph_address: AddressType | None
        """
        graph_address = _ensure_address(graph_address)
        channel = self._registry.get(graph_address, dict())[pub_id]
        channel.unregister_client(client_id)

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
