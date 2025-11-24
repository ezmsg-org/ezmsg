import asyncio

from uuid import UUID

from typing import Literal


class BufferLease:
    """
    Manages leases for a single buffer in the backpressure system.

    A BufferLease tracks which clients have active leases on a buffer and
    provides synchronization when the buffer becomes empty.
    """

    leases: set[UUID]
    empty: asyncio.Event

    def __init__(self) -> None:
        """
        Initialize a new BufferLease.

        The buffer starts empty with no active leases.
        """
        self.leases = set()
        self.empty = asyncio.Event()
        self.empty.set()

    def add(self, uuid: UUID) -> None:
        """
        Add a lease for the specified client.

        :param uuid: Unique identifier for the client
        :type uuid: UUID
        """
        self.empty.clear()
        self.leases.add(uuid)

    def remove(self, uuid: UUID) -> None:
        """
        Remove a lease for the specified client.

        :param uuid: Unique identifier for the client
        :type uuid: UUID
        """
        self.leases.remove(uuid)
        if not self.leases:
            self.empty.set()

    async def wait(self) -> Literal[True]:
        """
        Wait until the buffer becomes empty (no active leases).

        :return: Always returns True when the buffer is empty
        :rtype: Literal[True]
        """
        return await self.empty.wait()

    @property
    def is_empty(self) -> bool:
        """
        Check if the buffer has no active leases.

        :return: True if no leases are active, False otherwise
        :rtype: bool
        """
        return self.empty.is_set()


class Backpressure:
    """
    Manages backpressure for multiple buffers in a message passing system.

    The Backpressure class coordinates access to multiple buffers, tracking
    which buffers are in use and providing synchronization mechanisms to
    manage flow control between publishers and subscribers.

    :param num_buffers: Number of buffers to manage
    :type num_buffers: int
    """

    buffers: list[BufferLease]
    empty: asyncio.Event
    pressure: int

    def __init__(self, num_buffers: int) -> None:
        """
        Initialize backpressure management for the specified number of buffers.

        :param num_buffers: Number of buffers to create and manage
        :type num_buffers: int
        """
        self.buffers = [BufferLease() for _ in range(num_buffers)]
        self.empty = asyncio.Event()
        self.empty.set()
        self.pressure = 0

    @property
    def is_empty(self) -> bool:
        """
        Check if all buffers are empty (no active pressure).

        :return: True if no buffers have active leases, False otherwise
        :rtype: bool
        """
        return self.pressure == 0

    def available(self, buf_idx: int) -> bool:
        """
        Check if a specific buffer is available (has no active leases).

        :param buf_idx: Index of the buffer to check
        :type buf_idx: int
        :return: True if the buffer is available, False otherwise
        :rtype: bool
        """
        return self.buffers[buf_idx].is_empty

    async def wait(self, buf_idx: int) -> None:
        """
        Wait until a specific buffer becomes available.

        :param buf_idx: Index of the buffer to wait for
        :type buf_idx: int
        """
        await self.buffers[buf_idx].wait()

    def lease(self, uuid: UUID, buf_idx: int) -> None:
        """
        Create a lease on a specific buffer for the given client.

        :param uuid: Unique identifier for the client
        :type uuid: UUID
        :param buf_idx: Index of the buffer to lease
        :type buf_idx: int
        """
        if self.buffers[buf_idx].is_empty:
            self.pressure += 1
        self.buffers[buf_idx].add(uuid)
        self.empty.clear()

    def _free(self, uuid: UUID, buf_idx: int) -> None:
        """
        Internal method to free a lease on a specific buffer.

        :param uuid: Unique identifier for the client
        :type uuid: UUID
        :param buf_idx: Index of the buffer to free
        :type buf_idx: int
        """
        try:
            self.buffers[buf_idx].remove(uuid)
            if self.buffers[buf_idx].is_empty:
                self.pressure -= 1
        except KeyError:
            pass

    def free(self, uuid: UUID, buf_idx: int | None = None) -> None:
        """
        Free leases for the given client.

        If buf_idx is specified, only frees the lease on that buffer.
        If buf_idx is None, frees leases on all buffers for this client.

        :param uuid: Unique identifier for the client
        :type uuid: UUID
        :param buf_idx: Optional buffer index to free, or None to free all
        :type buf_idx: int | None
        """
        if buf_idx is None:
            for idx in range(len(self.buffers)):
                self._free(uuid, idx)
        else:
            self._free(uuid, buf_idx)

        if self.is_empty:
            self.empty.set()

    async def sync(self) -> Literal[True]:
        """
        Wait until all buffers are empty (no backpressure).

        :return: Always returns True when all buffers are empty
        :rtype: Literal[True]
        """
        return await self.empty.wait()
