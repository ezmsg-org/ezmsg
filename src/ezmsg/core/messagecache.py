import logging

from uuid import UUID
from contextlib import contextmanager

from .shm import SHMContext
from .messagemarshal import MessageMarshal, UninitializedMemory

from collections.abc import Generator
from typing import Any

logger = logging.getLogger("ezmsg")


class CacheMiss(Exception): 
    """
    Exception raised when a requested message is not found in cache.
    
    This occurs when trying to retrieve a message that has been evicted
    from the cache or was never stored in the first place.
    """
    ...


class Cache:
    """
    Shared-memory backed cache for objects.
    
    Provides a buffer cache that can store objects both in memory
    and in shared memory buffers, enabling efficient message passing between
    processes with automatic eviction based on buffer age.
    """

    num_buffers: int
    cache: list[Any]
    cache_id: list[int | None]

    def __init__(self, num_buffers: int) -> None:
        """
        Initialize the cache with specified number of buffers.
        
        :param num_buffers: Number of cache buffers to maintain.
        :type num_buffers: int
        """
        self.num_buffers = num_buffers
        self.cache_id = [None] * self.num_buffers
        self.cache = [None] * self.num_buffers

    def put(self, msg_id: int, msg: Any) -> None:
        """
        Put an object into cache at the position determined by message ID.
        
        :param msg_id: Unique message identifier.
        :type msg_id: int
        :param msg: The message object to cache.
        :type msg: Any
        """
        buf_idx = msg_id % self.num_buffers
        self.cache_id[buf_idx] = msg_id
        self.cache[buf_idx] = msg

    def push(self, msg_id: int, shm: SHMContext) -> None:
        """
        Push an object from cache into shared memory.
        
        If the message is not already in shared memory with the correct ID,
        retrieves it from cache and serializes it to the shared memory buffer.
        
        :param msg_id: Message identifier to push.
        :type msg_id: int
        :param shm: Shared memory context to write to.
        :type shm: SHMContext
        :raises ValueError: If shared memory has wrong number of buffers.
        """
        if self.num_buffers != shm.num_buffers:
            raise ValueError("shm has incorrect number of buffers")

        buf_idx = msg_id % self.num_buffers
        with shm.buffer(buf_idx) as mem:
            shm_msg_id = MessageMarshal.msg_id(mem)
            if shm_msg_id != msg_id:
                with self.get(msg_id) as obj:
                    MessageMarshal.to_mem(msg_id, obj, mem)

    @contextmanager
    def get(
        self, msg_id: int, shm: SHMContext | None = None
    ) -> Generator[Any, None, None]:
        """
        Get object from cache; if not in cache and shm provided -- get from shm.
        
        Provides a context manager for safe access to cached messages. If the
        message is not in memory cache, attempts to retrieve from shared memory.
        
        :param msg_id: Message identifier to retrieve.
        :type msg_id: int
        :param shm: Optional shared memory context as fallback.
        :type shm: SHMContext | None
        :return: Context manager yielding the requested message.
        :rtype: Generator[Any, None, None]
        :raises CacheMiss: If message not found in cache or shared memory.
        """

        buf_idx = msg_id % self.num_buffers
        if self.cache_id[buf_idx] == msg_id:
            yield self.cache[buf_idx]

        else:
            if shm is None:
                raise CacheMiss

            with shm.buffer(buf_idx, readonly=True) as mem:
                try:
                    if MessageMarshal.msg_id(mem) != msg_id:
                        raise CacheMiss
                except UninitializedMemory:
                    raise CacheMiss

                with MessageMarshal.obj_from_mem(mem) as obj:
                    yield obj

    def clear(self):
        """
        Clear all cached messages and identifiers.
        
        Resets all cache slots to None, effectively clearing the entire cache.
        """
        self.cache_id = [None] * self.num_buffers
        self.cache = [None] * self.num_buffers


# FIXME: This should be made thread-safe in the future
MessageCache: dict[UUID, Cache] = dict()
