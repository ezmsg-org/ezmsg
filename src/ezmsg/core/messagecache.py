import typing

from .messagemarshal import MessageMarshal


class CacheMiss(Exception): 
    """
    Exception raised when a requested message is not found in cache.
    
    This occurs when trying to retrieve a message that has been evicted
    from the cache or was never stored in the first place.
    """


class CacheEntry(typing.NamedTuple):
    object: typing.Any
    msg_id: int
    context: typing.ContextManager | None
    memory: memoryview | None


class MessageCache:
    """
    Cache for memoryview-backed objects.
    
    Provides a buffer cache that can store objects in memory 
    enabling efficient message passing between
    processes with automatic eviction based on buffer age.
    """
    _cache: list[CacheEntry | None]

    def __init__(self, num_buffers: int) -> None:
        """
        Initialize the cache with specified number of buffers.
        
        :param num_buffers: Number of cache buffers to maintain.
        :type num_buffers: int
        """
        self._cache = [None] * num_buffers

    def _buf_idx(self, msg_id: int) -> int:
        return msg_id % len(self._cache)

    def __getitem__(self, msg_id: int) -> typing.Any:
        """
        Get a cached object by msg_id

        :param msg_id: Message ID to retreive from cache
        :type msg_id: int
        :raises CacheMiss: If this msg_id does not exist in the cache.
        """
        entry = self._cache[self._buf_idx(msg_id)]
        if entry is None or entry.msg_id != msg_id:
            raise CacheMiss
        return entry.object

    def keys(self) -> list[int]:
        """
        Get a list of current cached msg_ids
        """
        return [entry.msg_id for entry in self._cache if entry is not None]

    def put_local(self, obj: typing.Any, msg_id: int) -> None:
        """
        Put an object with associated msg_id directly into cache

        :param obj: Object to put in cache.
        :type obj: typing.Any
        :param msg_id: ID associated with this message/object.
        :type msg_id: int
        """
        self._put(
            CacheEntry(
                object=obj,
                msg_id=msg_id,
                context=None,
                memory=None,
            )
        )

    def put_from_mem(self, mem: memoryview) -> None:
        """
        Reconstitute a message in mem and keep it in cache, releasing and
        overwriting the existing slot in cache.
        This method passes the lifecycle of the memoryview to the MessageCache
        and the memoryview will be properly released by the cache with `free`

        :param mem: Source memoryview containing serialized object.
        :type from_mem: memoryview
        :raises UninitializedMemory: If mem buffer is not properly initialized.
        """
        ctx = MessageMarshal.obj_from_mem(mem)
        self._put(
            CacheEntry(
                object=ctx.__enter__(),
                msg_id=MessageMarshal.msg_id(mem),
                context=ctx,
                memory=mem,
            )
        )

    def _put(self, entry: CacheEntry) -> None:
        buf_idx = self._buf_idx(entry.msg_id)
        self._release(buf_idx)
        self._cache[buf_idx] = entry

    def _release(self, buf_idx: int) -> None:
        entry = self._cache[buf_idx]
        if entry is not None:
            mem = entry.memory
            ctx = entry.context
            if ctx is not None:
                ctx.__exit__(None, None, None)
            del entry
            self._cache[buf_idx] = None
            if mem is not None:
                mem.release()

    def release(self, msg_id: int) -> None:
        """
        Release memory for the entry associated with msg_id

        :param msg_id: ID for the message to release.
        :type msg_id: int
        :raises CacheMiss: If requested msg_id is not in cache.
        """
        buf_idx = self._buf_idx(msg_id)
        entry = self._cache[buf_idx]
        if entry is None or entry.msg_id != msg_id:
            raise CacheMiss
        self._release(buf_idx)

    def clear(self) -> None:
        """
        Release all cached objects
        """
        for i in range(len(self._cache)):
            self._release(i)
