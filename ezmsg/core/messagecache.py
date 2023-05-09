import logging

from uuid import UUID
from contextlib import contextmanager

from .shmserver import SHMContext
from .messagemarshal import MessageMarshal, UninitializedMemory

from typing import Dict, Any, Optional, List, Generator

logger = logging.getLogger("ezmsg")


class CacheMiss(Exception):
    ...


class Cache:
    """shared-memory backed cache for objects"""

    num_buffers: int
    cache: List[Any]
    cache_id: List[Optional[int]]

    def __init__(self, num_buffers: int) -> None:
        self.num_buffers = num_buffers
        self.cache_id = [None] * self.num_buffers
        self.cache = [None] * self.num_buffers

    def put(self, msg_id: int, msg: Any) -> None:
        """put an object into cache"""
        buf_idx = msg_id % self.num_buffers
        self.cache_id[buf_idx] = msg_id
        self.cache[buf_idx] = msg

    def push(self, msg_id: int, shm: SHMContext) -> None:
        """push an object from cache into shm"""
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
        self, msg_id: int, shm: Optional[SHMContext] = None
    ) -> Generator[Any, None, None]:
        """get object from cache; if not in cache and shm provided -- get from shm"""

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
        self.cache_id = [None] * self.num_buffers
        self.cache = [None] * self.num_buffers


# NOTE: This should be made thread-safe in the future
MessageCache: Dict[UUID, Cache] = dict()
