import asyncio

from uuid import UUID

from typing import Set, Literal, List, Optional


class BufferLease:
    leases: Set[UUID]
    empty: asyncio.Event

    def __init__(self) -> None:
        self.leases = set()
        self.empty = asyncio.Event()
        self.empty.set()

    def add(self, uuid: UUID) -> None:
        self.empty.clear()
        self.leases.add(uuid)

    def remove(self, uuid: UUID) -> None:
        self.leases.remove(uuid)
        if not self.leases:
            self.empty.set()

    async def wait(self) -> Literal[True]:
        return await self.empty.wait()

    @property
    def is_empty(self) -> bool:
        return self.empty.is_set()


class Backpressure:
    buffers: List[BufferLease]
    empty: asyncio.Event
    pressure: int

    def __init__(self, num_buffers: int) -> None:
        self.buffers = [BufferLease() for _ in range(num_buffers)]
        self.empty = asyncio.Event()
        self.empty.set()
        self.pressure = 0

    @property
    def is_empty(self) -> bool:
        return self.pressure == 0

    def available(self, buf_idx: int) -> bool:
        return self.buffers[buf_idx].is_empty

    async def wait(self, buf_idx: int) -> None:
        await self.buffers[buf_idx].wait()

    def lease(self, uuid: UUID, buf_idx: int) -> None:
        if self.buffers[buf_idx].is_empty:
            self.pressure += 1
        self.buffers[buf_idx].add(uuid)
        self.empty.clear()

    def _free(self, uuid: UUID, buf_idx: int) -> None:
        try:
            self.buffers[buf_idx].remove(uuid)
            if self.buffers[buf_idx].is_empty:
                self.pressure -= 1
        except KeyError:
            pass

    def free(self, uuid: UUID, buf_idx: Optional[int] = None) -> None:
        if buf_idx is None:
            for idx in range(len(self.buffers)):
                self._free(uuid, idx)
        else:
            self._free(uuid, buf_idx)

        if self.is_empty:
            self.empty.set()

    async def sync(self) -> Literal[True]:
        return await self.empty.wait()
