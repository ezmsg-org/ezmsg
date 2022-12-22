import asyncio

from uuid import UUID

from typing import Set, Literal, List, Optional

class BufferLease:
    leases: Set[UUID] 
    empty: asyncio.Event

    def __init__(self) -> None:
        self.leases = set()
        self.empty= asyncio.Event()
        self.empty.set()

    def add(self, uuid: UUID) -> None:
        self.empty.clear()
        self.leases.add(uuid)

    def discard(self, uuid: UUID) -> None:
        self.leases.discard(uuid)
        if len(self) == 0:
            self.empty.set()

    async def wait(self) -> Literal[True]:
        return await self.empty.wait()

    def __len__(self) -> int:
        return len(self.leases)

class Backpressure:
    buffers: List[BufferLease]
    empty: asyncio.Event

    def __init__(self, num_buffers: int) -> None:
        self.buffers = [BufferLease() for _ in range(num_buffers)]
        self.empty = asyncio.Event()
        self.empty.set()

    @property
    def pressure(self) -> int:
        return sum(len(p) != 0 for p in self.buffers)

    async def wait(self, buf_idx: int) -> None:
        if self.pressure == len(self.buffers):
            await self.sync()
        else:
            await self.buffers[buf_idx].wait()

    def lease(self, uuid: UUID, buf_idx: int) -> None:
        self.buffers[buf_idx].add(uuid)
        self.empty.clear()

    def free(self, uuid: UUID, buf_idx: Optional[int] = None) -> None:
        if buf_idx is None:
            for idx in range(len(self.buffers)):
                self.buffers[idx].discard(uuid)
        else:
            self.buffers[buf_idx].discard(uuid)
        
        if self.pressure == 0:
            self.empty.set()

    async def sync(self) -> Literal[True]:
        return await self.empty.wait()