import asyncio
import logging
import signal

from dataclasses import dataclass, field
from contextlib import contextmanager, suppress
from multiprocessing import Process, Event, resource_tracker
from multiprocessing.synchronize import Event as EventType
from multiprocessing.shared_memory import SharedMemory
from uuid import getnode

from .netprotocol import (
    DEFAULT_SHM_SIZE,
    SHMSERVER_ADDR,
    Command,
    Address,
    encode_str,
    uint64_to_bytes,
    bytes_to_uint,
    read_str,
    read_int
)

from typing import Generator, List, Dict, Set, Optional

logger = logging.getLogger('ezmsg')

_std_register = resource_tracker.register


def _ignore_shm(name, rtype):
    if rtype == "shared_memory":
        return
    return resource_tracker._resource_tracker.register(self, name, rtype) # noqa: F821


@contextmanager
def _untracked_shm() -> Generator[None, None, None]:
    """ Disable SHM tracking within context - https://bugs.python.org/issue38119 """
    resource_tracker.register = _ignore_shm
    yield
    resource_tracker.register = _std_register


class SHMContext():
    """
    SHMContext manages the memory map of a block of shared memory, and
    exposes memoryview objects for reading and writing

    ezmsg shared memory format:
    [ UINT64 -- n_buffers ]
    [ UINT64 -- buf_size ]
    [ buf_size - 16 -- buf0 data_block ]
    ...
    [ 0x00 * 16 bytes -- reserved (header) ]
    [ buf_size - 16 -- buf1 data_block ]
    ...

    * n_buffers defines the number of shared memory buffers
    * buf_size defines the size of each shared memory buffer (including 16 bytes for header)
    * data_block is the remaining memory in this buffer which contains message information

    This format repeats itself for every buffer in the SharedMemory block.
    """

    _shm: SharedMemory
    _data_block_segs: List[slice]

    num_buffers: int
    buf_size: int

    monitor: asyncio.Future

    def __init__(self, name: str) -> None:

        with _untracked_shm():
            self._shm = SharedMemory(name=name, create=False)

        with self._shm.buf[0:8] as num_buffers_mem:
            self.num_buffers = bytes_to_uint(num_buffers_mem)

        with self._shm.buf[8:16] as buf_size_mem:
            self.buf_size = bytes_to_uint(buf_size_mem)

        buf_starts = [buf_idx * self.buf_size for buf_idx in range(self.num_buffers)]
        buf_stops = [buf_start + self.buf_size for buf_start in buf_starts]
        buf_data_block_starts = [buf_start + 16 for buf_start in buf_starts]

        self._data_block_segs = [slice(*seg) for seg in
                                 zip(buf_data_block_starts, buf_stops)
                                 ]

    @classmethod
    async def create(cls, num_buffers: int, buf_size: int = DEFAULT_SHM_SIZE) -> "SHMContext":
        reader, writer = await asyncio.open_connection(*SHMSERVER_ADDR)
        writer.write(Command.SHM_CREATE.value)
        writer.write(uint64_to_bytes(num_buffers))
        writer.write(uint64_to_bytes(buf_size))
        await writer.drain()

        response = await reader.read(1)
        if response != Command.COMPLETE.value:
            raise ValueError("Error creating SHM segment")

        shm_name = await read_str(reader)
        return SHMContext._create(shm_name, reader, writer)

    @classmethod
    async def attach(cls, name: str) -> "SHMContext":
        reader, writer = await asyncio.open_connection(*SHMSERVER_ADDR)
        writer.write(Command.SHM_ATTACH.value)
        writer.write(encode_str(name))
        await writer.drain()

        response = await reader.read(1)
        if response != Command.COMPLETE.value:
            raise ValueError('Invalid SHM Name')

        shm_name = await read_str(reader)
        return SHMContext._create(shm_name, reader, writer)

    @classmethod
    def _create(cls, shm_name: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> "SHMContext":
        context = cls(shm_name)

        async def monitor() -> None:
            try:
                await reader.read()
            except asyncio.CancelledError:
                pass
            finally:
                writer.close()
                await writer.wait_closed()

        def close(_: asyncio.Future) -> None:
            context.close()

        context.monitor = asyncio.create_task(monitor(), name=f'{shm_name}_monitor')
        context.monitor.add_done_callback(close)
        return context

    @contextmanager
    def buffer(self, idx: int, readonly: bool = False) -> Generator[memoryview, None, None]:
        if self._shm.buf is None:
            raise BufferError(f"cannot access {self._shm.name}: SHMServer disconnected")

        with self._shm.buf[self._data_block_segs[idx]] as mem:
            if readonly:
                ro_mem = mem.toreadonly()
                yield ro_mem
                ro_mem.release()
            else:
                yield mem

    def close(self) -> None:
        self._shm.close()
        self.monitor.cancel()

    async def wait_closed(self) -> None:
        with suppress(asyncio.CancelledError):
            await self.monitor

    @property
    def name(self) -> str:
        return self._shm.name

    @property
    def size(self) -> int:
        return self.buf_size - 16  # 16 byte header

@dataclass
class SHMInfo:
    shm: SharedMemory
    leases: Set["asyncio.Task[None]"] = field(default_factory = set)

    def lease(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> "asyncio.Task[None]":
        async def _wait_for_eof() -> None:
            try:
                await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()
        lease = asyncio.create_task(_wait_for_eof())
        lease.add_done_callback(self._release)
        self.leases.add(lease)
        return lease

    def _release(self, task: "asyncio.Task[None]"):
        self.leases.discard(task)
        if len(self.leases) == 0:
            logger.debug(f'unlinking {self.shm.name}')
            self.shm.close()
            self.shm.unlink()


class SHMServer(Process):
    """
    Lives in a dedicated process (one per machine)
    Handles SHMContext leases for this machine
    """

    node: int
    shms: Dict[str, SHMInfo]

    _server_up: EventType

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.node = getnode()
        self.shms = dict()
        self._server_up = Event()
        self._shutdown = Event()

    def run(self) -> None:
        handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        with suppress(asyncio.CancelledError):  
            asyncio.run(self._serve())
        signal.signal(signal.SIGINT, handler)

    def stop(self) -> None:
        self._shutdown.set()
        self.join()
        
    async def _serve(self) -> None:
        loop = asyncio.get_running_loop()
        server = await asyncio.start_server(self.api, *SHMSERVER_ADDR)

        async def monitor_shutdown() -> None:
            await loop.run_in_executor( None, self._shutdown.wait )
            server.close()

        monitor_task = loop.create_task( monitor_shutdown() )

        self._server_up.set()

        try:
            await server.serve_forever()

        finally:
            for info in self.shms.values():
                for lease_task in list(info.leases):
                    lease_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await lease_task
                info.leases.clear()

            monitor_task.cancel()
            with suppress( asyncio.CancelledError ):
                await monitor_task

    def start(self) -> None:
        super().start()
        self._server_up.wait()

    @classmethod
    async def ensure_running(cls) -> Optional["SHMServer"]:
        shm_server = None
        address = Address(*SHMSERVER_ADDR)
        try:
            await asyncio.open_connection(*address)
            logger.debug( f'SHMServer Exists: {address.host}:{address.port}' )
        except ConnectionRefusedError:
            shm_server = cls()
            shm_server.start()
            logger.info(f'Started SHMServer. PID:{shm_server.pid}@{address.host}:{address.port}')
        return shm_server

    @staticmethod
    async def shutdown_server() -> None:
        address = Address(*SHMSERVER_ADDR)
        reader, writer = await asyncio.open_connection(*address)
        writer.write(Command.SHUTDOWN.value)
        await writer.drain()

    async def api(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:

        try:
            cmd = await reader.read(1)
            if len(cmd) == 0:
                return

            if cmd == Command.SHUTDOWN.value:
                self._shutdown.set()
                return

            info: Optional[SHMInfo] = None

            if cmd == Command.SHM_CREATE.value:
                num_buffers = await read_int(reader)
                buf_size = await read_int(reader)

                # Create segment
                shm = SharedMemory(size=num_buffers * buf_size, create=True)
                shm.buf[:] = b'0' * len(shm.buf) # Guarantee zeros
                shm.buf[0:8] = uint64_to_bytes(num_buffers)
                shm.buf[8:16] = uint64_to_bytes(buf_size)
                info = SHMInfo(shm)
                self.shms[shm.name] = info
                logger.debug(f'created {shm.name}')

            elif cmd == Command.SHM_ATTACH.value:
                shm_name = await read_str(reader)
                info = self.shms.get(shm_name, None)

            if info is None:
                return
            
            writer.write(Command.COMPLETE.value)
            writer.write(encode_str(info.shm.name))

            with suppress(asyncio.CancelledError):
                await info.lease(reader, writer)

        finally:
            writer.close()
            await writer.wait_closed()
        



