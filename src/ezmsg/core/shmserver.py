import asyncio
from collections.abc import Generator
import logging
import typing

from dataclasses import dataclass, field
from contextlib import contextmanager, suppress
from multiprocessing import resource_tracker
from multiprocessing.shared_memory import SharedMemory
from uuid import getnode

from .server import ThreadedAsyncServer, ServiceManager
from .netprotocol import (
    DEFAULT_SHM_SIZE,
    close_stream_writer,
    Command,
    encode_str,
    uint64_to_bytes,
    bytes_to_uint,
    read_str,
    read_int,
    SHMSERVER_PORT_DEFAULT,
    SHMSERVER_ADDR_ENV,
    AddressType,
)

logger = logging.getLogger("ezmsg")

_std_register = resource_tracker.register


def _ignore_shm(name, rtype):
    if rtype == "shared_memory":
        return
    return resource_tracker._resource_tracker.register(self, name, rtype)  # noqa: F821


@contextmanager
def _untracked_shm() -> Generator[None, None, None]:
    """
    Disable SHM tracking within context - https://bugs.python.org/issue38119.
    
    This context manager temporarily disables shared memory tracking to work
    around a Python bug where shared memory segments are not properly cleaned up.
    
    :return: Context manager generator.
    :rtype: collections.abc.Generator[None, None, None]
    """
    resource_tracker.register = _ignore_shm
    yield
    resource_tracker.register = _std_register


class SHMContext:
    """
    SHMContext manages the memory map of a block of shared memory, and
    exposes memoryview objects for reading and writing.

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
    _data_block_segs: list[slice]

    num_buffers: int
    buf_size: int

    monitor: asyncio.Future

    def __init__(self, name: str) -> None:
        """
        Initialize SHMContext by connecting to an existing shared memory segment.
        
        :param name: The name of the shared memory segment to connect to.
        :type name: str
        :raises BufferError: If shared memory segment cannot be accessed.
        """
        with _untracked_shm():
            self._shm = SharedMemory(name=name, create=False)

        with self._shm.buf[0:8] as num_buffers_mem:
            self.num_buffers = bytes_to_uint(num_buffers_mem)

        with self._shm.buf[8:16] as buf_size_mem:
            self.buf_size = bytes_to_uint(buf_size_mem)

        buf_starts = [buf_idx * self.buf_size for buf_idx in range(self.num_buffers)]
        buf_stops = [buf_start + self.buf_size for buf_start in buf_starts]
        buf_data_block_starts = [buf_start + 16 for buf_start in buf_starts]

        self._data_block_segs = [
            slice(*seg) for seg in zip(buf_data_block_starts, buf_stops)
        ]

    @classmethod
    def _create(
        cls, shm_name: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> "SHMContext":
        """
        Create a new SHMContext with connection monitoring.
        
        :param shm_name: Name of the shared memory segment.
        :type shm_name: str
        :param reader: Stream reader for connection monitoring.
        :type reader: asyncio.StreamReader
        :param writer: Stream writer for connection cleanup.
        :type writer: asyncio.StreamWriter
        :return: New SHMContext instance with monitoring enabled.
        :rtype: SHMContext
        """
        context = cls(shm_name)

        async def monitor() -> None:
            try:
                await reader.read()
                logger.debug("Read from SHMContext monitor reader")
            except asyncio.CancelledError:
                pass
            finally:
                await close_stream_writer(writer)

        def close(_: asyncio.Future) -> None:
            context.close()

        context.monitor = asyncio.create_task(monitor(), name=f"{shm_name}_monitor")
        context.monitor.add_done_callback(close)
        return context

    @contextmanager
    def buffer(
        self, idx: int, readonly: bool = False
    ) -> Generator[memoryview, None, None]:
        """
        Get a memory view of a specific buffer in the shared memory segment.
        
        :param idx: Index of the buffer to access.
        :type idx: int
        :param readonly: Whether to provide read-only access to the buffer.
        :type readonly: bool
        :return: Context manager yielding a memoryview of the buffer.
        :rtype: collections.abc.Generator[memoryview, None, None]
        :raises BufferError: If shared memory is no longer accessible.
        """
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
        """
        Close the shared memory context and cancel monitoring.
        
        This initiates an asynchronous close operation and cancels the
        connection monitor task.
        """
        asyncio.create_task(self.close_shm(), name=f"Close {self._shm.name}")
        self.monitor.cancel()

    async def close_shm(self) -> None:
        """
        Asynchronously close the shared memory segment.
        
        Retries closing if BufferError is encountered, as the segment
        may still be in use by other processes.
        """
        while True:
            try:
                self._shm.close()
                logger.debug("Closed SHM segment.")
                return
            except BufferError:
                logger.debug("BufferError caught... Sleeping.")
                await asyncio.sleep(1)

    async def wait_closed(self) -> None:
        """
        Wait for the shared memory context to be fully closed.
        
        This method waits for the monitoring task to complete, indicating
        that the connection has been properly terminated.
        """
        with suppress(asyncio.CancelledError):
            await self.monitor

    @property
    def name(self) -> str:
        """
        Get the name of the shared memory segment.
        
        :return: The shared memory segment name.
        :rtype: str
        """
        return self._shm.name

    @property
    def size(self) -> int:
        """
        Get the usable size of each buffer (excluding header).
        
        :return: Buffer size minus 16-byte header.
        :rtype: int
        """
        return self.buf_size - 16  # 16 byte header


@dataclass
class SHMInfo:
    """
    Information about a shared memory segment and its active leases.
    
    Tracks the SharedMemory object and manages client connection leases.
    When all leases are released, the shared memory is automatically cleaned up.
    """
    shm: SharedMemory
    leases: set["asyncio.Task[None]"] = field(default_factory=set)

    def lease(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> "asyncio.Task[None]":
        """
        Create a lease for this shared memory segment.
        
        The lease monitors the client connection and automatically releases
        the shared memory when the client disconnects.
        
        :param reader: Stream reader to monitor for client disconnection.
        :type reader: asyncio.StreamReader
        :param writer: Stream writer for connection cleanup.
        :type writer: asyncio.StreamWriter
        :return: Task representing the active lease.
        :rtype: asyncio.Task[None]
        """
        async def _wait_for_eof() -> None:
            try:
                await reader.read()
            finally:
                await close_stream_writer(writer)

        lease = asyncio.create_task(_wait_for_eof())
        lease.add_done_callback(self._release)
        self.leases.add(lease)
        return lease

    def _release(self, task: "asyncio.Task[None]"):
        self.leases.discard(task)
        if len(self.leases) == 0:
            logger.debug(f"unlinking {self.shm.name}")
            self.shm.close()
            self.shm.unlink()


class SHMServer(ThreadedAsyncServer):
    """
    Server for managing shared memory segments across processes.
    
    This server handles creation and attachment of shared memory segments,
    tracking client connections and automatically cleaning up resources
    when no longer needed.
    """
    node: int
    shms: dict[str, SHMInfo]

    def __init__(self) -> None:
        """
        Initialize the shared memory server.
        
        Sets up the server with the current node identifier and
        an empty dictionary for tracking shared memory segments.
        """
        super().__init__()
        self.node = getnode()
        self.shms = dict()

    async def shutdown(self) -> None:
        """
        Shutdown the server and clean up all shared memory segments.
        
        Cancels all active leases and waits for them to complete,
        ensuring proper cleanup of all resources.
        """
        for info in self.shms.values():
            for lease_task in list(info.leases):
                lease_task.cancel()
                with suppress(asyncio.CancelledError):
                    await lease_task
            info.leases.clear()

    async def api(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """
        Handle client API requests for shared memory operations.
        
        Supports SHM_CREATE for creating new shared memory segments and
        SHM_ATTACH for attaching to existing segments.
        
        :param reader: Stream reader for receiving client commands.
        :type reader: asyncio.StreamReader
        :param writer: Stream writer for sending responses to client.
        :type writer: asyncio.StreamWriter
        """
        try:
            cmd = await reader.read(1)
            if len(cmd) == 0:
                return

            info: SHMInfo | None = None

            if cmd == Command.SHM_CREATE.value:
                num_buffers = await read_int(reader)
                buf_size = await read_int(reader)

                # Create segment
                shm = SharedMemory(size=num_buffers * buf_size, create=True)
                shm.buf[:] = b"0" * len(shm.buf)  # Guarantee zeros
                shm.buf[0:8] = uint64_to_bytes(num_buffers)
                shm.buf[8:16] = uint64_to_bytes(buf_size)
                info = SHMInfo(shm)
                self.shms[shm.name] = info
                logger.debug(f"created {shm.name}")

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
            await close_stream_writer(writer)


class SHMService(ServiceManager[SHMServer]):
    """
    Service manager for SHMServer instances.
    
    Provides high-level interface for creating and attaching to shared
    memory segments, managing the underlying server connection automatically.
    """
    ADDR_ENV = SHMSERVER_ADDR_ENV
    PORT_DEFAULT = SHMSERVER_PORT_DEFAULT

    def __init__(self, address: AddressType | None = None) -> None:
        """
        Initialize the SHM service manager.
        
        :param address: Optional address tuple (host, port) for the server.
        :type address: AddressType | None
        """
        super().__init__(SHMServer, address)

    async def create(
        self, num_buffers: int, buf_size: int = DEFAULT_SHM_SIZE
    ) -> SHMContext:
        """
        Create a new shared memory segment.
        
        :param num_buffers: Number of buffers to create in the segment.
        :type num_buffers: int
        :param buf_size: Size of each buffer in bytes.
        :type buf_size: int
        :return: Context for accessing the created shared memory segment.
        :rtype: SHMContext
        :raises ValueError: If server returns an error during creation.
        """
        reader, writer = await self.open_connection()
        writer.write(Command.SHM_CREATE.value)
        writer.write(uint64_to_bytes(num_buffers))
        writer.write(uint64_to_bytes(buf_size))
        await writer.drain()

        response = await reader.read(1)
        if response != Command.COMPLETE.value:
            raise ValueError("Error creating SHM segment")

        shm_name = await read_str(reader)
        return SHMContext._create(shm_name, reader, writer)

    async def attach(self, name: str) -> SHMContext:
        """
        Attach to an existing shared memory segment.
        
        :param name: Name of the shared memory segment to attach to.
        :type name: str
        :return: Context for accessing the attached shared memory segment.
        :rtype: SHMContext
        :raises ValueError: If shared memory segment name is invalid.
        """
        reader, writer = await self.open_connection()
        writer.write(Command.SHM_ATTACH.value)
        writer.write(encode_str(name))
        await writer.drain()

        response = await reader.read(1)
        if response != Command.COMPLETE.value:
            raise ValueError("Invalid SHM Name")

        shm_name = await read_str(reader)
        return SHMContext._create(shm_name, reader, writer)
